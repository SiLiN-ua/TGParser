"""Admin CLI tests: safety model (atomic mutation+audit, service-stopped
preflight, stale-version refusal, disable confirmations), secret hygiene,
no systemd side effects."""
import asyncio
import io
import json
import os
import re
import sys
import pytest
import pytest_asyncio
import asyncpg

from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser import admin as cli


from tgparser.config import load as _load_cfg
DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]

# Patterns that must never appear in stdout/stderr of any CLI invocation.
SECRET_PATTERNS = re.compile(
    r"(postgres(ql)?://|api_hash|access_hash|session|password|token|\+?\d{10,15})",
    re.I)


# --------------------------- fixtures --------------------------------------
@pytest_asyncio.fixture
async def temp_storage():
    tmp_db = f"tgparser_test_admin_{os.getpid()}"
    sys_c = await asyncpg.connect(DSN)
    try:
        await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        await sys_c.execute(f"CREATE DATABASE {tmp_db}")
    finally:
        await sys_c.close()
    s = Storage(DSN.rsplit("/", 1)[0] + "/" + tmp_db)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        await s.close()
        sys_c = await asyncpg.connect(DSN)
        try:
            await sys_c.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname='{tmp_db}' AND usename=current_user "
                f"AND pid<>pg_backend_pid() AND backend_type='client backend'")
            await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        finally:
            await sys_c.close()


class _FakeRunResult:
    def __init__(self, stdout=""):
        self.stdout = stdout
        self.stderr = ""
        self.returncode = 0


def _stopped_runner(cmd):
    if cmd[0] == "systemctl":
        return _FakeRunResult(
            "ActiveState=inactive\nSubState=dead\nMainPID=0\nNRestarts=0\n")
    if cmd[0] == "pgrep":
        return _FakeRunResult("")
    return _FakeRunResult("")


def _active_runner(cmd):
    if cmd[0] == "systemctl":
        return _FakeRunResult(
            "ActiveState=active\nSubState=running\nMainPID=12345\nNRestarts=0\n")
    if cmd[0] == "pgrep":
        return _FakeRunResult("12345 /path/to/python run.py")
    return _FakeRunResult("")


def _service(runner=_stopped_runner):
    return cli.ServiceState(run=runner)


async def _run_cli(argv, storage, runner=_stopped_runner, capsys=None):
    svc = _service(runner)
    rc = await cli._main_async(argv, storage=storage, service=svc)
    out = capsys.readouterr() if capsys else None
    return rc, (out.out if out else ""), (out.err if out else "")


# --------------------------- read-only commands ----------------------------
@pytest.mark.asyncio
async def test_status_no_secrets(temp_storage, capsys):
    rc, out, err = await _run_cli(["status"], temp_storage, capsys=capsys)
    assert rc == 0
    data = json.loads(out)
    assert data["schema_version"] == 11
    assert data["gate"] is False
    assert not SECRET_PATTERNS.search(out + err)


@pytest.mark.asyncio
async def test_policy_dry_run_runs(temp_storage, capsys):
    rc, out, _ = await _run_cli(["policy", "dry-run"], temp_storage, capsys=capsys)
    assert rc == 0
    d = json.loads(out)
    assert d["schema_version"] == 11
    assert d["would_disable"] == []
    assert not SECRET_PATTERNS.search(out)


# --------------------------- gate preflight --------------------------------
@pytest.mark.asyncio
async def test_gate_on_refuses_when_service_active(temp_storage, capsys):
    rc, out, _ = await _run_cli(
        ["gate", "on", "--reason", "x"], temp_storage, runner=_active_runner,
        capsys=capsys)
    assert rc == 1
    d = json.loads(out)
    assert d["reason"] == "service_not_stopped"
    async with temp_storage.pool.acquire() as con:
        enabled = await con.fetchval(
            "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
        audits = await con.fetchval("SELECT COUNT(*) FROM ui_audit_log")
    assert enabled is False
    assert audits == 0


@pytest.mark.asyncio
async def test_gate_on_refuses_when_db_has_other_connections(temp_storage, capsys):
    hog = await asyncpg.connect(temp_storage.dsn)
    try:
        rc, out, _ = await _run_cli(
            ["gate", "on", "--reason", "x"], temp_storage, capsys=capsys)
    finally:
        await hog.close()
    assert rc == 1
    assert json.loads(out)["reason"] == "db_connections_present"


@pytest.mark.asyncio
async def test_gate_on_success_when_stopped(temp_storage, capsys):
    # schema=11 + gate=False + dyn_engine=False + stopped service → ok
    rc, out, _ = await _run_cli(
        ["gate", "on", "--reason", "deploy"], temp_storage, capsys=capsys)
    assert rc == 0, out
    d = json.loads(out)
    assert d["ok"] is True
    assert d["enabled"] is True
    assert d["next_action"].startswith("run 'systemctl --user start")
    async with temp_storage.pool.acquire() as con:
        enabled = await con.fetchval(
            "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
        a = await con.fetchrow(
            "SELECT actor, action, outcome FROM ui_audit_log "
            "ORDER BY id DESC LIMIT 1")
    assert enabled is True
    assert dict(a) == {"actor": "admin_cli", "action": "gate.on", "outcome": "ok"}
    assert not SECRET_PATTERNS.search(out)


@pytest.mark.asyncio
async def test_gate_on_refuses_when_already_on(temp_storage, capsys):
    await _run_cli(["gate", "on", "--reason", "x"], temp_storage, capsys=capsys)
    capsys.readouterr()  # drop first call output
    # second call: gate already True → rejection + audit
    rc, out, _ = await _run_cli(
        ["gate", "on", "--reason", "x2"], temp_storage, capsys=capsys)
    assert rc == 1
    assert json.loads(out)["reason"] == "gate_state_mismatch"
    async with temp_storage.pool.acquire() as con:
        rejects = await con.fetchval(
            "SELECT COUNT(*) FROM ui_audit_log "
            "WHERE action='gate.on' AND outcome='rejected'")
    assert rejects == 1


@pytest.mark.asyncio
async def test_gate_atomic_rollback_on_audit_failure(temp_storage, capsys,
                                                    monkeypatch):
    """If the audit INSERT raises, the gate UPDATE must roll back."""
    async def boom(*a, **kw):
        raise RuntimeError("synthetic audit failure")
    monkeypatch.setattr(temp_storage, "_admin_audit", boom)
    with pytest.raises(RuntimeError):
        await _run_cli(["gate", "on", "--reason", "x"], temp_storage,
                       capsys=capsys)
    async with temp_storage.pool.acquire() as con:
        enabled = await con.fetchval(
            "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
        audits = await con.fetchval("SELECT COUNT(*) FROM ui_audit_log")
    assert enabled is False     # UPDATE rolled back
    assert audits == 0


# --------------------------- preferences -----------------------------------
async def _seed_discovered(storage, chat_id):
    async with storage.pool.acquire() as con:
        await con.execute(
            "INSERT INTO chats (id, type, title) "
            "VALUES ($1,'supergroup','T') ON CONFLICT DO NOTHING", chat_id)
        await con.execute(
            "INSERT INTO discovered_dialogs (chat_id, category, title) "
            "VALUES ($1, 'megagroup', 'T') ON CONFLICT DO NOTHING", chat_id)


async def _current_version(storage) -> int:
    async with storage.pool.acquire() as con:
        return int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))


@pytest.mark.asyncio
async def test_preferences_select_bumps_version_and_audits(temp_storage, capsys):
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    v = await _current_version(temp_storage)
    rc, out, _ = await _run_cli(
        ["preferences", "select", "--chat", str(cid),
         "--expected-policy-version", str(v)],
        temp_storage, capsys=capsys)
    assert rc == 0
    d = json.loads(out)
    assert d["ok"] is True
    assert d["requested_version_before"] == v
    assert d["requested_version_after"] == v + 1
    async with temp_storage.pool.acquire() as con:
        sel = await con.fetchval(
            "SELECT selected FROM target_preferences WHERE chat_id=$1", cid)
        audits = await con.fetchval(
            "SELECT COUNT(*) FROM ui_audit_log "
            "WHERE action='prefs.select' AND outcome='ok'")
    assert sel is True
    assert audits == 1


@pytest.mark.asyncio
async def test_preferences_select_rejects_stale_version(temp_storage, capsys):
    """CLI must refuse select when --expected-policy-version != current."""
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    v = await _current_version(temp_storage)
    rc, out, _ = await _run_cli(
        ["preferences", "select", "--chat", str(cid),
         "--expected-policy-version", str(v - 1)],
        temp_storage, capsys=capsys)
    assert rc == 1
    d = json.loads(out)
    assert d["reason"] == "stale_version"
    assert d["current_requested_version"] == v
    async with temp_storage.pool.acquire() as con:
        rows = await con.fetchval(
            "SELECT COUNT(*) FROM target_preferences WHERE chat_id=$1", cid)
        v_after = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
        rejects = await con.fetchval(
            "SELECT COUNT(*) FROM ui_audit_log "
            "WHERE action='prefs.select' AND outcome='rejected'")
    assert rows == 0                  # no row created
    assert v_after == v               # no bump
    assert rejects == 1


@pytest.mark.asyncio
async def test_preferences_select_concurrent_policy_change_refuses(temp_storage,
                                                                   capsys):
    """Simulates: operator did dry-run at version V; between dry-run and
    select, another intent change bumped the version. The select-with-
    --expected-policy-version=V must refuse without mutation."""
    cid_a = -1001234567890
    cid_b = -1001234567891
    await _seed_discovered(temp_storage, cid_a)
    await _seed_discovered(temp_storage, cid_b)
    observed = await _current_version(temp_storage)
    # Concurrent intent change between dry-run and select:
    await _run_cli(
        ["preferences", "select", "--chat", str(cid_b),
         "--expected-policy-version", str(observed)],
        temp_storage, capsys=capsys)
    capsys.readouterr()
    # Operator attempts select with the stale version they observed earlier.
    rc, out, _ = await _run_cli(
        ["preferences", "select", "--chat", str(cid_a),
         "--expected-policy-version", str(observed)],
        temp_storage, capsys=capsys)
    assert rc == 1
    d = json.loads(out)
    assert d["reason"] == "stale_version"
    assert d["current_requested_version"] == observed + 1
    async with temp_storage.pool.acquire() as con:
        n_a = await con.fetchval(
            "SELECT COUNT(*) FROM target_preferences WHERE chat_id=$1", cid_a)
    assert n_a == 0, "the refused select must not have inserted a row"


@pytest.mark.asyncio
async def test_preferences_select_audit_rollback_on_audit_failure(
        temp_storage, capsys, monkeypatch):
    """If the audit INSERT raises after a successful preference upsert, the
    whole transaction (preference + audit) must roll back together."""
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    v = await _current_version(temp_storage)

    async def boom(*a, **kw):
        raise RuntimeError("synthetic audit failure")
    monkeypatch.setattr(temp_storage, "_admin_audit", boom)
    with pytest.raises(RuntimeError):
        await _run_cli(
            ["preferences", "select", "--chat", str(cid),
             "--expected-policy-version", str(v)],
            temp_storage, capsys=capsys)
    async with temp_storage.pool.acquire() as con:
        n = await con.fetchval(
            "SELECT COUNT(*) FROM target_preferences WHERE chat_id=$1", cid)
        v_after = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
        a = await con.fetchval("SELECT COUNT(*) FROM ui_audit_log")
    assert n == 0
    assert v_after == v
    assert a == 0


@pytest.mark.asyncio
async def test_preferences_select_idempotent_no_double_bump(temp_storage, capsys):
    """Second select on the same chat_id with the same values must NOT bump
    requested_version (statement trigger uses EXCEPT ALL — no net change)."""
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    v0 = await _current_version(temp_storage)
    rc1, out1, _ = await _run_cli(
        ["preferences", "select", "--chat", str(cid),
         "--expected-policy-version", str(v0)],
        temp_storage, capsys=capsys)
    assert rc1 == 0
    v1 = int(json.loads(out1)["requested_version_after"])
    assert v1 == v0 + 1
    rc2, out2, _ = await _run_cli(
        ["preferences", "select", "--chat", str(cid),
         "--expected-policy-version", str(v1)],
        temp_storage, capsys=capsys)
    assert rc2 == 0
    v2 = int(json.loads(out2)["requested_version_after"])
    assert v2 == v1, (
        f"idempotent select must not bump policy version; got {v1} -> {v2}")


@pytest.mark.asyncio
async def test_preferences_unselect_requires_confirm_chat_id(temp_storage, capsys):
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    rc, out, _ = await _run_cli(
        ["preferences", "unselect", "--chat", str(cid),
         "--confirm-chat-id", str(cid + 1),
         "--expected-policy-version", "1"],
        temp_storage, capsys=capsys)
    assert rc == 1
    assert json.loads(out)["reason"] == "confirm_chat_id_mismatch"


@pytest.mark.asyncio
async def test_preferences_unselect_refuses_stale_version(temp_storage, capsys):
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    # bump version via a select first
    v0 = await _current_version(temp_storage)
    await _run_cli(["preferences", "select", "--chat", str(cid),
                    "--expected-policy-version", str(v0)],
                   temp_storage, capsys=capsys)
    capsys.readouterr()
    async with temp_storage.pool.acquire() as con:
        current_ver = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
    rc, out, _ = await _run_cli(
        ["preferences", "unselect", "--chat", str(cid),
         "--confirm-chat-id", str(cid),
         "--expected-policy-version", str(current_ver - 1)],
        temp_storage, capsys=capsys)
    assert rc == 1
    d = json.loads(out)
    assert d["reason"] == "stale_version"
    assert d["current_requested_version"] == current_ver


@pytest.mark.asyncio
async def test_preferences_unselect_ok_does_not_claim_disable_ack(temp_storage,
                                                                  capsys):
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    # Simulate target currently enabled so the unselect WOULD disable.
    async with temp_storage.pool.acquire() as con:
        await con.execute(
            "INSERT INTO target_state (chat_id, target, backfill_status, enabled) "
            "VALUES ($1, 't', 'syncing', TRUE) ON CONFLICT DO NOTHING", cid)
        ver = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
    rc, out, _ = await _run_cli(
        ["preferences", "unselect", "--chat", str(cid),
         "--confirm-chat-id", str(cid),
         "--expected-policy-version", str(ver)],
        temp_storage, capsys=capsys)
    assert rc == 0
    d = json.loads(out)
    assert d["ok"] is True
    # CLI must NOT claim disable ACK: target_state still enabled.
    async with temp_storage.pool.acquire() as con:
        still = await con.fetchval(
            "SELECT enabled FROM target_state WHERE chat_id=$1", cid)
    assert still is True
    assert "target_state.enabled changes" in d["note"]


@pytest.mark.asyncio
async def test_preferences_select_rejects_when_excluded(temp_storage, capsys):
    cid = -1001234567890
    await _seed_discovered(temp_storage, cid)
    async with temp_storage.pool.acquire() as con:
        ver = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
    # mark excluded first
    rc, _, _ = await _run_cli(
        ["preferences", "exclude", "--chat", str(cid),
         "--confirm-chat-id", str(cid),
         "--expected-policy-version", str(ver)],
        temp_storage, capsys=capsys)
    assert rc == 0
    capsys.readouterr()
    # now selecting must be rejected: DB CHECK tp_not_both catches it.
    v_now = await _current_version(temp_storage)
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await _run_cli(
            ["preferences", "select", "--chat", str(cid),
             "--expected-policy-version", str(v_now)],
            temp_storage, capsys=capsys)


# --------------------------- mass-gone confirm -----------------------------
@pytest.mark.asyncio
async def test_mass_gone_confirm_rejects_stale(temp_storage, capsys):
    # no active block → anything is stale
    rc, out, _ = await _run_cli(
        ["mass-gone", "confirm", "--version", "99",
         "--fingerprint", "0" * 64],
        temp_storage, capsys=capsys)
    assert rc == 1
    assert json.loads(out)["reason"] == "stale_block"
    async with temp_storage.pool.acquire() as con:
        n = await con.fetchval(
            "SELECT COUNT(*) FROM ui_audit_log "
            "WHERE action='mass_gone.confirm' AND outcome='rejected'")
    assert n == 1


# --------------------------- discovery request -----------------------------
@pytest.mark.asyncio
async def test_discovery_request_first_ok_then_pending_rejected(temp_storage,
                                                                 capsys):
    rc1, out1, _ = await _run_cli(
        ["discovery", "request"], temp_storage, capsys=capsys)
    assert rc1 == 0
    d1 = json.loads(out1)
    assert d1["ok"] is True
    # Re-request without a consumed_seq bump: pending_request.
    rc2, out2, _ = await _run_cli(
        ["discovery", "request"], temp_storage, capsys=capsys)
    assert rc2 == 1
    assert json.loads(out2)["reason"] == "pending_request"


# --------------------------- secret hygiene / no systemd side effects ------
@pytest.mark.asyncio
async def test_all_commands_never_expose_secrets(temp_storage, capsys):
    cmds = [
        ["status"],
        ["policy", "dry-run"],
        ["gate", "status"],
        ["mass-gone", "status"],
        ["preferences", "list"],
        ["collection-config", "show"],
        ["discovery", "status"],
    ]
    for argv in cmds:
        rc, out, err = await _run_cli(argv, temp_storage, capsys=capsys)
        assert rc == 0, argv
        assert not SECRET_PATTERNS.search(out + err), (argv, out, err)


def test_admin_module_source_does_not_shell_restart():
    """Static guard: admin.py must never invoke systemctl with a mutating
    verb (stop/start/restart/kill/reload). Mentioning those verbs inside
    operator-facing hint strings is fine; only actual subprocess argument
    lists are checked."""
    src = (os.path.join(os.path.dirname(__file__), "..",
                        "tgparser", "admin.py"))
    text = open(src, "r", encoding="utf-8").read()
    # Find subprocess.run([...]) or _run([...]) call arg lists containing
    # 'systemctl' and a mutating verb as adjacent string tokens.
    mutating = r"(?:stop|start|restart|kill|reload)"
    pattern = re.compile(
        r"""\(                             # opening paren
            \s*\[                          # start of list
            (?:[^\[\]]*?)                  # anything non-bracket
            ["']systemctl["']              # 'systemctl'
            (?:[^\[\]]*?)                  # anything
            ["']""" + mutating + r"""["']  # one of the verbs
        """, re.X | re.S)
    hits = pattern.findall(text)
    assert not hits, (
        f"admin.py appears to invoke systemctl with a mutating verb: {hits}")


# --------------------------- concurrency: no lost version ------------------
@pytest.mark.asyncio
async def test_concurrent_pref_updates_do_not_lose_version(temp_storage, capsys):
    """Two select upserts in parallel on different chats serialize cleanly
    via policy_state FOR UPDATE: final requested_version >= start + 2."""
    a, b = -1001234567890, -1001234567891
    await _seed_discovered(temp_storage, a)
    await _seed_discovered(temp_storage, b)
    async with temp_storage.pool.acquire() as con:
        start = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
    # Run concurrently using two storage sessions (simulate two CLI procs).
    # Both pass the SAME observed version; FOR UPDATE on policy_state
    # serializes them: the first wins and bumps, the second loses with
    # stale_version. Both rcs are observed, exactly one succeeds.
    async def _run(cid):
        svc = _service()
        rc = await cli._main_async(
            ["preferences", "select", "--chat", str(cid),
             "--expected-policy-version", str(start)],
            storage=temp_storage, service=svc)
        capsys.readouterr()  # discard
        return rc
    rcs = sorted(await asyncio.gather(_run(a), _run(b)))
    assert rcs == [0, 1], (
        f"expected one success and one stale_version refusal, got {rcs}")
    async with temp_storage.pool.acquire() as con:
        after = int(await con.fetchval(
            "SELECT requested_version FROM policy_state"))
        n_sel = int(await con.fetchval(
            "SELECT COUNT(*) FROM target_preferences WHERE selected=TRUE"))
        rejects = int(await con.fetchval(
            "SELECT COUNT(*) FROM ui_audit_log "
            "WHERE action='prefs.select' AND outcome='rejected'"))
    # Exactly one won; exactly one stale_version audit was recorded.
    assert n_sel == 1
    assert after == start + 1
    assert rejects == 1
