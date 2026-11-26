"""/control/add_chat and /control/sync_dialogs: auth, Origin, audit,
idempotency, no second TelegramClient, stopped-service behaviour,
secret hygiene."""
from __future__ import annotations

import asyncio
import inspect
import re

import asyncpg
import pytest

from web import control as control_mod


SECRET_PATTERNS = re.compile(
    r"(postgres(ql)?://|api_hash|access_hash|session|password|token|\+?[0-9]{10,15})",
    re.I)


def _origin(client) -> str:
    # The TestClient's default is http://testserver which the control endpoint
    # does not accept; we must spoof the real 127.0.0.1 origin the UI serves.
    return "http://127.0.0.1:8080"


def _headers(client, token: str) -> dict:
    return {
        "x-tgparser-control": token,
        "origin": _origin(client),
        "content-type": "application/json",
    }


def _token(client) -> str:
    return client.app.state.control_token


# ---------------------------------------------------------------- normalize

def test_normalize_raw_target_accepts_valid_forms():
    normalize = control_mod._normalize_raw_target
    assert normalize("@alice") == "@alice"
    assert normalize("alice") == "@alice"
    assert normalize("https://t.me/alice") == "@alice"
    assert normalize("http://t.me/alice/") == "@alice"
    assert normalize("https://t.me/+ABCdef123_hash") == "https://t.me/+ABCdef123_hash"
    assert normalize("https://t.me/joinchat/ABCdef123_hash") == "https://t.me/+ABCdef123_hash"
    assert normalize("-1001234567890") == "-1001234567890"
    assert normalize("1234567") == "1234567"


def test_normalize_raw_target_rejects_bogus():
    normalize = control_mod._normalize_raw_target
    for bad in ("", "  ", "a", "@1abc", "not a url", "https://example.com/x",
                "https://t.me/", "https://t.me/+", "a" * 400, "\x01bad"):
        with pytest.raises(ValueError):
            normalize(bad)


def test_sync_once_imports_and_reads_config_keys():
    """Regression guard: sync_once must read the SAME config keys as run.py
    (the media settings live under cfg['storage'], never cfg['media'])."""
    import pathlib
    from tgparser import sync_once as so
    src = pathlib.Path(so.__file__).read_text(encoding="utf-8")
    # This specific pattern caused a live KeyError('media') once.
    assert "cfg[\"media\"]" not in src
    assert "cfg['media']" not in src
    assert "cfg.get(\"storage\"" in src or 'cfg["storage"]' in src
    # The main run.py uses storage.media_dir / storage.max_media_mb; sync_once
    # must reference those same keys.
    assert "media_dir" in src and "max_media_mb" in src


# ---------------------------------------------------------------- add_chat

def test_add_chat_requires_token(ui_env):
    client, *_ = ui_env
    r = client.post("/control/add_chat", json={"raw_target": "@alice"})
    assert r.status_code == 403


def test_add_chat_requires_matching_origin(ui_env):
    client, *_ = ui_env
    r = client.post(
        "/control/add_chat",
        json={"raw_target": "@alice"},
        headers={"x-tgparser-control": _token(client),
                 "origin": "http://evil.example.com",
                 "content-type": "application/json"})
    assert r.status_code == 403


def test_add_chat_happy_path_and_audit(ui_env):
    client, _, dsn = ui_env
    r = client.post("/control/add_chat",
                    json={"raw_target": "https://t.me/alice"},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["inserted"] is True
    assert data["status"] == "pending"
    # DB state: the yaml_target_imports row + audit row.
    async def check():
        con = await asyncpg.connect(dsn)
        try:
            row = await con.fetchrow(
                "SELECT raw_target, status FROM yaml_target_imports")
            audit = await con.fetchrow(
                "SELECT actor, action, outcome FROM ui_audit_log "
                "WHERE action='chat.add' ORDER BY id DESC LIMIT 1")
            return row, audit
        finally:
            await con.close()
    row, audit = asyncio.get_event_loop().run_until_complete(check())
    assert row["raw_target"] == "@alice"
    assert row["status"] == "pending"
    assert dict(audit) == {"actor": "ui", "action": "chat.add", "outcome": "ok"}
    assert not SECRET_PATTERNS.search(r.text), r.text


def test_add_chat_is_idempotent(ui_env):
    client, _, dsn = ui_env
    r1 = client.post("/control/add_chat",
                     json={"raw_target": "@bobsmith"},
                     headers=_headers(client, _token(client)))
    r2 = client.post("/control/add_chat",
                     json={"raw_target": "@bobsmith"},
                     headers=_headers(client, _token(client)))
    assert r1.status_code == 200 and r2.status_code == 200, (r1.text, r2.text)
    d1, d2 = r1.json(), r2.json()
    assert d1["inserted"] is True and d2["inserted"] is False
    async def rows():
        con = await asyncpg.connect(dsn)
        try:
            n = await con.fetchval(
                "SELECT COUNT(*) FROM yaml_target_imports WHERE raw_target='@bobsmith'")
            ok_audits = await con.fetchval(
                "SELECT COUNT(*) FROM ui_audit_log "
                "WHERE action='chat.add' AND outcome='ok'")
            return int(n), int(ok_audits)
        finally:
            await con.close()
    n, oks = asyncio.get_event_loop().run_until_complete(rows())
    assert n == 1, "idempotent add must not create duplicates"
    assert oks == 2, "both attempts are audited"


def test_add_chat_rejects_invalid_input(ui_env):
    client, _, dsn = ui_env
    r = client.post("/control/add_chat",
                    json={"raw_target": "not a url"},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 409
    data = r.json()
    assert data["ok"] is False
    assert "не впізнано" in data["message"]
    async def audit():
        con = await asyncpg.connect(dsn)
        try:
            return await con.fetchrow(
                "SELECT actor, action, outcome FROM ui_audit_log "
                "WHERE action='chat.add' ORDER BY id DESC LIMIT 1")
        finally:
            await con.close()
    row = asyncio.get_event_loop().run_until_complete(audit())
    assert dict(row) == {"actor": "ui", "action": "chat.add",
                         "outcome": "rejected"}


def test_add_chat_422_on_missing_field(ui_env):
    client, *_ = ui_env
    r = client.post("/control/add_chat", json={},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 422


# ---------------------------------------------------------------- sync_dialogs

def test_sync_dialogs_requires_token(ui_env):
    client, *_ = ui_env
    r = client.post("/control/sync_dialogs", json={})
    assert r.status_code == 403


def test_sync_dialogs_queued_when_service_running(ui_env, monkeypatch):
    """Service already up → UI bumps discovery_control.requested_seq via
    admin_request_discovery; no subprocess is spawned."""
    client, _, dsn = ui_env
    async def fake_state():
        return control_mod.ServiceState("active", "running", 99999)
    monkeypatch.setattr(control_mod, "service_state", fake_state)

    spawned = {"n": 0}
    async def fail_spawn(*a, **kw):
        spawned["n"] += 1
        raise AssertionError("must not spawn sync_once when service is running")
    monkeypatch.setattr("asyncio.create_subprocess_exec", fail_spawn)

    r = client.post("/control/sync_dialogs", json={},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["mode"] == "queued"
    assert data["service_running"] is True
    assert data["requested_seq"] == 1
    assert spawned["n"] == 0
    async def check():
        con = await asyncpg.connect(dsn)
        try:
            seq = await con.fetchval(
                "SELECT requested_seq FROM discovery_control WHERE id=1")
            audit = await con.fetchrow(
                "SELECT actor, action, outcome FROM ui_audit_log "
                "WHERE action='discovery.request' ORDER BY id DESC LIMIT 1")
            return int(seq), audit
        finally:
            await con.close()
    seq, audit = asyncio.get_event_loop().run_until_complete(check())
    assert seq == 1
    assert dict(audit) == {"actor": "ui", "action": "discovery.request",
                           "outcome": "ok"}


def test_sync_dialogs_runs_once_when_service_stopped(ui_env, monkeypatch):
    """Service stopped → UI spawns `python -m tgparser.sync_once` and
    surfaces its JSON summary. No systemd start; no background queue."""
    client, _, dsn = ui_env
    async def fake_state():
        return control_mod.ServiceState("inactive", "dead", 0)
    monkeypatch.setattr(control_mod, "service_state", fake_state)

    captured = {}
    class _FakeProc:
        returncode = 0
        async def communicate(self):
            return (
                b'{"ok": true, "exit_code": 0, "duration_s": 1.2, '
                b'"discovery": {"ran": true, "run_id": 5, "ok": true, "seen": 7, "new": 2, "gone": 1, '
                b'"retry_s": 0.0}, '
                b'"resolver": {"attempted": 2, "selected": 1, "retry": 1, '
                b'"terminal": 0, "errors": 0}, "message": "sync complete"}\n',
                b"")
        async def wait(self):
            return 0
        def kill(self):
            pass
    async def fake_spawn(*cmd, **kw):
        captured["cmd"] = list(cmd)
        return _FakeProc()
    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_spawn)

    r = client.post("/control/sync_dialogs", json={},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True and data["mode"] == "ran"
    assert data["service_running"] is False
    assert data["resolver"] == {"attempted": 2, "selected": 1,
                                "retry": 1, "terminal": 0, "errors": 0}
    assert data["message"] == ("Синхронізацію виконано: діалогів 7, нових 2, "
                               "зникло 1, додано вручну 1.")
    # Correct command: argv[0] is the current python, then -m tgparser.sync_once
    assert any("tgparser.sync_once" in str(a) for a in captured["cmd"])
    assert "--timeout" in captured["cmd"]
    # Audit:
    async def audit():
        con = await asyncpg.connect(dsn)
        try:
            return await con.fetchrow(
                "SELECT actor, action, outcome FROM ui_audit_log "
                "WHERE action='discovery.sync_once' ORDER BY id DESC LIMIT 1")
        finally:
            await con.close()
    row = asyncio.get_event_loop().run_until_complete(audit())
    assert dict(row) == {"actor": "ui", "action": "discovery.sync_once",
                         "outcome": "ok"}


def test_sync_dialogs_surfaces_sync_once_failure(ui_env, monkeypatch):
    """A non-zero sync_once exit code becomes a 409 with the operator-facing
    message from the summary, and the audit is outcome='error'."""
    client, _, dsn = ui_env
    async def fake_state():
        return control_mod.ServiceState("inactive", "dead", 0)
    monkeypatch.setattr(control_mod, "service_state", fake_state)

    class _FakeProc:
        returncode = 1
        async def communicate(self):
            return (
                b'{"ok": false, "exit_code": 1, "duration_s": 0.0, '
                b'"message": "Telegram session not authorized"}\n',
                b"")
        async def wait(self): return 1
        def kill(self): pass
    async def fake_spawn(*cmd, **kw):
        return _FakeProc()
    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_spawn)

    r = client.post("/control/sync_dialogs", json={},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 409
    data = r.json()
    assert data["ok"] is False
    assert "не авторизована" in data["message"]
    async def audit_outcome():
        con = await asyncpg.connect(dsn)
        try:
            return await con.fetchval(
                "SELECT outcome FROM ui_audit_log "
                "WHERE action='discovery.sync_once' ORDER BY id DESC LIMIT 1")
        finally:
            await con.close()
    assert asyncio.get_event_loop().run_until_complete(audit_outcome()) == "error"


def test_sync_dialogs_running_rejects_pending_request(ui_env, monkeypatch):
    """When the service is running, back-to-back clicks still go through
    admin_request_discovery which refuses the second one as pending."""
    client, _, dsn = ui_env
    async def fake_state():
        return control_mod.ServiceState("active", "running", 99999)
    monkeypatch.setattr(control_mod, "service_state", fake_state)
    h = _headers(client, _token(client))
    r1 = client.post("/control/sync_dialogs", json={}, headers=h)
    r2 = client.post("/control/sync_dialogs", json={}, headers=h)
    assert r1.status_code == 200
    assert r2.status_code == 409
    assert r2.json()["reason"] == "pending_request"


# ---------------------------------------------------------------- safety

def test_endpoints_never_leak_secrets(ui_env):
    """Spin through the public surface of add_chat/sync_dialogs responses
    and verify none of them contains anything that looks like a secret."""
    client, *_ = ui_env
    h = _headers(client, _token(client))
    r1 = client.post("/control/add_chat", json={"raw_target": "@alice"},
                     headers=h)
    r2 = client.post("/control/sync_dialogs", json={}, headers=h)
    for r in (r1, r2):
        assert not SECRET_PATTERNS.search(r.text), r.text


def test_control_source_never_spawns_telegramclient():
    """Static guard: web/control.py must not import TelegramClient or open
    a second Telethon session — the UI goes through DB IPC only."""
    import pathlib
    src = pathlib.Path(control_mod.__file__).read_text(encoding="utf-8")
    for forbidden in ("telethon", "TelegramClient", ".session"):
        assert forbidden not in src, (
            f"web/control.py must not reference {forbidden!r}")


def test_control_source_never_shells_systemctl_startlike_from_add_sync():
    """Static guard: the add_chat/sync_dialogs helpers must not shell out
    to systemctl with a mutating verb. (The legacy start/stop helpers still
    may; we only forbid it from the new helpers' call graph.)"""
    import pathlib
    src = pathlib.Path(control_mod.__file__).read_text(encoding="utf-8")
    add_src    = inspect.getsource(control_mod.add_chat)
    sync_src   = inspect.getsource(control_mod.sync_dialogs)
    for forbidden in ("_systemctl(\"start", "_systemctl(\"stop",
                      "_systemctl(\"restart", "systemctl start",
                      "systemctl stop", "systemctl restart"):
        assert forbidden not in add_src
        assert forbidden not in sync_src
    # Both must still exist (sanity).
    assert callable(control_mod.add_chat)
    assert callable(control_mod.sync_dialogs)


# ---------------------------------------------------------------- delete_chat

def _run(dsn, sql, *args, fetch="val"):
    async def go():
        con = await asyncpg.connect(dsn)
        try:
            if fetch == "val":
                return await con.fetchval(sql, *args)
            await con.execute(sql, *args)
        finally:
            await con.close()
    return asyncio.get_event_loop().run_until_complete(go())


def test_delete_chat_requires_token(ui_env):
    client, *_ = ui_env
    assert client.post("/control/delete_chat", json={"chat_id": 1}).status_code == 403


def test_delete_chat_removes_empty_discovered_chat(ui_env):
    client, _, dsn = ui_env
    cid = -1009990001
    _run(dsn, "INSERT INTO discovered_dialogs (chat_id, category, title) "
              "VALUES ($1,'megagroup','Junk')", cid, fetch=None)
    assert "Junk" in client.get("/").text
    r = client.post("/control/delete_chat", json={"chat_id": cid},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 200, r.text
    assert _run(dsn, "SELECT COUNT(*) FROM discovered_dialogs WHERE chat_id=$1", cid) == 0
    assert _run(dsn, "SELECT excluded FROM target_preferences WHERE chat_id=$1", cid) is True
    assert _run(dsn, "SELECT outcome FROM ui_audit_log WHERE action='chat.delete' "
                     "ORDER BY id DESC LIMIT 1") == "ok"
    # Gone from the picker, and stays gone after a re-discovery.
    assert "Junk" not in client.get("/").text
    _run(dsn, "INSERT INTO discovered_dialogs (chat_id, category, title) "
              "VALUES ($1,'megagroup','Junk')", cid, fetch=None)
    assert "Junk" not in client.get("/").text


def test_delete_chat_refuses_chat_with_messages(ui_env):
    client, _, dsn = ui_env
    r = client.post("/control/delete_chat", json={"chat_id": -1001},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 409
    assert "повідомлень" in r.json()["message"]
    assert _run(dsn, "SELECT COUNT(*) FROM messages WHERE chat_id=-1001") == 3
    assert _run(dsn, "SELECT outcome FROM ui_audit_log WHERE action='chat.delete' "
                     "ORDER BY id DESC LIMIT 1") == "rejected"


def test_delete_chat_refuses_active_target(ui_env):
    client, _, dsn = ui_env
    cid = -1009990002
    _run(dsn, "INSERT INTO target_state (chat_id, target, backfill_status, enabled) "
              "VALUES ($1,'t','syncing',TRUE)", cid, fetch=None)
    r = client.post("/control/delete_chat", json={"chat_id": cid},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 409
    assert _run(dsn, "SELECT enabled FROM target_state WHERE chat_id=$1", cid) is True


def test_start_accepts_chat_known_only_from_discovery(ui_env, monkeypatch):
    """A chat that exists only in discovered_dialogs (no chats row yet) must
    be accepted by /control/start's selection check."""
    client, _, dsn = ui_env
    cid = -1009990003
    _run(dsn, "INSERT INTO discovered_dialogs (chat_id, category, title) "
              "VALUES ($1,'megagroup','Fresh')", cid, fetch=None)
    _run(dsn, "UPDATE deployment_gates SET enabled=TRUE WHERE key='dynamic_targets'",
         fetch=None)
    async def fake_systemctl(*args, timeout=25):
        return 0, ""
    async def fake_state():
        return control_mod.ServiceState("active", "running", 1)
    async def fake_wait(running, timeout=30):
        return control_mod.ServiceState("active", "running", 1)
    monkeypatch.setattr(control_mod, "_systemctl", fake_systemctl)
    monkeypatch.setattr(control_mod, "service_state", fake_state)
    monkeypatch.setattr(control_mod, "_wait", fake_wait)
    async def no_stop(storage):
        return None
    monkeypatch.setattr(control_mod, "stop_service", no_stop)
    r = client.post("/control/start", json={"mode": "selected", "chats": [cid]},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 200, r.text
    assert _run(dsn, "SELECT selected FROM target_preferences WHERE chat_id=$1", cid) is True


def test_discovery_only_chats_are_named_and_counted_everywhere(ui_env):
    """A chat known only from discovery (no `chats` row) must show its real
    title in the targets table, the graph list and the 'Чати' counter."""
    client, _, dsn = ui_env
    cid = -1009990004
    _run(dsn, "INSERT INTO discovered_dialogs (chat_id, category, title, username) "
              "VALUES ($1,'megagroup','OnlyDiscovered','onlydisc')", cid, fetch=None)
    _run(dsn, "INSERT INTO target_state (chat_id, target, backfill_status, enabled) "
              "VALUES ($1,'t','running',TRUE)", cid, fetch=None)
    # targets table on /chats
    assert "OnlyDiscovered" in client.get("/chats").text
    # graph chat list
    rows = client.get("/graph/data").json()["rows"]
    assert any(r["id"] == cid and r["title"] == "OnlyDiscovered" for r in rows)
    # 'Чати' counter = seeded -1001 + this one
    import web.queries as q
    async def chats_total():
        con = await asyncpg.connect(dsn)
        try:
            return (await con.fetchrow(q.Q_TOTALS))["chats"]
        finally:
            await con.close()
    assert asyncio.get_event_loop().run_until_complete(chats_total()) == 2
    assert "OnlyDiscovered" in client.get("/").text



def _fake_proc(payload: bytes, rc: int):
    class _P:
        returncode = rc
        async def communicate(self):
            return payload, b""
        async def wait(self):
            return rc
        def kill(self):
            pass
    return _P()


def test_sync_dialogs_throttled_is_explicit(ui_env, monkeypatch):
    """A click inside the Telegram rate window must not claim success."""
    client, *_ = ui_env
    async def fake_state():
        return control_mod.ServiceState("inactive", "dead", 0)
    monkeypatch.setattr(control_mod, "service_state", fake_state)
    async def fake_spawn(*cmd, **kw):
        return _fake_proc(b'{"ok": false, "exit_code": 3, "message": "throttled", '
                          b'"discovery": {"ran": false, "reason": "throttled", "wait_s": 42}}', 3)
    monkeypatch.setattr("asyncio.create_subprocess_exec", fake_spawn)
    r = client.post("/control/sync_dialogs", json={},
                    headers=_headers(client, _token(client)))
    assert r.status_code == 409
    assert "42 с" in r.json()["message"]


def test_sync_status_reports_progress(ui_env):
    client, _, dsn = ui_env
    h = _headers(client, _token(client))
    assert client.post("/control/sync_status", json={}).status_code == 403
    assert client.post("/control/sync_status", json={}, headers=h).json()["done"] is True
    _run(dsn, "UPDATE discovery_control SET requested_seq = consumed_seq + 1 WHERE id=1",
         fetch=None)
    assert client.post("/control/sync_status", json={}, headers=h).json()["done"] is False


def test_readd_after_not_found_retries(ui_env):
    """Re-adding a chat that previously failed to resolve puts it back to
    pending (the account may have joined it since)."""
    client, _, dsn = ui_env
    h = _headers(client, _token(client))
    client.post("/control/add_chat", json={"raw_target": "@laterjoined"}, headers=h)
    _run(dsn, "UPDATE yaml_target_imports SET status='terminal', attempts=5 "
              "WHERE raw_target='@laterjoined'", fetch=None)
    r = client.post("/control/add_chat", json={"raw_target": "@laterjoined"}, headers=h)
    assert r.status_code == 200
    assert _run(dsn, "SELECT status FROM yaml_target_imports "
                     "WHERE raw_target='@laterjoined'") == "pending"


def test_sync_once_takes_over_dead_runtime_pid():
    """A runtime_state pid left by a killed sync_once must not block the
    next sync (the session lock already guarantees exclusivity)."""
    from tgparser import sync_once as so
    import os
    assert so._pid_alive(os.getpid()) is True
    assert so._pid_alive(2**22 + 12345) is False
