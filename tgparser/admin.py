"""TGParser Admin CLI (v1, read-safe + atomic mutations).

Mutating commands never start, stop or restart the service. They only
read-only query ``systemctl --user show`` / ``pgrep`` to confirm the service
is already stopped by the operator. All mutation + audit pairs run in one
transaction via the ``admin_*`` wrappers in ``tgparser.storage``.

Usage:
  python -m tgparser.admin status
  python -m tgparser.admin policy dry-run
  python -m tgparser.admin gate status
  python -m tgparser.admin gate on  --reason "..."
  python -m tgparser.admin gate off --reason "..."
  python -m tgparser.admin mass-gone status
  python -m tgparser.admin mass-gone confirm --version N --fingerprint HEX
  python -m tgparser.admin preferences list [--chat ID]
  python -m tgparser.admin preferences select   --chat ID [--note TXT]
  python -m tgparser.admin preferences unselect --chat ID \\
                                           --confirm-chat-id ID \\
                                           --expected-policy-version N [--note TXT]
  python -m tgparser.admin preferences clear    --chat ID \\
                                           --confirm-chat-id ID \\
                                           --expected-policy-version N
  python -m tgparser.admin preferences exclude  --chat ID \\
                                           --confirm-chat-id ID \\
                                           --expected-policy-version N [--note TXT]
  python -m tgparser.admin collection-config show
  python -m tgparser.admin discovery status
  python -m tgparser.admin discovery request [--reason TXT]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import json
import subprocess
import sys
from typing import Any

from tgparser.config import load as load_cfg
from tgparser.storage import Storage


SECRET_HINTS = ("password", "token", "session", "api_hash", "access_hash",
                "dsn", "phone", "secret")


def _scrub(obj: Any) -> Any:
    """Defensive scrub: mask any value under a sensitive-looking key."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if any(h in str(k).lower() for h in SECRET_HINTS):
                out[k] = "***"
            else:
                out[k] = _scrub(v)
        return out
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    return obj


SERVICE_UNIT = "tgparser.service"
ACTOR = "admin_cli"

# A Storage ctor needs the DSN but nothing user-visible ever prints the DSN,
# password, API hash, session or access_hash; the audit layer blocks that at
# the DB boundary already.


def _default_subprocess_run(cmd):
    """Thin wrapper so tests can monkeypatch subprocess invocations."""
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


class ServiceState:
    """Read-only view of tgparser.service, obtained via systemctl show +
    pgrep. Never mutates systemd. ``stopped`` is True only when all four
    required signals agree."""

    def __init__(self, run=_default_subprocess_run):
        self._run = run
        self.active_state: str | None = None
        self.sub_state: str | None = None
        self.main_pid: str | None = None
        self.n_restarts: str | None = None
        self.run_py_count: int = 0

    def refresh(self):
        out = self._run([
            "systemctl", "--user", "show", SERVICE_UNIT,
            "--property=ActiveState,SubState,MainPID,NRestarts"])
        kv = {}
        for line in (out.stdout or "").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k] = v
        self.active_state = kv.get("ActiveState")
        self.sub_state = kv.get("SubState")
        self.main_pid = kv.get("MainPID")
        self.n_restarts = kv.get("NRestarts")
        ps = self._run(["pgrep", "-af", "python.*run.py"])
        lines = [line for line in (ps.stdout or "").splitlines()
                 if line.strip() and "shell-snapshots" not in line
                 and "admin.py" not in line and "pytest" not in line]
        self.run_py_count = len(lines)
        return self

    @property
    def stopped(self) -> bool:
        return (self.active_state == "inactive"
                and self.sub_state == "dead"
                and self.main_pid == "0"
                and self.run_py_count == 0)

    def to_dict(self) -> dict:
        return {"ActiveState": self.active_state,
                "SubState": self.sub_state,
                "MainPID": self.main_pid,
                "NRestarts": self.n_restarts,
                "run_py_processes": self.run_py_count}


async def _db_connections_count(storage: Storage) -> int:
    """Number of backends to the current DB besides this one."""
    async with storage.pool.acquire() as con:
        return int(await con.fetchval(
            "SELECT COUNT(*) FROM pg_stat_activity "
            "WHERE datname=current_database() AND pid<>pg_backend_pid() "
            "AND backend_type='client backend'"))


def _as_json_default(x):
    if isinstance(x, _dt.datetime):
        return x.isoformat()
    if isinstance(x, set):
        return sorted(x)
    raise TypeError(repr(x))


def _emit(result: dict, *, ok: bool = True):
    print(json.dumps(result, indent=2, ensure_ascii=False,
                     default=_as_json_default))
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# read-only commands
# ---------------------------------------------------------------------------
async def cmd_status(storage, service):
    service.refresh()
    schema = await storage.schema_version()
    gate = await storage.dynamic_gate_enabled()
    async with storage.pool.acquire() as con:
        rt = await con.fetchrow(
            "SELECT status, pid, dyn_engine, telegram_connected, heartbeat_at "
            "FROM runtime_state WHERE id=1")
        ps = await con.fetchrow(
            "SELECT requested_version, applied_version, reconcile_state, "
            "       reconcile_error, blocked_version "
            "FROM policy_state WHERE id=1")
        n_enabled = int(await con.fetchval(
            "SELECT COUNT(*) FROM target_state WHERE enabled=TRUE"))
    return _emit({
        "schema_version": schema,
        "gate": gate,
        "runtime": dict(rt) if rt else None,
        "policy": dict(ps) if ps else None,
        "targets_enabled": n_enabled,
        "service": service.to_dict(),
    })


async def cmd_policy_dry_run(storage, service):
    return _emit(await storage.policy_dry_run())


async def cmd_gate_status(storage, service):
    service.refresh()
    async with storage.pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT key, enabled, updated_at, updated_by, note "
            "FROM deployment_gates WHERE key='dynamic_targets'")
        rt_dyn = await con.fetchval(
            "SELECT dyn_engine FROM runtime_state WHERE id=1")
    return _emit({
        "gate": dict(row) if row else None,
        "runtime_dyn_engine": rt_dyn,
        "service": service.to_dict(),
    })


async def cmd_mass_gone_status(storage, service):
    ps = await storage.get_policy_state()
    state = storage.mass_gone_state(ps) if ps else "unknown"
    return _emit({
        "state": state,
        "blocked_version": ps["blocked_version"] if ps else None,
        "blocked_count": ps["blocked_count"] if ps else None,
        "blocked_fingerprint": ps["blocked_fingerprint"] if ps else None,
        "requested_version": int(ps["requested_version"]) if ps else None,
        "confirmed_version": ps["confirmed_version"] if ps else None,
    })


async def cmd_preferences_list(storage, service, chat):
    async with storage.pool.acquire() as con:
        if chat is None:
            rows = await con.fetch(
                "SELECT chat_id, selected, excluded, note, updated_by, updated_at "
                "FROM target_preferences ORDER BY chat_id")
        else:
            rows = await con.fetch(
                "SELECT chat_id, selected, excluded, note, updated_by, updated_at "
                "FROM target_preferences WHERE chat_id=$1", int(chat))
    return _emit({"preferences": [dict(r) for r in rows]})


async def cmd_collection_config_show(storage, service):
    cfg = await storage.get_collection_config()
    return _emit({"collection_config": cfg or None})


async def cmd_discovery_status(storage, service):
    async with storage.pool.acquire() as con:
        ctl = await con.fetchrow(
            "SELECT id, requested_seq, consumed_seq, last_requested_at, "
            "       last_requested_by, last_consumed_at, last_run_id, "
            "       next_allowed_at, backoff_level "
            "FROM discovery_control WHERE id=1")
        recent = await con.fetch(
            "SELECT id, status, trigger_kind, dialogs_seen, gone_detected, "
            "       started_at, finished_at "
            "FROM dialog_discovery_runs ORDER BY id DESC LIMIT 5")
    return _emit({
        "discovery_control": dict(ctl) if ctl else None,
        "recent_runs": [dict(r) for r in recent],
    })


# ---------------------------------------------------------------------------
# mutating commands — each requires a stopped service (gate) or an exact
# confirmation (preferences/mass-gone) and a clean preflight.
# ---------------------------------------------------------------------------
async def _preflight_service_stopped(storage, service) -> dict | None:
    service.refresh()
    if not service.stopped:
        return {"ok": False, "reason": "service_not_stopped",
                "service": service.to_dict()}
    n = await _db_connections_count(storage)
    if n != 0:
        return {"ok": False, "reason": "db_connections_present",
                "other_connections": n,
                "service": service.to_dict()}
    return None


async def _gate_mutate(storage, service, *, enable: bool, reason: str,
                       action: str):
    err = await _preflight_service_stopped(storage, service)
    if err is not None:
        return _emit(err, ok=False)
    res = await storage.admin_gate_update(
        enable=enable,
        from_enabled=(not enable),
        actor=ACTOR,
        action=action,
        note=reason,
        detail={"reason": reason[:120] if reason else "none"},
        expected_dyn_engine=(not enable),
    )
    hint = ("run 'systemctl --user start tgparser.service' to apply "
            "(service was and remains stopped)")
    out = dict(res)
    out["service"] = service.to_dict()
    out["next_action"] = hint
    return _emit(out, ok=bool(res.get("ok")))


async def cmd_gate_on(storage, service, reason):
    return await _gate_mutate(storage, service, enable=True,
                              reason=reason, action="gate.on")


async def cmd_gate_off(storage, service, reason):
    return await _gate_mutate(storage, service, enable=False,
                              reason=reason, action="gate.off")


async def cmd_mass_gone_confirm(storage, service, version, fingerprint):
    res = await storage.admin_confirm_mass_gone(
        policy_version=int(version),
        fingerprint=fingerprint,
        actor=ACTOR,
        action="mass_gone.confirm",
        detail={"version": int(version)},
    )
    return _emit(res, ok=bool(res.get("ok")))


def _dry_run_shows_disable(dry: dict, chat_id: int) -> bool:
    for item in dry.get("would_disable", []):
        if int(item.get("chat_id", 0)) == int(chat_id):
            return True
    return False


async def _pref_mutate(storage, service, *, chat_id: int,
                       selected, excluded, note,
                       action: str,
                       requires_disable_confirmation: bool,
                       confirm_chat_id: int | None,
                       expected_version: int | None):
    detail = {"chat_id": int(chat_id), "field": action.split(".", 1)[-1]}
    if requires_disable_confirmation:
        if confirm_chat_id is None or int(confirm_chat_id) != int(chat_id):
            return _emit({"ok": False, "reason": "confirm_chat_id_mismatch",
                          "chat_id": int(chat_id),
                          "confirm_chat_id": confirm_chat_id}, ok=False)
        if expected_version is None:
            return _emit({"ok": False,
                          "reason": "expected_policy_version_required"},
                         ok=False)
    res = await storage.admin_preference_update(
        chat_id=int(chat_id),
        selected=selected, excluded=excluded, note=note,
        actor=ACTOR, action=action, detail=detail,
        expected_requested_version=expected_version,
    )
    res["note"] = ("preference recorded; reconciler will observe at "
                   "requested_version="
                   f"{res.get('requested_version_after')}. "
                   "Target disable/enable is only final once "
                   "target_state.enabled changes — verify with "
                   "'admin status' later.")
    return _emit(res, ok=bool(res.get("ok")))


async def cmd_pref_select(storage, service, chat, expected_version, note):
    return await _pref_mutate(storage, service,
                              chat_id=chat, selected=True, excluded=None,
                              note=note, action="prefs.select",
                              requires_disable_confirmation=False,
                              confirm_chat_id=None,
                              expected_version=expected_version)


async def cmd_pref_unselect(storage, service, chat, confirm_chat_id,
                            expected_version, note):
    return await _pref_mutate(storage, service,
                              chat_id=chat, selected=False, excluded=None,
                              note=note, action="prefs.unselect",
                              requires_disable_confirmation=True,
                              confirm_chat_id=confirm_chat_id,
                              expected_version=expected_version)


async def cmd_pref_clear(storage, service, chat, confirm_chat_id,
                         expected_version, note):
    return await _pref_mutate(storage, service,
                              chat_id=chat, selected=False, excluded=False,
                              note=note, action="prefs.clear",
                              requires_disable_confirmation=True,
                              confirm_chat_id=confirm_chat_id,
                              expected_version=expected_version)


async def cmd_pref_exclude(storage, service, chat, confirm_chat_id,
                           expected_version, note):
    return await _pref_mutate(storage, service,
                              chat_id=chat, selected=False, excluded=True,
                              note=note, action="prefs.exclude",
                              requires_disable_confirmation=True,
                              confirm_chat_id=confirm_chat_id,
                              expected_version=expected_version)


async def cmd_discovery_request(storage, service, reason):
    res = await storage.admin_request_discovery(
        actor=ACTOR, action="discovery.request",
        detail={"reason": (reason or "manual")[:120]},
    )
    return _emit(res, ok=bool(res.get("ok")))


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------
async def _legacy_policy_dry_run(args) -> int:
    """Back-compat entry used by `policy-dry-run [--json]` (top-level).
    Returns exit code 0 on v11, 2 when migration 0011 is not applied yet."""
    cfg = load_cfg()
    storage = Storage(cfg["postgres"]["dsn"])
    await storage.connect()
    try:
        level = await storage.schema_version()
        if level < 11:
            msg = ("migration 0011 is not applied yet (max schema_migrations="
                   f"{level}). Dry-run requires v11. Nothing was changed.")
            if getattr(args, "json", False):
                print(json.dumps({"ok": False, "error": msg},
                                 ensure_ascii=False))
            else:
                print(msg, file=sys.stderr)
            return 2
        result = _scrub(await storage.policy_dry_run())
    finally:
        await storage.close()
    if getattr(args, "json", False):
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    # human-readable summary (legacy stage-1 layout)
    print(f"schema_version: {result['schema_version']}  "
          f"gate_enabled: {result['gate_enabled']}  "
          f"engine_active: {result['engine_active']}  "
          f"requires_restart: {result['requires_restart']}")
    print(f"policy version: requested={result['requested_version']} "
          f"applied={result['applied_version']}  "
          f"block: {result['block']['state']}  "
          f"awaiting_discovery: {result['awaiting_discovery']}")
    if result["disable_suppressed_by"]:
        print("disables suppressed by: " + ", ".join(result["disable_suppressed_by"]))
    print(f"mode: {result.get('mode')}  "
          f"include_archived: {result.get('include_archived')}")
    print(f"would_enable:  {len(result.get('would_enable', []))}")
    print(f"would_disable: {len(result.get('would_disable', []))}")
    print(f"would_keep:    {result.get('would_keep_count', 0)}")
    for key in ("enabled_without_discovery", "enabled_without_preference"):
        if result.get(key):
            print(f"{key} (seed gap): " + ", ".join(str(c) for c in result[key]))
    print("reason counts:")
    for k, v in sorted(result.get("reason_counts", {}).items(),
                       key=lambda kv: -kv[1]):
        print(f"  {k:20s} {v}")
    for section in ("would_enable", "would_disable"):
        rows = result.get(section) or []
        if not rows:
            continue
        print(f"\n{section}:")
        for r in rows:
            print(f"  {r['chat_id']:>16}  {(r['category'] or '-'):<10} "
                  f"{(r['title'] or '')[:40]:<40}  reason={r['reason']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Public parser factory. Exposes both the v1 subcommand groups and the
    legacy top-level ``policy-dry-run`` used by stage-1 tests."""
    return _build_parser()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tgparser-admin",
        description="TGParser dynamic-targets Admin CLI (v1).")
    g = p.add_subparsers(dest="group", required=True)
    g.add_parser("status")

    # legacy top-level: policy-dry-run [--json]
    legacy = g.add_parser("policy-dry-run",
                          help="read-only policy preview (legacy flat form)")
    legacy.add_argument("--json", action="store_true")
    legacy.set_defaults(func=_legacy_policy_dry_run)

    pol = g.add_parser("policy").add_subparsers(dest="cmd", required=True)
    pol.add_parser("dry-run")

    gate = g.add_parser("gate").add_subparsers(dest="cmd", required=True)
    gate.add_parser("status")
    gon = gate.add_parser("on"); gon.add_argument("--reason", required=True)
    gof = gate.add_parser("off"); gof.add_argument("--reason", required=True)

    mg = g.add_parser("mass-gone").add_subparsers(dest="cmd", required=True)
    mg.add_parser("status")
    mgc = mg.add_parser("confirm")
    mgc.add_argument("--version", type=int, required=True)
    mgc.add_argument("--fingerprint", required=True)

    pref = g.add_parser("preferences").add_subparsers(dest="cmd", required=True)
    pl = pref.add_parser("list"); pl.add_argument("--chat", type=int)
    psel = pref.add_parser("select")
    psel.add_argument("--chat", type=int, required=True)
    psel.add_argument("--expected-policy-version", type=int, required=True)
    psel.add_argument("--note")
    for name in ("unselect", "clear", "exclude"):
        sp = pref.add_parser(name)
        sp.add_argument("--chat", type=int, required=True)
        sp.add_argument("--confirm-chat-id", type=int, required=True)
        sp.add_argument("--expected-policy-version", type=int, required=True)
        sp.add_argument("--note")

    cc = g.add_parser("collection-config").add_subparsers(dest="cmd",
                                                          required=True)
    cc.add_parser("show")

    disc = g.add_parser("discovery").add_subparsers(dest="cmd", required=True)
    disc.add_parser("status")
    dr = disc.add_parser("request"); dr.add_argument("--reason")
    return p


async def _dispatch(args, storage: Storage, service: ServiceState) -> int:
    grp = args.group
    cmd = getattr(args, "cmd", None)
    if grp == "status":
        return await cmd_status(storage, service)
    if grp == "policy" and cmd == "dry-run":
        return await cmd_policy_dry_run(storage, service)
    if grp == "gate" and cmd == "status":
        return await cmd_gate_status(storage, service)
    if grp == "gate" and cmd == "on":
        return await cmd_gate_on(storage, service, args.reason)
    if grp == "gate" and cmd == "off":
        return await cmd_gate_off(storage, service, args.reason)
    if grp == "mass-gone" and cmd == "status":
        return await cmd_mass_gone_status(storage, service)
    if grp == "mass-gone" and cmd == "confirm":
        return await cmd_mass_gone_confirm(storage, service,
                                           args.version, args.fingerprint)
    if grp == "preferences":
        if cmd == "list":
            return await cmd_preferences_list(storage, service, args.chat)
        if cmd == "select":
            return await cmd_pref_select(storage, service, args.chat,
                                         args.expected_policy_version,
                                         args.note)
        if cmd == "unselect":
            return await cmd_pref_unselect(storage, service, args.chat,
                                           args.confirm_chat_id,
                                           args.expected_policy_version,
                                           args.note)
        if cmd == "clear":
            return await cmd_pref_clear(storage, service, args.chat,
                                        args.confirm_chat_id,
                                        args.expected_policy_version,
                                        args.note)
        if cmd == "exclude":
            return await cmd_pref_exclude(storage, service, args.chat,
                                          args.confirm_chat_id,
                                          args.expected_policy_version,
                                          args.note)
    if grp == "collection-config" and cmd == "show":
        return await cmd_collection_config_show(storage, service)
    if grp == "discovery":
        if cmd == "status":
            return await cmd_discovery_status(storage, service)
        if cmd == "request":
            return await cmd_discovery_request(storage, service, args.reason)
    raise SystemExit(f"unknown command: {grp} {cmd}")


async def _main_async(argv, storage: Storage | None = None,
                      service: ServiceState | None = None) -> int:
    args = _build_parser().parse_args(argv)
    own_storage = storage is None
    if own_storage:
        storage = Storage(load_cfg()["postgres"]["dsn"])
        await storage.connect()
    service = service or ServiceState()
    try:
        return await _dispatch(args, storage, service)
    finally:
        if own_storage:
            await storage.close()


def main(argv=None) -> int:
    return asyncio.run(_main_async(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    sys.exit(main())
