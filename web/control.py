"""Local, audited collector controls used by the dashboard.

This module is intentionally separate from ``web.queries``: the latter stays
strictly read-only.  Every preference change goes through Storage's atomic
admin wrapper and service transitions use systemd's user manager.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

from tgparser.storage import Storage


SERVICE = "tgparser.service"


# --- raw-target normalization for `Додати чат` ---------------------------

# Public invite links (https://t.me/+HASH) keep the hash verbatim; the
# resolver in dynamic.resolve_yaml handles both username and invite forms.
_RE_USERNAME = re.compile(r"^@?([A-Za-z][A-Za-z0-9_]{4,31})$")
_RE_TME_LINK = re.compile(
    r"^https?://t\.me/(?:joinchat/|\+)?(?P<body>[A-Za-z0-9_-]{5,80})/?$")
_RE_NUMERIC_CHAT = re.compile(r"^-?\d{3,20}$")


def _normalize_raw_target(raw: str) -> str:
    """Accept @username, https://t.me/username, https://t.me/+HASH or a
    numeric chat_id; return a canonical raw_target that the main process's
    yaml-resolver will interpret through the single main-process client.

    Invalid input raises ValueError and the UI surfaces the message.
    The actual access check happens in the main process, not here — this
    stays read-only against PostgreSQL.
    """
    if raw is None:
        raise ValueError("порожнє значення")
    s = raw.strip()
    if not s:
        raise ValueError("порожнє значення")
    if len(s) > 300:
        raise ValueError("рядок задовгий")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in s):
        raise ValueError("недозволені символи")
    m = _RE_USERNAME.match(s)
    if m:
        return "@" + m.group(1)
    m = _RE_TME_LINK.match(s)
    if m:
        body = m.group("body")
        # /+HASH or /joinchat/HASH keep the hash form; /username becomes @u.
        if s.startswith("https://t.me/+") or s.startswith("http://t.me/+") \
                or "/joinchat/" in s:
            return "https://t.me/+" + body
        if _RE_USERNAME.match(body):
            return "@" + body
        raise ValueError("незрозумілий формат посилання")
    if _RE_NUMERIC_CHAT.match(s):
        # Numeric IDs go in as-is; the resolver will call get_input_entity.
        return s
    raise ValueError("не впізнано як @username, посилання або chat_id")


class ControlError(RuntimeError):
    pass


@dataclass(frozen=True)
class ServiceState:
    active: str
    sub: str
    pid: int

    @property
    def running(self) -> bool:
        return self.active == "active" and self.sub == "running" and self.pid > 0


async def _systemctl(*args: str, timeout: float = 25) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--user", *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise ControlError("systemd не відповів вчасно")
    return proc.returncode, out.decode("utf-8", "replace").strip()


async def service_state() -> ServiceState:
    rc, out = await _systemctl(
        "show", SERVICE, "--property=ActiveState,SubState,MainPID", timeout=8)
    if rc:
        raise ControlError("не вдалося прочитати стан служби")
    fields = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    return ServiceState(fields.get("ActiveState", "unknown"),
                        fields.get("SubState", "unknown"),
                        int(fields.get("MainPID") or 0))


async def _wait(running: bool, timeout: float = 30) -> ServiceState:
    end = asyncio.get_running_loop().time() + timeout
    last = await service_state()
    while last.running != running and asyncio.get_running_loop().time() < end:
        await asyncio.sleep(.4)
        last = await service_state()
    if last.running != running:
        raise ControlError("служба не перейшла в очікуваний стан")
    return last


async def stop_service(storage: Storage) -> ServiceState:
    before = await service_state()
    if before.running:
        rc, _ = await _systemctl("stop", SERVICE)
        if rc:
            await storage.ui_audit("ui", "service.stop", "error",
                                   {"service_action": "stop", "status_code": rc})
            raise ControlError("systemd не зміг зупинити збір")
    state = await _wait(False)
    await storage.ui_audit("ui", "service.stop", "ok",
                           {"service_action": "stop", "count": 0})
    return state


async def _available_ids(storage: Storage) -> set[int]:
    """Same id set the dashboard picker shows: discovered dialogs, known
    preferences and targets, plus legacy chats — minus deleted (excluded)."""
    async with storage.pool.acquire() as con:
        rows = await con.fetch("""
            WITH ids AS (
                SELECT chat_id AS id FROM discovered_dialogs
                UNION SELECT chat_id FROM target_preferences
                UNION SELECT chat_id FROM target_state
                UNION SELECT id      FROM chats
            )
            SELECT i.id FROM ids i
              LEFT JOIN target_preferences p ON p.chat_id = i.id
             WHERE NOT COALESCE(p.excluded, FALSE)
        """)
    return {int(r["id"]) for r in rows}


async def _apply_selection(storage: Storage, desired: set[int]) -> None:
    available = await _available_ids(storage)
    if not desired or not desired <= available:
        raise ControlError("обрано невідомий чат або список порожній")
    async with storage.pool.acquire() as con:
        gate = await con.fetchval(
            "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
        version = int(await con.fetchval(
            "SELECT requested_version FROM policy_state WHERE id=1"))
        prefs = await con.fetch(
            "SELECT chat_id,selected,excluded FROM target_preferences")
    if not gate:
        raise ControlError("dynamic-targets gate вимкнено")
    current = {int(r["chat_id"]): (bool(r["selected"]), bool(r["excluded"]))
               for r in prefs}
    for chat_id in sorted(available):
        selected = chat_id in desired
        old_selected, old_excluded = current.get(chat_id, (False, False))
        if old_selected == selected and (not selected or not old_excluded):
            continue
        result = await storage.admin_preference_update(
            chat_id=chat_id, selected=selected,
            excluded=False if selected else None,
            note="ui collector control", actor="ui",
            action="prefs.select" if selected else "prefs.unselect",
            detail={"chat_id": chat_id,
                    "field": "selected", "new": selected},
            expected_requested_version=version,
        )
        if not result.get("ok"):
            raise ControlError("політика змінилася паралельно; повторіть запуск")
        version = int(result["requested_version_after"])


async def add_chat(storage: Storage, raw_target: str) -> dict:
    """UI entry point for `Додати чат`. Only normalizes + enqueues through
    the existing yaml_target_imports mechanism; resolution happens inside
    the main TGParser process when it is running. Idempotent — re-adding
    an existing entry returns its current status without overwrite."""
    try:
        normalized = _normalize_raw_target(raw_target)
    except ValueError as exc:
        # Record the rejection in the audit log so the operator sees it.
        await storage.ui_audit(
            "ui", "chat.add", "rejected",
            {"reason": "invalid_input", "status_code": 422})
        raise ControlError(str(exc))
    res = await storage.admin_add_yaml_import(
        raw_target=normalized, actor="ui", action="chat.add",
        detail={"field": "raw_target"})
    return {
        "ok": True,
        "inserted": bool(res.get("inserted")),
        "status": res.get("status"),
        "resolved_chat_id": res.get("resolved_chat_id"),
    }


async def sync_dialogs(storage: Storage, *,
                       timeout: float = 120.0) -> dict:
    """UI entry point for `Синхронізувати чати`.

    If ``tgparser.service`` is already running, bump the discovery state
    machine and let its own loop do the sweep.

    If the service is stopped, spawn the one-shot ``python -m
    tgparser.sync_once`` process. That process opens THE ONLY Telegram
    session (held under the same ``tgparser.lock`` file), runs discovery
    and the yaml resolver once, then exits. The UI never opens a second
    client.
    """
    service = await service_state()
    if service.running:
        res = await storage.admin_request_discovery(
            actor="ui", action="discovery.request",
            detail={"reason": "ui_sync", "status_code": 202})
        return {
            "mode": "queued",
            "ok": bool(res.get("ok")),
            "reason": res.get("reason"),
            "requested_seq": res.get("requested_seq"),
            "next_allowed_at": res.get("next_allowed_at"),
            "service_running": True,
        }

    # Service stopped — run the one-shot. No gate change, no systemd.
    import sys as _sys
    cmd = [_sys.executable, "-m", "tgparser.sync_once",
           "--timeout", str(int(timeout))]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(),
                                          timeout=timeout + 20)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        await storage.ui_audit(
            "ui", "discovery.sync_once", "error",
            {"reason": "timeout", "status_code": 504})
        raise ControlError("синхронізація не завершилася вчасно")

    # Parse the JSON summary sync_once prints on its final line. Everything
    # before that is python logging noise we deliberately discard — we do
    # not surface worker internals to the operator.
    summary: dict = {}
    tail = (out or b"").decode("utf-8", "replace").strip().splitlines()
    for line in reversed(tail):
        line = line.strip()
        if line.startswith("{"):
            try:
                summary = json.loads(line)
                break
            except Exception:
                continue
    code = int(summary.get("exit_code", proc.returncode or 1))
    audit_outcome = "ok" if code == 0 and summary.get("ok") else "error"
    # Never place the raw sync_once message into audit detail: the whitelist
    # would reject anything matching session/token/phone-like substrings.
    # A single short classification tag is enough for the audit trail.
    audit_reason = "done" if audit_outcome == "ok" else "sync_once_failed"
    await storage.ui_audit(
        "ui", "discovery.sync_once", audit_outcome,
        {"status_code": code,
         "count": int(summary.get("resolver", {}).get("selected", 0)),
         "reason": audit_reason})
    if code != 0 or not summary.get("ok"):
        raise ControlError(_sync_error_message(code, summary))
    disc = summary.get("discovery") or {}
    resolver = summary.get("resolver") or {}
    return {
        "mode": "ran",
        "ok": True,
        "service_running": False,
        "discovery": {
            "ran": bool(disc.get("ran")),
            "run_id": disc.get("run_id"),
            "seen": int(disc.get("seen") or 0),
            "new": int(disc.get("new") or 0),
            "gone": int(disc.get("gone") or 0),
        },
        "resolver": {
            "attempted": int(resolver.get("attempted", 0)),
            "selected": int(resolver.get("selected", 0)),
            "retry": int(resolver.get("retry", 0)),
            "terminal": int(resolver.get("terminal", 0)),
            "errors": int(resolver.get("errors", 0)),
        },
        "duration_s": float(summary.get("duration_s") or 0),
    }


def _sync_error_message(code: int, summary: dict) -> str:
    """Operator-facing Ukrainian text for a failed sync_once run. Never
    echoes raw worker internals."""
    if code == 3:
        wait_s = int((summary.get("discovery") or {}).get("wait_s") or 0)
        return (f"Список щойно оновлювався; Telegram дозволяє наступну "
                f"синхронізацію через {max(wait_s, 1)} с.")
    if code == 2:
        return "Інший процес TGParser тримає сесію Telegram — спробуйте пізніше."
    msg = (summary.get("message") or "").lower()
    if "not authorized" in msg:
        return "Сесія Telegram не авторизована — потрібен повторний вхід."
    if "floodwait" in msg or "discovery failed" in msg:
        return "Telegram тимчасово обмежив запити; спробуйте за хвилину."
    if "timed out" in msg or "timeout" in msg:
        return "Telegram не відповів вчасно; спробуйте ще раз."
    return "Синхронізацію не завершено; подробиці — у журналі UI."


async def sync_status(storage: Storage) -> dict:
    """Read-only progress of a queued discovery request (running service)."""
    async with storage.pool.acquire() as con:
        ctl = await con.fetchrow(
            "SELECT requested_seq, consumed_seq FROM discovery_control WHERE id=1")
        last = await con.fetchrow(
            "SELECT id, status FROM dialog_discovery_runs ORDER BY id DESC LIMIT 1")
    pending = bool(ctl and ctl["requested_seq"] > ctl["consumed_seq"])
    running = bool(last and last["status"] == "running")
    return {"done": not pending and not running,
            "last_status": last["status"] if last else None}


async def delete_chat(storage: Storage, chat_id: int) -> dict:
    """Remove a chat from the platform. Refuses an active target and any
    chat that already has collected messages; otherwise deletes its
    discovery/sync/target rows and leaves an excluded preference as a
    blocklist so the next Telegram sync does not bring it back."""
    cid = int(chat_id)
    async with storage.pool.acquire() as con:
        enabled = await con.fetchval(
            "SELECT enabled FROM target_state WHERE chat_id=$1", cid)
    if enabled:
        await storage.ui_audit("ui", "chat.delete", "rejected",
                               {"chat_id": cid, "reason": "active_target",
                                "status_code": 409})
        raise ControlError("це активна ціль збору — спершу зніміть її з вибору "
                           "і перезапустіть збір")
    res = await storage.admin_delete_chat(
        chat_id=cid, actor="ui", action="chat.delete",
        detail={"chat_id": cid})
    if not res.get("ok"):
        if res.get("reason") == "has_messages":
            raise ControlError(f"у чаті вже збережено {res['messages']} "
                               "повідомлень — видалення заблоковано")
        raise ControlError("не вдалося видалити чат")
    return res


async def configure_and_start(storage: Storage, chat_ids: list[int],
                              use_all: bool) -> ServiceState:
    before = await service_state()
    if before.running:
        await stop_service(storage)
    available = await _available_ids(storage)
    desired = available if use_all else {int(x) for x in chat_ids}
    await _apply_selection(storage, desired)
    rc, _ = await _systemctl("start", SERVICE)
    if rc:
        await storage.ui_audit("ui", "service.start", "error",
                               {"service_action": "start", "status_code": rc,
                                "count": len(desired)})
        raise ControlError("systemd не зміг запустити збір")
    state = await _wait(True)
    await storage.ui_audit("ui", "service.start", "ok",
                           {"service_action": "start", "count": len(desired),
                            "mode": "all" if use_all else "selected"})
    return state
