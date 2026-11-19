"""Diagnostic snapshot of a stuck TGParser without restarting it.

Reads:
  - runtime_state, sync_state, target_state (DB)
  - PostgreSQL pg_stat_activity (sessions by our DSN only)
  - runtime media_pending age distribution
  - user_profile_queue sizes
  - the OS process (via ctypes/psutil-free Windows APIs) — PID is alive? handle count?
  - the newest 50 lines of logs/tgparser.log (filtered)

Writes JSON to stdout (or --out <path>) with no secrets, texts, or usernames.
"""
import argparse
import asyncio
import ctypes
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from dotenv import load_dotenv


HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")


def _pid_alive(pid: int) -> dict:
    if sys.platform != "win32" or not pid:
        return {"alive": None}
    STILL_ACTIVE = 259
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    h = ctypes.windll.kernel32.OpenProcess(
        PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
    if not h:
        err = ctypes.windll.kernel32.GetLastError()
        if err == 5:  # ACCESS_DENIED => process exists, but we can't inspect
            return {"alive": True, "note": "ACCESS_DENIED (elevated process)"}
        return {"alive": False, "error": f"OpenProcess err={err}"}
    try:
        code = ctypes.c_ulong(0)
        ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
        return {"alive": (code.value == STILL_ACTIVE), "exit_code": code.value}
    finally:
        ctypes.windll.kernel32.CloseHandle(h)


async def snapshot() -> dict:
    con = await asyncpg.connect(os.environ["PG_DSN"], ssl=False)
    try:
        rt = dict(await con.fetchrow(
            "SELECT pid, status, telegram_connected, "
            "started_at, heartbeat_at, telegram_last_ok_at, "
            "update_last_received_at, note "
            "FROM runtime_state WHERE id=1"))

        sync = [dict(r) for r in await con.fetch(
            "SELECT chat_id, last_backfilled_message_id, backfill_status, "
            "batches_completed, last_batch_at, "
            "EXTRACT(EPOCH FROM (NOW() - last_batch_at))::int AS last_batch_age_sec "
            "FROM sync_state ORDER BY chat_id")]

        targets = [dict(r) for r in await con.fetch(
            "SELECT chat_id, backfill_status, pending_count, dead_count, "
            "enabled, disabled_reason, last_success_at, last_error_at, last_error "
            "FROM target_state ORDER BY chat_id")]

        # Media age/throughput
        media_stats = dict(await con.fetchrow("""
            SELECT COUNT(*) FILTER (WHERE status='done') AS done,
                   COUNT(*) FILTER (WHERE status='pending') AS pending,
                   COUNT(*) FILTER (WHERE status='failed') AS failed,
                   COUNT(*) FILTER (WHERE status='metadata_only') AS metadata_only,
                   COUNT(*) FILTER (WHERE status='unavailable') AS unavailable,
                   EXTRACT(EPOCH FROM (NOW() - MIN(first_seen) FILTER (WHERE status IN ('pending','failed'))))::int AS oldest_pending_age_sec,
                   EXTRACT(EPOCH FROM (NOW() - MAX(completed_at) FILTER (WHERE status='done')))::int AS last_done_age_sec
            FROM media
        """))

        mpm_1 = await con.fetchval("SELECT COUNT(*)::float FROM messages WHERE collected_at > NOW() - interval '1 minute'")
        mpm_5 = await con.fetchval("SELECT COUNT(*)::float/5.0 FROM messages WHERE collected_at > NOW() - interval '5 minutes'")
        mpm_15 = await con.fetchval("SELECT COUNT(*)::float/15.0 FROM messages WHERE collected_at > NOW() - interval '15 minutes'")
        last_msg_age = await con.fetchval("SELECT EXTRACT(EPOCH FROM (NOW() - MAX(collected_at)))::int FROM messages")

        pg_act = [dict(r) for r in await con.fetch("""
            SELECT pid, usename, state, wait_event_type, wait_event,
                   query_start,
                   EXTRACT(EPOCH FROM (NOW() - query_start))::int AS query_age_sec,
                   LEFT(query, 160) AS query_preview
            FROM pg_stat_activity
            WHERE usename=current_user AND datname=current_database()
              AND pid <> pg_backend_pid()
            ORDER BY query_start NULLS LAST
        """)]

        upq = {r["status"]: r["count"] for r in await con.fetch(
            "SELECT status, count(*) FROM user_profile_queue GROUP BY status")}

        errors_recent = [dict(r) for r in await con.fetch(
            "SELECT at, kind, COALESCE(error_group,'') AS error_group, "
            "LEFT(COALESCE(message,''), 180) AS msg "
            "FROM errors WHERE resolved_at IS NULL "
            "ORDER BY at DESC LIMIT 20")]

        # Media failed causes
        media_failed_causes = [dict(r) for r in await con.fetch(
            "SELECT LEFT(COALESCE(last_error,''), 120) AS cause, count(*) "
            "FROM media WHERE status='failed' GROUP BY 1 ORDER BY count DESC LIMIT 10")]

    finally:
        await con.close()

    proc = _pid_alive(rt.get("pid"))

    # Tail of log (safe: no secrets in logs)
    log_tail = []
    log_path = HERE / "logs" / "tgparser.log"
    if log_path.exists():
        try:
            txt = log_path.read_text(encoding="utf-8", errors="replace")
            lines = txt.splitlines()[-100:]
            # keep only lines about coordinator / backfill / live / TG reconnect
            kept = [l for l in lines if re.search(
                r"coordinator|backfill|TGParser starting|signed in|shut|Server closed|get_me|FloodWait|FileReference",
                l, re.I)]
            log_tail = kept[-40:]
        except Exception as e:
            log_tail = [f"<log read error: {e}>"]

    now = datetime.now(timezone.utc)
    return {
        "generated_at": now.isoformat(),
        "runtime": {
            "pid": rt["pid"],
            "status": rt["status"],
            "note": rt["note"],
            "uptime_sec": (now - rt["started_at"]).total_seconds() if rt["started_at"] else None,
            "db_heartbeat_age_sec": (now - rt["heartbeat_at"]).total_seconds() if rt["heartbeat_at"] else None,
            "telegram_connected": rt["telegram_connected"],
            "telegram_last_ok_age_sec": (now - rt["telegram_last_ok_at"]).total_seconds() if rt["telegram_last_ok_at"] else None,
            "update_last_received_age_sec": (now - rt["update_last_received_at"]).total_seconds() if rt["update_last_received_at"] else None,
            "process": proc,
        },
        "sync_state": sync,
        "target_state": targets,
        "media": media_stats,
        "messages_per_minute": {"m1": mpm_1, "m5": mpm_5, "m15": mpm_15, "last_message_age_sec": last_msg_age},
        "pg_activity_this_app": pg_act,
        "user_profile_queue": upq,
        "errors_recent_unresolved": errors_recent,
        "media_failed_causes": media_failed_causes,
        "log_tail": log_tail,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=str, default=None)
    ap.add_argument("--json", action="store_true", default=True)
    args = ap.parse_args()
    snap = asyncio.run(snapshot())
    text = json.dumps(snap, default=str, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
