"""CLI: python health.py [--json]

Strict health check with exit codes 0=healthy, 1=degraded, 2=unhealthy.
`--json` emits a machine-readable JSON document instead of text.
Does NOT create a second TelegramClient — it reads Telegram heartbeat
fields that the running collector persists to `runtime_state`.
"""
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from tgparser.config import load as load_config
from tgparser.storage import Storage


HEARTBEAT_STALE_SEC = 120
TG_STALE_WARN_SEC = 180       # degraded if TG heartbeat older than this
TG_STALE_FAIL_SEC = 900       # unhealthy past this


def _pid_alive(pid) -> bool:
    """Return True if the process with this PID is running.

    On Windows a non-elevated caller cannot `OpenProcess` an elevated target
    even with PROCESS_QUERY_LIMITED_INFORMATION because UAC strips the token
    — the call returns NULL with GetLastError()==5 (ERROR_ACCESS_DENIED),
    which must NOT be interpreted as "process is gone". Treat ACCESS_DENIED
    as "exists but inaccessible => alive".
    """
    if pid is None:
        return False
    try:
        if sys.platform == "win32":
            import ctypes
            STILL_ACTIVE = 259
            h = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
            if not h:
                err = ctypes.windll.kernel32.GetLastError()
                if err == 5:            # ERROR_ACCESS_DENIED -> exists
                    return True
                return False
            try:
                code = ctypes.c_ulong(0)
                ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
                return code.value == STILL_ACTIVE
            finally:
                ctypes.windll.kernel32.CloseHandle(h)
        else:
            os.kill(int(pid), 0)
            return True
    except Exception:
        return False


def _age(now, ts):
    if ts is None:
        return None
    return (now - ts).total_seconds()


async def build_snapshot() -> dict:
    cfg = load_config()
    storage = Storage(cfg["postgres"]["dsn"])
    pg_ok = False
    try:
        await storage.connect()
        pg_ok = await storage.ping()
    except Exception as e:
        return {"status": "unhealthy", "postgres": False,
                "error": f"postgres connect failed: {e}"[:400]}
    if not pg_ok:
        return {"status": "unhealthy", "postgres": False}

    rt = await storage.get_runtime()
    now = datetime.now(timezone.utc)

    pid = rt["pid"] if rt else None
    alive = _pid_alive(pid)
    uptime = _age(now, rt["started_at"]) if rt else None
    hb_age = _age(now, rt["heartbeat_at"]) if rt else None
    tg_ok_age = _age(now, rt["telegram_last_ok_at"]) if rt else None
    upd_age = _age(now, rt["update_last_received_at"]) if rt else None
    last_msg = await storage.last_message_at()
    last_msg_age = _age(now, last_msg)

    targets_rows = await storage.targets_all()
    targets = []
    disabled_targets = []
    for r in targets_rows:
        prob = await storage.backfill_queue_problem_ids(r["chat_id"], limit=20)
        info = {
            "chat_id": r["chat_id"], "target": r["target"],
            "backfill_status": r["backfill_status"],
            "pending_count": r["pending_count"],
            "dead_count": r["dead_count"],
            "last_success_at": r["last_success_at"].isoformat() if r["last_success_at"] else None,
            "last_error_at": r["last_error_at"].isoformat() if r["last_error_at"] else None,
            "last_error": r["last_error"],
            "enabled": bool(r.get("enabled", True)),
            "disabled_reason": r.get("disabled_reason"),
            "problem_message_ids": prob,
        }
        if info["enabled"]:
            targets.append(info)
        else:
            disabled_targets.append(info)

    async with storage.pool.acquire() as con:
        totals = dict(await con.fetchrow("""
            SELECT (SELECT COUNT(*) FROM messages) msgs,
                   (SELECT COUNT(*) FROM messages WHERE is_deleted) deletes,
                   (SELECT COUNT(*) FROM message_versions) versions,
                   (SELECT COUNT(*) FROM message_reactions) reactions,
                   (SELECT COUNT(*) FROM message_reaction_authors) react_authors,
                   (SELECT COUNT(*) FROM users) users,
                   (SELECT COUNT(*) FROM user_profile_history) profile_hist,
                   (SELECT COUNT(*) FROM media) media_rows,
                   (SELECT COUNT(*) FROM media WHERE status='done') media_done,
                   (SELECT COUNT(*) FROM media WHERE status='failed') media_failed,
                   (SELECT COUNT(*) FROM media WHERE status IN ('pending','failed')) media_pending,
                   (SELECT COUNT(*) FROM media WHERE status='metadata_only') media_meta,
                   (SELECT COUNT(*) FROM message_media) message_media_links,
                   (SELECT COUNT(*) FROM backfill_queue WHERE status='pending') dlq_pending,
                   (SELECT COUNT(*) FROM backfill_queue WHERE status='dead' AND NOT acknowledged) dlq_dead,
                   (SELECT COUNT(*) FROM deletion_events WHERE resolved_at IS NULL) unresolved_deletions,
                   (SELECT COUNT(*) FROM avatar_history) avatar_hist,
                   (SELECT COUNT(*) FROM memberships) memberships,
                   (SELECT COUNT(*) FROM errors) errors,
                   (SELECT COUNT(*) FROM errors WHERE at > NOW() - interval '1 hour') errors_last_hour,
                   (SELECT MAX(at) FROM errors) last_error_at
        """))
        # v7.1: run the count + distinct + duplicate_count in ONE snapshot
        # inside a REPEATABLE READ transaction so concurrent live inserts can
        # never produce a ghost "dupe". Also override totals["msgs"] with the
        # count from this snapshot so the derived numbers agree.
        async with con.transaction(isolation="repeatable_read"):
            dup_row = await con.fetchrow("""
                SELECT COUNT(*) AS c,
                       COUNT(DISTINCT (chat_id, message_id)) AS distinct_c,
                       COUNT(*) - COUNT(DISTINCT (chat_id, message_id)) AS dups
                FROM messages
            """)
        totals["msgs"] = dup_row["c"]
        totals["duplicate_count"] = dup_row["dups"]
        distinct = dup_row["distinct_c"]
        totals["errors_unresolved"] = await con.fetchval(
            "SELECT COUNT(*) FROM errors WHERE resolved_at IS NULL")
        totals["errors_unresolved_1h"] = await con.fetchval(
            "SELECT COUNT(*) FROM errors WHERE resolved_at IS NULL AND at > NOW() - interval '1 hour'")
    totals["last_error_at"] = (
        totals["last_error_at"].isoformat() if totals.get("last_error_at") else None)
    totals["distinct_match"] = (totals["msgs"] == distinct)

    # v5: throughput / backlog measurements
    mpm = await storage.messages_per_minute(window_minutes=5)
    oldest_pending = await storage.media_oldest_pending_age()
    upq_stats = await storage.user_profile_queue_stats()
    errors_by_group = await storage.errors_unresolved_by_group()

    # v6: coordinator progress + media throughput + worker state
    coord_rows = await storage.coordinator_progress_all()
    media_th = await storage.media_throughput()
    media_status = await storage.media_status_breakdown()
    workers = await storage.workers_all()

    # v5: active backfill progressing counts as syncing (not degraded).
    # We detect progress as last_batch_at freshness within the chat's state.
    checks = {
        "pid_alive": alive,
        "heartbeat_fresh": hb_age is not None and hb_age < HEARTBEAT_STALE_SEC,
        "postgres_ok": True,
        "telegram_fresh": tg_ok_age is not None and tg_ok_age < TG_STALE_WARN_SEC,
        "telegram_not_failed": tg_ok_age is not None and tg_ok_age < TG_STALE_FAIL_SEC,
        "any_target_active": bool(targets),
        "no_blocked_backfill": all(
            t["backfill_status"] not in ("blocked",) for t in targets),
        "no_unacknowledged_dead": all(t["dead_count"] == 0 for t in targets),
        "distinct_match": totals["distinct_match"],
        "no_unresolved_errors_1h": totals["errors_unresolved_1h"] == 0,
    }

    unhealthy_reasons = []
    degraded_reasons = []
    syncing_reasons = []
    if not alive:
        unhealthy_reasons.append("pid_not_alive")
    if not checks["heartbeat_fresh"]:
        unhealthy_reasons.append("db_heartbeat_stale")
    if not checks["telegram_not_failed"]:
        unhealthy_reasons.append("telegram_heartbeat_fail")
    elif not checks["telegram_fresh"]:
        degraded_reasons.append("telegram_heartbeat_stale")
    if not checks["any_target_active"]:
        unhealthy_reasons.append("no_active_targets")
    if not checks["distinct_match"]:
        unhealthy_reasons.append("duplicate_messages")
    if not checks["no_unresolved_errors_1h"]:
        degraded_reasons.append(
            f"unresolved_errors_last_hour={totals['errors_unresolved_1h']}")
    # v6: coordinator progress-staleness check (the fix for the 16h stuck v5 bug)
    coord_by_chat = {c["chat_id"]: c for c in coord_rows}
    for t in targets:
        bs = t["backfill_status"]
        cp = coord_by_chat.get(t["chat_id"]) or {}
        progress_age = None
        if cp.get("batch_last_progress_at"):
            progress_age = (now - cp["batch_last_progress_at"]).total_seconds()
        coord_hb_age = None
        if cp.get("coordinator_heartbeat_at"):
            coord_hb_age = (now - cp["coordinator_heartbeat_at"]).total_seconds()

        if bs == "blocked":
            degraded_reasons.append(f"target_{t['chat_id']}_blocked")
        elif bs == "stalled":
            degraded_reasons.append(f"target_{t['chat_id']}_stalled")
        elif bs == "degraded":
            unhealthy_reasons.append(f"target_{t['chat_id']}_degraded")
        elif bs == "syncing_paused_media_backpressure":
            syncing_reasons.append(f"target_{t['chat_id']}_paused_backpressure")
        elif bs in ("syncing", "running"):
            # syncing requires fresh progress OR an idle/complete chat OR a
            # fresh batch actively running.
            if cp.get("batch_running") and progress_age is not None \
                    and progress_age > 300:
                # batch alive but no progress for 5+ minutes - STALE
                degraded_reasons.append(
                    f"target_{t['chat_id']}_progress_stale_{int(progress_age)}s")
            elif coord_hb_age is not None and coord_hb_age > 600:
                degraded_reasons.append(
                    f"target_{t['chat_id']}_coord_hb_stale_{int(coord_hb_age)}s")
            else:
                syncing_reasons.append(f"target_{t['chat_id']}_syncing")
        elif bs == "incomplete":
            syncing_reasons.append(f"target_{t['chat_id']}_syncing_legacy")
        elif bs == "complete_with_errors" and t["dead_count"] > 0:
            degraded_reasons.append(f"target_{t['chat_id']}_dead")

    # v6: worker heartbeat staleness
    for w in workers:
        if w["heartbeat_age_sec"] is not None and w["heartbeat_age_sec"] > 300:
            degraded_reasons.append(
                f"worker_{w['worker_id']}_hb_stale_{int(w['heartbeat_age_sec'])}s")

    if unhealthy_reasons:
        status = "unhealthy"
    elif degraded_reasons:
        status = "degraded"
    elif syncing_reasons:
        status = "syncing"
    else:
        status = "healthy"

    await storage.close()
    return {
        "status": status,
        "pid": pid,
        "pid_alive": alive,
        "uptime_sec": uptime,
        "db_heartbeat_age_sec": hb_age,
        "postgres": True,
        "telegram_connected": rt["telegram_connected"] if rt else None,
        "telegram_last_ok_age_sec": tg_ok_age,
        "telegram_last_error": rt["telegram_last_error"] if rt else None,
        "update_last_received_age_sec": upd_age,
        "last_message_age_sec": last_msg_age,
        "messages_per_minute": mpm,
        "media_oldest_pending_age_sec": oldest_pending,
        "media_throughput": media_th,
        "media_status_breakdown": media_status,
        "workers": workers,
        "coordinator_progress": [
            {k: (v.isoformat() if hasattr(v, "isoformat") else v)
             for k, v in row.items()} for row in coord_rows
        ],
        "user_profile_queue": upq_stats,
        "errors_unresolved_by_group": errors_by_group,
        "active_targets": targets,
        "disabled_targets": disabled_targets,
        "totals": totals,
        "checks": checks,
        "reasons_unhealthy": unhealthy_reasons,
        "reasons_degraded": degraded_reasons,
        "reasons_syncing": syncing_reasons,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    snap = await build_snapshot()
    if args.json:
        print(json.dumps(snap, default=str, indent=2))
    else:
        print("=== TGParser health ===")
        print(f"status: {snap.get('status')}")
        print(f"pid: {snap.get('pid')} alive: {snap.get('pid_alive')}")
        print(f"uptime_sec: {snap.get('uptime_sec')}")
        print(f"db_heartbeat_age_sec: {snap.get('db_heartbeat_age_sec')}")
        print(f"postgres: {'OK' if snap.get('postgres') else 'FAIL'}")
        print(f"telegram_connected: {snap.get('telegram_connected')} "
              f"last_ok_age_sec: {snap.get('telegram_last_ok_age_sec')}")
        if snap.get("telegram_last_error"):
            print(f"telegram_last_error: {snap['telegram_last_error']}")
        print(f"last_message_age_sec: {snap.get('last_message_age_sec')}")
        print(f"reasons_unhealthy: {snap.get('reasons_unhealthy')}")
        print(f"reasons_degraded: {snap.get('reasons_degraded')}")
        print(f"messages_per_minute: {snap.get('messages_per_minute')}")
        print(f"media_oldest_pending_age_sec: {snap.get('media_oldest_pending_age_sec')}")
        print(f"user_profile_queue: {snap.get('user_profile_queue')}")
        print(f"errors_unresolved_by_group: {snap.get('errors_unresolved_by_group')}")
        print(f"reasons_syncing: {snap.get('reasons_syncing')}")
        print("active targets:")
        for t in snap.get("active_targets", []):
            print(f"  chat={t['chat_id']} target={t['target']} "
                  f"status={t['backfill_status']} "
                  f"pending={t['pending_count']} dead={t['dead_count']} "
                  f"problem_ids={t['problem_message_ids']}")
        print("disabled targets:")
        for t in snap.get("disabled_targets", []):
            print(f"  chat={t['chat_id']} target={t['target']} "
                  f"reason={t['disabled_reason']}")
        print("totals:")
        for k, v in (snap.get("totals") or {}).items():
            print(f"  {k}={v}")
    s = snap.get("status")
    if s in ("healthy", "syncing"):
        return 0
    if s == "degraded":
        return 1
    return 2


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
