"""Synchronous watch loop that runs in-process (avoids PS/bash background quirks).
Takes N ticks separated by SLEEP_SEC and appends to argv[1]."""
import asyncio, json, sys, os, time, datetime as dt
sys.path.insert(0, r"E:\TGParser")
os.chdir(r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

OUT = sys.argv[1]
N = int(sys.argv[2])
SLEEP_SEC = int(sys.argv[3])

async def tick(conn):
    rec = {"ts": dt.datetime.now(dt.timezone.utc).isoformat()}
    rec["media_by_status"] = {r["status"]: r["n"] for r in await conn.fetch(
        "SELECT status, COUNT(*)::int AS n FROM media GROUP BY status")}
    rt = await conn.fetchrow("SELECT * FROM runtime_state LIMIT 1")
    rec["runtime"] = {
        "pid": rt["pid"],
        "heartbeat_at": rt["heartbeat_at"].isoformat() if rt["heartbeat_at"] else None,
        "status": rt["status"],
        "telegram_last_ok_at": rt["telegram_last_ok_at"].isoformat() if rt["telegram_last_ok_at"] else None,
        "telegram_last_error": (rt["telegram_last_error"] or "")[:80],
        "update_last_received_at": rt["update_last_received_at"].isoformat() if rt["update_last_received_at"] else None,
    }
    ss = await conn.fetchrow(
        "SELECT backfill_status, last_backfilled_message_id, batches_completed "
        "FROM sync_state WHERE chat_id=-1001000000001")
    rec["sync"] = dict(ss) if ss else None
    cp = await conn.fetchrow(
        "SELECT coordinator_heartbeat_at, batch_running, consecutive_batch_failures, "
        "last_batch_added FROM coordinator_progress WHERE chat_id=-1001000000001")
    if cp:
        cp = dict(cp)
        cp["coordinator_heartbeat_at"] = cp["coordinator_heartbeat_at"].isoformat() if cp["coordinator_heartbeat_at"] else None
    rec["coord"] = cp
    rec["msgs"] = await conn.fetchval("SELECT COUNT(*) FROM messages")
    rec["users"] = await conn.fetchval("SELECT COUNT(*) FROM users")
    rec["obs"] = await conn.fetchval("SELECT COUNT(*) FROM user_observations")
    try:
        rec["errors_1h"] = await conn.fetchval(
            "SELECT COUNT(*) FROM errors WHERE occurred_at > now() - interval '1 hour'")
    except Exception:
        try:
            rec["errors_1h"] = await conn.fetchval(
                "SELECT COUNT(*) FROM errors WHERE created_at > now() - interval '1 hour'")
        except Exception as e:
            rec["errors_1h_err"] = str(e)[:80]
    try:
        rec["media_eligible_pending"] = await conn.fetchval(
            "SELECT COUNT(*) FROM media WHERE status IN ('pending','failed') "
            "AND attempts < 5 AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    except Exception as e:
        rec["media_eligible_pending_err"] = str(e)[:60]
    try:
        last_done = await conn.fetchval(
            "SELECT MAX(completed_at) FROM media WHERE status='done'")
        rec["media_last_done_at"] = last_done.isoformat() if last_done else None
    except Exception as e:
        rec["media_last_done_err"] = str(e)[:60]
    return rec

async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    with open(OUT, "a", encoding="utf-8") as fh:
        for i in range(N):
            rec = await tick(conn)
            fh.write(json.dumps(rec, default=str) + "\n"); fh.flush()
            if i < N - 1:
                await asyncio.sleep(SLEEP_SEC)
    await conn.close()
    print(f"captured {N} ticks")

asyncio.run(main())
