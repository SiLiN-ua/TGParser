"""One-tick snapshot for post-fix watch. Appends JSON line to argv[1]."""
import asyncio, json, sys, os, datetime as dt
sys.path.insert(0, r"E:\TGParser")
os.chdir(r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load
# Chat to watch is supplied at run time; no real chat ids live in the repo.
WATCH_CHAT_ID = int(os.environ["TGPARSER_WATCH_CHAT_ID"])


async def main(out_path):
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    tick = {"ts": dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        tick["media_by_status"] = {
            r["status"]: r["n"] for r in await conn.fetch(
                "SELECT status, COUNT(*)::int AS n FROM media GROUP BY status")
        }
        rt = await conn.fetchrow("SELECT * FROM runtime_state LIMIT 1")
        tick["runtime"] = {
            "pid": rt["pid"],
            "heartbeat_at": rt["heartbeat_at"].isoformat() if rt["heartbeat_at"] else None,
            "status": rt["status"],
            "telegram_last_ok_at": rt["telegram_last_ok_at"].isoformat() if rt["telegram_last_ok_at"] else None,
            "telegram_last_error": (rt["telegram_last_error"] or "")[:80],
            "update_last_received_at": rt["update_last_received_at"].isoformat() if rt["update_last_received_at"] else None,
        }
        ss = await conn.fetchrow(
            "SELECT backfill_status, last_backfilled_message_id, batches_completed "
            f"FROM sync_state WHERE chat_id={WATCH_CHAT_ID}")
        tick["sync"] = dict(ss) if ss else None
        cp = await conn.fetchrow(
            "SELECT coordinator_heartbeat_at, batch_running, consecutive_batch_failures, "
            f"last_batch_added FROM coordinator_progress WHERE chat_id={WATCH_CHAT_ID}")
        if cp:
            cp = dict(cp)
            cp["coordinator_heartbeat_at"] = cp["coordinator_heartbeat_at"].isoformat() if cp["coordinator_heartbeat_at"] else None
        tick["coord"] = cp
        tick["msgs"] = await conn.fetchval("SELECT COUNT(*) FROM messages")
        tick["users"] = await conn.fetchval("SELECT COUNT(*) FROM users")
        try:
            tick["errors_1h"] = await conn.fetchval(
                "SELECT COUNT(*) FROM errors WHERE occurred_at > now() - interval '1 hour'")
        except Exception as e:
            tick["errors_1h_err"] = f"{type(e).__name__}: {str(e)[:60]}"
    finally:
        await conn.close()
    with open(out_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(tick, default=str) + "\n")
    print(json.dumps(tick, default=str, indent=1)[:600])

asyncio.run(main(sys.argv[1]))
