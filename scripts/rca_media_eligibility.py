import asyncio, json, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

QUERIES = {
    "pending_by_attempts": """
        SELECT COALESCE(attempts,0) AS attempts, COUNT(*) AS n
        FROM media WHERE status='pending'
        GROUP BY attempts ORDER BY attempts
    """,
    "retryable_eligible_lt5": """
        SELECT COUNT(*) FROM media
        WHERE status IN ('pending','failed') AND attempts < 5
          AND chat_id IS NOT NULL AND message_id IS NOT NULL
    """,
    "retryable_attempts_gte5": """
        SELECT COUNT(*) FROM media
        WHERE status IN ('pending','failed') AND attempts >= 5
    """,
    "worker_heartbeats": """
        SELECT * FROM media_workers ORDER BY worker_id
    """,
    "needs_refresh": """
        SELECT COUNT(*) FROM media WHERE status='needs_refresh'
    """,
    "pending_null_pointer": """
        SELECT COUNT(*) FROM media
        WHERE status='pending' AND (chat_id IS NULL OR message_id IS NULL)
    """,
    "most_common_errors": """
        SELECT substring(last_error, 1, 80) AS err, COUNT(*) AS n
        FROM media WHERE last_error IS NOT NULL AND status IN ('pending','failed')
        GROUP BY 1 ORDER BY n DESC LIMIT 20
    """,
    "sync_state": "SELECT * FROM sync_state",
    "coordinator_progress": "SELECT * FROM coordinator_progress",
}
async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    for name, sql in QUERIES.items():
        try:
            async with conn.transaction():
                rows = await conn.fetch(sql)
                print(f"\n=== {name} ===")
                for r in rows:
                    print(dict(r))
        except Exception as e:
            print(f"\n=== {name} ===\n  ERROR: {type(e).__name__}: {str(e)[:150]}")
    await conn.close()
asyncio.run(main())
