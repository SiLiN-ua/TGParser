import asyncio, json, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    for tbl in ("media", "users", "access_hash_history", "runtime_state", "backfill_checkpoints", "user_observations"):
        rows = await conn.fetch(f"""
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_name='{tbl}' ORDER BY ordinal_position
        """)
        print(f"\n=== {tbl} ===")
        for r in rows:
            print(f"  {r['column_name']:30s} {r['data_type']}")
    await conn.close()

asyncio.run(main())
