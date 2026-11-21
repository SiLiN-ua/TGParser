import asyncio, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    rows = await conn.fetch("""
      SELECT table_name FROM information_schema.tables
      WHERE table_schema='public' AND table_name LIKE '%migration%'
    """)
    print("migration tables:", [r["table_name"] for r in rows])
    for t in [r["table_name"] for r in rows]:
        print(f"\n=== {t} ===")
        cols = await conn.fetch(f"""
          SELECT column_name FROM information_schema.columns
          WHERE table_name='{t}' ORDER BY ordinal_position
        """)
        print("  cols:", [c["column_name"] for c in cols])
        data = await conn.fetch(f'SELECT * FROM "{t}" ORDER BY 1')
        for r in data:
            print(" ", dict(r))
    await conn.close()
asyncio.run(main())
