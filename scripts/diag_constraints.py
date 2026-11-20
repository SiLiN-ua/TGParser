import asyncio, os, sys, json
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT); sys.path.insert(0, PROJECT)
from tgparser.config import load
import asyncpg

async def m():
    c = await asyncpg.connect(load()["postgres"]["dsn"])
    r = await c.fetch("""
        SELECT conname, conrelid::regclass::text AS t, contype::text, convalidated
        FROM pg_constraint
        WHERE NOT convalidated AND connamespace = 'public'::regnamespace
    """)
    print("invalid_constraints:", json.dumps([dict(x) for x in r], indent=2))
    orph = await c.fetch("""
        SELECT mm.chat_id, mm.message_id, COUNT(*) OVER () AS total
        FROM message_media mm
        LEFT JOIN messages m ON m.chat_id=mm.chat_id AND m.message_id=mm.message_id
        WHERE m.message_id IS NULL
        LIMIT 3
    """)
    print("orphan_media_sample:", json.dumps([{k:v for k,v in dict(x).items() if k!='message_id'} for x in orph], default=str, indent=2))
    print("orphan_media_total:", orph[0]['total'] if orph else 0)
    await c.close()

asyncio.run(m())
