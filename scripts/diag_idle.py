import asyncio, os, sys
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT); sys.path.insert(0, PROJECT)
from tgparser.config import load
import asyncpg

async def m():
    c = await asyncpg.connect(load()["postgres"]["dsn"])
    rows = await c.fetch("SELECT at, kind, resolved_at, LEFT(message,120) AS m FROM errors ORDER BY at DESC LIMIT 10")
    for r in rows: print(r["at"], r["kind"], "resolved=" + str(r["resolved_at"] is not None), r["m"])
    print("--runtime--")
    r = await c.fetchrow("SELECT * FROM runtime_state")
    print(dict(r))
    print("--media sample pending--")
    r = await c.fetch("SELECT chat_id, message_id, media_type, status, retries, last_attempt_at, LEFT(error,100) AS err FROM media WHERE status='pending' ORDER BY last_attempt_at DESC NULLS FIRST LIMIT 5")
    for x in r: print(dict(x))
    await c.close()

asyncio.run(m())
