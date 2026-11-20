import asyncio, os, sys, json
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT); sys.path.insert(0, PROJECT)
from tgparser.config import load
import asyncpg

async def m():
    c = await asyncpg.connect(load()["postgres"]["dsn"])
    QS = {
        "users_hash_but_no_kind": "SELECT count(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL",
        "users_hash_and_fp_null": "SELECT count(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_auth_fingerprint IS NULL",
        "users_hash_and_fp_set": "SELECT count(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_auth_fingerprint IS NOT NULL",
        "users_with_hash_no_history_row": """
            SELECT count(*) FROM users u
             WHERE u.access_hash IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM access_hash_history h
                               WHERE h.user_id=u.id AND h.is_active)
        """,
        "users_with_multiple_active_history": """
            SELECT count(*) FROM (
              SELECT user_id FROM access_hash_history WHERE is_active
              GROUP BY user_id HAVING count(*) > 1
            ) t
        """,
        "users_hash_no_kind_sample_count": """
            SELECT count(*) FROM users u
             WHERE u.access_hash IS NOT NULL
               AND u.access_hash_kind IS NULL
               AND EXISTS (SELECT 1 FROM access_hash_history h
                           WHERE h.user_id=u.id AND h.source='message_sender')
        """,
        "legacy_still_active_but_user_promoted": """
            SELECT count(*) FROM access_hash_history h
            JOIN users u ON u.id=h.user_id
            WHERE h.source='legacy' AND h.is_active=TRUE
              AND u.access_hash_priority > 0
        """,
    }
    out = {}
    for k, q in QS.items():
        try:
            out[k] = await c.fetchval(q)
        except Exception as e:
            out[k] = f"ERR {e}"
    await c.close()
    print(json.dumps(out, indent=2))

asyncio.run(m())
