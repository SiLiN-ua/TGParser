"""Post-repair verification: check normative uniqueness with the chosen rule."""
import asyncio, json, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

QUERIES = {
    "rule_1_active_per_user_auth_fp_violations":
        "SELECT COUNT(*) FROM (SELECT user_id, auth_fingerprint FROM access_hash_history "
        "WHERE is_active=TRUE GROUP BY user_id, auth_fingerprint HAVING COUNT(*)>1) t",
    "rule_1_active_per_user_total_violations":
        "SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id FROM access_hash_history "
        "WHERE is_active=TRUE GROUP BY user_id HAVING COUNT(*)>1) t",
    "ah_notnull_kind_null":
        "SELECT COUNT(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL",
    "users_with_hash_but_no_history":
        "SELECT COUNT(*) FROM users u WHERE u.access_hash IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)",
    "active_by_auth_fingerprint":
        "SELECT auth_fingerprint, COUNT(*) AS n FROM access_hash_history "
        "WHERE is_active=TRUE GROUP BY auth_fingerprint ORDER BY n DESC",
    "orphan_messages": "SELECT COUNT(*) FROM messages m WHERE NOT EXISTS (SELECT 1 FROM chats c WHERE c.id=m.chat_id)",
    "orphan_history":  "SELECT COUNT(*) FROM access_hash_history h WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id=h.user_id)",
    "users_total":     "SELECT COUNT(*) FROM users",
    "ahh_total":       "SELECT COUNT(*) FROM access_hash_history",
    "duplicate_fingerprints":
        "SELECT COUNT(*) FROM (SELECT user_id, hash_fingerprint, auth_fingerprint, COUNT(*) "
        "FROM access_hash_history GROUP BY 1,2,3 HAVING COUNT(*)>1) t",
}

async def main():
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    out = {}
    for k, sql in QUERIES.items():
        try:
            r = await conn.fetch(sql)
            if len(r) == 1 and len(r[0]) == 1:
                out[k] = list(r[0].values())[0]
            else:
                out[k] = [dict(row) for row in r]
        except Exception as e:
            out[k] = f"ERR: {type(e).__name__}: {e}"[:200]
    await conn.close()
    print(json.dumps(out, indent=2, default=str))

asyncio.run(main())
