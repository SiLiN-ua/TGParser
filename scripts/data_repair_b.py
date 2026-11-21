"""v7.3 repair pass B (broader normative rule): ONE is_active row per user.

Winner within each user_id group is chosen by:
  - priority DESC NULLS LAST         (higher classification wins)
  - became_active_at DESC NULLS LAST (freshest observation wins)
  - last_seen DESC NULLS LAST        (most recent sighting wins)
  - id DESC                          (tiebreak)

All losers move to is_active=FALSE, replaced_at=NOW(),
replacement_reason set according to WHY they lost.
Nothing is deleted; append-only semantics preserved.
"""
import asyncio, json, sys, datetime as dt, pathlib
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

STAMP = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BAK = f"access_hash_history_bakB_{STAMP}"

SQL = """
WITH ranked AS (
    SELECT id, user_id, auth_fingerprint, priority,
           ROW_NUMBER() OVER (
               PARTITION BY user_id
               ORDER BY priority DESC NULLS LAST,
                        became_active_at DESC NULLS LAST,
                        last_seen DESC NULLS LAST,
                        id DESC
           ) AS rn
    FROM access_hash_history
    WHERE is_active = TRUE
)
UPDATE access_hash_history ahh
SET is_active = FALSE,
    replaced_at = NOW(),
    replacement_reason = CASE
        WHEN ranked.auth_fingerprint = 'legacy'
        THEN 'superseded_legacy_by_current_session_v7_3_repair'
        ELSE 'repair_v7_3_one_active_per_user'
    END
FROM ranked
WHERE ahh.id = ranked.id AND ranked.rn > 1
RETURNING ahh.id
"""

async def main(apply):
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    audit = {"started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
             "mode": "APPLY" if apply else "DRY-RUN"}
    audit["before_multi_active_users"] = await conn.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id FROM access_hash_history "
        "WHERE is_active=TRUE GROUP BY user_id HAVING COUNT(*)>1) t")
    audit["before_active_total"] = await conn.fetchval(
        "SELECT COUNT(*) FROM access_hash_history WHERE is_active=TRUE")
    if apply:
        async with conn.transaction():
            await conn.execute(f'CREATE TABLE "{BAK}" AS SELECT * FROM access_hash_history')
            audit["backup_table"] = BAK
            audit["backup_rows"] = await conn.fetchval(f'SELECT COUNT(*) FROM "{BAK}"')
            rows = await conn.fetch(SQL)
            audit["rows_deactivated"] = len(rows)
    audit["after_multi_active_users"] = await conn.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id FROM access_hash_history "
        "WHERE is_active=TRUE GROUP BY user_id HAVING COUNT(*)>1) t")
    audit["after_active_total"] = await conn.fetchval(
        "SELECT COUNT(*) FROM access_hash_history WHERE is_active=TRUE")
    audit["after_active_by_auth_fp"] = [
        dict(r) for r in await conn.fetch(
            "SELECT auth_fingerprint, COUNT(*)::int AS n FROM access_hash_history "
            "WHERE is_active=TRUE GROUP BY auth_fingerprint ORDER BY n DESC")]
    await conn.close()
    out = pathlib.Path(r"E:\TGParser\reports") / f"_data_repair_b_{STAMP}.json"
    out.write_text(json.dumps(audit, indent=2, default=str), encoding="utf-8")
    print(json.dumps(audit, indent=2, default=str))

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    asyncio.run(main(ap.parse_args().apply))
