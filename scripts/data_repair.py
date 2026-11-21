"""v7.3 data repair: close historical access_hash backlog A + B.
Steps:
  1. Timestamped backup of users + access_hash_history (CREATE TABLE AS).
  2. Dry-run: before counts.
  3. Repair A: 1865 users with access_hash NOT NULL + kind NULL + no history.
     Apply migration 0010-equivalent: kind='unknown' priority=0 auth_fp='legacy'
     + insert history row, idempotent via ON CONFLICT DO NOTHING.
  4. Repair B: 224 users with >1 active history rows.
     Winner selection: (priority DESC, became_active_at DESC, last_seen DESC) per
     (user_id, auth_fingerprint). Losers -> is_active=FALSE, replaced_at=NOW(),
     replacement_reason='repair_v7_3_promotion_winner_kept'.
  5. After counts + integrity checks.
  6. Print full audit JSON.

Usage:
  python scripts/data_repair.py --dry-run     (no writes)
  python scripts/data_repair.py --apply       (performs repair inside a single tx)

NEVER writes outside its tx. Idempotent: running twice is a no-op on second run.
NO hash values, NO PII are printed; only counts.
"""
import argparse, asyncio, json, pathlib, sys, datetime as dt
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

STAMP = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BAK_USERS = f"users_bak_{STAMP}"
BAK_AHH   = f"access_hash_history_bak_{STAMP}"

COUNT_QUERIES = {
    "A_users_ah_notnull_kind_null": "SELECT COUNT(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL",
    "A_users_without_history":       "SELECT COUNT(*) FROM users u WHERE u.access_hash IS NOT NULL AND u.access_hash_kind IS NULL AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)",
    "B_users_with_multi_active":     "SELECT COUNT(DISTINCT user_id) FROM (SELECT user_id FROM access_hash_history WHERE is_active=TRUE GROUP BY user_id HAVING COUNT(*)>1) t",
    "B_active_rows_total":           "SELECT COUNT(*) FROM access_hash_history WHERE is_active=TRUE",
    "users_total":                    "SELECT COUNT(*) FROM users",
    "ahh_rows_total":                 "SELECT COUNT(*) FROM access_hash_history",
}

REPAIR_A_HISTORY = """
    INSERT INTO access_hash_history
        (user_id, hash_fingerprint, access_hash, kind, priority,
         source, auth_fingerprint,
         first_seen, last_seen, became_active_at, is_active)
    SELECT u.id,
           md5(u.access_hash::text),
           u.access_hash,
           'unknown', 0, 'legacy', 'legacy',
           COALESCE(u.access_hash_updated_at, NOW()),
           NOW(),
           COALESCE(u.access_hash_updated_at, NOW()),
           TRUE
    FROM users u
    WHERE u.access_hash IS NOT NULL
      AND u.access_hash_kind IS NULL
      AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)
    ON CONFLICT (user_id, hash_fingerprint, auth_fingerprint) DO NOTHING
    RETURNING 1
"""
REPAIR_A_USERS = """
    UPDATE users SET
        access_hash_kind = 'unknown',
        access_hash_priority = 0,
        access_hash_auth_fingerprint = 'legacy'
    WHERE access_hash IS NOT NULL
      AND access_hash_kind IS NULL
    RETURNING id
"""
# Repair B: within each (user_id, auth_fingerprint) group, keep ONE winner
# active, flip the rest to is_active=FALSE. Winner = highest priority, latest
# became_active_at, then latest last_seen. Append-only: rows are NOT deleted.
REPAIR_B_LOSERS = """
    WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY user_id, auth_fingerprint
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
        replacement_reason = 'repair_v7_3_promotion_winner_kept'
    FROM ranked
    WHERE ahh.id = ranked.id AND ranked.rn > 1
    RETURNING ahh.id
"""

async def counts(conn):
    out = {}
    for k, sql in COUNT_QUERIES.items():
        out[k] = await conn.fetchval(sql)
    return out

async def integrity(conn):
    return {
        "users_with_hash_but_no_history": await conn.fetchval(
            "SELECT COUNT(*) FROM users u WHERE u.access_hash IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)"),
        "orphan_history_rows":           await conn.fetchval(
            "SELECT COUNT(*) FROM access_hash_history h "
            "WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id=h.user_id)"),
        "unclassified_but_has_history": await conn.fetchval(
            "SELECT COUNT(*) FROM users u WHERE u.access_hash IS NOT NULL "
            "AND u.access_hash_kind IS NULL "
            "AND EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)"),
    }

async def main(apply: bool):
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    audit = {"started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
             "mode": "APPLY" if apply else "DRY-RUN",
             "backup_tables": {"users": BAK_USERS, "ahh": BAK_AHH}}
    audit["before_counts"] = await counts(conn)
    audit["before_integrity"] = await integrity(conn)
    if apply:
        async with conn.transaction():
            await conn.execute(f'CREATE TABLE "{BAK_USERS}" AS SELECT * FROM users')
            audit["backup_users_rows"] = await conn.fetchval(f'SELECT COUNT(*) FROM "{BAK_USERS}"')
            await conn.execute(f'CREATE TABLE "{BAK_AHH}" AS SELECT * FROM access_hash_history')
            audit["backup_ahh_rows"] = await conn.fetchval(f'SELECT COUNT(*) FROM "{BAK_AHH}"')
            # Repair A: history first, then users (A_HISTORY uses users.access_hash_kind=NULL
            # as filter, so it must run BEFORE the users update).
            rows_a_h = await conn.fetch(REPAIR_A_HISTORY)
            audit["A_history_rows_inserted"] = len(rows_a_h)
            rows_a_u = await conn.fetch(REPAIR_A_USERS)
            audit["A_users_updated"] = len(rows_a_u)
            # Repair B
            rows_b = await conn.fetch(REPAIR_B_LOSERS)
            audit["B_history_rows_deactivated"] = len(rows_b)
    audit["after_counts"] = await counts(conn)
    audit["after_integrity"] = await integrity(conn)
    audit["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    await conn.close()
    out = pathlib.Path(r"E:\TGParser\reports") / f"_data_repair_{STAMP}.json"
    out.write_text(json.dumps(audit, indent=2, default=str), encoding="utf-8")
    print(json.dumps(audit, indent=2, default=str))
    print(f"\nwrote audit to {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    asyncio.run(main(apply=args.apply))
