"""v7.1 acceptance: access_hash + new-user invariants on live data.

Produces an anonymized JSON for the final report.
"""
import asyncio, json, os, sys, datetime as dt
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT)
sys.path.insert(0, PROJECT)
from tgparser.config import load as _load_cfg
import asyncpg

DSN = _load_cfg()["postgres"]["dsn"]

ACCESS_HASH_Q = {
    "users_by_kind_live": """
        SELECT COALESCE(access_hash_kind,'NONE') AS k, count(*) AS c
        FROM users GROUP BY 1 ORDER BY 1
    """,
    "users_by_priority_live": """
        SELECT access_hash_priority AS p, count(*) AS c
        FROM users WHERE access_hash IS NOT NULL GROUP BY 1 ORDER BY 1
    """,
    "stale_count": "SELECT count(*) AS c FROM users WHERE access_hash_is_stale",
    "users_without_hash": "SELECT count(*) AS c FROM users WHERE access_hash IS NULL",
    "auth_fp_distinct_in_users":
        "SELECT count(DISTINCT access_hash_auth_fingerprint) AS c FROM users WHERE access_hash_auth_fingerprint IS NOT NULL",
    "ahh_total": "SELECT count(*) AS c FROM access_hash_history",
    "ahh_active": "SELECT count(*) AS c FROM access_hash_history WHERE is_active",
    "ahh_by_kind": """
        SELECT kind, count(*) AS c FROM access_hash_history GROUP BY kind ORDER BY kind
    """,
    "ahh_new_since_v71": """
        SELECT count(*) AS c FROM access_hash_history
        WHERE source != 'legacy'
    """,
    "ahh_by_source": """
        SELECT source, count(*) AS c FROM access_hash_history GROUP BY source ORDER BY source
    """,
    # Lower-priority hashes that live only in history (not active in users)
    "lower_priority_quarantined": """
        SELECT count(*) AS c
        FROM access_hash_history h
        JOIN users u ON u.id = h.user_id
        WHERE h.is_active = FALSE
          AND h.priority < u.access_hash_priority
          AND u.access_hash IS NOT NULL
    """,
    # Users that have both a higher-priority ACTIVE hash and a lower-priority HISTORY hash
    "active_stayed_against_lower_in_history": """
        SELECT count(DISTINCT u.id) AS c
        FROM users u
        JOIN access_hash_history h ON h.user_id = u.id
        WHERE h.is_active = FALSE
          AND u.access_hash IS NOT NULL
          AND h.priority < u.access_hash_priority
    """,
    # Promotion cases: users whose current kind is strictly higher than any earlier history row
    "promotion_cases": """
        SELECT count(*) AS c FROM (
          SELECT u.id,
                 MIN(h.priority) FILTER (WHERE NOT h.is_active) AS min_prev,
                 u.access_hash_priority AS now_prio
          FROM users u
          JOIN access_hash_history h ON h.user_id = u.id
          GROUP BY u.id, u.access_hash_priority
          HAVING u.access_hash_priority > COALESCE(MIN(h.priority) FILTER (WHERE NOT h.is_active), -1)
        ) t
    """,
    # Auth fingerprints distinct in history (should be >= distinct in users)
    "ahh_auth_fp_distinct":
        "SELECT count(DISTINCT auth_fingerprint) AS c FROM access_hash_history",
    # Users with hash from a different auth session than current user row
    "cross_session_hashes_in_history": """
        SELECT count(*) AS c
        FROM access_hash_history h
        JOIN users u ON u.id = h.user_id
        WHERE h.auth_fingerprint IS NOT NULL
          AND u.access_hash_auth_fingerprint IS NOT NULL
          AND h.auth_fingerprint != u.access_hash_auth_fingerprint
          AND h.is_active = FALSE
    """,
    "cross_session_hashes_active": """
        SELECT count(*) AS c
        FROM access_hash_history h
        JOIN users u ON u.id = h.user_id
        WHERE h.auth_fingerprint IS NOT NULL
          AND u.access_hash_auth_fingerprint IS NOT NULL
          AND h.auth_fingerprint != u.access_hash_auth_fingerprint
          AND h.is_active = TRUE
    """,
}

ENRICHMENT_Q = {
    "ieq_by_status": """
        SELECT status, count(*) AS c FROM identity_enrichment_queue GROUP BY status ORDER BY status
    """,
    "ieq_by_priority_bucket": """
        SELECT CASE WHEN priority <= 100 THEN 'high'
                    WHEN priority <= 1000 THEN 'normal'
                    ELSE 'low' END AS bucket,
               count(*) AS c
        FROM identity_enrichment_queue GROUP BY 1 ORDER BY 1
    """,
    "ieq_oldest_pending_age_sec": """
        SELECT EXTRACT(EPOCH FROM (NOW() - MIN(first_seen)))::bigint AS c
        FROM identity_enrichment_queue WHERE status='pending'
    """,
    "ieq_max_attempts": """
        SELECT COALESCE(MAX(attempts),0) AS c FROM identity_enrichment_queue
    """,
    "ieq_waiting_entity": """
        SELECT count(*) AS c FROM identity_enrichment_queue WHERE status='waiting_entity'
    """,
}

OBS_Q = {
    # Observations dedup: duplicates per unique key should be 0 (unique index exists)
    "obs_duplicates": """
        SELECT count(*) AS c FROM (
          SELECT user_id, chat_id, COALESCE(message_id,0) AS m, source_type, count(*) n
          FROM user_observations
          GROUP BY 1,2,3,4
          HAVING count(*) > 1
        ) t
    """,
    # Observations recorded via sender_user_id fallback: source_type='message_sender'
    # and where user row has minimal profile (no username+first_name) → proxy for 'get_sender() returned None'
    "obs_from_fallback_estimate": """
        SELECT count(*) AS c
        FROM user_observations o
        JOIN users u ON u.id = o.user_id
        WHERE o.source_type = 'message_sender'
          AND o.created_at > '2026-11-19'::date
          AND u.username IS NULL
          AND u.first_name IS NULL
    """,
    "obs_total": "SELECT count(*) AS c FROM user_observations",
    "obs_by_source_type": """
        SELECT source_type, count(*) AS c FROM user_observations GROUP BY source_type ORDER BY source_type
    """,
    "obs_new_since_v71": """
        SELECT count(*) AS c FROM user_observations WHERE created_at > '2026-11-19 11:00:00+02'::timestamptz
    """,
    # Multi-chat users
    "multi_chat_users": """
        SELECT count(*) AS c FROM (
          SELECT user_id FROM user_observations
          GROUP BY user_id HAVING count(DISTINCT chat_id) > 1
        ) t
    """,
}

ORPHAN_Q = {
    "orphan_messages_without_chat": """
        SELECT count(*) AS c FROM messages m
        LEFT JOIN chats c ON c.id = m.chat_id WHERE c.id IS NULL
    """,
    "orphan_observations_without_user": """
        SELECT count(*) AS c FROM user_observations o
        LEFT JOIN users u ON u.id = o.user_id WHERE u.id IS NULL
    """,
    "orphan_media_without_message": """
        SELECT count(*) AS c FROM message_media mm
        LEFT JOIN messages m ON m.chat_id=mm.chat_id AND m.message_id=mm.message_id
        WHERE m.message_id IS NULL
    """,
    "invalid_indexes": """
        SELECT count(*) AS c FROM pg_index WHERE NOT indisvalid
    """,
    "invalid_constraints": """
        SELECT count(*) AS c FROM pg_constraint WHERE NOT convalidated
    """,
    "largest_tables": """
        SELECT relname AS t, pg_size_pretty(pg_relation_size(c.oid)) AS size,
               pg_relation_size(c.oid) AS bytes
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='r'
        ORDER BY pg_relation_size(c.oid) DESC LIMIT 10
    """,
    "last_message_at": "SELECT max(date) AS c FROM messages",
    "db_size_bytes": "SELECT pg_database_size(current_database()) AS c",
}


async def runset(name, qs, conn):
    out = {}
    for k, sql in qs.items():
        try:
            async with conn.transaction():
                rows = await conn.fetch(sql)
            if len(rows) == 1 and len(rows[0]) == 1:
                out[k] = rows[0][0]
            else:
                out[k] = [dict(r) for r in rows]
        except Exception as e:
            out[k] = f"ERR {e}"
    return out


async def main():
    conn = await asyncpg.connect(DSN)
    try:
        # discover observation column name
        cols = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='user_observations'"
        )
        colnames = [r["column_name"] for r in cols]
        OBS_Q["_obs_columns"] = (
            "SELECT array_agg(column_name::text ORDER BY column_name) AS c "
            "FROM information_schema.columns WHERE table_name='user_observations'"
        )
        # swap created_at for whatever timestamp column actually exists
        ts_col = next((c for c in ("created_at","observed_at","first_seen","seen_at","at")
                       if c in colnames), None)
        if ts_col:
            OBS_Q["obs_from_fallback_estimate"] = OBS_Q["obs_from_fallback_estimate"].replace("o.created_at", f"o.{ts_col}")
            OBS_Q["obs_new_since_v71"] = OBS_Q["obs_new_since_v71"].replace("created_at", ts_col)
        else:
            OBS_Q.pop("obs_from_fallback_estimate", None)
            OBS_Q.pop("obs_new_since_v71", None)

        result = {
            "at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "observations_ts_column": ts_col,
            "access_hash": await runset("ah", ACCESS_HASH_Q, conn),
            "enrichment_queue": await runset("ieq", ENRICHMENT_Q, conn),
            "observations": await runset("obs", OBS_Q, conn),
            "orphans_and_integrity": await runset("orph", ORPHAN_Q, conn),
        }
    finally:
        await conn.close()

    def default(o):
        if isinstance(o, (dt.datetime, dt.date)):
            return o.isoformat()
        raise TypeError

    out = os.path.join(PROJECT, "reports", "_acceptance_queries.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, default=default, indent=2, ensure_ascii=False)
    print("WROTE", out)


asyncio.run(main())
