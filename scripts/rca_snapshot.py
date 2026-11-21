"""Comprehensive RCA snapshot: media queue, access_hash invariants, live-handler, orphans, constraints.
No secrets, aggregates only. Writes JSON to reports/_rca_snapshot.json.
"""
import asyncio, json, sys, os, pathlib, datetime as dt
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

OUT = pathlib.Path(r"E:\TGParser\reports\_rca_snapshot.json")
OUT.parent.mkdir(parents=True, exist_ok=True)

QUERIES = {
    # --- media queue state ---
    "media_by_status": "SELECT status, COUNT(*) AS n FROM media GROUP BY status ORDER BY n DESC",
    "media_pending_eligible_now": """
        SELECT COUNT(*) AS n FROM media
        WHERE status='pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    """,
    "media_pending_backoff": """
        SELECT COUNT(*) AS n, MIN(next_attempt_at) AS earliest, MAX(next_attempt_at) AS latest
        FROM media WHERE status='pending' AND next_attempt_at IS NOT NULL AND next_attempt_at > now()
    """,
    "media_attempts_hist": """
        SELECT COALESCE(attempts,0) AS attempts, COUNT(*) AS n
        FROM media WHERE status='pending'
        GROUP BY COALESCE(attempts,0) ORDER BY attempts
    """,
    "media_pending_age": """
        SELECT MIN(created_at) AS oldest_created, MAX(created_at) AS newest_created,
               MIN(updated_at) AS oldest_updated, MAX(updated_at) AS newest_updated
        FROM media WHERE status='pending'
    """,
    "media_by_chat_pending": """
        SELECT chat_id, COUNT(*) AS n FROM media WHERE status='pending'
        GROUP BY chat_id ORDER BY n DESC LIMIT 10
    """,
    "media_error_classes": """
        SELECT substring(COALESCE(last_error,''), 1, 60) AS err_prefix, COUNT(*) AS n
        FROM media WHERE last_error IS NOT NULL
        GROUP BY err_prefix ORDER BY n DESC LIMIT 15
    """,
    "media_recent_done": """
        SELECT COUNT(*) FILTER (WHERE updated_at > now() - interval '1 hour') AS done_1h,
               COUNT(*) FILTER (WHERE updated_at > now() - interval '6 hour') AS done_6h,
               COUNT(*) FILTER (WHERE updated_at > now() - interval '24 hour') AS done_24h
        FROM media WHERE status='done'
    """,
    # --- pg_stat_activity ---
    "pg_activity": """
        SELECT pid, state, wait_event_type, wait_event,
               age(now(), xact_start) AS xact_age,
               substring(query, 1, 120) AS q
        FROM pg_stat_activity
        WHERE datname = current_database() AND pid <> pg_backend_pid()
        ORDER BY xact_start NULLS LAST
    """,
    "pg_locks_blocked": """
        SELECT count(*) AS n FROM pg_locks WHERE NOT granted
    """,
    # --- access_hash invariants ---
    "ah_1976_class": """
        SELECT COUNT(*) AS n FROM users
        WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
    """,
    "ah_1976_with_history": """
        SELECT COUNT(DISTINCT u.id) AS n
        FROM users u
        WHERE u.access_hash IS NOT NULL AND u.access_hash_kind IS NULL
          AND EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id = u.id)
    """,
    "ah_1976_no_history": """
        SELECT COUNT(*) AS n FROM users u
        WHERE u.access_hash IS NOT NULL AND u.access_hash_kind IS NULL
          AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id = u.id)
    """,
    "ah_users_first_seen_range_unclass": """
        SELECT MIN(created_at) AS oldest_user, MAX(created_at) AS newest_user,
               MIN(updated_at) AS oldest_upd,  MAX(updated_at) AS newest_upd
        FROM users WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL
    """,
    "ah_users_classified_counts": """
        SELECT COALESCE(access_hash_kind,'<null>') AS kind,
               COALESCE(access_hash_priority,-1)  AS prio,
               COUNT(*) AS n
        FROM users WHERE access_hash IS NOT NULL
        GROUP BY 1,2 ORDER BY 3 DESC
    """,
    "ah_users_by_source": """
        SELECT COALESCE(access_hash_source,'<null>') AS source, COUNT(*) AS n
        FROM users WHERE access_hash IS NOT NULL
        GROUP BY source ORDER BY n DESC
    """,
    "ah_active_history_per_user": """
        SELECT cnt AS active_rows_per_user, COUNT(*) AS users
        FROM (
            SELECT user_id, COUNT(*) AS cnt
            FROM access_hash_history WHERE is_active = TRUE
            GROUP BY user_id
        ) t GROUP BY cnt ORDER BY cnt
    """,
    "ah_197_promoted_but_legacy_still_active": """
        SELECT COUNT(DISTINCT h1.user_id) AS n
        FROM access_hash_history h1
        JOIN users u ON u.id = h1.user_id
        WHERE h1.is_active = TRUE
          AND h1.auth_fingerprint = 'legacy'
          AND u.access_hash_kind IS NOT NULL
          AND u.access_hash_kind <> 'unknown'
    """,
    "ah_183_cross_session_active_in_users": """
        SELECT COUNT(*) AS n
        FROM users u
        JOIN access_hash_history h
          ON h.user_id = u.id AND h.access_hash = u.access_hash
         AND h.is_active = TRUE
        WHERE h.auth_fingerprint IS NOT NULL
          AND h.auth_fingerprint <> 'legacy'
          AND h.auth_fingerprint <> (SELECT auth_fingerprint FROM runtime_state LIMIT 1)
    """,
    # --- live-handler ---
    "runtime_state": "SELECT * FROM runtime_state",
    # --- orphans / constraints ---
    "orphan_messages": """
        SELECT COUNT(*) AS n FROM messages m
        WHERE NOT EXISTS (SELECT 1 FROM targets t WHERE t.chat_id = m.chat_id)
    """,
    "orphan_media_fk_user": """
        SELECT COUNT(*) AS n FROM media mm
        LEFT JOIN messages m ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id
        WHERE m.chat_id IS NULL
    """,
    "orphan_observations": """
        SELECT COUNT(*) AS n FROM user_observations o
        LEFT JOIN users u ON u.id = o.user_id
        WHERE u.id IS NULL
    """,
    "invalid_constraints": """
        SELECT conname, conrelid::regclass::text AS tbl, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c WHERE NOT convalidated
    """,
    "checkpoint_rows": """
        SELECT chat_id, last_backfilled_msg, updated_at
        FROM backfill_checkpoints ORDER BY chat_id
    """,
}


def _default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    if isinstance(o, dt.timedelta):
        return o.total_seconds()
    return str(o)


async def main():
    cfg = cfg_load()
    dsn = cfg["postgres"]["dsn"]
    conn = await asyncpg.connect(dsn)
    out = {"captured_at": dt.datetime.now(dt.timezone.utc).isoformat(), "queries": {}}
    for name, sql in QUERIES.items():
        try:
            async with conn.transaction():
                rows = await conn.fetch(sql)
                out["queries"][name] = [dict(r) for r in rows]
        except Exception as e:  # noqa: BLE001
            out["queries"][name] = {"__error__": f"{type(e).__name__}: {e}"[:200]}
    await conn.close()
    OUT.write_text(json.dumps(out, indent=2, default=_default), encoding="utf-8")
    print(f"wrote {OUT} bytes={OUT.stat().st_size}")
    # Print brief for console
    for k in ("media_by_status", "ah_1976_class", "ah_1976_no_history",
              "ah_active_history_per_user", "ah_197_promoted_but_legacy_still_active",
              "ah_183_cross_session_active_in_users",
              "pg_activity", "invalid_constraints", "runtime_state",
              "media_recent_done", "media_pending_eligible_now"):
        print(f"\n--- {k} ---")
        print(json.dumps(out["queries"].get(k), indent=2, default=_default)[:900])

asyncio.run(main())
