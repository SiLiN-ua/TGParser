"""Post-migration acceptance: read-only DB + process snapshots.

Writes JSON records to E:\\TGParser\\reports\\_post_mig_ticks.jsonl (one per tick).
Baseline is tick 0; final snapshot is the last tick.
"""
import asyncio, json, os, sys, time, datetime as dt
import asyncpg
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT)
sys.path.insert(0, PROJECT)
from tgparser.config import load as _load_cfg
DSN = _load_cfg()["postgres"]["dsn"]
OUT = os.path.join(PROJECT, "reports", "_post_mig_ticks.jsonl")

AGG_SQL = """
WITH c AS (
  SELECT count(*) AS n, count(DISTINCT (chat_id,message_id)) AS d FROM messages
)
SELECT jsonb_build_object(
  'schema_migrations', (SELECT array_agg(version ORDER BY version) FROM schema_migrations),
  'messages',          (SELECT n FROM c),
  'messages_distinct', (SELECT d FROM c),
  'duplicate_count',   (SELECT n - d FROM c),
  'users',             (SELECT count(*) FROM users),
  'chats',             (SELECT count(*) FROM chats),
  'memberships',       (SELECT count(*) FROM memberships),
  'observations',      (SELECT count(*) FROM user_observations),
  'ahh_total',         (SELECT count(*) FROM access_hash_history),
  'ahh_active',        (SELECT count(*) FROM access_hash_history WHERE is_active),
  'ahh_by_kind', (
    SELECT jsonb_object_agg(kind, c) FROM (
      SELECT kind, count(*) c FROM access_hash_history WHERE is_active GROUP BY kind
    ) t
  ),
  'ahh_by_priority', (
    SELECT jsonb_object_agg(priority::text, c) FROM (
      SELECT priority, count(*) c FROM access_hash_history WHERE is_active GROUP BY priority
    ) t
  ),
  'users_by_hash_kind', (
    SELECT jsonb_object_agg(k, c) FROM (
      SELECT COALESCE(access_hash_kind, 'NONE') AS k, count(*) c FROM users GROUP BY 1
    ) t
  ),
  'users_stale',       (SELECT count(*) FROM users WHERE access_hash_is_stale),
  'users_without_hash',(SELECT count(*) FROM users WHERE access_hash IS NULL),
  'auth_fp_count',     (SELECT count(DISTINCT access_hash_auth_fingerprint) FROM users WHERE access_hash_auth_fingerprint IS NOT NULL),
  'ahh_auth_fp_count', (SELECT count(DISTINCT auth_fingerprint) FROM access_hash_history WHERE auth_fingerprint IS NOT NULL),
  'ieq_by_status', (
    SELECT jsonb_object_agg(status, c) FROM (
      SELECT status, count(*) c FROM identity_enrichment_queue GROUP BY status
    ) t
  ),
  'ieq_oldest_pending_age_sec', (
    SELECT EXTRACT(EPOCH FROM (NOW() - MIN(first_seen)))::bigint
    FROM identity_enrichment_queue WHERE status='pending'
  ),
  'ieq_max_attempts', (
    SELECT max(attempts) FROM identity_enrichment_queue WHERE status='pending'
  ),
  'media_by_status', (
    SELECT jsonb_object_agg(status, c) FROM (
      SELECT status, count(*) c FROM media GROUP BY status
    ) t
  ),
  'avatars', (SELECT count(*) FROM avatar_history),
  'errors_total', (SELECT count(*) FROM errors),
  'errors_unresolved', (SELECT count(*) FROM errors WHERE resolved_at IS NULL),
  'errors_last_hour', (SELECT count(*) FROM errors WHERE at > NOW() - INTERVAL '1 hour'),
  'last_error_at', (SELECT max(at) FROM errors),
  'checkpoint', (
    SELECT jsonb_object_agg(chat_id::text, last_backfilled_message_id) FROM sync_state
  ),
  'batches_by_target', (
    SELECT jsonb_object_agg(chat_id::text, batches_completed) FROM sync_state
  ),
  'backfill_status', (
    SELECT jsonb_object_agg(chat_id::text, backfill_status) FROM sync_state
  ),
  'runtime', (
    SELECT jsonb_build_object(
      'pid', pid, 'status', status,
      'heartbeat_at', heartbeat_at,
      'telegram_last_ok_at', telegram_last_ok_at,
      'heartbeat_age_sec', EXTRACT(EPOCH FROM (NOW() - heartbeat_at))::bigint,
      'tg_age_sec', EXTRACT(EPOCH FROM (NOW() - telegram_last_ok_at))::bigint
    ) FROM runtime_state LIMIT 1
  ),
  'last_message_at', (SELECT max(date) FROM messages),
  'live_lag_sec', (SELECT EXTRACT(EPOCH FROM (NOW() - MAX(date)))::bigint FROM messages),
  'db_size_mb', (SELECT pg_database_size(current_database())::bigint / (1024*1024)),
  'data_directory', NULL::text  -- requires pg_read_all_settings; checked externally
) AS snapshot;
"""


async def snapshot():
    conn = await asyncpg.connect(DSN)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            row = await conn.fetchval(AGG_SQL)
    finally:
        await conn.close()
    return row


def default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    raise TypeError


async def one_tick(idx):
    snap = await snapshot()
    rec = {
        "tick": idx,
        "at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "snapshot": json.loads(snap) if isinstance(snap, str) else snap,
    }
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=default) + "\n")
    s = rec["snapshot"]
    rt = s.get("runtime") or {}
    return (
        f"tick={idx} msgs={s['messages']} users={s['users']} obs={s['observations']} "
        f"dup={s['duplicate_count']} media_done={(s.get('media_by_status') or {}).get('done',0)} "
        f"err1h={s['errors_last_hour']} pid={rt.get('pid')} status={rt.get('status')} "
        f"hb_age={rt.get('heartbeat_age_sec')} ckpt={list((s.get('checkpoint') or {}).values())}"
    )


async def run(ticks=31, interval=60):
    # wipe prior run's file
    with open(OUT, "w", encoding="utf-8") as f:
        pass
    for i in range(ticks):
        try:
            line = await one_tick(i)
            print(line, flush=True)
        except Exception as e:
            err = {"tick": i, "at": dt.datetime.now(dt.timezone.utc).isoformat(), "error": str(e)}
            with open(OUT, "a", encoding="utf-8") as f:
                f.write(json.dumps(err) + "\n")
            print(f"tick={i} ERROR {e}", flush=True)
        if i < ticks - 1:
            await asyncio.sleep(interval)


if __name__ == "__main__":
    ticks = int(sys.argv[1]) if len(sys.argv) > 1 else 31
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    asyncio.run(run(ticks, interval))
