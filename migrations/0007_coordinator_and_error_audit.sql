-- Version 7: coordinator, target lifecycle, error audit, user profile retry.
BEGIN;

-- --- 1. target lifecycle: disabled flag --------------------------------------
ALTER TABLE target_state
    ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS disabled_reason TEXT;
-- NB: the old column `backfill_status` already accommodates values like
-- 'running' | 'syncing' | 'incomplete' | 'blocked' | 'complete' | 'complete_with_errors';
-- `enabled=FALSE` short-circuits these for health.

-- --- 2. error audit: resolution + occurrence grouping -----------------------
ALTER TABLE errors
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolution TEXT,
    ADD COLUMN IF NOT EXISTS occurrence_count INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS error_group TEXT;
CREATE INDEX IF NOT EXISTS idx_errors_unresolved ON errors(at DESC) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_errors_group ON errors(error_group, resolved_at);

-- --- 3. durable user profile retry queue ------------------------------------
CREATE TABLE IF NOT EXISTS user_profile_queue (
    user_id BIGINT PRIMARY KEY,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending',       -- pending | done | dead
    hint_chat_id BIGINT,                           -- where we first saw this user
    hint_message_id BIGINT                         -- message that referenced them
);
CREATE INDEX IF NOT EXISTS idx_upq_status ON user_profile_queue(status, next_retry_at);

-- --- 4. backfill batch progress tracking ------------------------------------
ALTER TABLE sync_state
    ADD COLUMN IF NOT EXISTS last_batch_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS batches_completed INT NOT NULL DEFAULT 0;

-- --- 5. tighten BIGINT where any 0 literal snuck in -------------------------
-- (0 as INT4 inside expression indexes bites us; keep columns BIGINT only, no index change needed)
-- The avatar index was already fixed in 0006; this is a defensive no-op kept
-- for auditability. The equivalent in message_media/media is already BIGINT.

-- --- 6. mark historical errors resolved --------------------------------------
-- Classify known-fixed classes so health stops counting them as active.
UPDATE errors
   SET error_group = CASE
       WHEN message LIKE '%fk_mm_chat_msg%' OR traceback LIKE '%fk_mm_chat_msg%'
           THEN 'legacy_fk_message_media'
       WHEN message LIKE '%value out of int32 range%' OR traceback LIKE '%value out of int32 range%'
           THEN 'legacy_photo_file_id_int32'
       WHEN message LIKE '%Could not find the input entity for PeerUser%'
         OR traceback LIKE '%Could not find the input entity for PeerUser%'
           THEN 'legacy_input_peer_user_not_found'
       WHEN kind LIKE 'bg_user:%' OR kind LIKE 'ensure_user%'
           THEN 'legacy_ensure_user'
       ELSE kind
   END
 WHERE error_group IS NULL;

UPDATE errors
   SET resolved_at = NOW(),
       resolution = CASE error_group
           WHEN 'legacy_fk_message_media' THEN
               'fixed in migration 0005 (FK dropped) + collector reorders upsert_message before media enqueue'
           WHEN 'legacy_photo_file_id_int32' THEN
               'fixed in migration 0006 (bigint index literal) + explicit $::bigint casts in storage.avatar_*'
           WHEN 'legacy_input_peer_user_not_found' THEN
               'fixed in v5 via msg.get_sender() + durable user_profile_queue with dedup'
           WHEN 'legacy_ensure_user' THEN
               'fixed in v5 via durable user_profile_queue (dedup + bounded retry)'
           ELSE NULL
       END
 WHERE resolved_at IS NULL
   AND error_group IN ('legacy_fk_message_media','legacy_photo_file_id_int32',
                       'legacy_input_peer_user_not_found','legacy_ensure_user');

COMMIT;
