-- Version 3: durability, media M2M, deletions without context, avatar history,
-- participants-sync state, and defensive fix for migration 0002.

-- --- 1. Repair for migration 0002 (chats.id canonicalization) -------------
-- 0002 converted any chats.id whose value appeared as -100<id> in messages.
-- For a channel/supergroup that is correct. For a user/group we must not do it.
-- Current data snapshot has only channel rows, so this block is a defensive
-- revert in case future repair is needed. Safe to re-run (idempotent).
DO $$
BEGIN
    -- If a non-channel row ended up with -100... id, flip back.
    -- There are no such rows today; the UPDATE is a no-op unless someone imports
    -- a broken state. Keeps migration 0002 behavior but prevents misuse.
    UPDATE chats SET id = CAST(SUBSTRING(CAST(id AS TEXT) FROM 5) AS BIGINT)
    WHERE type IN ('user','group')
      AND id::text LIKE '-100%';
END $$;

-- --- 2. Media many-to-many -----------------------------------------------
CREATE TABLE IF NOT EXISTS message_media (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    file_id TEXT NOT NULL,
    attached_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chat_id, message_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_message_media_file ON message_media(file_id);

-- Backfill from the legacy single-column pointer on messages.
INSERT INTO message_media (chat_id, message_id, file_id)
SELECT chat_id, message_id, media_file_id
FROM messages
WHERE media_file_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- --- 3. Backfill durable retry / dead-letter -----------------------------
CREATE TABLE IF NOT EXISTS backfill_queue (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | done | dead
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_bq_status ON backfill_queue(status, next_retry_at);

-- --- 4. Deletions without associated chat_id -----------------------------
CREATE TABLE IF NOT EXISTS deletion_events (
    id BIGSERIAL PRIMARY KEY,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_ids JSONB NOT NULL
);

-- --- 5. Avatar history ---------------------------------------------------
CREATE TABLE IF NOT EXISTS avatar_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    photo_file_id BIGINT,
    local_path TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_avatar_hist
    ON avatar_history(user_id, COALESCE(photo_file_id, 0));
CREATE INDEX IF NOT EXISTS idx_avatar_hist_user ON avatar_history(user_id);

-- --- 6. Participants sync state ------------------------------------------
ALTER TABLE sync_state
    ADD COLUMN IF NOT EXISTS participants_synced BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS participants_is_complete BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS participants_limitation TEXT,
    ADD COLUMN IF NOT EXISTS participants_offset INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS participants_updated_at TIMESTAMPTZ;

ALTER TABLE memberships
    ADD COLUMN IF NOT EXISTS source TEXT,       -- initial_sync | chat_action | deduced
    ADD COLUMN IF NOT EXISTS is_bot BOOLEAN;

-- --- 7. Health runtime table --------------------------------------------
CREATE TABLE IF NOT EXISTS runtime_state (
    id INT PRIMARY KEY DEFAULT 1,
    pid INT,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    status TEXT,
    targets JSONB,
    note TEXT,
    CHECK (id = 1)
);
INSERT INTO runtime_state (id) VALUES (1) ON CONFLICT DO NOTHING;
