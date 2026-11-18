-- Version 2: migrate legacy v0 schema (pre-TGParser rewrite) into v1 shape and canonicalize ids.
-- Idempotent: safe to re-run.

-- 1) Add any columns that v0 did not have.
ALTER TABLE chats
    ADD COLUMN IF NOT EXISTS is_forum BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_broadcast BOOLEAN,
    ADD COLUMN IF NOT EXISTS raw JSONB;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN,
    ADD COLUMN IF NOT EXISTS photo_dc INT,
    ADD COLUMN IF NOT EXISTS photo_file_id BIGINT,
    ADD COLUMN IF NOT EXISTS full_fetched_at TIMESTAMPTZ;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS sender_user_id BIGINT,
    ADD COLUMN IF NOT EXISTS sender_chat_id BIGINT,
    ADD COLUMN IF NOT EXISTS reply_to_peer_id BIGINT,
    ADD COLUMN IF NOT EXISTS thread_id BIGINT,
    ADD COLUMN IF NOT EXISTS post_author TEXT,
    ADD COLUMN IF NOT EXISTS media_group_id BIGINT,
    ADD COLUMN IF NOT EXISTS fwd_from_user_id BIGINT,
    ADD COLUMN IF NOT EXISTS fwd_from_chat_id BIGINT,
    ADD COLUMN IF NOT EXISTS fwd_from_msg_id BIGINT,
    ADD COLUMN IF NOT EXISTS fwd_channel_post TEXT,
    ADD COLUMN IF NOT EXISTS replies_count INT,
    ADD COLUMN IF NOT EXISTS reactions_total INT,
    ADD COLUMN IF NOT EXISTS is_service BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS service_action TEXT,
    ADD COLUMN IF NOT EXISTS service_payload JSONB,
    ADD COLUMN IF NOT EXISTS geo JSONB,
    ADD COLUMN IF NOT EXISTS contact JSONB,
    ADD COLUMN IF NOT EXISTS poll JSONB,
    ADD COLUMN IF NOT EXISTS webpage JSONB;

CREATE INDEX IF NOT EXISTS idx_messages_sender_user ON messages(sender_user_id);
CREATE INDEX IF NOT EXISTS idx_messages_sender_chat ON messages(sender_chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(chat_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(media_group_id) WHERE media_group_id IS NOT NULL;

-- 2) Canonicalize chat ids.

DO $$
DECLARE
    r RECORD;
    canonical_id BIGINT;
    msg_chat_id BIGINT;
BEGIN
    FOR r IN SELECT id FROM chats WHERE id > 0 LOOP
        SELECT DISTINCT chat_id INTO msg_chat_id
          FROM messages
         WHERE chat_id = -1000000000000 - r.id
         LIMIT 1;
        IF msg_chat_id IS NOT NULL THEN
            canonical_id := msg_chat_id;
        ELSE
            canonical_id := -1000000000000 - r.id;
        END IF;

        IF NOT EXISTS (SELECT 1 FROM chats WHERE id = canonical_id) THEN
            UPDATE chats SET id = canonical_id WHERE id = r.id;
        ELSE
            DELETE FROM chats WHERE id = r.id;
        END IF;
    END LOOP;
END$$;

-- Seed sync_state from existing messages (max id per chat = contiguous checkpoint; conservative).
INSERT INTO sync_state (chat_id, last_backfilled_message_id, first_message_id, backfill_complete)
SELECT chat_id, MAX(message_id), MIN(message_id), FALSE
  FROM messages
 GROUP BY chat_id
ON CONFLICT (chat_id) DO UPDATE SET
    last_backfilled_message_id = GREATEST(sync_state.last_backfilled_message_id,
                                          EXCLUDED.last_backfilled_message_id),
    first_message_id = LEAST(COALESCE(sync_state.first_message_id, EXCLUDED.first_message_id),
                             EXCLUDED.first_message_id),
    updated_at = NOW();

-- Legacy columns (message_edits table from v0) stay available for inspection but aren't used by code.
-- We migrate its rows into message_versions so no history is lost.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_name = 'message_edits') THEN
        INSERT INTO message_versions (chat_id, message_id, edit_date, text, captured_at, is_initial)
        SELECT chat_id, message_id, edit_date, new_text, captured_at, FALSE
          FROM message_edits
        ON CONFLICT DO NOTHING;
    END IF;
END$$;

-- Legacy single sender_id column -> sender_user_id when the sender was a real user.
-- We cannot reconstruct what wasn't captured, but if an old 'sender_id' column still exists we copy it.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_name = 'messages' AND column_name = 'sender_id') THEN
        UPDATE messages SET sender_user_id = sender_id
         WHERE sender_user_id IS NULL AND sender_id IS NOT NULL AND sender_id > 0;
        UPDATE messages SET sender_chat_id = sender_id
         WHERE sender_chat_id IS NULL AND sender_id IS NOT NULL AND sender_id < 0;
    END IF;
END$$;

-- Seed media table from existing messages so we don't re-download what's already on disk.
INSERT INTO media (file_id, chat_id, message_id, media_type, local_path, status, completed_at)
SELECT media_file_id, chat_id, message_id, media_type, media_path, 'done', collected_at
  FROM messages
 WHERE media_file_id IS NOT NULL AND media_path IS NOT NULL
ON CONFLICT (file_id) DO NOTHING;
