-- Version 4: final hardening.
--   * safe type-aware canonical-id repair (user stays positive, basic group -id, channel -100id)
--   * target_state (per-target runtime + sync status)
--   * sync_status enum-like TEXT (running | complete | incomplete | blocked | complete_with_errors)
--   * telegram heartbeat columns on runtime_state
--   * backfill_queue acknowledgement + status progression
--   * media metadata_only status + atomic rename lifecycle columns
--   * deletion_events: full peer + raw_payload + resolved_at
--   * FKs where they do not break deletion stubs
--   * everything idempotent, re-runnable, wrapped in a top-level transaction

BEGIN;

-- --- 1. Type-aware canonical-id repair (safety net for migration 0002) ----
-- 0002 unconditionally flipped positive chats.id when it matched a -100<id>
-- on messages. For 'user' that must stay positive. For basic 'group' the
-- canonical form is -<raw_id>. For channel/supergroup it's -100<raw_id>.
-- We only touch rows whose current id disagrees with the correct form for
-- their type. The one real channel in prod (-1001006503122) is already
-- correct → no-op for it.
DO $$
DECLARE
    r RECORD;
    want BIGINT;
BEGIN
    FOR r IN SELECT id, type FROM chats LOOP
        IF r.type IS NULL THEN CONTINUE; END IF;
        IF r.type = 'user' THEN
            -- users must be positive; strip any accidental -100 prefix
            IF r.id < 0 AND r.id::text LIKE '-100%' THEN
                want := CAST(SUBSTRING(r.id::text FROM 5) AS BIGINT);
                UPDATE chats SET id = want WHERE id = r.id;
            END IF;
        ELSIF r.type = 'group' THEN
            IF r.id > 0 THEN
                want := -r.id;
                UPDATE chats SET id = want WHERE id = r.id;
            ELSIF r.id::text LIKE '-100%' THEN
                want := -CAST(SUBSTRING(r.id::text FROM 5) AS BIGINT);
                UPDATE chats SET id = want WHERE id = r.id;
            END IF;
        ELSIF r.type IN ('channel','supergroup') THEN
            IF r.id > 0 THEN
                want := CAST('-100' || r.id::text AS BIGINT);
                UPDATE chats SET id = want WHERE id = r.id;
            END IF;
        END IF;
    END LOOP;
END $$;

-- --- 2. Per-target state ---------------------------------------------------
CREATE TABLE IF NOT EXISTS target_state (
    chat_id BIGINT PRIMARY KEY,
    target TEXT NOT NULL,
    backfill_status TEXT NOT NULL DEFAULT 'running',
        -- running | complete | incomplete | blocked | complete_with_errors
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error TEXT,
    pending_count INT DEFAULT 0,
    dead_count INT DEFAULT 0,
    note TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_target_state_status ON target_state(backfill_status);

-- --- 3. Telegram heartbeat on runtime_state --------------------------------
ALTER TABLE runtime_state
    ADD COLUMN IF NOT EXISTS telegram_connected BOOLEAN,
    ADD COLUMN IF NOT EXISTS telegram_last_ok_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS telegram_last_error TEXT,
    ADD COLUMN IF NOT EXISTS update_last_received_at TIMESTAMPTZ;

-- --- 4. Deletion events: full payload + resolution flag --------------------
ALTER TABLE deletion_events
    ADD COLUMN IF NOT EXISTS channel_id BIGINT,
    ADD COLUMN IF NOT EXISTS update_type TEXT,
    ADD COLUMN IF NOT EXISTS raw_payload JSONB,
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_deletion_events_unresolved
    ON deletion_events(received_at)
    WHERE resolved_at IS NULL;

-- --- 5. Media: metadata-only + atomic lifecycle -----------------------------
-- status is already free-text; we add: 'metadata_only' for unsupported media
-- that cannot be downloaded (webpage, geo, contact, poll, etc.) plus
-- lifecycle columns so a partial file never looks 'done'.
ALTER TABLE media
    ADD COLUMN IF NOT EXISTS is_downloadable BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS media_group_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_media_group ON media(media_group_id)
    WHERE media_group_id IS NOT NULL;

-- --- 6. Backfill queue status progression ----------------------------------
ALTER TABLE backfill_queue
    ADD COLUMN IF NOT EXISTS acknowledged BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS acknowledged_by TEXT;

-- --- 7. Sync status on sync_state -------------------------------------------
ALTER TABLE sync_state
    ADD COLUMN IF NOT EXISTS backfill_status TEXT DEFAULT 'running';

UPDATE sync_state SET backfill_status = 'complete'
    WHERE backfill_complete IS TRUE AND backfill_status = 'running';

-- --- 8. Foreign keys (idempotent) ------------------------------------------
-- Use ON DELETE CASCADE where safe; NO action where child rows must outlive
-- their parent (deletion stubs, backfill_queue entries).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_mm_chat_msg') THEN
        ALTER TABLE message_media
            ADD CONSTRAINT fk_mm_chat_msg
            FOREIGN KEY (chat_id, message_id)
            REFERENCES messages(chat_id, message_id)
            ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_mm_media') THEN
        ALTER TABLE message_media
            ADD CONSTRAINT fk_mm_media
            FOREIGN KEY (file_id) REFERENCES media(file_id)
            ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_mr_chat_msg') THEN
        ALTER TABLE message_reactions
            ADD CONSTRAINT fk_mr_chat_msg
            FOREIGN KEY (chat_id, message_id)
            REFERENCES messages(chat_id, message_id)
            ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_mra_chat_msg') THEN
        ALTER TABLE message_reaction_authors
            ADD CONSTRAINT fk_mra_chat_msg
            FOREIGN KEY (chat_id, message_id)
            REFERENCES messages(chat_id, message_id)
            ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_mv_chat_msg') THEN
        ALTER TABLE message_versions
            ADD CONSTRAINT fk_mv_chat_msg
            FOREIGN KEY (chat_id, message_id)
            REFERENCES messages(chat_id, message_id)
            ON DELETE CASCADE;
    END IF;
END $$;

-- --- 9. Seed target_state from current sync_state --------------------------
INSERT INTO target_state (chat_id, target, backfill_status, last_success_at)
SELECT s.chat_id,
       COALESCE(c.username, c.title, s.chat_id::text) AS target,
       CASE WHEN s.backfill_complete THEN 'complete' ELSE 'running' END AS backfill_status,
       s.updated_at
FROM sync_state s
LEFT JOIN chats c ON c.id = s.chat_id
ON CONFLICT (chat_id) DO NOTHING;

-- --- 10. Message media metadata_only backfill ------------------------------
-- Rows that were seeded as 'other:...' in migration 0003 and never downloaded
-- are metadata only — mark them so the retry sweep stops picking them up.
UPDATE media
   SET status = 'metadata_only', is_downloadable = FALSE
 WHERE file_id LIKE 'other:%'
   AND status IN ('pending','failed');

COMMIT;
