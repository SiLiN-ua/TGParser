-- Version 9: user identity enrichment (research-backed model).
--
-- Primary identity = Telegram user.id (BIGINT). access_hash is a technical
-- credential that rotates; it never participates in primary keys.
-- Channel senders live in `chats`/`sender_chat_id`, not in `users`.
-- Anonymous admins and hidden forwards do NOT produce fake user rows.

BEGIN;

-- --- users: identity + quality + rotation --------------------------------
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS access_hash BIGINT,
    ADD COLUMN IF NOT EXISTS access_hash_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS is_contact BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_mutual_contact BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_close_friend BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_min BOOLEAN,
    ADD COLUMN IF NOT EXISTS entity_source TEXT,        -- 'message_sender'|'full'|'participant'|'forward'
    ADD COLUMN IF NOT EXISTS entity_quality TEXT,       -- 'min' | 'normal' | 'full'
    ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_full_fetch_error TEXT;

-- defensive: enforce user_id stays positive
ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_id_positive;
ALTER TABLE users
    ADD CONSTRAINT users_id_positive CHECK (id > 0) NOT VALID;
-- `NOT VALID` so existing rows aren't re-scanned (should be fine anyway).

-- --- per-chat observation rows ------------------------------------------
-- Allows seeing which chats the same user_id was seen in without duplicating
-- the person. NO message text is copied here.
CREATE TABLE IF NOT EXISTS user_observations (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    message_id BIGINT,
    observed_at TIMESTAMPTZ DEFAULT NOW(),
    source_type TEXT,            -- 'message_sender' | 'forward' | 'participant'
    entity_quality TEXT,         -- 'min' | 'normal' | 'full'
    username_at_time TEXT,
    display_name_at_time TEXT,
    access_hash_present BOOLEAN,
    raw_meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_user_obs_user ON user_observations(user_id);
CREATE INDEX IF NOT EXISTS idx_user_obs_chat_msg ON user_observations(chat_id, message_id);

-- --- user_profile_history: new fields -----------------------------------
ALTER TABLE user_profile_history
    ADD COLUMN IF NOT EXISTS source_chat_id BIGINT,
    ADD COLUMN IF NOT EXISTS source_message_id BIGINT;

-- --- user_profile_queue: entity-quality-aware status --------------------
-- old status values 'pending'|'done'|'dead' extended with 'waiting_entity'.
-- Not a type, so just allow new string values and document.

-- --- backfill: mark channel-ish entries (mis-categorised) --------------
-- Any row in users whose id matches a chat id canonical form (~-100...)
-- shouldn't exist; the users_id_positive CHECK would catch future inserts.
-- We don't delete historical rows; just report count for diagnostics.
DO $$
DECLARE n INT;
BEGIN
    SELECT count(*) INTO n FROM users WHERE id < 0;
    IF n > 0 THEN
        RAISE NOTICE 'users with negative id (expected 0 after cleanup): %', n;
    END IF;
END $$;

COMMIT;
