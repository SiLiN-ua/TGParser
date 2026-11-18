-- Version 1: baseline schema for TGParser

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chats (
    id BIGINT PRIMARY KEY,                -- canonical marked id (e.g. -100... for channels)
    type TEXT,                            -- user | channel | supergroup | group
    title TEXT,
    username TEXT,
    about TEXT,
    participants_count INT,
    linked_chat_id BIGINT,
    is_forum BOOLEAN DEFAULT FALSE,
    is_broadcast BOOLEAN,
    raw JSONB,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    phone TEXT,
    bio TEXT,
    is_bot BOOLEAN,
    is_premium BOOLEAN,
    is_scam BOOLEAN,
    is_fake BOOLEAN,
    is_verified BOOLEAN,
    is_deleted BOOLEAN,
    lang_code TEXT,
    photo_path TEXT,
    photo_dc INT,
    photo_file_id BIGINT,
    raw JSONB,
    full_fetched_at TIMESTAMPTZ,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_profile_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    field TEXT NOT NULL,                  -- username | first_name | last_name | bio | photo_file_id
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_uph_user ON user_profile_history(user_id);

CREATE TABLE IF NOT EXISTS memberships (
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    role TEXT,                            -- member | admin | creator | left | kicked | restricted
    joined_at TIMESTAMPTZ,
    left_at TIMESTAMPTZ,
    raw JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    chat_id BIGINT PRIMARY KEY,
    last_backfilled_message_id BIGINT DEFAULT 0,   -- highest contiguous id seen by backfill
    backfill_complete BOOLEAN DEFAULT FALSE,
    first_message_id BIGINT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS messages (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    sender_user_id BIGINT,                -- when a human/bot posted
    sender_chat_id BIGINT,                -- when a channel posted (broadcast or anon admin)
    date TIMESTAMPTZ,
    edit_date TIMESTAMPTZ,
    reply_to_message_id BIGINT,
    reply_to_top_id BIGINT,
    reply_to_peer_id BIGINT,
    thread_id BIGINT,                     -- forum topic id if applicable
    post_author TEXT,
    text TEXT,
    entities JSONB,
    media_type TEXT,
    media_path TEXT,
    media_file_id TEXT,
    media_group_id BIGINT,                -- grouped album id
    fwd_from_user_id BIGINT,
    fwd_from_chat_id BIGINT,
    fwd_from_name TEXT,
    fwd_from_msg_id BIGINT,
    fwd_date TIMESTAMPTZ,
    fwd_channel_post TEXT,
    views INT,
    forwards INT,
    replies_count INT,
    reactions_total INT,
    is_service BOOLEAN DEFAULT FALSE,
    service_action TEXT,
    service_payload JSONB,
    geo JSONB,
    contact JSONB,
    poll JSONB,
    webpage JSONB,
    raw JSONB,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chat_id, message_id)
);

-- If the table pre-existed from legacy v0 schema, bring it up to v1.
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

ALTER TABLE chats
    ADD COLUMN IF NOT EXISTS is_forum BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_broadcast BOOLEAN,
    ADD COLUMN IF NOT EXISTS raw JSONB;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN,
    ADD COLUMN IF NOT EXISTS photo_dc INT,
    ADD COLUMN IF NOT EXISTS photo_file_id BIGINT,
    ADD COLUMN IF NOT EXISTS full_fetched_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_messages_sender_user ON messages(sender_user_id);
CREATE INDEX IF NOT EXISTS idx_messages_sender_chat ON messages(sender_chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC);
CREATE INDEX IF NOT EXISTS idx_messages_reply ON messages(chat_id, reply_to_message_id);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(chat_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_group ON messages(media_group_id) WHERE media_group_id IS NOT NULL;

-- Keep every version we observe: text and media changes alike.
CREATE TABLE IF NOT EXISTS message_versions (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    edit_date TIMESTAMPTZ,                -- Telegram-reported edit timestamp (may be NULL for initial version)
    text TEXT,
    entities JSONB,
    media_type TEXT,
    media_path TEXT,
    media_file_id TEXT,
    captured_at TIMESTAMPTZ DEFAULT NOW(),
    is_initial BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_mver_msg ON message_versions(chat_id, message_id, captured_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_mver_dedup
    ON message_versions(chat_id, message_id, COALESCE(edit_date, 'epoch'::timestamptz));

CREATE TABLE IF NOT EXISTS message_reactions (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    reaction TEXT NOT NULL,               -- emoji or "custom:<id>"
    count INT NOT NULL,
    chosen_order INT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chat_id, message_id, reaction)
);

CREATE TABLE IF NOT EXISTS message_reaction_authors (
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    reaction TEXT NOT NULL,
    peer_id BIGINT NOT NULL,
    peer_kind TEXT,                       -- user | channel
    reacted_at TIMESTAMPTZ,
    captured_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (chat_id, message_id, reaction, peer_id)
);

CREATE TABLE IF NOT EXISTS media (
    file_id TEXT PRIMARY KEY,
    chat_id BIGINT,
    message_id BIGINT,
    media_type TEXT,
    mime_type TEXT,
    size BIGINT,
    file_name TEXT,
    local_path TEXT,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | done | skipped | failed
    attempts INT DEFAULT 0,
    last_error TEXT,
    duration INT,
    width INT,
    height INT,
    sha256 TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_media_status ON media(status);
CREATE INDEX IF NOT EXISTS idx_media_msg ON media(chat_id, message_id);

CREATE TABLE IF NOT EXISTS chat_actions (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT,
    message_id BIGINT,
    actor_user_id BIGINT,
    action TEXT,
    payload JSONB,
    at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_actions_chat ON chat_actions(chat_id, at DESC);

CREATE TABLE IF NOT EXISTS errors (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ DEFAULT NOW(),
    kind TEXT,
    chat_id BIGINT,
    message_id BIGINT,
    user_id BIGINT,
    message TEXT,
    traceback TEXT
);
CREATE INDEX IF NOT EXISTS idx_errors_at ON errors(at DESC);
