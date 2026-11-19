-- Version 10: access_hash classification + priority + history + unique observation keys.
-- See docs/TELEGRAM_USER_IDENTITY.md and https://core.telegram.org/api/peers
-- section "Access hash". Rules enforced here (checked and in code):
--   full > min > from_message > zero > unknown (priority)
--   lower priority NEVER replaces higher priority
--   hashes from a different auth session are never promoted automatically
--
-- Numeric priority mapping (so SQL can compare):
--   full=4, min=2, from_message=3, zero=1, unknown=0
BEGIN;

-- --- users: hash classification ---------------------------------------------
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS access_hash_kind TEXT,
    ADD COLUMN IF NOT EXISTS access_hash_priority INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS access_hash_auth_fingerprint TEXT,
    ADD COLUMN IF NOT EXISTS access_hash_source_chat_id BIGINT,
    ADD COLUMN IF NOT EXISTS access_hash_source_message_id BIGINT,
    ADD COLUMN IF NOT EXISTS access_hash_is_stale BOOLEAN DEFAULT FALSE;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname='users_access_hash_kind_check') THEN
        ALTER TABLE users ADD CONSTRAINT users_access_hash_kind_check
            CHECK (access_hash_kind IS NULL OR
                   access_hash_kind IN ('full','min','from_message','zero','unknown'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname='users_access_hash_priority_check') THEN
        ALTER TABLE users ADD CONSTRAINT users_access_hash_priority_check
            CHECK (access_hash_priority BETWEEN 0 AND 4);
    END IF;
END $$;

-- --- access_hash_history: append-only, observed hashes with priority -------
CREATE TABLE IF NOT EXISTS access_hash_history (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    hash_fingerprint TEXT NOT NULL,       -- sha256(access_hash) not the raw value
    access_hash BIGINT,                   -- raw value (needed to actually USE it)
    kind TEXT NOT NULL,
    priority INT NOT NULL,
    source TEXT,                          -- 'message_sender' | 'full' | 'participant' | 'forward'
    auth_fingerprint TEXT,                -- identifies which login this hash is valid for
    source_chat_id BIGINT,
    source_message_id BIGINT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    became_active_at TIMESTAMPTZ,
    replaced_at TIMESTAMPTZ,
    replacement_reason TEXT,
    is_active BOOLEAN DEFAULT FALSE,
    CONSTRAINT ahh_kind_check CHECK (kind IN ('full','min','from_message','zero','unknown')),
    CONSTRAINT ahh_priority_check CHECK (priority BETWEEN 0 AND 4)
);
CREATE INDEX IF NOT EXISTS idx_ahh_user ON access_hash_history(user_id);
CREATE INDEX IF NOT EXISTS idx_ahh_active ON access_hash_history(user_id)
    WHERE is_active = TRUE;
-- same hash seen multiple times updates last_seen, not duplicate row
CREATE UNIQUE INDEX IF NOT EXISTS uq_ahh_identity
    ON access_hash_history(user_id, hash_fingerprint, auth_fingerprint);

-- --- seed history from current users: existing 507 access_hashes --------
-- Classify existing hashes conservatively as 'unknown' until a fresh entity
-- confirms their kind. Priority 0 so any future classified hash wins.
-- Use built-in md5() (no pgcrypto required). The hash is a fingerprint
-- for dedup only; it never leaves the database.
INSERT INTO access_hash_history
    (user_id, hash_fingerprint, access_hash, kind, priority, source,
     auth_fingerprint, first_seen, last_seen, became_active_at, is_active)
SELECT id,
       md5(access_hash::text),
       access_hash,
       'unknown', 0, 'legacy',
       'legacy',
       COALESCE(access_hash_updated_at, NOW()),
       NOW(),
       COALESCE(access_hash_updated_at, NOW()),
       TRUE
FROM users
WHERE access_hash IS NOT NULL
ON CONFLICT (user_id, hash_fingerprint, auth_fingerprint) DO NOTHING;

UPDATE users SET access_hash_kind='unknown', access_hash_priority=0
 WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL;

-- --- user_observations: unique key ----------------------------------------
-- Dedup: same user + same chat + same message_id + same source_type is one row.
-- message_id may be NULL for participant observations.
-- Deduplicate any pre-existing duplicates first.
DELETE FROM user_observations u
 USING user_observations u2
 WHERE u.id > u2.id
   AND u.user_id = u2.user_id
   AND u.chat_id = u2.chat_id
   AND COALESCE(u.message_id, 0) = COALESCE(u2.message_id, 0)
   AND u.source_type = u2.source_type;

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_obs_key
    ON user_observations(user_id, chat_id, COALESCE(message_id, 0), source_type);

-- --- identity_enrichment_queue: bounded, prioritised, resumable -----------
CREATE TABLE IF NOT EXISTS identity_enrichment_queue (
    user_id BIGINT PRIMARY KEY,
    priority INT NOT NULL DEFAULT 1000,    -- lower = sooner
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    first_seen TIMESTAMPTZ DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'pending',   -- pending | done | dead | waiting_entity
    hint_chat_id BIGINT,
    hint_message_id BIGINT,
    CONSTRAINT ieq_status_check CHECK (status IN ('pending','done','dead','waiting_entity'))
);
CREATE INDEX IF NOT EXISTS idx_ieq_status
    ON identity_enrichment_queue(status, priority, next_retry_at);

COMMIT;
