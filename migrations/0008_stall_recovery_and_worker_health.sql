-- Version 8: stall recovery, coordinator progress watchdog, media worker
-- heartbeats, media "unavailable" terminal status.
BEGIN;

-- --- 1. coordinator per-target progress state -------------------------------
CREATE TABLE IF NOT EXISTS coordinator_progress (
    chat_id BIGINT PRIMARY KEY,
    batch_started_at TIMESTAMPTZ,
    batch_last_progress_at TIMESTAMPTZ,
    batch_last_message_id BIGINT,
    batch_running BOOLEAN NOT NULL DEFAULT FALSE,
    consecutive_batch_failures INT NOT NULL DEFAULT 0,
    last_batch_duration_sec INT,
    last_batch_added INT,
    last_recovery_at TIMESTAMPTZ,
    last_recovery_reason TEXT,
    coordinator_heartbeat_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- --- 2. media worker heartbeats --------------------------------------------
CREATE TABLE IF NOT EXISTS media_worker_state (
    worker_id INT PRIMARY KEY,
    pid INT,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    current_file_id TEXT,
    current_started_at TIMESTAMPTZ,
    completed_count INT NOT NULL DEFAULT 0,
    failed_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- --- 3. media terminal "unavailable" status ---------------------------------
-- status column is free-text TEXT; just document the new value and add an
-- index so health can count them cheaply.
CREATE INDEX IF NOT EXISTS idx_media_status_unavailable
    ON media(first_seen) WHERE status='unavailable';

-- --- 4. target_state degraded / stalled flags -------------------------------
-- backfill_status gets two new terminal-ish values documented but not enforced
-- (storage.set_backfill_status accept-list is widened in code).
--   'stalled'  - progress_timeout tripped once
--   'degraded' - consecutive_batch_failures exceeded threshold

-- --- 5. mark the 15 known-expired file references ---------------------------
-- These are the FileReferenceExpiredError records from the stuck v5 run.
-- v6 collector's refresh_message path will retry them; this just makes them
-- visible in health and distinct from transient failures.
UPDATE media
   SET status = 'needs_refresh',
       last_error = COALESCE(last_error, 'FileReferenceExpired')
 WHERE status = 'failed'
   AND (last_error LIKE '%FileReference%' OR last_error LIKE '%file reference%' OR last_error LIKE '%file_reference%');

-- --- 6. seed coordinator_progress for the single active target ---------------
INSERT INTO coordinator_progress (chat_id, batch_running,
    coordinator_heartbeat_at, updated_at)
SELECT chat_id, FALSE, NOW(), NOW() FROM sync_state
ON CONFLICT (chat_id) DO NOTHING;

COMMIT;
