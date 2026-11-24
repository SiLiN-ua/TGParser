-- Version 11: dynamic targets foundation (additive, gate off).
--
-- Promoted from design/0011_dynamic_targets.draft.sql (no logic change) after
-- stage-2 rehearsal of upgrade, rollback and gate-off compatibility on
-- temporary databases.
--
-- Contract:
--   * Fully re-runnable: a second execution changes no rows, no timestamps,
--     no policy version and sends no NOTIFY.
--   * The chat write guard is gated on runtime_state.dyn_engine (the engine
--     actually running), never on deployment_gates. Gate-off cost is one
--     single-row SELECT per guarded row (see benchmark in the stage report).
--   * No session-level bypass of the guard exists.
--   * Intent tables (collection_config, target_preferences,
--     collection_title_rules) bump policy_state.requested_version once per
--     statement that really changes policy fields; the bump and its NOTIFY
--     are transactional (rollback leaves nothing).
--   * Discovery writes only to staging (dialog_discovery_items); the live
--     snapshot (discovered_dialogs) changes only in the single transaction
--     that completes a run with both passes done.
--   * Mass-gone rule: block if gone_count > 5 OR gone_count * 5 > active_count;
--     active_count = 0 never blocks. Example: 100 active / 20 gone -> block
--     (count branch).
--
-- migrations.py wraps a file in one con.transaction(); no BEGIN/COMMIT here.

-- =============================================================================
-- 1. deployment_gates (deployment-only; UI/config code never writes here)
-- =============================================================================
CREATE TABLE IF NOT EXISTS deployment_gates (
    key TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT,
    note TEXT,
    CONSTRAINT dg_key_fmt CHECK (key ~ '^[a-z][a-z0-9_]{1,63}$'),
    CONSTRAINT dg_updated_by_len CHECK (updated_by IS NULL OR char_length(updated_by) <= 80),
    CONSTRAINT dg_note_len CHECK (note IS NULL OR char_length(note) <= 500)
);
INSERT INTO deployment_gates (key, enabled, note) VALUES
    ('dynamic_targets', FALSE, 'off; admin CLI + managed restart only')
ON CONFLICT (key) DO NOTHING;

-- =============================================================================
-- 2. collection_config (singleton)
-- =============================================================================
CREATE TABLE IF NOT EXISTS collection_config (
    id INT PRIMARY KEY DEFAULT 1,
    mode TEXT NOT NULL DEFAULT 'selected',
    include_archived BOOLEAN NOT NULL DEFAULT FALSE,
    category_megagroup BOOLEAN NOT NULL DEFAULT TRUE,
    category_channel   BOOLEAN NOT NULL DEFAULT TRUE,
    category_group     BOOLEAN NOT NULL DEFAULT TRUE,
    category_private   BOOLEAN NOT NULL DEFAULT FALSE,
    category_bot       BOOLEAN NOT NULL DEFAULT FALSE,
    discovery_interval_sec INT NOT NULL DEFAULT 300,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT,
    CONSTRAINT cc_singleton CHECK (id = 1),
    CONSTRAINT cc_mode CHECK (mode IN ('selected','all')),
    CONSTRAINT cc_interval CHECK (discovery_interval_sec BETWEEN 60 AND 86400),
    CONSTRAINT cc_updated_by_len CHECK (updated_by IS NULL OR char_length(updated_by) <= 80)
);
INSERT INTO collection_config (id) VALUES (1) ON CONFLICT DO NOTHING;

-- =============================================================================
-- 3. collection_title_rules (exclude-only; include-rules do not exist in v1)
-- =============================================================================
CREATE TABLE IF NOT EXISTS collection_title_rules (
    id BIGSERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    pattern TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT ctr_action CHECK (action = 'exclude'),
    CONSTRAINT ctr_pattern_len CHECK (char_length(pattern) BETWEEN 2 AND 120),
    CONSTRAINT ctr_pattern_noctl CHECK (pattern !~ '[[:cntrl:]]'),
    CONSTRAINT ctr_created_by_len CHECK (created_by IS NULL OR char_length(created_by) <= 80)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ctr_pattern ON collection_title_rules(lower(pattern));

-- =============================================================================
-- 4. target_preferences (human intent; collector never writes)
-- =============================================================================
CREATE TABLE IF NOT EXISTS target_preferences (
    chat_id BIGINT PRIMARY KEY,
    selected BOOLEAN NOT NULL DEFAULT FALSE,
    excluded BOOLEAN NOT NULL DEFAULT FALSE,
    note TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT,
    CONSTRAINT tp_not_both CHECK (NOT (selected AND excluded)),
    CONSTRAINT tp_note_len CHECK (note IS NULL OR char_length(note) <= 500),
    CONSTRAINT tp_updated_by_len CHECK (updated_by IS NULL OR char_length(updated_by) <= 80)
);

-- =============================================================================
-- 5. yaml_target_imports (YAML only ADDS; retry state)
-- =============================================================================
CREATE TABLE IF NOT EXISTS yaml_target_imports (
    id BIGSERIAL PRIMARY KEY,
    raw_target TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    last_error_class TEXT,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    resolved_chat_id BIGINT,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT yti_status CHECK (status IN ('pending','retry','selected','terminal')),
    CONSTRAINT yti_attempts CHECK (attempts BETWEEN 0 AND 1000),
    CONSTRAINT yti_raw_len CHECK (char_length(raw_target) BETWEEN 1 AND 300),
    CONSTRAINT yti_raw_noctl CHECK (raw_target !~ '[[:cntrl:]]'),
    CONSTRAINT yti_err_fmt CHECK (last_error_class IS NULL
                                  OR last_error_class ~ '^[A-Za-z0-9_.:]{1,120}$'),
    CONSTRAINT yti_selected_has_id CHECK (status <> 'selected' OR resolved_chat_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_yti_status ON yaml_target_imports(status, next_retry_at);

-- =============================================================================
-- 6. discovery: runs, staging items, live snapshot, control singleton
-- =============================================================================
CREATE TABLE IF NOT EXISTS dialog_discovery_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    trigger_kind TEXT NOT NULL,
    normal_pass_done BOOLEAN NOT NULL DEFAULT FALSE,
    archived_pass_done BOOLEAN NOT NULL DEFAULT FALSE,
    dialogs_seen INT NOT NULL DEFAULT 0,
    gone_detected INT NOT NULL DEFAULT 0,
    error TEXT,
    CONSTRAINT ddr_status CHECK (status IN ('running','complete','failed','cancelled')),
    CONSTRAINT ddr_trigger CHECK (trigger_kind IN ('scheduled','request','startup')),
    CONSTRAINT ddr_error_len CHECK (error IS NULL OR char_length(error) <= 500),
    CONSTRAINT ddr_complete_passes CHECK (status <> 'complete'
                                          OR (normal_pass_done AND archived_pass_done))
);
CREATE INDEX IF NOT EXISTS idx_ddr_status ON dialog_discovery_runs(status, started_at DESC);
-- At most one running discovery run, enforced by the database itself.
CREATE UNIQUE INDEX IF NOT EXISTS uq_ddr_one_running
    ON dialog_discovery_runs ((TRUE)) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS dialog_discovery_items (
    run_id BIGINT NOT NULL REFERENCES dialog_discovery_runs(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    category TEXT NOT NULL,
    title TEXT,
    username TEXT,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    participants_count INT,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, chat_id),
    CONSTRAINT ddi_category CHECK (category IN ('megagroup','channel','group','private',
                                                'bot','saved','unknown','forbidden')),
    CONSTRAINT ddi_title_len CHECK (title IS NULL OR char_length(title) <= 256),
    CONSTRAINT ddi_username_len CHECK (username IS NULL OR char_length(username) <= 64)
);

CREATE TABLE IF NOT EXISTS discovered_dialogs (
    chat_id BIGINT PRIMARY KEY,
    category TEXT NOT NULL,
    title TEXT,
    username TEXT,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    participants_count INT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_run_id BIGINT,
    gone_at TIMESTAMPTZ,
    CONSTRAINT dd_category CHECK (category IN ('megagroup','channel','group','private',
                                               'bot','saved','unknown','forbidden')),
    CONSTRAINT dd_title_len CHECK (title IS NULL OR char_length(title) <= 256),
    CONSTRAINT dd_username_len CHECK (username IS NULL OR char_length(username) <= 64)
);
CREATE INDEX IF NOT EXISTS idx_dd_category ON discovered_dialogs(category);
CREATE INDEX IF NOT EXISTS idx_dd_gone ON discovered_dialogs(gone_at) WHERE gone_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS discovery_control (
    id INT PRIMARY KEY DEFAULT 1,
    requested_seq BIGINT NOT NULL DEFAULT 0,
    consumed_seq  BIGINT NOT NULL DEFAULT 0,
    last_requested_at TIMESTAMPTZ,
    last_requested_by TEXT,
    last_consumed_at TIMESTAMPTZ,
    last_run_id BIGINT,
    next_allowed_at TIMESTAMPTZ,
    backoff_level INT NOT NULL DEFAULT 0,
    CONSTRAINT dc_singleton CHECK (id = 1),
    CONSTRAINT dc_seq CHECK (consumed_seq <= requested_seq),
    CONSTRAINT dc_backoff CHECK (backoff_level BETWEEN 0 AND 20),
    CONSTRAINT dc_req_by_len CHECK (last_requested_by IS NULL OR char_length(last_requested_by) <= 80)
);
INSERT INTO discovery_control (id) VALUES (1) ON CONFLICT DO NOTHING;

-- =============================================================================
-- 7. policy_state (singleton) + mass-gone block history
-- =============================================================================
CREATE TABLE IF NOT EXISTS policy_state (
    id INT PRIMARY KEY DEFAULT 1,
    requested_version BIGINT NOT NULL DEFAULT 0,
    applied_version   BIGINT NOT NULL DEFAULT 0,
    requested_at TIMESTAMPTZ,
    applied_at   TIMESTAMPTZ,
    -- CURRENT block (NULL = no current block)
    blocked_version     BIGINT,
    blocked_reason      TEXT,
    blocked_count       INT,
    blocked_fingerprint TEXT,
    blocked_at          TIMESTAMPTZ,
    -- LAST confirmation (kept for audit; valid only if it matches the current block)
    confirmed_version     BIGINT,
    confirmed_fingerprint TEXT,
    confirmed_at          TIMESTAMPTZ,
    confirmed_by          TEXT,
    -- last reconcile attempt of the dynamic engine (visible state/error)
    reconcile_version BIGINT,
    reconcile_state   TEXT,
    reconcile_error   TEXT,
    reconciled_at     TIMESTAMPTZ,
    CONSTRAINT ps_singleton CHECK (id = 1),
    CONSTRAINT ps_reconcile_state CHECK (reconcile_state IS NULL OR reconcile_state IN
        ('ok','awaiting_discovery','mass_gone_blocked','error')),
    CONSTRAINT ps_reconcile_error CHECK (reconcile_error IS NULL OR char_length(reconcile_error) <= 500),
    CONSTRAINT ps_applied_le CHECK (applied_version <= requested_version),
    CONSTRAINT ps_blocked_reason CHECK (blocked_reason IS NULL OR blocked_reason IN ('mass_gone')),
    CONSTRAINT ps_blocked_fp CHECK (blocked_fingerprint IS NULL OR blocked_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ps_confirmed_fp CHECK (confirmed_fingerprint IS NULL OR confirmed_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ps_confirmed_by CHECK (confirmed_by IS NULL OR confirmed_by IN ('admin_cli')),
    CONSTRAINT ps_block_complete CHECK (
        (blocked_version IS NULL AND blocked_fingerprint IS NULL AND blocked_count IS NULL)
        OR (blocked_version IS NOT NULL AND blocked_fingerprint IS NOT NULL AND blocked_count IS NOT NULL))
);
INSERT INTO policy_state (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS policy_block_history (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event TEXT NOT NULL,
    policy_version BIGINT NOT NULL,
    fingerprint TEXT NOT NULL,
    gone_count INT,
    active_count INT,
    actor TEXT NOT NULL,
    CONSTRAINT pbh_event CHECK (event IN ('blocked','confirmed','cleared')),
    CONSTRAINT pbh_fp CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT pbh_actor CHECK (actor IN ('collector','admin_cli'))
);

CREATE OR REPLACE FUNCTION tgp_policy_notify() RETURNS trigger AS $$
BEGIN
    IF NEW.requested_version IS DISTINCT FROM OLD.requested_version THEN
        PERFORM pg_notify('tgp_policy', NEW.requested_version::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_policy_notify ON policy_state;
CREATE TRIGGER trg_policy_notify
    AFTER UPDATE ON policy_state
    FOR EACH ROW EXECUTE FUNCTION tgp_policy_notify();

-- =============================================================================
-- 8. ui_audit_log (strict constraints; never request bodies or secrets)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ui_audit_log (
    id BIGSERIAL PRIMARY KEY,
    at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    client_ip INET,
    user_agent TEXT,
    error TEXT,
    CONSTRAINT ual_actor CHECK (actor IN ('ui','admin_cli','system')),
    CONSTRAINT ual_action CHECK (action ~ '^[a-z][a-z0-9_.:-]{1,63}$'),
    CONSTRAINT ual_outcome CHECK (outcome IN ('ok','rejected','error')),
    CONSTRAINT ual_detail_obj CHECK (jsonb_typeof(detail) = 'object'),
    CONSTRAINT ual_detail_size CHECK (pg_column_size(detail) <= 4096),
    CONSTRAINT ual_ua_len CHECK (user_agent IS NULL OR char_length(user_agent) <= 200),
    CONSTRAINT ual_error_len CHECK (error IS NULL OR char_length(error) <= 500)
);
CREATE INDEX IF NOT EXISTS idx_ui_audit_at ON ui_audit_log(at DESC);

-- =============================================================================
-- 9. runtime_state dynamic-engine indicator; target_state label
-- =============================================================================
ALTER TABLE runtime_state
    ADD COLUMN IF NOT EXISTS dyn_engine BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS dyn_engine_set_at TIMESTAMPTZ;

ALTER TABLE target_state
    ADD COLUMN IF NOT EXISTS label TEXT;

-- =============================================================================
-- 10. intent -> policy version bump (statement-level, transactional)
-- =============================================================================
-- tgp_bump_policy is the single place the intent triggers bump the version.
-- policy_state has only the NOTIFY trigger, so there is no recursion.
CREATE OR REPLACE FUNCTION tgp_bump_policy() RETURNS void AS $$
    UPDATE policy_state
       SET requested_version = requested_version + 1,
           requested_at = NOW()
     WHERE id = 1;
$$ LANGUAGE sql;

-- Generic INSERT/DELETE handlers: bump iff the statement touched >= 1 row.
CREATE OR REPLACE FUNCTION tgp_intent_new_rows() RETURNS trigger AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM nt) THEN PERFORM tgp_bump_policy(); END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tgp_intent_old_rows() RETURNS trigger AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM ot) THEN PERFORM tgp_bump_policy(); END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tgp_intent_truncate() RETURNS trigger AS $$
BEGIN
    PERFORM tgp_bump_policy();
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- UPDATE handlers compare only policy-relevant columns (multiset difference),
-- so note/updated_at/updated_by/interval-only updates do not bump.
CREATE OR REPLACE FUNCTION tgp_intent_prefs_upd() RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        (SELECT chat_id, selected, excluded FROM nt
         EXCEPT ALL SELECT chat_id, selected, excluded FROM ot)
        UNION ALL
        (SELECT chat_id, selected, excluded FROM ot
         EXCEPT ALL SELECT chat_id, selected, excluded FROM nt)
    ) THEN
        PERFORM tgp_bump_policy();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tgp_intent_config_upd() RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        (SELECT mode, include_archived, category_megagroup, category_channel,
                category_group, category_private, category_bot FROM nt
         EXCEPT ALL
         SELECT mode, include_archived, category_megagroup, category_channel,
                category_group, category_private, category_bot FROM ot)
        UNION ALL
        (SELECT mode, include_archived, category_megagroup, category_channel,
                category_group, category_private, category_bot FROM ot
         EXCEPT ALL
         SELECT mode, include_archived, category_megagroup, category_channel,
                category_group, category_private, category_bot FROM nt)
    ) THEN
        PERFORM tgp_bump_policy();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tgp_intent_rules_upd() RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        (SELECT id, action, pattern FROM nt EXCEPT ALL SELECT id, action, pattern FROM ot)
        UNION ALL
        (SELECT id, action, pattern FROM ot EXCEPT ALL SELECT id, action, pattern FROM nt)
    ) THEN
        PERFORM tgp_bump_policy();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- PostgreSQL allows transition tables only on single-event triggers, hence
-- one trigger per event.
DROP TRIGGER IF EXISTS trg_intent_prefs_ins ON target_preferences;
CREATE TRIGGER trg_intent_prefs_ins AFTER INSERT ON target_preferences
    REFERENCING NEW TABLE AS nt
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_new_rows();
DROP TRIGGER IF EXISTS trg_intent_prefs_upd ON target_preferences;
CREATE TRIGGER trg_intent_prefs_upd AFTER UPDATE ON target_preferences
    REFERENCING OLD TABLE AS ot NEW TABLE AS nt
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_prefs_upd();
DROP TRIGGER IF EXISTS trg_intent_prefs_del ON target_preferences;
CREATE TRIGGER trg_intent_prefs_del AFTER DELETE ON target_preferences
    REFERENCING OLD TABLE AS ot
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_old_rows();
DROP TRIGGER IF EXISTS trg_intent_prefs_trunc ON target_preferences;
CREATE TRIGGER trg_intent_prefs_trunc AFTER TRUNCATE ON target_preferences
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_truncate();

DROP TRIGGER IF EXISTS trg_intent_config_upd ON collection_config;
CREATE TRIGGER trg_intent_config_upd AFTER UPDATE ON collection_config
    REFERENCING OLD TABLE AS ot NEW TABLE AS nt
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_config_upd();

DROP TRIGGER IF EXISTS trg_intent_rules_ins ON collection_title_rules;
CREATE TRIGGER trg_intent_rules_ins AFTER INSERT ON collection_title_rules
    REFERENCING NEW TABLE AS nt
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_new_rows();
DROP TRIGGER IF EXISTS trg_intent_rules_upd ON collection_title_rules;
CREATE TRIGGER trg_intent_rules_upd AFTER UPDATE ON collection_title_rules
    REFERENCING OLD TABLE AS ot NEW TABLE AS nt
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_rules_upd();
DROP TRIGGER IF EXISTS trg_intent_rules_del ON collection_title_rules;
CREATE TRIGGER trg_intent_rules_del AFTER DELETE ON collection_title_rules
    REFERENCING OLD TABLE AS ot
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_old_rows();
DROP TRIGGER IF EXISTS trg_intent_rules_trunc ON collection_title_rules;
CREATE TRIGGER trg_intent_rules_trunc AFTER TRUNCATE ON collection_title_rules
    FOR EACH STATEMENT EXECUTE FUNCTION tgp_intent_truncate();

-- =============================================================================
-- 11. v_target_policy — strict truth table over the UNION of known ids
-- =============================================================================
-- ids = discovered_dialogs ∪ target_preferences ∪ target_state, so a manual
-- selection survives without discovery.
-- Priority:
--   excluded > dialog_gone > forbidden
--   > selected mode: selected | not_selected
--   > all mode: not_discovered > saved_or_unknown > archived_policy
--               > category_off > exclude_rule > in_collection
-- desired = reason IN ('selected','in_collection').
-- Exclude rules match a case-insensitive plain substring (no wildcards) of
-- "title username".
CREATE OR REPLACE VIEW v_target_policy AS
WITH cfg AS (
    SELECT * FROM collection_config WHERE id = 1
), ids AS (
    SELECT chat_id FROM discovered_dialogs
    UNION
    SELECT chat_id FROM target_preferences
    UNION
    SELECT chat_id FROM target_state
), base AS (
    SELECT
        i.chat_id,
        (d.chat_id IS NOT NULL) AS discovered,
        d.category,
        COALESCE(d.title, c.title) AS title,
        COALESCE(d.username, c.username) AS username,
        COALESCE(d.is_archived, FALSE) AS is_archived,
        d.gone_at,
        (p.chat_id IS NOT NULL) AS has_preference,
        COALESCE(p.selected, FALSE) AS selected,
        COALESCE(p.excluded, FALSE) AS excluded,
        (ts.chat_id IS NOT NULL) AS in_target_state,
        COALESCE(ts.enabled, FALSE) AS current_enabled,
        cfg.mode,
        cfg.include_archived,
        CASE d.category
            WHEN 'megagroup' THEN cfg.category_megagroup
            WHEN 'channel'   THEN cfg.category_channel
            WHEN 'group'     THEN cfg.category_group
            WHEN 'private'   THEN cfg.category_private
            WHEN 'bot'       THEN cfg.category_bot
            ELSE FALSE
        END AS category_on,
        EXISTS (
            SELECT 1 FROM collection_title_rules r
            WHERE r.action = 'exclude'
              AND strpos(lower(COALESCE(d.title, c.title, '') || ' '
                               || COALESCE(d.username, c.username, '')),
                         lower(r.pattern)) > 0
        ) AS rule_excluded
    FROM ids i
    LEFT JOIN discovered_dialogs d ON d.chat_id = i.chat_id
    LEFT JOIN target_preferences p ON p.chat_id = i.chat_id
    LEFT JOIN target_state ts ON ts.chat_id = i.chat_id
    LEFT JOIN chats c ON c.id = i.chat_id
    CROSS JOIN cfg
), r AS (
    SELECT b.*,
        CASE
            WHEN b.excluded                              THEN 'excluded'
            WHEN b.gone_at IS NOT NULL                   THEN 'dialog_gone'
            WHEN b.category = 'forbidden'                THEN 'forbidden'
            WHEN b.mode = 'selected' THEN
                CASE WHEN b.selected THEN 'selected' ELSE 'not_selected' END
            WHEN NOT b.discovered                        THEN 'not_discovered'
            WHEN b.category IN ('saved','unknown')       THEN 'saved_or_unknown'
            WHEN b.is_archived AND NOT b.include_archived THEN 'archived_policy'
            WHEN NOT b.category_on                       THEN 'category_off'
            WHEN b.rule_excluded                         THEN 'exclude_rule'
            ELSE 'in_collection'
        END AS reason
    FROM base b
)
SELECT chat_id, discovered, category, title, username, is_archived, gone_at,
       has_preference, selected, excluded, in_target_state, current_enabled,
       mode, reason,
       (reason IN ('selected','in_collection')) AS desired
FROM r;

-- =============================================================================
-- 12. chat write guard — gated on the RUNNING engine (runtime_state.dyn_engine)
-- =============================================================================
--   * dyn_engine IS NOT TRUE  -> RETURN NEW after one single-row PK lookup on
--     runtime_state. No advisory lock, no target_state lookup.
--   * dyn_engine IS TRUE      -> shared advisory xact lock on chat_id, then
--     target_state.enabled must be exactly TRUE (missing row rejects),
--     otherwise SQLSTATE TGP01.
--   * chat_id IS NULL passes (only chat_actions allows NULL).
--   * deployment_gates is not consulted: flipping the gate without a managed
--     restart cannot activate the guard in the running legacy process.
--   * No session-setting bypass.
CREATE OR REPLACE FUNCTION tgp_guard_chat_write() RETURNS trigger AS $$
DECLARE
    v_dyn     BOOLEAN;
    v_enabled BOOLEAN;
BEGIN
    SELECT dyn_engine INTO v_dyn FROM runtime_state WHERE id = 1;
    IF v_dyn IS NOT TRUE THEN
        RETURN NEW;
    END IF;

    IF NEW.chat_id IS NULL THEN
        RETURN NEW;
    END IF;

    PERFORM pg_advisory_xact_lock_shared(NEW.chat_id);

    SELECT enabled INTO v_enabled FROM target_state WHERE chat_id = NEW.chat_id;
    IF v_enabled IS NOT TRUE THEN
        RAISE EXCEPTION 'tgparser: chat_id=% not enabled (dynamic engine)', NEW.chat_id
            USING ERRCODE = 'TGP01';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    t_name TEXT;
    t_list TEXT[] := ARRAY[
        'messages', 'message_versions', 'message_media', 'message_reactions',
        'message_reaction_authors', 'memberships', 'chat_actions',
        'user_observations'
    ];
BEGIN
    FOREACH t_name IN ARRAY t_list LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = t_name
              AND column_name = 'chat_id'
        ) THEN
            RAISE EXCEPTION 'tgparser 0011: table %.chat_id missing', t_name;
        END IF;
        EXECUTE format('DROP TRIGGER IF EXISTS trg_tgp_guard ON %I', t_name);
        EXECUTE format(
            'CREATE TRIGGER trg_tgp_guard BEFORE INSERT OR UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION tgp_guard_chat_write()', t_name);
    END LOOP;
END $$;

-- =============================================================================
-- 13. SEED — idempotent; bumps the policy version exactly once, only if
--     something was really inserted.
-- =============================================================================
-- Inserting into target_preferences fires trg_intent_prefs_ins, which bumps
-- once when rows were inserted. The explicit UPDATE below covers the only
-- other case (discovered_dialogs gained rows, preferences did not). On a
-- re-run both CTEs insert nothing, so nothing is bumped and no NOTIFY is sent.
WITH dd AS (
    INSERT INTO discovered_dialogs
        (chat_id, category, title, username, is_archived)
    SELECT ts.chat_id,
           CASE
               WHEN c.type = 'supergroup' THEN 'megagroup'
               WHEN c.type = 'channel' AND COALESCE(c.is_broadcast, FALSE) THEN 'channel'
               WHEN c.type = 'channel' THEN 'megagroup'
               WHEN c.type = 'group'   THEN 'group'
               WHEN c.type = 'user'    THEN 'private'
               ELSE 'unknown'
           END,
           left(c.title, 256),
           left(c.username, 64),
           FALSE
    FROM target_state ts
    LEFT JOIN chats c ON c.id = ts.chat_id
    WHERE ts.enabled = TRUE
    ON CONFLICT (chat_id) DO NOTHING
    RETURNING chat_id
), tp AS (
    INSERT INTO target_preferences (chat_id, selected, excluded, updated_by)
    SELECT chat_id, TRUE, FALSE, 'migration_0011_seed'
    FROM target_state
    WHERE enabled = TRUE
    ON CONFLICT (chat_id) DO NOTHING
    RETURNING chat_id
)
UPDATE policy_state
   SET requested_version = requested_version + 1,
       requested_at = NOW()
 WHERE id = 1
   AND EXISTS (SELECT 1 FROM dd)
   AND NOT EXISTS (SELECT 1 FROM tp);
