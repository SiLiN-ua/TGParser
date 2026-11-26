#!/usr/bin/env bash
# compare_staging_vs_live.sh — READ-ONLY comparison of a restored staging DB
# (built from the Windows pg_dump) against the current live Linux tgparser DB.
#
# Guarantees:
#   - only SELECTs run against both databases;
#   - never installs extensions (no FDW, no dblink);
#   - never touches media/, PostgreSQL/, *.dump, *.session, backup_*/ or .env;
#   - does NOT launch TGParser;
#   - temp files via mktemp + trap EXIT/INT/TERM;
#   - passwords are passed via PGPASSWORD env to child psql only, never in argv.
#
# Flow:
#   1) Parse LIVE_DSN (from .env or env) and STAGING_DSN (from env).
#   2) SCHEMA PREFLIGHT — read SCHEMA_SPEC from this file, check each table and
#      every bare column identifier used in PK/payload exists in LIVE/STAGING's
#      information_schema. Abort BEFORE the heavy comparison on any miss.
#   3) Row counts + min/max per key table (side by side).
#   4) PK-set diffs: only-live / only-staging / common / payload conflicts.
#      PK and payload are canonically encoded via to_jsonb(ROW(...))::text so
#      NULLs, delimiters inside text, and JSONB are handled losslessly. The PK
#      is the EXACT SQL expression from the table's unique index (including
#      COALESCE(...) for expression indexes), not the bare column list.
#   5) Append-only table sizes (union candidates, no per-row diff).
#   6) Orphan / FK sanity (both sides).
#
# Usage:
#   STAGING_DSN='postgresql://tgparser:...@127.0.0.1:5432/tgparser_staging' \
#   bash compare_staging_vs_live.sh

set -euo pipefail

PROJ="/home/user/Desktop/Work/TGParser"
cd "$PROJ"

TMPDIR_RUN=$(mktemp -d -t tgparser-compare.XXXXXX)
trap 'rm -rf "$TMPDIR_RUN"' EXIT INT TERM

die() { echo "ERROR: $*" >&2; exit 1; }

# =========================================================================
# SCHEMA SPEC — kept in sync with migrations 1..10 and live pg_catalog.
#
# Format, one record per line, PK- and payload-parts separated by '|':
#     TABLE : pk_expr_1 ; pk_expr_2 ; ... | payload_col_1 ; payload_col_2 ; ...
#
# Each pk_expr is an SQL expression that MUST match a unique index on the
# table. For simple PKs it's just a column name. For expression indexes it is
# the exact expression (e.g. `COALESCE(edit_date, 'epoch'::timestamptz)`).
#
# Payload parts are plain column names (no expressions); they are compared
# via to_jsonb(ROW(...))::text for NULL-safe, delimiter-safe canonical hashing.
#
# The schema-preflight / static-test extracts bare identifiers [a-z_][a-z_0-9]*
# from each pk_expr and payload part, filters SQL keywords/literals, and
# validates every resulting column exists in public.<TABLE>.
# BEGIN SCHEMA SPEC
read -r -d '' SCHEMA_SPEC <<'SPEC' || true
# Natural PKs — diffable by PK + payload md5(canonical jsonb):
messages            : chat_id ; message_id | sender_user_id ; sender_chat_id ; date ; edit_date ; reply_to_message_id ; reply_to_top_id ; reply_to_peer_id ; thread_id ; post_author ; text ; entities ; media_type ; media_path ; media_file_id ; media_group_id ; fwd_from_user_id ; fwd_from_chat_id ; fwd_from_name ; fwd_from_msg_id ; fwd_date ; fwd_channel_post ; views ; forwards ; replies_count ; reactions_total ; is_service ; service_action ; service_payload ; geo ; contact ; poll ; webpage ; raw ; is_deleted ; deleted_at
message_versions    : chat_id ; message_id ; COALESCE(edit_date, 'epoch'::timestamptz) | text ; entities ; media_type ; media_path ; media_file_id ; is_initial
message_media       : chat_id ; message_id ; file_id | attached_at
message_reactions   : chat_id ; message_id ; reaction | count ; chosen_order ; updated_at
message_reaction_authors : chat_id ; message_id ; reaction ; peer_id | peer_kind ; reacted_at ; captured_at
users               : id | username ; first_name ; last_name ; phone ; bio ; is_bot ; is_premium ; is_scam ; is_fake ; is_verified ; is_deleted ; lang_code ; photo_path ; photo_dc ; photo_file_id ; raw ; full_fetched_at ; access_hash ; access_hash_updated_at ; is_contact ; is_mutual_contact ; is_close_friend ; is_min ; entity_source ; entity_quality ; last_seen ; access_hash_kind ; access_hash_priority ; access_hash_auth_fingerprint ; access_hash_source_chat_id ; access_hash_source_message_id ; access_hash_is_stale
media               : file_id | chat_id ; message_id ; media_type ; mime_type ; size ; file_name ; local_path ; status ; attempts ; last_error ; duration ; width ; height ; sha256 ; completed_at ; is_downloadable ; verified_at ; media_group_id
avatar_history      : user_id ; COALESCE(photo_file_id, 0::bigint) | local_path ; first_seen
memberships         : chat_id ; user_id | role ; joined_at ; left_at ; raw ; source ; is_bot
chats               : id | type ; title ; username ; about ; participants_count ; linked_chat_id ; is_forum ; is_broadcast ; raw
sync_state          : chat_id | last_backfilled_message_id ; backfill_complete ; first_message_id ; participants_synced ; participants_is_complete ; participants_limitation ; participants_offset ; backfill_status ; batches_completed
target_state        : chat_id | target ; backfill_status ; last_success_at ; last_error_at ; last_error ; pending_count ; dead_count ; note ; enabled ; disabled_at ; disabled_reason
backfill_queue      : chat_id ; message_id | attempts ; last_error ; next_retry_at ; status ; acknowledged ; acknowledged_at ; acknowledged_by

# Append-only logs — not diffed row-by-row; only counts.
APPENDLOG: user_profile_history,access_hash_history,user_observations,errors,deletion_events,chat_actions

# Work queues — not diffed row-by-row.
WORKQUEUE: identity_enrichment_queue,user_profile_queue

# Transient/not-merged tables — never compared.
EPHEMERAL: runtime_state,coordinator_progress,media_worker_state,schema_migrations
SPEC
# END SCHEMA SPEC
# =========================================================================

# --- 1. DSNs --------------------------------------------------------------
[[ -f .env ]] || die ".env not found"
LIVE_DSN="${LIVE_DSN:-}"
if [[ -z "$LIVE_DSN" ]]; then
    LIVE_DSN=$(awk -F= '
      /^[[:space:]]*#/ { next }
      /^[[:space:]]*PG_DSN[[:space:]]*=/ {
        sub(/^[^=]*=/, ""); sub(/[[:space:]]+$/, "")
        if ($0 ~ /^".*"$/ || $0 ~ /^'\''.*'\''$/) { $0 = substr($0, 2, length($0)-2) }
        print; exit
      }' .env)
fi
[[ -n "$LIVE_DSN"    ]] || die "LIVE_DSN empty (and PG_DSN not in .env)"
[[ -n "${STAGING_DSN:-}" ]] || die "STAGING_DSN env var is required"
[[ "$LIVE_DSN" != "$STAGING_DSN" ]] || die "LIVE_DSN and STAGING_DSN are identical — refusing"

parse_dsn() {
    DSN_RAW="$1" python3 - <<'PY'
import os, sys, urllib.parse as u
p = u.urlparse(os.environ["DSN_RAW"])
for v in (p.scheme or "", u.unquote(p.username or ""), u.unquote(p.password or ""),
          (p.hostname or "").strip("[]"), str(p.port or ""), (p.path or "/").lstrip("/")):
    sys.stdout.write(v + "\0")
PY
}
validate_dsn() {
    local label=$1 scheme=$2 user=$3 pass=$4 host=$5 port=$6 db=$7
    [[ "$scheme" == postgresql || "$scheme" == postgres ]] || die "$label: bad scheme"
    [[ -n "$user" && -n "$pass" && -n "$host" && -n "$db" ]] || die "$label: missing user/pass/host/db"
    [[ "$port" =~ ^[0-9]+$ ]] || die "$label: port not numeric"
    case "$host" in localhost|127.0.0.1|::1) : ;; *) die "$label host '$host' is not local" ;; esac
    case "$db" in postgres|template0|template1) die "$label db '$db' is reserved" ;; esac
}
mapfile -d '' -t L < <(parse_dsn "$LIVE_DSN")
mapfile -d '' -t S < <(parse_dsn "$STAGING_DSN")
validate_dsn LIVE    "${L[0]}" "${L[1]}" "${L[2]}" "${L[3]}" "${L[4]}" "${L[5]}"
validate_dsn STAGING "${S[0]}" "${S[1]}" "${S[2]}" "${S[3]}" "${S[4]}" "${S[5]}"
[[ "${L[5]}" != "${S[5]}" ]] || die "LIVE and STAGING point at the same database '${L[5]}'"

psql_live()    { PGPASSWORD="${L[2]}" psql -h "${L[3]}" -p "${L[4]}" -U "${L[1]}" -d "${L[5]}" \
                   --quiet --no-psqlrc -At -F $'\t' -P pager=off -v ON_ERROR_STOP=1 "$@"; }
psql_staging() { PGPASSWORD="${S[2]}" psql -h "${S[3]}" -p "${S[4]}" -U "${S[1]}" -d "${S[5]}" \
                   --quiet --no-psqlrc -At -F $'\t' -P pager=off -v ON_ERROR_STOP=1 "$@"; }

# --- 2. parse SCHEMA_SPEC -------------------------------------------------
# PK_EXPR_OF[tbl]      — SQL text, ';'-separated expressions for ROW(...) in queries
# PK_CHECK_COLS[tbl]   — comma-separated bare identifiers extracted from PK expressions
# PAYLOAD_OF[tbl]      — comma-separated column names
declare -A PK_EXPR_OF PK_CHECK_COLS PAYLOAD_OF
DIFF_TABLES=()
APPEND_TABLES=()
WORK_TABLES=()
EPHEMERAL_TABLES=()

# SQL keywords/literal tokens we strip when extracting bare column identifiers
# from pk_expr. Case-insensitive match.
SQL_NOISE_RE='^(coalesce|null|true|false|epoch|timestamptz|bigint|integer|text|boolean|jsonb|now|date|and|or|not|in|is|cast|as)$'

extract_cols_from_expr() {   # stdin: expression; stdout: one identifier per line
    tr '[:upper:]' '[:lower:]' \
      | grep -oE "[a-z_][a-z_0-9]*" \
      | awk -v noise="$SQL_NOISE_RE" '!(tolower($0) ~ noise)'
}

while IFS= read -r line; do
    line="${line%%#*}"
    line="$(printf '%s' "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    case "$line" in
        APPENDLOG:*)  IFS=',' read -r -a APPEND_TABLES    <<<"${line#APPENDLOG:}" ;;
        WORKQUEUE:*)  IFS=',' read -r -a WORK_TABLES      <<<"${line#WORKQUEUE:}" ;;
        EPHEMERAL:*)  IFS=',' read -r -a EPHEMERAL_TABLES <<<"${line#EPHEMERAL:}" ;;
        *":"*"|"*)
            tbl=${line%%:*}
            tbl=$(printf '%s' "$tbl" | tr -d '[:space:]')
            rest=${line#*:}
            pk_part=${rest%%|*}
            payload_part=${rest#*|}

            # PK: ';'-separated expressions. Build a comma-joined expression
            # list for SQL, plus the bare-identifier list for preflight.
            pk_expr_list=$(printf '%s' "$pk_part" | awk -F';' '{
                for(i=1;i<=NF;i++){
                    gsub(/^[[:space:]]+|[[:space:]]+$/,"",$i);
                    if($i!="") printf("%s%s",(i>1)?", ":"",$i);
                }}')
            PK_EXPR_OF[$tbl]="$pk_expr_list"

            # Identifiers to validate — extract from each pk expression.
            cols=$(printf '%s\n' "$pk_part" | tr ';' '\n' | extract_cols_from_expr \
                   | awk '!seen[$0]++' | paste -sd, -)
            PK_CHECK_COLS[$tbl]="$cols"

            # Payload: ';'-separated plain column names.
            payload_cols=$(printf '%s' "$payload_part" | awk -F';' '{
                for(i=1;i<=NF;i++){
                    gsub(/^[[:space:]]+|[[:space:]]+$/,"",$i);
                    if($i!="") printf("%s%s",(i>1)?",":"",$i);
                }}')
            PAYLOAD_OF[$tbl]="$payload_cols"
            DIFF_TABLES+=("$tbl")
            ;;
    esac
done <<<"$SCHEMA_SPEC"

# --- 3. SCHEMA PREFLIGHT --------------------------------------------------
preflight_one() {
    local side=$1 call=$2 errs=0
    for tbl in "${DIFF_TABLES[@]}"; do
        local cols="${PK_CHECK_COLS[$tbl]},${PAYLOAD_OF[$tbl]}"
        for col in ${cols//,/ }; do
            [[ -z "$col" ]] && continue
            found=$($call -c "SELECT 1 FROM information_schema.columns
                              WHERE table_schema='public' AND table_name='$tbl' AND column_name='$col'")
            if [[ "$found" != "1" ]]; then
                echo "  MISSING  $side  ${tbl}.${col}"
                errs=$((errs+1))
            fi
        done
    done
    for tbl in "${APPEND_TABLES[@]}" "${WORK_TABLES[@]}" "${EPHEMERAL_TABLES[@]}"; do
        found=$($call -c "SELECT 1 FROM information_schema.tables
                          WHERE table_schema='public' AND table_name='$tbl'")
        if [[ "$found" != "1" ]]; then
            echo "  MISSING  $side  table ${tbl}"
            errs=$((errs+1))
        fi
    done
    return $errs
}

echo "=== schema preflight ==="
errs_live=0; errs_stage=0
preflight_one LIVE    psql_live    || errs_live=$?
preflight_one STAGING psql_staging || errs_stage=$?
if (( errs_live > 0 || errs_stage > 0 )); then
    die "schema preflight failed (live=$errs_live, staging=$errs_stage); not starting comparison"
fi
echo "  OK — all tables and columns present on both sides."

# --- 4. schema_migrations ------------------------------------------------
echo; echo "=== schema_migrations ==="
printf 'LIVE   : '; psql_live    -c "SELECT string_agg(version::text, ',' ORDER BY version) FROM schema_migrations;"
printf 'STAGING: '; psql_staging -c "SELECT string_agg(version::text, ',' ORDER BY version) FROM schema_migrations;"

# --- 5. row counts + min/max ---------------------------------------------
echo; echo "=== row counts, min/max ==="
CNT_SQL="
WITH t AS (
  SELECT 'messages'        AS tbl, count(*) AS n, min(message_id)::text AS lo, max(message_id)::text AS hi,
         min(date)::text   AS d_lo, max(date)::text AS d_hi FROM messages
  UNION ALL SELECT 'users',             count(*), min(id)::text, max(id)::text,
         min(full_fetched_at)::text, max(full_fetched_at)::text FROM users
  UNION ALL SELECT 'media',             count(*), NULL, NULL,
         min(first_seen)::text, max(first_seen)::text FROM media
  UNION ALL SELECT 'message_media',     count(*), NULL, NULL,
         min(attached_at)::text, max(attached_at)::text FROM message_media
  UNION ALL SELECT 'message_versions',  count(*), NULL, NULL,
         min(edit_date)::text, max(edit_date)::text FROM message_versions
  UNION ALL SELECT 'message_reactions', count(*), NULL, NULL,
         min(updated_at)::text, max(updated_at)::text FROM message_reactions
  UNION ALL SELECT 'message_reaction_authors', count(*), NULL, NULL,
         min(reacted_at)::text, max(reacted_at)::text FROM message_reaction_authors
  UNION ALL SELECT 'avatar_history',    count(*), NULL, NULL,
         min(first_seen)::text, max(first_seen)::text FROM avatar_history
  UNION ALL SELECT 'user_profile_history', count(*), NULL, NULL,
         min(changed_at)::text, max(changed_at)::text FROM user_profile_history
  UNION ALL SELECT 'access_hash_history', count(*), NULL, NULL,
         min(first_seen)::text, max(last_seen)::text FROM access_hash_history
  UNION ALL SELECT 'user_observations', count(*), NULL, NULL,
         min(observed_at)::text, max(observed_at)::text FROM user_observations
  UNION ALL SELECT 'memberships',       count(*), NULL, NULL,
         min(updated_at)::text, max(updated_at)::text FROM memberships
  UNION ALL SELECT 'chats',             count(*), NULL, NULL,
         min(first_seen)::text, max(last_updated)::text FROM chats
  UNION ALL SELECT 'sync_state',        count(*), NULL, NULL, NULL, NULL FROM sync_state
  UNION ALL SELECT 'target_state',      count(*), NULL, NULL, NULL, NULL FROM target_state
  UNION ALL SELECT 'backfill_queue',    count(*), NULL, NULL,
         min(first_seen)::text, max(last_attempt_at)::text FROM backfill_queue
  UNION ALL SELECT 'errors',            count(*), NULL, NULL,
         min(at)::text, max(at)::text FROM errors
  UNION ALL SELECT 'deletion_events',   count(*), NULL, NULL,
         min(received_at)::text, max(received_at)::text FROM deletion_events
  UNION ALL SELECT 'chat_actions',      count(*), NULL, NULL,
         min(at)::text, max(at)::text FROM chat_actions
) SELECT tbl, n, coalesce(lo,'-'), coalesce(hi,'-'), coalesce(d_lo,'-'), coalesce(d_hi,'-') FROM t ORDER BY tbl;"
printf 'LIVE:\n';    psql_live    -c "$CNT_SQL" | awk -F'\t' '{printf "  %-26s n=%-10s id=[%s..%s] d=[%s..%s]\n",$1,$2,$3,$4,$5,$6}'
printf 'STAGING:\n'; psql_staging -c "$CNT_SQL" | awk -F'\t' '{printf "  %-26s n=%-10s id=[%s..%s] d=[%s..%s]\n",$1,$2,$3,$4,$5,$6}'

# --- 6. PK diffs via canonical jsonb encoding ----------------------------
# The ROW(...) in each query gets the EXACT pk_expr (possibly COALESCE(...))
# from SCHEMA_SPEC, so the encoded key matches the table's unique index
# definition one-to-one.
echo; echo "=== PK set diffs (only_live / only_staging / conflicting payload) ==="
for tbl in "${DIFF_TABLES[@]}"; do
    pk_expr=${PK_EXPR_OF[$tbl]}
    payload_list=${PAYLOAD_OF[$tbl]//,/, }
    q="SELECT to_jsonb(ROW(${pk_expr}))::text || E'\t' || md5(to_jsonb(ROW(${payload_list}))::text) FROM ${tbl} ORDER BY 1"
    psql_live    -c "$q" > "$TMPDIR_RUN/${tbl}.live"    || die "live query failed on $tbl"
    psql_staging -c "$q" > "$TMPDIR_RUN/${tbl}.staging" || die "staging query failed on $tbl"
    cut -f1 "$TMPDIR_RUN/${tbl}.live"    | sort > "$TMPDIR_RUN/${tbl}.live.pk"
    cut -f1 "$TMPDIR_RUN/${tbl}.staging" | sort > "$TMPDIR_RUN/${tbl}.staging.pk"
    only_live=$( comm -23 "$TMPDIR_RUN/${tbl}.live.pk" "$TMPDIR_RUN/${tbl}.staging.pk" | wc -l)
    only_stage=$(comm -13 "$TMPDIR_RUN/${tbl}.live.pk" "$TMPDIR_RUN/${tbl}.staging.pk" | wc -l)
    common=$(    comm -12 "$TMPDIR_RUN/${tbl}.live.pk" "$TMPDIR_RUN/${tbl}.staging.pk" | wc -l)
    sort "$TMPDIR_RUN/${tbl}.live"    > "$TMPDIR_RUN/${tbl}.live.s"
    sort "$TMPDIR_RUN/${tbl}.staging" > "$TMPDIR_RUN/${tbl}.staging.s"
    conflicts=$(join -t $'\t' "$TMPDIR_RUN/${tbl}.live.s" "$TMPDIR_RUN/${tbl}.staging.s" \
                | awk -F'\t' '$2 != $3' | wc -l)
    printf '  %-26s common=%-9d only_live=%-9d only_staging=%-9d conflicts=%-9d\n' \
           "$tbl" "$common" "$only_live" "$only_stage" "$conflicts"
done

# --- 7. append-only log + work queue sizes -------------------------------
echo; echo "=== append-only log sizes ==="
for tbl in "${APPEND_TABLES[@]}"; do
    L=$(psql_live    -c "SELECT count(*) FROM ${tbl};")
    S=$(psql_staging -c "SELECT count(*) FROM ${tbl};")
    printf '  %-26s live=%-10s staging=%-10s\n' "$tbl" "$L" "$S"
done
echo; echo "=== work queue sizes ==="
for tbl in "${WORK_TABLES[@]}"; do
    L=$(psql_live    -c "SELECT count(*) FROM ${tbl};")
    S=$(psql_staging -c "SELECT count(*) FROM ${tbl};")
    printf '  %-26s live=%-10s staging=%-10s\n' "$tbl" "$L" "$S"
done

# --- 8. orphan / FK checks -----------------------------------------------
echo; echo "=== orphan / FK checks (both sides) ==="
ORPHAN_SQL="
SELECT 'message_media_without_msg' AS k, count(*) FROM message_media mm
  LEFT JOIN messages m ON m.chat_id=mm.chat_id AND m.message_id=mm.message_id
  WHERE m.chat_id IS NULL
UNION ALL SELECT 'message_media_without_media', count(*) FROM message_media mm
  LEFT JOIN media md ON md.file_id=mm.file_id WHERE md.file_id IS NULL
UNION ALL SELECT 'reactions_without_msg', count(*) FROM message_reactions r
  LEFT JOIN messages m ON m.chat_id=r.chat_id AND m.message_id=r.message_id
  WHERE m.chat_id IS NULL
UNION ALL SELECT 'reaction_authors_without_msg', count(*) FROM message_reaction_authors r
  LEFT JOIN messages m ON m.chat_id=r.chat_id AND m.message_id=r.message_id
  WHERE m.chat_id IS NULL
UNION ALL SELECT 'versions_without_msg', count(*) FROM message_versions v
  LEFT JOIN messages m ON m.chat_id=v.chat_id AND m.message_id=v.message_id
  WHERE m.chat_id IS NULL
UNION ALL SELECT 'media_by_status: ' || status, count(*) FROM media GROUP BY status;
"
printf 'LIVE:\n';    psql_live    -c "$ORPHAN_SQL" | awk -F'\t' '{printf "  %-34s %s\n",$1,$2}'
printf 'STAGING:\n'; psql_staging -c "$ORPHAN_SQL" | awk -F'\t' '{printf "  %-34s %s\n",$1,$2}'

echo
echo "Comparison complete. Nothing was modified."
