#!/usr/bin/env bash
# Restore a tgparser PostgreSQL dump into the local Linux cluster — safely.
#
# Steps, in order:
#   1. Read ONLY the PG_DSN line from .env (never source .env).
#   2. Parse & validate the DSN; refuse non-local host, refuse reserved dbnames.
#   3. Accept a dump path as $1, else pick newest backup_*/tgparser.dump.
#   4. pg_restore --list to sanity-check the dump; abort if unreadable.
#   5. If the live DB is non-empty, take a timestamped pg_dump of it into
#      restore_safety_backups/ and verify the safety backup (size>0 AND
#      pg_restore --list succeeds).
#   6. Prompt with explicit source and target; require user to type YES.
#   7. pg_restore --exit-on-error --clean --if-exists --no-owner --role=<user>.
#      On failure print the safety backup path and how to roll back.
#   8. Read-only verification: table count + per-table row counts.
#
# Does NOT: start TGParser, delete any dump or backup, touch media/,
# PostgreSQL/ (Windows snapshot), or .env.
#
# Usage: bash restore_db.sh [path/to/tgparser.dump]

set -euo pipefail

PROJ="/home/user/Desktop/Work/TGParser"
cd "$PROJ"

# --- temp files & cleanup -------------------------------------------------
TMPDIR_RUN=$(mktemp -d -t tgparser-restore.XXXXXX)
trap 'rm -rf "$TMPDIR_RUN"' EXIT INT TERM
TOC_FILE="$TMPDIR_RUN/toc.txt"
TOC_ERR="$TMPDIR_RUN/toc.err"

# --- 1. read PG_DSN line only (no sourcing) -------------------------------
[[ -f .env ]] || { echo "ERROR: .env not found" >&2; exit 1; }
PG_DSN_RAW=$(awk -F= '
  /^[[:space:]]*#/ { next }
  /^[[:space:]]*PG_DSN[[:space:]]*=/ {
    sub(/^[^=]*=/, "")
    sub(/[[:space:]]+$/, "")
    if ($0 ~ /^".*"$/ || $0 ~ /^'\''.*'\''$/) { $0 = substr($0, 2, length($0)-2) }
    print; exit
  }' .env)
[[ -n "$PG_DSN_RAW" ]] || { echo "ERROR: PG_DSN missing in .env" >&2; exit 1; }

# --- 2. parse DSN NUL-delimited, then validate ----------------------------
mapfile -d '' -t DSN < <(PG_DSN_RAW="$PG_DSN_RAW" python3 - <<'PY'
import os, sys, urllib.parse as u
p = u.urlparse(os.environ["PG_DSN_RAW"])
for v in (
    p.scheme or "",
    u.unquote(p.username or ""),
    u.unquote(p.password or ""),
    (p.hostname or "").strip("[]"),
    str(p.port or ""),
    (p.path or "/").lstrip("/"),
):
    sys.stdout.write(v + "\0")
PY
) || { echo "ERROR: cannot parse PG_DSN" >&2; exit 1; }

PG_SCHEME=${DSN[0]} PG_USER=${DSN[1]} PG_PASS=${DSN[2]} PG_HOST=${DSN[3]} PG_PORT=${DSN[4]} PG_DB=${DSN[5]}

die() { echo "ERROR: $*" >&2; exit 1; }
[[ "$PG_SCHEME" == "postgresql" || "$PG_SCHEME" == "postgres" ]] || die "DSN scheme must be postgresql:// (got '$PG_SCHEME')"
[[ -n "$PG_USER" && -n "$PG_PASS" && -n "$PG_HOST" && -n "$PG_DB" ]] || die "DSN is missing user/password/host/dbname"
[[ "$PG_PORT" =~ ^[0-9]+$ ]] || die "DSN port is not numeric"
case "$PG_HOST" in
    localhost|127.0.0.1|::1) : ;;
    *) die "refusing to restore to non-local host '$PG_HOST'" ;;
esac
case "$PG_DB" in
    postgres|template0|template1) die "refusing to restore into reserved db '$PG_DB'" ;;
esac

export PGPASSWORD="$PG_PASS"      # scoped to this script's processes only
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -At -P pager=off -v ON_ERROR_STOP=1)
PGR=(pg_restore -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB"
     --exit-on-error --clean --if-exists --no-owner --role="$PG_USER")

# --- 3. locate dump -------------------------------------------------------
DUMP="${1:-}"
if [[ -z "$DUMP" ]]; then
    DUMP=$(ls -t backup_*/tgparser.dump 2>/dev/null | head -1 || true)
fi
[[ -n "$DUMP" && -f "$DUMP" ]] || die "dump not found: '$DUMP'"

DUMP_ABS=$(readlink -f -- "$DUMP")
DUMP_SIZE=$(stat -c '%s' "$DUMP_ABS")
DUMP_MTIME=$(stat -c '%y' "$DUMP_ABS")
(( DUMP_SIZE > 0 )) || die "dump is empty"

echo "dump          : $DUMP_ABS"
echo "  size        : $DUMP_SIZE bytes"
echo "  mtime       : $DUMP_MTIME"

# --- 4. inspect dump TOC --------------------------------------------------
if ! pg_restore --list "$DUMP_ABS" >"$TOC_FILE" 2>"$TOC_ERR"; then
    echo "ERROR: pg_restore cannot read this dump:" >&2
    sed 's/^/    /' "$TOC_ERR" >&2
    exit 1
fi
grep -E '^;( +(dbname|Dumped|TOC|Format|Compression|Dump Version):)' "$TOC_FILE" || true
# pg_restore --list TOC lines look like "217; 1259 16391 TABLE public chats owner"
# and "244; 1259 193062 TABLE DATA public coordinator_progress owner".
# Match the exact 4th word so TABLE and TABLE DATA never collide.
TABLE_COUNT=$(awk '/^[0-9]+;/ && $4 == "TABLE" && $5 != "DATA" {n++} END{print n+0}' "$TOC_FILE")
TABLE_DATA_COUNT=$(awk '/^[0-9]+;/ && $4 == "TABLE" && $5 == "DATA" {n++} END{print n+0}' "$TOC_FILE")
echo "  TABLE defs     : $TABLE_COUNT"
echo "  TABLE DATA     : $TABLE_DATA_COUNT"

# --- 5. live DB state + safety backup -------------------------------------
LIVE_TABLES=$("${PSQL[@]}" -c "SELECT count(*) FROM pg_tables WHERE schemaname='public';")
echo "live target   : $PG_DB on $PG_HOST:$PG_PORT — $LIVE_TABLES tables in public"

SAFE_FILE=""
if (( LIVE_TABLES > 0 )); then
    TS=$(date +%Y%m%d_%H%M%S)
    SAFE_DIR="$PROJ/restore_safety_backups"
    mkdir -p "$SAFE_DIR"
    SAFE_FILE="$SAFE_DIR/pre_restore_${PG_DB}_${TS}.dump"
    echo "safety backup : $SAFE_FILE"
    pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -F c -f "$SAFE_FILE"
    SAFE_SIZE=$(stat -c '%s' "$SAFE_FILE")
    (( SAFE_SIZE > 0 )) || die "safety backup is empty: $SAFE_FILE"
    if ! pg_restore --list "$SAFE_FILE" >"$TMPDIR_RUN/safe_toc.txt" 2>"$TMPDIR_RUN/safe_toc.err"; then
        echo "ERROR: safety backup is unreadable by pg_restore:" >&2
        sed 's/^/    /' "$TMPDIR_RUN/safe_toc.err" >&2
        die "aborting — will not proceed without a verified safety backup"
    fi
    echo "  verified    : $SAFE_SIZE bytes, pg_restore --list OK"
fi

# --- 6. confirmation ------------------------------------------------------
cat <<EOF

================  CONFIRM RESTORE  ================
source dump   : $DUMP_ABS
                size   $DUMP_SIZE bytes
                mtime  $DUMP_MTIME
target DB     : $PG_DB  (host $PG_HOST  port $PG_PORT  role $PG_USER)
live tables   : $LIVE_TABLES (in schema public, before restore)
safety backup : ${SAFE_FILE:-"(none — target was empty)"}

This will DROP and REPLACE existing tables in '$PG_DB'.
Type exactly  YES  to proceed, anything else cancels.
===================================================
EOF
read -r REPLY
if [[ "$REPLY" != "YES" ]]; then
    echo "cancelled; nothing changed."
    exit 0
fi

# --- 7. restore -----------------------------------------------------------
echo "restoring ..."
if ! "${PGR[@]}" "$DUMP_ABS"; then
    echo >&2
    echo "ERROR: pg_restore failed — target database may be PARTIALLY MODIFIED." >&2
    if [[ -n "$SAFE_FILE" ]]; then
        echo "       To roll back to the pre-restore state, run:" >&2
        echo "         pg_restore -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB \\" >&2
        echo "                    --clean --if-exists --no-owner --role=$PG_USER \\" >&2
        echo "                    '$SAFE_FILE'" >&2
    else
        echo "       No safety backup was taken (target was empty)." >&2
    fi
    exit 1
fi
echo "pg_restore returned 0"

# --- 8. read-only verification -------------------------------------------
echo
echo "post-restore summary:"
"${PSQL[@]}" <<'SQL'
SELECT 'public tables: ' || count(*) FROM pg_tables WHERE schemaname='public';
\echo per-table row counts:
SELECT format('  %-32s %12s', relname, n_live_tup::text)
FROM pg_stat_user_tables
ORDER BY relname;
SQL

echo
echo "Done. TGParser was NOT started."
