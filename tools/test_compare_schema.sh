#!/usr/bin/env bash
# tools/test_compare_schema.sh — static, READ-ONLY test for compare_staging_vs_live.sh
#
# Parses the SCHEMA SPEC block out of ../compare_staging_vs_live.sh, extracts
# every table and bare column identifier the compare script will reference,
# and verifies each exists in the live PostgreSQL DB named by PG_DSN (.env).
#
# Guarantees:
#   - only SELECTs from information_schema;
#   - no DDL, no writes;
#   - temp files via mktemp -d + trap cleanup;
#   - leaves NOTHING in the repository (no artifacts in tools/ or anywhere else).
#
# Usage:  bash tools/test_compare_schema.sh

set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PROJ=$(cd "$HERE/.." && pwd)
SCRIPT="$PROJ/compare_staging_vs_live.sh"
[[ -f "$SCRIPT" ]] || { echo "ERROR: $SCRIPT not found" >&2; exit 1; }
[[ -f "$PROJ/.env" ]] || { echo "ERROR: .env not found" >&2; exit 1; }

TMPDIR_RUN=$(mktemp -d -t tgparser-schema-test.XXXXXX)
trap 'rm -rf "$TMPDIR_RUN"' EXIT INT TERM

# --- DSN from .env (safe, no sourcing) ------------------------------------
PG_DSN_RAW=$(awk -F= '
  /^[[:space:]]*#/ { next }
  /^[[:space:]]*PG_DSN[[:space:]]*=/ {
    sub(/^[^=]*=/, ""); sub(/[[:space:]]+$/, "")
    if ($0 ~ /^".*"$/ || $0 ~ /^'\''.*'\''$/) { $0 = substr($0, 2, length($0)-2) }
    print; exit
  }' "$PROJ/.env")
mapfile -d '' -t D < <(PG_DSN_RAW="$PG_DSN_RAW" python3 - <<'PY'
import os, sys, urllib.parse as u
p = u.urlparse(os.environ["PG_DSN_RAW"])
for v in (p.scheme or "", u.unquote(p.username or ""), u.unquote(p.password or ""),
          (p.hostname or "").strip("[]"), str(p.port or ""), (p.path or "/").lstrip("/")):
    sys.stdout.write(v + "\0")
PY
)
case "${D[3]}" in localhost|127.0.0.1|::1) : ;;
  *) echo "ERROR: non-local LIVE host '${D[3]}'; refusing" >&2; exit 1 ;; esac

psql_live() { PGPASSWORD="${D[2]}" psql -h "${D[3]}" -p "${D[4]}" -U "${D[1]}" -d "${D[5]}" \
                --quiet --no-psqlrc -At -F $'\t' -P pager=off -v ON_ERROR_STOP=1 "$@"; }

# --- Extract SCHEMA SPEC between markers ----------------------------------
SPEC=$(awk '/^# BEGIN SCHEMA SPEC/{on=1;next} /^# END SCHEMA SPEC/{on=0} on' "$SCRIPT")
[[ -n "$SPEC" ]] || { echo "ERROR: SCHEMA SPEC block not found" >&2; exit 1; }

# SQL keywords/literal tokens filtered out when extracting bare identifiers
# from pk expressions — kept in sync with compare_staging_vs_live.sh.
NOISE='^(coalesce|null|true|false|epoch|timestamptz|bigint|integer|text|boolean|jsonb|now|date|and|or|not|in|is|cast|as)$'

# --- Build (table, column) and (table) lists in TMPDIR_RUN ----------------
: > "$TMPDIR_RUN/spec_cols.tsv"
: > "$TMPDIR_RUN/spec_tables.txt"

while IFS= read -r line; do
    line="${line%%#*}"
    line="$(printf '%s' "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    case "$line" in
        "read -r -d"*|"SPEC") continue ;;
        APPENDLOG:*|WORKQUEUE:*|EPHEMERAL:*)
            rest=${line#*:}
            for tbl in ${rest//,/ }; do
                [[ -z "$tbl" ]] && continue
                printf '%s\n' "$tbl" >> "$TMPDIR_RUN/spec_tables.txt"
            done
            ;;
        *":"*"|"*)
            tbl=${line%%:*}; tbl=$(printf '%s' "$tbl" | tr -d '[:space:]')
            rest=${line#*:}
            pk_part=${rest%%|*}
            payload_part=${rest#*|}
            printf '%s\n' "$tbl" >> "$TMPDIR_RUN/spec_tables.txt"
            # extract bare identifiers from each pk expression
            printf '%s\n' "$pk_part" | tr ';' '\n' \
              | tr '[:upper:]' '[:lower:]' \
              | grep -oE "[a-z_][a-z_0-9]*" \
              | awk -v noise="$NOISE" -v t="$tbl" \
                    '!(tolower($0) ~ noise){printf "%s\t%s\n", t, $0}' \
              >> "$TMPDIR_RUN/spec_cols.tsv"
            # payload: plain column names, ';'-split
            printf '%s\n' "$payload_part" | tr ';' '\n' \
              | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
              | awk -v t="$tbl" 'NF>0{printf "%s\t%s\n", t, $0}' \
              >> "$TMPDIR_RUN/spec_cols.tsv"
            ;;
    esac
done <<<"$SPEC"

sort -u "$TMPDIR_RUN/spec_cols.tsv"   -o "$TMPDIR_RUN/spec_cols.tsv"
sort -u "$TMPDIR_RUN/spec_tables.txt" -o "$TMPDIR_RUN/spec_tables.txt"

n_cols=$(wc -l < "$TMPDIR_RUN/spec_cols.tsv")
n_tables=$(wc -l < "$TMPDIR_RUN/spec_tables.txt")
echo "parsed from $SCRIPT:"
echo "  tables referenced : $n_tables"
echo "  column references : $n_cols"

# --- Pull live schema into sorted lookup tables --------------------------
psql_live -c "SELECT table_name||E'\t'||column_name FROM information_schema.columns WHERE table_schema='public';" \
    | sort -u > "$TMPDIR_RUN/live_cols.tsv"
psql_live -c "SELECT table_name FROM information_schema.tables WHERE table_schema='public';" \
    | sort -u > "$TMPDIR_RUN/live_tables.txt"

miss_tables=$(comm -23 "$TMPDIR_RUN/spec_tables.txt" "$TMPDIR_RUN/live_tables.txt")
miss_cols=$(  comm -23 "$TMPDIR_RUN/spec_cols.tsv"   "$TMPDIR_RUN/live_cols.tsv")

status=0
echo
if [[ -n "$miss_tables" ]]; then
    echo "MISSING TABLES in live DB:"; printf '  %s\n' $miss_tables; status=1
fi
if [[ -n "$miss_cols" ]]; then
    echo "MISSING COLUMNS in live DB (table.column):"
    while IFS=$'\t' read -r t c; do printf '  %s.%s\n' "$t" "$c"; done <<<"$miss_cols"
    status=1
fi
if (( status == 0 )); then
    echo "OK — every table.column in SCHEMA_SPEC exists in live DB."
else
    echo "FAIL — schema spec drifted; update compare_staging_vs_live.sh and migrations." >&2
fi
exit $status
