#!/usr/bin/env bash
# TGParser Linux setup — idempotent, safe to re-run.
#
# Does:
#   - apt-installs python3.12-venv + build toolchain and PostgreSQL 17 (PGDG) if missing
#   - ensures the postgresql@17-main service is enabled
#   - reads ONLY the PG_DSN line from .env (never sources .env as shell code)
#   - creates/updates the role and database to match PG_DSN
#   - creates .venv only if missing (never wipes an existing one) and pip-installs requirements.txt
#   - verifies DB connectivity
#
# Does NOT:
#   - launch run.py, systemd units, or any Telegram client
#   - touch media/, PostgreSQL/ (the Windows cluster snapshot), *.dump, *.session,
#     backup_*/ or .env
#   - attempt to attach the Windows-built cluster to Linux Postgres
#
# Refuses to run when PG_DSN host is non-local — this script configures the
# local cluster only.
#
# SQL design note: role/db/password are passed as psql variables and referenced
# ONLY at top level as :'pguser' / :'pgpass' / :'pgdb'. psql variables are NOT
# interpolated inside dollar-quoted strings (e.g. DO $$..$$ bodies) or inside
# literal strings — verified in this repo's docs for the task. CREATE ROLE /
# ALTER ROLE / CREATE DATABASE are emitted by top-level SELECT format(...)
# combined with \gexec, which keeps the password out of the client-visible SQL
# echo. Server-side exposure in logs is suppressed by setting
# log_statement=none and log_min_error_statement=panic for just this session.

set -euo pipefail

PROJ="/home/user/Desktop/Work/TGParser"
cd "$PROJ"

[[ -f .env ]] || { echo "ERROR: $PROJ/.env not found" >&2; exit 1; }

# --- 1. safe PG_DSN extraction --------------------------------------------
PG_DSN_RAW=$(awk -F= '
  /^[[:space:]]*#/ { next }
  /^[[:space:]]*PG_DSN[[:space:]]*=/ {
    sub(/^[^=]*=/, "")
    sub(/[[:space:]]+$/, "")
    if ($0 ~ /^".*"$/ || $0 ~ /^'\''.*'\''$/) { $0 = substr($0, 2, length($0)-2) }
    print; exit
  }' .env)
[[ -n "$PG_DSN_RAW" ]] || { echo "ERROR: PG_DSN not found in .env" >&2; exit 1; }

# --- 2. parse DSN NUL-delimited -------------------------------------------
mapfile -d '' -t DSN < <(PG_DSN_RAW="$PG_DSN_RAW" python3 - <<'PY'
import os, sys, urllib.parse as u
try:
    p = u.urlparse(os.environ["PG_DSN_RAW"])
except ValueError as e:
    sys.stderr.write(f"bad DSN: {e}\n"); sys.exit(2)
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

# --- 3. validate ----------------------------------------------------------
die() { echo "ERROR: $*" >&2; exit 1; }
[[ "$PG_SCHEME" == "postgresql" || "$PG_SCHEME" == "postgres" ]] || die "DSN scheme must be postgresql:// (got '$PG_SCHEME')"
[[ -n "$PG_USER" ]] || die "DSN username missing"
[[ -n "$PG_PASS" ]] || die "DSN password missing"
[[ -n "$PG_HOST" ]] || die "DSN hostname missing"
[[ "$PG_PORT" =~ ^[0-9]+$ ]] || die "DSN port missing or not numeric"
[[ -n "$PG_DB"   ]] || die "DSN dbname missing"
case "$PG_HOST" in
    localhost|127.0.0.1|::1) : ;;
    *) die "this script only configures a LOCAL cluster; DSN host '$PG_HOST' is not local" ;;
esac
case "$PG_DB" in
    postgres|template0|template1) die "refusing to use reserved database name '$PG_DB'" ;;
esac
[[ "$PG_USER" != "postgres" ]] || die "refusing to manage the superuser role 'postgres' here"

# --- 4. apt dependencies (idempotent) -------------------------------------
echo "==[1/5]== apt dependencies"
need_apt=0
for pkg in python3.12-venv python3-dev build-essential curl ca-certificates gnupg lsb-release; do
    dpkg -s "$pkg" >/dev/null 2>&1 || need_apt=1
done
if (( need_apt )); then
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3.12-venv python3-dev build-essential \
                                 curl ca-certificates gnupg lsb-release
fi

if ! dpkg -s postgresql-17 >/dev/null 2>&1; then
    echo "   adding PGDG repo for PostgreSQL 17"
    sudo install -d /usr/share/postgresql-common/pgdg
    sudo curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
    codename=$(lsb_release -cs)
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${codename}-pgdg main" \
        | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq postgresql-17 postgresql-contrib-17
fi

echo "==[2/5]== PostgreSQL service"
# Windows-built PostgreSQL/17/data/ is NOT attached. restore_db.sh uses a dump.
sudo systemctl enable --now postgresql@17-main >/dev/null

# --- 5. role + database via top-level SELECT format(...) + \gexec ---------
# Password/role/db flow:
#   PG_USER, PG_PASS, PG_DB -> psql -v pguser=... pgpass=... pgdb=...
#   SQL body references them ONLY at top level as :'pguser' / :'pgpass' /
#   :'pgdb' (psql substitutes as properly-quoted string literals).
#   SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', ...) builds the actual
#   DDL on the server side, then \gexec runs it. %I quotes identifiers,
#   %L quotes literals — safe for any payload.
#
# Password exposure — KNOWN RESIDUAL LEAKS and controls.
#
#   LEAK #1: psql's `--set=pgpass=$PG_PASS` places the password in argv for the
#   psql process; `ps -eo args` shows it to any process on the machine that
#   can read /proc of this UID (same user, and root). On a single-user laptop
#   the window is short, but the leak is real and this script does NOT claim
#   the password is fully hidden.
#
#   LEAK #2 (server-side): pg_stat_statements, if enabled, normalises and
#   stores DDL text, so a CREATE/ALTER ROLE ... PASSWORD lands in that table
#   until it's reset. Not loaded in the current cluster, but worth tracking.
#
#   Controls applied here:
#   - PGOPTIONS='-c log_statement=none -c log_min_error_statement=panic
#       -c log_min_duration_statement=-1' stops the server writing the DDL to
#       its own log on success, on error, or by duration.
#   - -q + --no-psqlrc keeps psql from echoing the SQL generated by \gexec.
#
#   TODO: secure credential transport requires redesign.
#     A heredoc-based `\set pgpass '...' ; \gexec` transport looks like it
#     keeps the password out of /proc/<pid>/cmdline, but it has its own
#     problems: a newline inside the password escapes the \set meta-command,
#     and any character that survives the single-quote-doubling pass can
#     break out into a psql meta-command (`\! …`, `\i …`). Fixing this
#     properly means not shelling out to psql with the plaintext password at
#     all — a small Python helper using asyncpg (already installed in .venv)
#     would read .env, parse PG_DSN, open a direct asyncpg connection as
#     role 'postgres' through peer auth, and issue the DDL via parametrised
#     queries. That change is deferred; this script keeps its argv-based path
#     and the current cluster already has the correct role/db.
#     After any such rotation, also consider resetting pg_stat_statements if
#     it ever gets loaded in this cluster.
echo "==[3/5]== role + database"
sudo -u postgres \
    PGOPTIONS='-c log_statement=none -c log_min_error_statement=panic -c log_min_duration_statement=-1' \
    psql --quiet --no-psqlrc -v ON_ERROR_STOP=1 \
         --set=pguser="$PG_USER" \
         --set=pgpass="$PG_PASS" \
         --set=pgdb="$PG_DB" <<'SQL' >/dev/null
-- Suppress NOTICE chatter; \gexec output ("CREATE ROLE", "ALTER ROLE") is
-- harmless (it doesn't include the password) but we still quiet it below.
\set QUIET on

-- 1) role: create only if absent
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'pguser', :'pgpass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'pguser')
\gexec

-- 2) role: always re-sync login flag + password (idempotent; also refreshes a
--    rotated password without needing to drop the role).
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'pguser', :'pgpass')
\gexec

-- 3) database: create only if absent, owned by our role.
--    CREATE DATABASE cannot run inside a transaction — \gexec is fine since
--    each generated statement is its own top-level command.
SELECT format('CREATE DATABASE %I OWNER %I', :'pgdb', :'pguser')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'pgdb')
\gexec
SQL

# pg_hba: scram-sha-256 for our user over 127.0.0.1; appended once.
PGHBA=$(sudo -u postgres psql --quiet --no-psqlrc -At -c "SHOW hba_file")
HBA_LINE=$(printf 'host    %s    %s    127.0.0.1/32    scram-sha-256' "$PG_DB" "$PG_USER")
if ! sudo grep -qF "$HBA_LINE" "$PGHBA"; then
    printf '%s\n' "$HBA_LINE" | sudo tee -a "$PGHBA" >/dev/null
    sudo systemctl reload postgresql@17-main
fi

# --- 6. venv + deps -------------------------------------------------------
echo "==[4/5]== Python venv + deps (preserving existing .venv)"
if [[ ! -x .venv/bin/python ]]; then
    python3 -m venv .venv
fi
./.venv/bin/pip install --quiet --upgrade pip
./.venv/bin/pip install --quiet -r requirements.txt

# --- 7. connectivity check ------------------------------------------------
echo "==[5/5]== connectivity check"
PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
    -P pager=off -At -c "SELECT 'OK ' || version();" | head -1

echo
echo "Setup finished. TGParser was NOT started."
echo "  - role/database ready; schema NOT modified by this script."
echo "  - to restore a dump, run: bash restore_db.sh [path/to/tgparser.dump]"
