"""Shared pytest fixtures with safe DB teardown.

Uses asyncpg against the user's own tgparser Postgres DSN (from .env). Each test
gets a UNIQUE test DB (pid + os.urandom suffix) so parallel/sequential runs
never collide. Teardown:
  1. closes the test Storage pool;
  2. filters pg_stat_activity to the current user only (we can't terminate
     superuser backends and shouldn't try);
  3. drops the test DB; on failure leaves a WARN but never a traceback.
"""
import asyncio
import os
import secrets
import pytest
import pytest_asyncio
import asyncpg
from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.config import load as _load_cfg


BASE_DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


def _admin_dsn() -> str:
    """DSN pointing at the postgres default db, same creds as our role."""
    base, _ = BASE_DSN.rsplit("/", 1)
    return base + "/postgres"


async def _safe_drop(dbname: str):
    con = await asyncpg.connect(_admin_dsn())
    try:
        # Only touch connections owned by our role AND belonging to the target db,
        # excluding ourselves. Never touch superuser / autovacuum / replication.
        await con.execute(f"""
            SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE datname = $1
              AND usename = current_user
              AND pid <> pg_backend_pid()
              AND backend_type = 'client backend'
        """, dbname)
        # tiny breath
        await asyncio.sleep(0.1)
        await con.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
    except asyncpg.exceptions.InsufficientPrivilegeError:
        # some backend we can't kill was holding the DB; try again without force
        try:
            await con.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        except Exception as e:
            import warnings
            warnings.warn(f"could not drop {dbname}: {e}")
    except Exception as e:
        import warnings
        warnings.warn(f"could not drop {dbname}: {e}")
    finally:
        await con.close()


@pytest_asyncio.fixture
async def storage():
    """Per-test Storage instance with its own DB."""
    tmp_db = "tgparser_test_" + secrets.token_hex(6) + f"_{os.getpid()}"
    admin = await asyncpg.connect(_admin_dsn())
    try:
        await admin.execute(f'CREATE DATABASE "{tmp_db}"')
    finally:
        await admin.close()
    test_dsn = BASE_DSN.rsplit("/", 1)[0] + "/" + tmp_db
    s = Storage(test_dsn)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        try:
            await s.close()
        except Exception:
            pass
        await _safe_drop(tmp_db)


def pytest_sessionfinish(session, exitstatus):
    """Sweep any leftover tgparser_test_* DBs from past crashes."""
    async def _sweep():
        try:
            con = await asyncpg.connect(_admin_dsn())
        except Exception:
            return
        try:
            rows = await con.fetch(
                "SELECT datname FROM pg_database "
                "WHERE datname LIKE 'tgparser_test_%' AND datistemplate=false")
            for r in rows:
                await _safe_drop(r["datname"])
        finally:
            await con.close()
    try:
        asyncio.run(_sweep())
    except Exception:
        pass
