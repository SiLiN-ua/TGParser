"""Shared fixtures/helpers for the v11 dynamic-targets tests.

Migration 0011 lives in migrations/ as a normal numbered migration, so
`apply_migrations` picks it up automatically. These fixtures build temporary
databases only:
  storage_v10 - applies ONLY migrations/0001..0010 (for upgrade/seed tests)
  storage_v11 - applies the full sequence through 0011
"""
from __future__ import annotations

import asyncio
import os
import re
import secrets
from pathlib import Path

import asyncpg
import pytest_asyncio

from tgparser.migrations import apply as apply_migrations
from tgparser.storage import Storage
from tests.conftest import BASE_DSN, _admin_dsn, _safe_drop

MIG_0011 = Path("migrations/0011_dynamic_targets.sql")
# Kept as a backward-compatible alias for tests that import DRAFT.
DRAFT = MIG_0011

SNAPSHOT_TABLES = (
    "runtime_state", "policy_state", "policy_block_history", "discovery_control",
    "dialog_discovery_runs", "dialog_discovery_items", "discovered_dialogs",
    "target_state", "target_preferences", "collection_config",
    "collection_title_rules", "deployment_gates", "ui_audit_log",
    "yaml_target_imports", "sync_state", "messages",
)


async def _create_blank_db() -> tuple[Storage, str]:
    name = f"tgparser_test_{secrets.token_hex(6)}_{os.getpid()}"
    admin = await asyncpg.connect(_admin_dsn())
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    s = Storage(BASE_DSN.rsplit("/", 1)[0] + "/" + name)
    await s.connect()
    return s, name


async def make_db() -> tuple[Storage, str]:
    """Temporary DB with the full migration sequence applied (currently
    through v11)."""
    s, name = await _create_blank_db()
    await apply_migrations(s.pool, "migrations")
    return s, name


MIG_RX = re.compile(r"^(\d+)_.*\.sql$")


async def apply_up_to(storage: Storage, max_version: int):
    """Apply every migration whose version is <= max_version, skipping any
    already registered. The migration file list comes from the real
    migrations/ directory — no per-version duplication of file paths here."""
    files = sorted(Path("migrations").glob("*.sql"))
    async with storage.pool.acquire() as con:
        await con.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version INT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW())")
        done = {r["version"] for r in
                await con.fetch("SELECT version FROM schema_migrations")}
        for f in files:
            m = MIG_RX.match(f.name)
            if not m:
                continue
            ver = int(m.group(1))
            if ver > max_version or ver in done:
                continue
            async with con.transaction():
                await con.execute(f.read_text(encoding="utf-8"))
                await con.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1)", ver)


async def make_db_v10() -> tuple[Storage, str]:
    """Temporary DB with ONLY migrations 0001..0010 applied, for v10 shape /
    upgrade tests."""
    s, name = await _create_blank_db()
    await apply_up_to(s, 10)
    return s, name


async def apply_migration_11(storage: Storage, register: bool = True):
    """Apply migrations/0011_dynamic_targets.sql explicitly to a v10 DB
    and record it in schema_migrations. For the upgrade/seed tests."""
    sql = MIG_0011.read_text(encoding="utf-8")
    async with storage.pool.acquire() as con:
        async with con.transaction():
            await con.execute(sql)
            if register:
                await con.execute(
                    "INSERT INTO schema_migrations (version) VALUES (11) "
                    "ON CONFLICT DO NOTHING")


# Backward-compatible alias.
apply_draft = apply_migration_11


async def _dispose(s: Storage, name: str):
    try:
        await s.close()
    except Exception:
        pass
    await _safe_drop(name)


@pytest_asyncio.fixture
async def storage_v10():
    s, name = await make_db_v10()
    try:
        yield s
    finally:
        await _dispose(s, name)


@pytest_asyncio.fixture
async def storage_v11():
    s, name = await make_db()
    await apply_draft(s)
    try:
        yield s
    finally:
        await _dispose(s, name)


async def snapshot(storage: Storage) -> dict:
    """md5 of every row (including timestamps) of the tables we care about."""
    out = {}
    async with storage.pool.acquire() as con:
        for t in SNAPSHOT_TABLES:
            if not await con.fetchval("SELECT to_regclass($1) IS NOT NULL", t):
                continue
            out[t] = await con.fetchval(
                f"SELECT md5(COALESCE(string_agg(x::text, '|' ORDER BY x::text), '')) "
                f"FROM {t} x")
    return out


class NotifyCounter:
    """LISTEN tgp_policy on a dedicated connection."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.payloads: list[str] = []
        self.con = None

    async def __aenter__(self):
        self.con = await asyncpg.connect(self.dsn)
        await self.con.add_listener("tgp_policy", self._cb)
        return self

    def _cb(self, con, pid, channel, payload):
        self.payloads.append(payload)

    async def settle(self, delay: float = 0.3) -> int:
        await asyncio.sleep(delay)
        return len(self.payloads)

    async def __aexit__(self, *exc):
        await self.con.close()


async def version(storage: Storage) -> int:
    async with storage.pool.acquire() as con:
        return int(await con.fetchval("SELECT requested_version FROM policy_state WHERE id=1"))


async def add_target(storage: Storage, chat_id: int, enabled: bool = True):
    async with storage.pool.acquire() as con:
        await con.execute(
            "INSERT INTO target_state (chat_id, target, backfill_status, enabled) "
            "VALUES ($1,'t','running',$2) "
            "ON CONFLICT (chat_id) DO UPDATE SET enabled=EXCLUDED.enabled",
            chat_id, enabled)


async def add_dialog(storage: Storage, chat_id: int, category: str = "megagroup",
                     is_archived: bool = False, title: str | None = "t",
                     username: str | None = None, gone: bool = False):
    async with storage.pool.acquire() as con:
        await con.execute(
            "INSERT INTO discovered_dialogs (chat_id, category, title, username, "
            "is_archived, gone_at) VALUES ($1,$2,$3,$4,$5, "
            "CASE WHEN $6 THEN NOW() END)",
            chat_id, category, title, username, is_archived, gone)


async def add_pref(storage: Storage, chat_id: int, selected=False, excluded=False):
    async with storage.pool.acquire() as con:
        await con.execute(
            "INSERT INTO target_preferences (chat_id, selected, excluded) VALUES ($1,$2,$3)",
            chat_id, selected, excluded)


async def set_config(storage: Storage, **cols):
    keys = list(cols)
    sets = ", ".join(f"{k}=${i + 1}" for i, k in enumerate(keys))
    async with storage.pool.acquire() as con:
        await con.execute(f"UPDATE collection_config SET {sets} WHERE id=1",
                          *[cols[k] for k in keys])


async def policy(storage: Storage, chat_id: int):
    async with storage.pool.acquire() as con:
        return await con.fetchrow("SELECT * FROM v_target_policy WHERE chat_id=$1", chat_id)


async def unthrottle(storage: Storage):
    async with storage.pool.acquire() as con:
        await con.execute("UPDATE discovery_control SET next_allowed_at=NULL WHERE id=1")
