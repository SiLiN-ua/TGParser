"""Shared fixtures/helpers for the v11 (draft) dynamic-targets tests.

The draft lives in design/ on purpose (run.py must not auto-apply it). These
fixtures build temporary databases only:
  storage_v10 - migrations/0001..0010 (whatever migrations/ contains)
  storage_v11 - the same, then the draft applied explicitly as version 11
"""
from __future__ import annotations

import asyncio
import os
import secrets
from pathlib import Path

import asyncpg
import pytest_asyncio

from tgparser.migrations import apply as apply_migrations
from tgparser.storage import Storage
from tests.conftest import BASE_DSN, _admin_dsn, _safe_drop

DRAFT = Path("design/0011_dynamic_targets.draft.sql")

SNAPSHOT_TABLES = (
    "runtime_state", "policy_state", "policy_block_history", "discovery_control",
    "dialog_discovery_runs", "dialog_discovery_items", "discovered_dialogs",
    "target_state", "target_preferences", "collection_config",
    "collection_title_rules", "deployment_gates", "ui_audit_log",
    "yaml_target_imports", "sync_state", "messages",
)


async def make_db() -> tuple[Storage, str]:
    name = f"tgparser_test_{secrets.token_hex(6)}_{os.getpid()}"
    admin = await asyncpg.connect(_admin_dsn())
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    s = Storage(BASE_DSN.rsplit("/", 1)[0] + "/" + name)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    return s, name


async def apply_draft(storage: Storage, register: bool = True):
    sql = DRAFT.read_text(encoding="utf-8")
    async with storage.pool.acquire() as con:
        async with con.transaction():
            await con.execute(sql)
            if register:
                await con.execute(
                    "INSERT INTO schema_migrations (version) VALUES (11) "
                    "ON CONFLICT DO NOTHING")


async def _dispose(s: Storage, name: str):
    try:
        await s.close()
    except Exception:
        pass
    await _safe_drop(name)


@pytest_asyncio.fixture
async def storage_v10():
    s, name = await make_db()
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
