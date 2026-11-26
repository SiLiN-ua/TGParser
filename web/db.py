"""asyncpg pool for the UI. Connects with a statement timeout so a runaway
query from the UI can never block the collector's workload."""
from __future__ import annotations

import logging

import asyncpg

from .config import UIConfig

log = logging.getLogger("tgparser.ui.db")


_POOL: asyncpg.Pool | None = None


async def _set_defaults(con: asyncpg.Connection) -> None:
    """Session-level guards for every UI connection."""
    # Hard cap on any single query from the UI.
    await con.execute("SET statement_timeout = '15s'")
    await con.execute("SET idle_in_transaction_session_timeout = '30s'")
    # UI only reads. Belt-and-braces alongside the role-level grants.
    await con.execute("SET default_transaction_read_only = on")


async def init_pool(cfg: UIConfig) -> asyncpg.Pool:
    global _POOL
    if _POOL is None:
        _POOL = await asyncpg.create_pool(
            dsn=cfg.pg_dsn,
            min_size=1, max_size=4,
            command_timeout=15,
            init=_set_defaults,
        )
    return _POOL


async def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        await _POOL.close()
        _POOL = None


def pool() -> asyncpg.Pool:
    if _POOL is None:
        raise RuntimeError("db pool not initialised")
    return _POOL
