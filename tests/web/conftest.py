"""Shared fixtures for web tests: build an isolated test DB, seed minimal
rows, and point the UI pool at it via UI_PG_DSN before importing the app."""
from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.config import load as _load_cfg


BASE_DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


async def _make_db():
    tmp = f"tgparser_ui_{os.getpid()}_{secrets.token_hex(4)}"
    sys = await asyncpg.connect(BASE_DSN)
    try:
        await sys.execute(f"DROP DATABASE IF EXISTS {tmp}")
        await sys.execute(f"CREATE DATABASE {tmp}")
    finally:
        await sys.close()
    dsn = BASE_DSN.rsplit("/", 1)[0] + f"/{tmp}"
    st = Storage(dsn)
    await st.connect()
    await apply_migrations(st.pool, "migrations")
    return st, tmp, dsn


async def _drop_db(name: str):
    sys = await asyncpg.connect(BASE_DSN.rsplit("/", 1)[0] + "/postgres")
    try:
        await sys.execute(
            """SELECT pg_terminate_backend(pid) FROM pg_stat_activity
               WHERE datname=$1 AND usename=current_user AND pid<>pg_backend_pid()""",
            name)
        await sys.execute(f"DROP DATABASE IF EXISTS {name}")
    finally:
        await sys.close()


@pytest_asyncio.fixture
async def ui_env(tmp_path, monkeypatch):
    """Spin up a test DB, seed, point the UI at it, and yield the TestClient."""
    st, name, dsn = await _make_db()
    media_dir = tmp_path / "media"
    media_dir.mkdir()
    # seed chat/user/messages/media
    async with st.pool.acquire() as con:
        await con.execute(
            "INSERT INTO chats(id, type, title, username) VALUES ($1,$2,$3,$4)",
            -1001, "channel", "Test", "testchan")
        await con.execute(
            "INSERT INTO users(id, username, first_name) VALUES ($1,$2,$3)",
            42, "alice", "Alice")
        for i in range(3):
            await con.execute(
                """INSERT INTO messages(chat_id, message_id, sender_user_id, date, text)
                   VALUES ($1,$2,$3,$4,$5)""",
                -1001, 100 + i, 42,
                datetime(2026, 11, 20, 12, i, tzinfo=timezone.utc),
                f"<b>hello</b> world #{i} ')" + (' DROP TABLE users;' if i == 2 else ''))
        # one media (done + a real file for raw endpoint test)
        fid = "photo:1"
        rel = "-1001/safe.bin"
        (media_dir / "-1001").mkdir()
        (media_dir / rel).write_bytes(b"png-ish bytes")
        await con.execute(
            """INSERT INTO media(file_id, chat_id, message_id, media_type,
                                 mime_type, size, local_path, status, first_seen, completed_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            fid, -1001, 100, "photo", "image/png", 13, rel, "done",
            datetime.now(timezone.utc), datetime.now(timezone.utc))
        await con.execute(
            "INSERT INTO message_media(chat_id, message_id, file_id) VALUES ($1,$2,$3)",
            -1001, 100, fid)
        # runtime_state is a singleton (id=1) auto-upserted; set a value so
        # dashboard renders.
        await con.execute(
            """INSERT INTO runtime_state(id, pid, started_at, heartbeat_at,
                                         status, telegram_connected, telegram_last_ok_at)
               VALUES (1, 99999, NOW(), NOW(), 'syncing', TRUE, NOW())
               ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status,
                    pid=EXCLUDED.pid, heartbeat_at=EXCLUDED.heartbeat_at""")

    monkeypatch.setenv("UI_PG_DSN", dsn)
    monkeypatch.setenv("TGPARSER_MEDIA_DIR", str(media_dir))

    # Import the app FRESH so lifespan picks up our env vars.
    import importlib
    import web.app as _app_mod
    importlib.reload(_app_mod)
    from fastapi.testclient import TestClient
    with TestClient(_app_mod.app) as client:
        yield client, media_dir, dsn

    # Teardown
    try:
        await st.close()
    finally:
        await _drop_db(name)
