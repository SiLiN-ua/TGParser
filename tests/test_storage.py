import os
from datetime import datetime, timezone
import pytest
import pytest_asyncio
import asyncpg
from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.config import load as _load_cfg

DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


@pytest_asyncio.fixture
async def storage(tmp_path_factory):
    tmp_db = f"tgparser_test_{os.getpid()}"
    sys = await asyncpg.connect(DSN)
    try:
        await sys.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        await sys.execute(f"CREATE DATABASE {tmp_db}")
    finally:
        await sys.close()
    test_dsn = DSN.rsplit("/", 1)[0] + "/" + tmp_db
    s = Storage(test_dsn)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        await s.close()
        sys = await asyncpg.connect(DSN)
        try:
            await sys.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{tmp_db}'")
            await sys.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        finally:
            await sys.close()


@pytest.mark.asyncio
async def test_migrations_tracked(storage):
    async with storage.pool.acquire() as c:
        rows = await c.fetch("SELECT version FROM schema_migrations ORDER BY version")
        versions = [r["version"] for r in rows]
        assert 1 in versions
        assert 2 in versions


@pytest.mark.asyncio
async def test_message_upsert_idempotent(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    m = {"chat_id": -1001, "message_id": 100, "date": None, "text": "hi"}
    await storage.upsert_message(m)
    await storage.upsert_message(m)
    async with storage.pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM messages")
        v = await c.fetchval("SELECT COUNT(*) FROM message_versions WHERE message_id=100")
    assert n == 1
    assert v == 1  # initial version only


@pytest.mark.asyncio
async def test_edit_creates_new_version(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 100, "text": "v1"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 100, "text": "v2",
                                  "edit_date": datetime(2026, 1, 1, tzinfo=timezone.utc)})
    async with storage.pool.acquire() as c:
        texts = [r["text"] for r in await c.fetch(
            "SELECT text FROM message_versions WHERE message_id=100 ORDER BY id")]
    assert texts == ["v1", "v2"]


@pytest.mark.asyncio
async def test_delete_marks_without_losing_content(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 200, "text": "bye"})
    await storage.mark_deleted(-1001, [200])
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT text, is_deleted FROM messages WHERE message_id=200")
    assert row["text"] == "bye"
    assert row["is_deleted"] is True


@pytest.mark.asyncio
async def test_delete_stub_when_no_prior(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.ensure_message_stub_deleted(-1001, [999])
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT is_deleted, text FROM messages WHERE message_id=999")
    assert row["is_deleted"] is True
    assert row["text"] is None


@pytest.mark.asyncio
async def test_reactions_replace(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 300})
    await storage.replace_reactions(-1001, 300, [{"reaction": "👍", "count": 5}])
    await storage.replace_reactions(-1001, 300, [
        {"reaction": "👍", "count": 7}, {"reaction": "❤️", "count": 2}])
    async with storage.pool.acquire() as c:
        rows = {r["reaction"]: r["count"] for r in await c.fetch(
            "SELECT reaction, count FROM message_reactions WHERE message_id=300")}
    assert rows == {"👍": 7, "❤️": 2}


@pytest.mark.asyncio
async def test_sync_checkpoint_monotonic(storage):
    await storage.set_backfill_checkpoint(-1001, 100, 1)
    await storage.set_backfill_checkpoint(-1001, 50, None)  # should not decrease
    row = await storage.get_sync_state(-1001)
    assert row["last_backfilled_message_id"] == 100


@pytest.mark.asyncio
async def test_media_dedup_keeps_done(storage):
    await storage.upsert_media_record({
        "file_id": "photo:1", "chat_id": -1001, "message_id": 1,
        "media_type": "photo", "status": "done", "local_path": "x.jpg",
    })
    await storage.upsert_media_record({
        "file_id": "photo:1", "status": "pending",
    })
    status, path = await storage.media_existing_path("photo:1")
    assert status == "done"
    assert path == "x.jpg"


@pytest.mark.asyncio
async def test_profile_history_captures_change(storage):
    uid = 777
    await storage.upsert_user({"id": uid, "username": "old", "full_fetched_at": None})
    await storage.upsert_user({"id": uid, "username": "new", "full_fetched_at": None})
    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT field, old_value, new_value FROM user_profile_history WHERE user_id=$1 ORDER BY id",
            uid)
    assert any(r["field"] == "username" and r["old_value"] == "old" and r["new_value"] == "new"
               for r in rows)
