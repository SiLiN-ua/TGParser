"""Additional storage-level tests for v3."""
import os
from datetime import datetime, timedelta, timezone
import pytest
import pytest_asyncio
import asyncpg
from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations


from tgparser.config import load as _load_cfg
DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


@pytest_asyncio.fixture
async def storage():
    tmp_db = f"tgparser_test_x_{os.getpid()}"
    sys_c = await asyncpg.connect(DSN)
    try:
        await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        await sys_c.execute(f"CREATE DATABASE {tmp_db}")
    finally:
        await sys_c.close()
    test_dsn = DSN.rsplit("/", 1)[0] + "/" + tmp_db
    s = Storage(test_dsn)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        await s.close()
        sys_c = await asyncpg.connect(DSN)
        try:
            await sys_c.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{tmp_db}'")
            await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        finally:
            await sys_c.close()


@pytest.mark.asyncio
async def test_migration_3_tables_exist(storage):
    async with storage.pool.acquire() as c:
        names = {r["table_name"] for r in await c.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")}
    assert {"message_media", "backfill_queue", "deletion_events",
            "avatar_history", "runtime_state"}.issubset(names)


@pytest.mark.asyncio
async def test_message_media_m2m(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 1})
    await storage.upsert_message({"chat_id": -1001, "message_id": 2})
    await storage.upsert_media_record({
        "file_id": "photo:X", "media_type": "photo",
        "status": "done", "local_path": "p.jpg",
    })
    await storage.link_message_media(-1001, 1, "photo:X")
    await storage.link_message_media(-1001, 2, "photo:X")
    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT message_id FROM message_media WHERE file_id='photo:X' ORDER BY message_id")
    assert [r["message_id"] for r in rows] == [1, 2]
    # idempotent
    await storage.link_message_media(-1001, 1, "photo:X")
    async with storage.pool.acquire() as c:
        n = await c.fetchval(
            "SELECT COUNT(*) FROM message_media WHERE file_id='photo:X'")
    assert n == 2


@pytest.mark.asyncio
async def test_backfill_queue_does_not_skip(storage):
    """Reproduces §1 requirement: queued failure prevents checkpoint advance."""
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})

    # Simulate successful processing of ids 100, 101 (not queued) and failure of 102.
    now = datetime.now(timezone.utc)
    await storage.backfill_queue_upsert(cid, 102, "boom", 3, now + timedelta(minutes=5))
    # min_pending is 102.
    assert await storage.backfill_queue_min_pending(cid) == 102

    # Mark done — min disappears.
    await storage.backfill_queue_mark_done(cid, 102)
    assert await storage.backfill_queue_min_pending(cid) is None


@pytest.mark.asyncio
async def test_avatar_change_history(storage):
    uid = 55
    assert await storage.avatar_lookup(uid, 100) is None
    await storage.avatar_record(uid, 100, "a/55_100.jpg")
    await storage.avatar_record(uid, 200, "a/55_200.jpg")
    assert await storage.avatar_lookup(uid, 100) == "a/55_100.jpg"
    assert await storage.avatar_lookup(uid, 200) == "a/55_200.jpg"
    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT photo_file_id, local_path FROM avatar_history WHERE user_id=$1 ORDER BY photo_file_id",
            uid)
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_failed_media_retry_metadata(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 50})
    await storage.upsert_media_record({
        "file_id": "doc:1", "chat_id": -1001, "message_id": 50,
        "media_type": "document", "status": "failed",
        "attempts": 1, "last_error": "net",
    })
    await storage.media_increment_attempt("doc:1", "retry")
    rows = await storage.media_get_retryable()
    assert any(r["file_id"] == "doc:1" and r["attempts"] >= 1 for r in rows)
    # Attempt counter advanced.
    async with storage.pool.acquire() as c:
        n = await c.fetchval("SELECT attempts FROM media WHERE file_id='doc:1'")
    assert n >= 2


@pytest.mark.asyncio
async def test_deletion_event_without_chat(storage):
    await storage.record_deletion_event([111, 222, 333])
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT deleted_ids FROM deletion_events ORDER BY id DESC LIMIT 1")
    import json
    ids = json.loads(row["deleted_ids"])
    assert ids == [111, 222, 333]


@pytest.mark.asyncio
async def test_participants_sync_state(storage):
    cid = -1002
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "g"})
    await storage.init_sync_state(cid)
    await storage.set_participants_sync(cid, 42, False, "admin_required")
    row = await storage.get_sync_state(cid)
    assert row["participants_synced"] is True
    assert row["participants_is_complete"] is False
    assert row["participants_limitation"] == "admin_required"
    assert row["participants_offset"] == 42


@pytest.mark.asyncio
async def test_runtime_and_heartbeat(storage):
    await storage.set_runtime(1234, "healthy", [-1001], note=None, started=True)
    r = await storage.get_runtime()
    assert r["pid"] == 1234
    assert r["status"] == "healthy"
    await storage.heartbeat()


@pytest.mark.asyncio
async def test_db_reconnect_after_pool_close(storage):
    """§4: pool survives a lost connection and transparently recovers."""
    # Forcibly close the pool: next op must re-create it via _ensure_pool.
    await storage.pool.close()
    storage.pool = None
    await storage.upsert_chat({"id": -1003, "type": "channel", "title": "x"})
    async with storage.pool.acquire() as c:
        v = await c.fetchval("SELECT title FROM chats WHERE id=-1003")
    assert v == "x"


@pytest.mark.asyncio
async def test_db_reconnect_after_terminated_backends(storage):
    """§4: terminate all backends to the test DB and verify retry recovers."""
    # Save the test DSN before we break it.
    dsn = storage.dsn
    sys_c = await asyncpg.connect(DSN)
    try:
        dbname = dsn.rsplit("/", 1)[-1]
        await sys_c.execute(
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname='{dbname}' AND pid <> pg_backend_pid()")
    finally:
        await sys_c.close()
    # Pool connections are now dead; retry should re-open transparently.
    await storage.upsert_chat({"id": -1004, "type": "channel", "title": "y"})
    async with storage.pool.acquire() as c:
        v = await c.fetchval("SELECT title FROM chats WHERE id=-1004")
    assert v == "y"


@pytest.mark.asyncio
async def test_reaction_authors_replace_when_complete(storage):
    await storage.upsert_chat({"id": -1001, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": -1001, "message_id": 77})
    # First partial write (not complete): both rows remain.
    await storage.replace_reaction_authors(-1001, 77, [
        {"reaction": "👍", "peer_id": 10, "peer_kind": "user"},
        {"reaction": "👍", "peer_id": 20, "peer_kind": "user"},
    ], is_complete=False)
    # Now Telegram returns the complete list: only peer 10 remains.
    await storage.replace_reaction_authors(-1001, 77, [
        {"reaction": "👍", "peer_id": 10, "peer_kind": "user"},
    ], is_complete=True)
    async with storage.pool.acquire() as c:
        pids = sorted(r["peer_id"] for r in await c.fetch(
            "SELECT peer_id FROM message_reaction_authors WHERE message_id=77"))
    assert pids == [10]
