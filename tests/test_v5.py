"""v5 tests: batch coordinator, target lifecycle, user profile queue,
bigint photo_file_id, error audit, live-during-backfill."""
import asyncio
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
    tmp = f"tgparser_test_v5_{os.getpid()}"
    sys_c = await asyncpg.connect(DSN)
    try:
        await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp}")
        await sys_c.execute(f"CREATE DATABASE {tmp}")
    finally:
        await sys_c.close()
    s = Storage(DSN.rsplit("/", 1)[0] + "/" + tmp)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        await s.close()
        sys_c = await asyncpg.connect(DSN)
        try:
            await sys_c.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname='{tmp}' AND usename=current_user "
                f"AND pid<>pg_backend_pid() AND backend_type='client backend'")
            await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp}")
        finally:
            await sys_c.close()


# §2 target lifecycle --------------------------------------------------------
@pytest.mark.asyncio
async def test_target_lifecycle_disable(storage):
    await storage.target_upsert(-1001, "@a", "complete")
    await storage.target_upsert(-1002, "@b", "syncing")
    await storage.target_disable(-1001, reason="removed_from_config")

    active = await storage.targets_active()
    all_targets = await storage.targets_all()
    assert len(active) == 1 and active[0]["chat_id"] == -1002
    assert len(all_targets) == 2
    by = {r["chat_id"]: r for r in all_targets}
    assert by[-1001]["enabled"] is False
    assert by[-1001]["disabled_reason"] == "removed_from_config"
    assert by[-1002]["enabled"] is True

    await storage.target_enable(-1001)
    assert len(await storage.targets_active()) == 2


# §5 bigint photo_file_id ----------------------------------------------------
@pytest.mark.asyncio
async def test_photo_file_id_bigint_roundtrip(storage):
    huge = 5_328_000_000_000_000_000  # > 2^31
    assert huge > 2**31
    await storage.avatar_record(user_id=99, photo_file_id=huge,
                                local_path="a/99_huge.jpg")
    assert await storage.avatar_lookup(user_id=99, photo_file_id=huge) == "a/99_huge.jpg"
    async with storage.pool.acquire() as c:
        v = await c.fetchval(
            "SELECT photo_file_id FROM avatar_history WHERE user_id=99")
    assert v == huge


# §6 user profile queue dedup -----------------------------------------------
@pytest.mark.asyncio
async def test_user_profile_queue_dedup_and_retry(storage):
    uid = 123456
    await storage.user_profile_queue_upsert(uid, "entity_missing",
                                            hint_chat_id=-100, hint_message_id=5)
    await storage.user_profile_queue_upsert(uid, "entity_missing_again",
                                            hint_chat_id=-100, hint_message_id=6)
    async with storage.pool.acquire() as c:
        n = await c.fetchval(
            "SELECT count(*) FROM user_profile_queue WHERE user_id=$1::bigint", uid)
        row = await c.fetchrow(
            "SELECT attempts, hint_chat_id, hint_message_id, status "
            "FROM user_profile_queue WHERE user_id=$1::bigint", uid)
    assert n == 1, "one row per user_id (dedup)"
    assert row["attempts"] == 2
    assert row["hint_chat_id"] == -100
    assert row["hint_message_id"] == 5  # kept first-seen hint

    # move next_retry to past so pending() returns it
    async with storage.pool.acquire() as c:
        await c.execute("UPDATE user_profile_queue SET next_retry_at = NOW() - interval '1 minute' WHERE user_id=$1::bigint", uid)
    pending = await storage.user_profile_queue_pending(limit=5)
    assert [r["user_id"] for r in pending] == [uid]

    await storage.user_profile_queue_mark_done(uid)
    assert len(await storage.user_profile_queue_pending(limit=5)) == 0
    stats = await storage.user_profile_queue_stats()
    assert stats.get("done") == 1


# §3 error audit -------------------------------------------------------------
@pytest.mark.asyncio
async def test_errors_unresolved_by_group(storage):
    async with storage.pool.acquire() as c:
        for g in ["kind_A", "kind_A", "kind_B"]:
            await c.execute(
                "INSERT INTO errors (kind, error_group, message, resolved_at) "
                "VALUES ($1, $1, 'x', NULL)", g)
        await c.execute(
            "INSERT INTO errors (kind, error_group, message, resolved_at) "
            "VALUES ('legacy', 'old_group', 'x', NOW())")
    groups = await storage.errors_unresolved_by_group()
    g_map = {r["group"]: r["n"] for r in groups}
    assert g_map == {"kind_A": 2, "kind_B": 1}

    await storage.errors_resolve_group("kind_A", "fixed in test")
    groups = await storage.errors_unresolved_by_group()
    g_map = {r["group"]: r["n"] for r in groups}
    assert g_map == {"kind_B": 1}


# §1 coordinator — two batches same PID (storage-level proof) ----------------
@pytest.mark.asyncio
async def test_two_batches_in_one_pid(storage):
    """Proves that `record_batch_progress` increments the counter without
    needing a restart; the coordinator uses this to know how many batches
    it ran this PID."""
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.init_sync_state(cid)
    await storage.record_batch_progress(cid)
    await storage.record_batch_progress(cid)
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT batches_completed, last_batch_at FROM sync_state WHERE chat_id=$1", cid)
    assert row["batches_completed"] == 2
    assert row["last_batch_at"] is not None


# §7 throughput / oldest pending ---------------------------------------------
@pytest.mark.asyncio
async def test_throughput_and_oldest_pending(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    # Insert a batch of fresh messages
    for mid in range(100, 110):
        await storage.upsert_message({"chat_id": cid, "message_id": mid,
                                      "date": datetime.now(timezone.utc)})
    # Sanity: messages_per_minute should be > 0 over 5-min window
    mpm = await storage.messages_per_minute(window_minutes=5)
    assert mpm > 0.0

    # Insert an old pending media record
    await storage.upsert_media_record({
        "file_id": "photo:old", "chat_id": cid, "message_id": 100,
        "media_type": "photo", "status": "pending",
    })
    # Backdate first_seen to simulate age
    async with storage.pool.acquire() as c:
        await c.execute(
            "UPDATE media SET first_seen = NOW() - interval '600 seconds' WHERE file_id='photo:old'")
    age = await storage.media_oldest_pending_age()
    assert age is not None and age >= 590


# §1 live message during backfill does not block (storage-level proof) -------
@pytest.mark.asyncio
async def test_live_during_backfill_no_block(storage):
    """Backfill batch writes and a concurrent live-message upsert must both
    complete without blocking each other."""
    cid = -1002
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})

    async def do_batch():
        for mid in range(1, 50):
            await storage.upsert_message({"chat_id": cid, "message_id": mid})

    async def do_live():
        # Concurrent "new message" arrival - unrelated higher id
        await storage.upsert_message({"chat_id": cid, "message_id": 10_000,
                                      "text": "live!"})
    await asyncio.gather(do_batch(), do_live())
    async with storage.pool.acquire() as c:
        live_row = await c.fetchrow(
            "SELECT text FROM messages WHERE chat_id=$1 AND message_id=10000", cid)
        batch_count = await c.fetchval(
            "SELECT COUNT(*) FROM messages WHERE chat_id=$1 AND message_id<100", cid)
    assert live_row["text"] == "live!"
    assert batch_count == 49
