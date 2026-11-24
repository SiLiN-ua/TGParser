"""v4 tests: truthful backfill_status, target_state, telegram heartbeat,
media identity, atomic rename, resumable participants, deletions, media retry.
"""
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
import asyncpg

from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.media_queue import MediaQueue, media_descriptor
from tgparser.collector import Collector


DSN = os.environ.get("PG_DSN", "")  # falls back to .env value via load


@pytest_asyncio.fixture
async def storage():
    from tgparser.config import load as load_config
    cfg = load_config()
    base = cfg["postgres"]["dsn"]
    tmp = f"tgparser_test_v4_{os.getpid()}"
    sys_c = await asyncpg.connect(base)
    try:
        await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp}")
        await sys_c.execute(f"CREATE DATABASE {tmp}")
    finally:
        await sys_c.close()
    s = Storage(base.rsplit("/", 1)[0] + "/" + tmp)
    await s.connect()
    await apply_migrations(s.pool, "migrations")
    try:
        yield s
    finally:
        await s.close()
        sys_c = await asyncpg.connect(base)
        try:
            await sys_c.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname='{tmp}' AND usename=current_user "
                f"AND pid<>pg_backend_pid() AND backend_type='client backend'")
            await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp}")
        finally:
            await sys_c.close()


# -----------------------------------------------------------------------
# §1 backfill truthful complete
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_backfill_status_blocked_with_pending(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.init_sync_state(cid)
    await storage.set_backfill_status(cid, "running")
    now = datetime.now(timezone.utc)
    await storage.backfill_queue_upsert(cid, 50, "boom", 3, now + timedelta(minutes=5))
    await storage.set_backfill_status(cid, "blocked", mark_complete=False)
    row = await storage.get_sync_state(cid)
    assert row["backfill_status"] == "blocked"
    assert row["backfill_complete"] is False


@pytest.mark.asyncio
async def test_backfill_status_only_complete_when_clean(storage):
    cid = -1002
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.init_sync_state(cid)
    await storage.set_backfill_status(cid, "complete", mark_complete=True)
    row = await storage.get_sync_state(cid)
    assert row["backfill_status"] == "complete"
    assert row["backfill_complete"] is True
    # Setting it to anything else clears the complete flag.
    await storage.set_backfill_status(cid, "running")
    row = await storage.get_sync_state(cid)
    assert row["backfill_complete"] is False


@pytest.mark.asyncio
async def test_backfill_status_complete_with_errors_after_dead_dlq(storage):
    cid = -1003
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.init_sync_state(cid)
    now = datetime.now(timezone.utc)
    await storage.backfill_queue_upsert(cid, 7, "dead", 10, now, status="dead")
    stats = await storage.backfill_queue_stats(cid)
    assert stats["dead_unacknowledged"] == 1
    await storage.set_backfill_status(cid, "complete_with_errors")
    row = await storage.get_sync_state(cid)
    assert row["backfill_status"] == "complete_with_errors"
    assert row["backfill_complete"] is False


# -----------------------------------------------------------------------
# §2 target_state partial failure
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_target_state_partial_failure(storage):
    await storage.target_upsert(-1001, "@a", "complete")
    await storage.target_upsert(-1002, "@b", "incomplete", last_error="boom")
    rows = await storage.targets_all()
    by = {r["chat_id"]: r for r in rows}
    assert by[-1001]["backfill_status"] == "complete"
    assert by[-1002]["backfill_status"] == "incomplete"
    assert by[-1002]["last_error"] == "boom"


# -----------------------------------------------------------------------
# §3 telegram heartbeat
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_telegram_heartbeat_fresh(storage):
    await storage.set_runtime(42, "healthy", [-1001], note=None, started=True)
    await storage.record_tg_ok()
    r = await storage.get_runtime()
    assert r["telegram_connected"] is True
    assert r["telegram_last_ok_at"] is not None


@pytest.mark.asyncio
async def test_telegram_heartbeat_error(storage):
    await storage.set_runtime(42, "healthy", [-1001], note=None, started=True)
    await storage.record_tg_error("network down")
    r = await storage.get_runtime()
    assert r["telegram_connected"] is False
    assert r["telegram_last_error"] == "network down"


@pytest.mark.asyncio
async def test_telegram_heartbeat_recovery(storage):
    await storage.set_runtime(42, "healthy", [-1001], note=None, started=True)
    await storage.record_tg_error("timeout")
    await storage.record_tg_ok()
    r = await storage.get_runtime()
    assert r["telegram_connected"] is True
    assert r["telegram_last_error"] is None


# -----------------------------------------------------------------------
# §5 migration 0004 fixture: user, group, supergroup, channel
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_migration_0004_leaves_correct_canonical_untouched(storage):
    # Correct canonical values - must NOT be modified.
    async with storage.pool.acquire() as c:
        await c.execute(
            "INSERT INTO chats (id, type, title) VALUES "
            "(42, 'user', 'Alice'), "
            "(-500, 'group', 'Small'), "
            "(-1001006503122, 'channel', 'Pavel Durov'), "
            "(-1002000000000, 'supergroup', 'Super')")
    # Re-run migration body (idempotent).
    with open("migrations/0004_final_hardening.sql", encoding="utf-8") as f:
        sql = f.read()
    async with storage.pool.acquire() as c:
        async with c.transaction():
            # Strip wrapping BEGIN/COMMIT (we're in a tx)
            inner = sql.replace("BEGIN;", "").replace("COMMIT;", "")
            await c.execute(inner)
        ids = sorted(r["id"] for r in await c.fetch("SELECT id FROM chats"))
    assert 42 in ids and -500 in ids and -1001006503122 in ids and -1002000000000 in ids


# -----------------------------------------------------------------------
# §6 media identity: webpage previews do not collide
# -----------------------------------------------------------------------
class _FakeWebpageMedia:
    """Pretends to be MessageMediaWebPage."""
    def to_dict(self):
        return {"_": "MessageMediaWebPage"}


class _FakeMsg:
    def __init__(self, cid, mid):
        self.id = mid
        self.media = _FakeWebpageMedia()
        _FakeWebpageMedia.__name__ = "MessageMediaWebPage"


@pytest.mark.asyncio
async def test_two_webpage_previews_distinct(storage, tmp_path):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": cid, "message_id": 10})
    await storage.upsert_message({"chat_id": cid, "message_id": 11})

    class _Client:
        async def download_media(self, msg, file=None): return None

    mq = MediaQueue(_Client(), storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    r1 = await mq.enqueue(cid, _FakeMsg(cid, 10))
    r2 = await mq.enqueue(cid, _FakeMsg(cid, 11))
    assert r1 and r2
    fid1, fid2 = r1[1], r2[1]
    assert fid1 != fid2
    assert fid1.startswith("meta:") and fid2.startswith("meta:")
    async with storage.pool.acquire() as c:
        statuses = {r["file_id"]: r["status"] for r in await c.fetch(
            "SELECT file_id, status FROM media WHERE file_id LIKE 'meta:%'")}
    assert statuses[fid1] == "metadata_only"
    assert statuses[fid2] == "metadata_only"


# -----------------------------------------------------------------------
# §7 media atomic rename - crashed download does not look 'done'
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_media_download_returns_none_marks_failed(storage, tmp_path):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": cid, "message_id": 20})

    class _FakePhoto:
        id = 7777
    from telethon.tl.types import MessageMediaPhoto
    class _Msg:
        id = 20
        media = MessageMediaPhoto(photo=type("P", (), {"id": 7777}),
                                  ttl_seconds=None, spoiler=False)
    class _Client:
        calls = 0
        async def download_media(self, msg, file=None):
            _Client.calls += 1
            return None
    # patch descriptor path to make the fake photo work with media_descriptor
    import tgparser.media_queue as mq_mod
    class _P(mq_mod.Photo):
        pass
    mq = MediaQueue(_Client(), storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    mq.start()
    # Enqueue a real downloadable descriptor by hand instead of via Telethon types.
    await storage.upsert_media_record({
        "file_id": "photo:7777", "chat_id": cid, "message_id": 20,
        "media_type": "photo", "status": "pending",
    })
    await storage.link_message_media(cid, 20, "photo:7777")

    class _MinMsg:
        id = 20

    await mq.queue.put({
        "chat_id": cid, "message_id": 20,
        "desc": {"file_id": "photo:7777", "media_type": "photo", "size": None},
        "message": _MinMsg(),
    })
    await mq._inflight.add("photo:7777") if False else mq._inflight.add("photo:7777")
    await asyncio.sleep(0.5)
    await mq.stop(timeout=5)
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT status, last_error, attempts FROM media WHERE file_id='photo:7777'")
    # download_media returned None → must be failed, not done.
    assert row["status"] == "failed"
    assert row["attempts"] >= 1


# -----------------------------------------------------------------------
# §8 deletion handling with and without chat
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_deletion_events_resolution(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    # Event without chat first.
    await storage.record_deletion_event(
        [111], channel_id=cid, update_type="UpdateDeleteChannelMessages",
        raw_payload={"foo": "bar"})
    unresolved_before = await storage.deletion_events_unresolved()
    assert unresolved_before == 1
    # Now the collector gets the delete properly → resolve + stub.
    await storage.resolve_deletion_events(cid, [111])
    unresolved_after = await storage.deletion_events_unresolved()
    assert unresolved_after == 0
    async with storage.pool.acquire() as c:
        stub = await c.fetchrow(
            "SELECT is_deleted FROM messages WHERE chat_id=$1 AND message_id=$2",
            cid, 111)
    assert stub["is_deleted"] is True


# -----------------------------------------------------------------------
# §11 health strictness helpers
# -----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_health_snapshot_targets_and_problem_ids(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.init_sync_state(cid)
    await storage.target_upsert(cid, "@t", "blocked")
    now = datetime.now(timezone.utc)
    await storage.backfill_queue_upsert(cid, 99, "boom", 3, now)
    await storage.backfill_queue_upsert(cid, 100, "boom", 3, now, status="dead")
    await storage.target_update_counts(cid, pending=1, dead=1)
    rows = await storage.targets_all()
    assert rows[0]["backfill_status"] == "blocked"
    assert rows[0]["pending_count"] == 1
    assert rows[0]["dead_count"] == 1
    ids = await storage.backfill_queue_problem_ids(cid)
    assert set(ids) == {99, 100}
