"""v7 tests: identity enrichment + real coordinator recovery integration test."""
import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock
import pytest


# ------------------------------------------------------------------ IDENTITY
@pytest.mark.asyncio
async def test_full_user_stores_access_hash(storage):
    await storage.upsert_user_from_entity({
        "id": 1001, "access_hash": 7876543210123456789,
        "username": "alice", "first_name": "A", "last_name": "B",
        "entity_quality": "full", "entity_source": "full",
        "full_fetched_at": datetime.now(timezone.utc),
    })
    assert await storage.user_access_hash(1001) == 7876543210123456789
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT entity_quality, username FROM users WHERE id=1001")
    assert row["entity_quality"] == "full"


@pytest.mark.asyncio
async def test_min_entity_does_not_overwrite_full(storage):
    now = datetime.now(timezone.utc)
    await storage.upsert_user_from_entity({
        "id": 1002, "access_hash": 111, "username": "full_name",
        "first_name": "Full", "bio": "I am full",
        "entity_quality": "full", "entity_source": "full",
        "full_fetched_at": now,
    })
    # a later min entity arrives with a different access_hash and no name
    await storage.upsert_user_from_entity({
        "id": 1002, "access_hash": 222,
        "entity_quality": "min", "entity_source": "message_sender",
        "is_min": True,
    })
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT username, first_name, bio, entity_quality, access_hash "
            "FROM users WHERE id=1002")
    assert row["username"] == "full_name"           # preserved
    assert row["first_name"] == "Full"              # preserved
    assert row["bio"] == "I am full"                # preserved
    assert row["entity_quality"] == "full"          # not downgraded
    assert row["access_hash"] == 222                # access_hash refreshed


@pytest.mark.asyncio
async def test_same_user_two_chats_single_row(storage):
    data = {"id": 1003, "username": "x",
            "entity_quality": "normal", "entity_source": "message_sender"}
    await storage.upsert_user_from_entity(data)
    await storage.upsert_user_from_entity(data)
    await storage.record_user_observation(
        1003, chat_id=-1001, message_id=1,
        source_type="message_sender", entity_quality="normal",
        username_at_time="x", display_name_at_time=None,
        access_hash_present=False)
    await storage.record_user_observation(
        1003, chat_id=-1002, message_id=1,
        source_type="message_sender", entity_quality="normal",
        username_at_time="x", display_name_at_time=None,
        access_hash_present=False)
    async with storage.pool.acquire() as c:
        n_users = await c.fetchval("SELECT count(*) FROM users WHERE id=1003")
        n_obs = await c.fetchval("SELECT count(*) FROM user_observations WHERE user_id=1003")
    assert n_users == 1
    assert n_obs == 2
    assert await storage.users_multi_chat() >= 1


@pytest.mark.asyncio
async def test_username_history_on_change(storage):
    await storage.upsert_user_from_entity({
        "id": 1004, "username": "old",
        "entity_quality": "normal", "entity_source": "message_sender"})
    await storage.upsert_user_from_entity({
        "id": 1004, "username": "new",
        "entity_quality": "normal", "entity_source": "message_sender",
        "source_chat_id": -1001, "source_message_id": 42})
    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT field, old_value, new_value, source_chat_id, source_message_id "
            "FROM user_profile_history WHERE user_id=1004 ORDER BY id")
    assert any(r["field"] == "username" and r["old_value"] == "old" and r["new_value"] == "new"
               for r in rows)


@pytest.mark.asyncio
async def test_access_hash_rotation(storage):
    await storage.upsert_user_from_entity({
        "id": 1005, "access_hash": 100, "username": "u",
        "entity_quality": "normal", "entity_source": "message_sender"})
    await storage.upsert_user_from_entity({
        "id": 1005, "access_hash": 200, "username": "u",
        "entity_quality": "normal", "entity_source": "message_sender"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_updated_at FROM users WHERE id=1005")
    assert row["access_hash"] == 200
    assert row["access_hash_updated_at"] is not None


@pytest.mark.asyncio
async def test_user_without_username_is_stored(storage):
    await storage.upsert_user_from_entity({
        "id": 1006, "first_name": "NoUsername",
        "entity_quality": "normal", "entity_source": "message_sender"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT username, first_name FROM users WHERE id=1006")
    assert row["username"] is None
    assert row["first_name"] == "NoUsername"


@pytest.mark.asyncio
async def test_privacy_null_bio_phone_is_not_error(storage):
    # User whose bio and phone are hidden (null) — must still succeed
    await storage.upsert_user_from_entity({
        "id": 1007, "username": "priv", "bio": None, "phone": None,
        "entity_quality": "full", "entity_source": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT bio, phone FROM users WHERE id=1007")
    assert row["bio"] is None and row["phone"] is None


@pytest.mark.asyncio
async def test_big_user_id_and_photo_bigint(storage):
    big_uid = 7_234_567_890_123
    big_photo = 5_328_000_000_000_000_000
    big_ah = 6_000_000_000_000_000_001
    await storage.upsert_user_from_entity({
        "id": big_uid, "access_hash": big_ah,
        "photo_file_id": big_photo, "username": "big",
        "entity_quality": "normal", "entity_source": "message_sender"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT id, access_hash, photo_file_id FROM users WHERE id=$1::bigint",
            big_uid)
    assert row["id"] == big_uid
    assert row["access_hash"] == big_ah
    assert row["photo_file_id"] == big_photo


@pytest.mark.asyncio
async def test_channel_id_not_in_users(storage):
    # users.id CHECK (id > 0) should refuse any negative value
    import asyncpg
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        async with storage.pool.acquire() as c:
            await c.execute(
                "INSERT INTO users (id, username) VALUES (-1001006503122, 'ch')")


@pytest.mark.asyncio
async def test_user_observations_no_text(storage):
    """Guard: user_observations table must not have a `text` column —
    the schema should prevent accidentally storing message text here."""
    async with storage.pool.acquire() as c:
        cols = {r["column_name"] for r in await c.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='user_observations'")}
    assert "text" not in cols


# ------------------------------------------- real coordinator recovery test
class _StallingClient:
    """Mock Telethon client whose iter_messages hangs after N messages."""
    def __init__(self, messages, hang_after=5):
        self.messages = messages
        self.hang_after = hang_after
        self.disconnected = 0
        self.connected = 0
        self.catch_up_count = 0
        self.handlers = []
        self._connected = True

    def is_connected(self): return self._connected
    def on(self, *_a, **_kw):
        def deco(f): self.handlers.append(f); return f
        return deco
    async def get_entity(self, x): return x
    async def get_input_entity(self, x): return x
    async def catch_up(self): self.catch_up_count += 1
    async def disconnect(self): self.disconnected += 1; self._connected = False
    async def connect(self): self.connected += 1; self._connected = True
    def iter_messages(self, entity, min_id=0, reverse=True):
        client = self
        async def _gen():
            emitted = 0
            for m in sorted(client.messages, key=lambda x: x.id):
                if m.id <= min_id: continue
                yield m
                emitted += 1
                if emitted >= client.hang_after:
                    await asyncio.sleep(999)  # the hang
        return _gen()


class _FakeMsg:
    def __init__(self, mid, chat_id):
        from telethon.tl.types import PeerChannel
        self.id = mid
        self.peer_id = PeerChannel(channel_id=abs(chat_id) - 1_000_000_000_000)
        self.from_id = None; self.sender_id = None
        self.media = None; self.fwd_from = None
        self.reply_to = None
        self.date = datetime.now(timezone.utc); self.edit_date = None
        self.post_author = None; self.message = f"msg{mid}"
        self.entities = []; self.grouped_id = None
        self.views = None; self.forwards = None
        self.replies = None; self.reactions = None
    def to_dict(self): return {"id": self.id}
    async def get_sender(self): return None


@pytest.mark.asyncio
async def test_real_coordinator_stall_recovery_integration(storage, tmp_path):
    """Full coordinator path: 5 messages succeed, then iterator hangs.
    Batch timeout fires; _recover_after_stall calls disconnect+connect+catch_up;
    checkpoint retains last-ok id; retry batch continues from checkpoint."""
    from tgparser.collector import Collector
    from tgparser.media_queue import MediaQueue
    cid = -1001234567000
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    msgs = [_FakeMsg(i, cid) for i in range(1, 20)]
    client = _StallingClient(msgs, hang_after=5)

    mq = MediaQueue(client, storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage, mq, {
        "backfill_batch_size": 100,
        "backfill_batch_timeout_seconds": 1,  # tiny so timeout fires fast
        "backfill_batch_pause_seconds": 0,
        "backfill_max_consecutive_failures": 2,
        "backfill_recovery_backoff_seconds": 0,
        "download_media": False,
        "follow_linked_discussion": False,
        "participants_sync": False,
    })
    coll._targets = [cid]

    # First invocation: iterator hangs after 5 messages.
    try:
        await asyncio.wait_for(coll.backfill(cid), timeout=1)
    except asyncio.TimeoutError:
        await coll._recover_after_stall(cid, "test_stall")

    state = await storage.get_sync_state(cid)
    # checkpoint must have advanced to 5 (the 5 processed messages)
    assert state["last_backfilled_message_id"] == 5
    # Telegram client was disconnect+connect+catch_up'd
    assert client.disconnected >= 1
    assert client.connected >= 1
    assert client.catch_up_count >= 1
    async with storage.pool.acquire() as c:
        n = await c.fetchval("SELECT count(*) FROM messages WHERE chat_id=$1", cid)
    assert n == 5

    # Now swap in a non-stalling client and prove the next batch resumes.
    client2 = _StallingClient(msgs, hang_after=999)
    coll.client = client2
    coll.media_q.client = client2
    added, status, more = await coll.backfill(cid)
    assert added == len(msgs) - 5  # resumed from checkpoint, got the rest
    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == msgs[-1].id
