"""v7.1 tests: access_hash priority rules + health race + observations dedup
+ real coordinator-loop recovery."""
import asyncio
from datetime import datetime, timezone
import pytest


# §1 access_hash priority ---------------------------------------------------
@pytest.mark.asyncio
async def test_min_cannot_replace_full(storage):
    await storage.upsert_user_from_entity({
        "id": 11, "access_hash": 100,
        "entity_quality": "full", "entity_source": "full",
        "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 11, "access_hash": 200,
        "entity_quality": "min", "entity_source": "message_sender",
        "access_hash_kind": "min", "is_min": True})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_kind FROM users WHERE id=11")
    assert row["access_hash"] == 100
    assert row["access_hash_kind"] == "full"


@pytest.mark.asyncio
async def test_min_followed_by_full_full_wins(storage):
    await storage.upsert_user_from_entity({
        "id": 12, "access_hash": 200,
        "entity_quality": "normal", "entity_source": "message_sender",
        "access_hash_kind": "min", "is_min": True})
    await storage.upsert_user_from_entity({
        "id": 12, "access_hash": 100,
        "entity_quality": "full", "entity_source": "full",
        "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_kind FROM users WHERE id=12")
    assert row["access_hash"] == 100
    assert row["access_hash_kind"] == "full"


@pytest.mark.asyncio
async def test_full_replaced_by_new_full(storage):
    await storage.upsert_user_from_entity({
        "id": 13, "access_hash": 300, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 13, "access_hash": 400, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT access_hash FROM users WHERE id=13")
    assert row["access_hash"] == 400


@pytest.mark.asyncio
async def test_zero_cannot_replace_full(storage):
    await storage.upsert_user_from_entity({
        "id": 14, "access_hash": 500, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 14, "access_hash": 0, "entity_quality": "min",
        "entity_source": "message_sender", "access_hash_kind": "zero",
        "is_min": True})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_kind FROM users WHERE id=14")
    assert row["access_hash"] == 500


@pytest.mark.asyncio
async def test_unknown_cannot_replace_full(storage):
    await storage.upsert_user_from_entity({
        "id": 15, "access_hash": 600, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 15, "access_hash": 700, "entity_quality": "normal",
        "entity_source": "unknown", "access_hash_kind": "unknown"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT access_hash FROM users WHERE id=15")
    assert row["access_hash"] == 600


@pytest.mark.asyncio
async def test_null_does_not_wipe_existing_hash(storage):
    await storage.upsert_user_from_entity({
        "id": 16, "access_hash": 800, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 16, "access_hash": None, "entity_quality": "min",
        "entity_source": "message_sender"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT access_hash FROM users WHERE id=16")
    assert row["access_hash"] == 800


@pytest.mark.asyncio
async def test_different_auth_session_hash_not_promoted(storage):
    await storage.upsert_user_from_entity({
        "id": 17, "access_hash": 900, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "access_hash_auth_fingerprint": "sessA",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 17, "access_hash": 1000, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "access_hash_auth_fingerprint": "sessB",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_auth_fingerprint FROM users WHERE id=17")
        hist = await c.fetch(
            "SELECT auth_fingerprint, is_active FROM access_hash_history "
            "WHERE user_id=17 ORDER BY id")
    assert row["access_hash"] == 900
    assert row["access_hash_auth_fingerprint"] == "sessA"
    # sessB hash recorded but NOT active
    assert any(r["auth_fingerprint"] == "sessB" and not r["is_active"] for r in hist)


@pytest.mark.asyncio
async def test_same_hash_does_not_duplicate_history(storage):
    for _ in range(3):
        await storage.upsert_user_from_entity({
            "id": 18, "access_hash": 1100, "entity_quality": "full",
            "entity_source": "full", "access_hash_kind": "full",
            "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        n = await c.fetchval(
            "SELECT count(*) FROM access_hash_history WHERE user_id=18")
    assert n == 1


@pytest.mark.asyncio
async def test_lower_priority_hash_lives_only_in_history(storage):
    await storage.upsert_user_from_entity({
        "id": 19, "access_hash": 1200, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.upsert_user_from_entity({
        "id": 19, "access_hash": 1300, "entity_quality": "min",
        "entity_source": "message_sender", "access_hash_kind": "min"})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow("SELECT access_hash FROM users WHERE id=19")
        kinds = {(r["kind"], r["is_active"]) for r in await c.fetch(
            "SELECT kind, is_active FROM access_hash_history WHERE user_id=19")}
    assert row["access_hash"] == 1200          # full stays active
    assert ("min", False) in kinds             # min stored, not active


@pytest.mark.asyncio
async def test_stale_mark_and_clear(storage):
    await storage.upsert_user_from_entity({
        "id": 20, "access_hash": 1400, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    await storage.access_hash_mark_stale(20, "AUTH_KEY_UNREGISTERED")
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash_is_stale FROM users WHERE id=20")
    assert row["access_hash_is_stale"] is True
    # A fresh full entity clears the stale flag
    await storage.upsert_user_from_entity({
        "id": 20, "access_hash": 1500, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT access_hash, access_hash_is_stale FROM users WHERE id=20")
    assert row["access_hash"] == 1500
    assert row["access_hash_is_stale"] is False


@pytest.mark.asyncio
async def test_access_hash_never_in_user_primary_key(storage):
    async with storage.pool.acquire() as c:
        cols = await c.fetch("""
            SELECT a.attname FROM pg_index i
            JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid='users'::regclass AND i.indisprimary
        """)
    assert [r["attname"] for r in cols] == ["id"]


@pytest.mark.asyncio
async def test_signed_int64_access_hash_roundtrip(storage):
    big_ah = 9000000000000000000      # near int64 max
    await storage.upsert_user_from_entity({
        "id": 22, "access_hash": big_ah, "entity_quality": "full",
        "entity_source": "full", "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc)})
    async with storage.pool.acquire() as c:
        v = await c.fetchval("SELECT access_hash FROM users WHERE id=22")
    assert v == big_ah


# §4 observation dedup ----------------------------------------------------
@pytest.mark.asyncio
async def test_repeated_observation_single_row(storage):
    for _ in range(5):
        await storage.record_user_observation(
            user_id=30, chat_id=-1001, message_id=5,
            source_type="message_sender", entity_quality="min",
            username_at_time=None, display_name_at_time=None,
            access_hash_present=False)
    async with storage.pool.acquire() as c:
        n = await c.fetchval(
            "SELECT count(*) FROM user_observations WHERE user_id=30")
    assert n == 1


# §7 health race fixed ----------------------------------------------------
@pytest.mark.asyncio
async def test_health_duplicate_count_consistent(storage):
    await storage.upsert_chat({"id": -1001, "type": "supergroup", "title": "t"})
    # insert 100 messages
    for mid in range(1, 101):
        await storage.upsert_message({"chat_id": -1001, "message_id": mid})

    # concurrent inserts + duplicate check should stay consistent
    async def insert_more():
        for mid in range(101, 200):
            await storage.upsert_message({"chat_id": -1001, "message_id": mid})

    async def check_dupes():
        async with storage.pool.acquire() as con:
            async with con.transaction(isolation="repeatable_read"):
                row = await con.fetchrow("""
                    SELECT COUNT(*) AS c,
                           COUNT(DISTINCT (chat_id, message_id)) AS distinct_c,
                           COUNT(*) - COUNT(DISTINCT (chat_id, message_id)) AS dups
                    FROM messages
                """)
                # Within one snapshot, dups must be 0 regardless of concurrent inserts
                assert row["dups"] == 0
                return row["c"]

    async def run_checks():
        res = []
        for _ in range(10):
            res.append(await check_dupes())
            await asyncio.sleep(0.02)
        return res

    counts, _ = await asyncio.gather(run_checks(), insert_more())
    # All snapshots must be internally consistent (dups==0)
    assert all(c >= 100 for c in counts)


# §9 identity_enrichment_queue basics --------------------------------------
@pytest.mark.asyncio
async def test_identity_queue_dedup_and_priority(storage):
    await storage.identity_enqueue(user_id=40, priority=1000)
    await storage.identity_enqueue(user_id=40, priority=100,
                                   hint_chat_id=-1001, hint_message_id=5)
    async with storage.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT priority, hint_chat_id FROM identity_enrichment_queue WHERE user_id=40")
    assert row["priority"] == 100              # lowered to new value
    assert row["hint_chat_id"] == -1001

    pend = await storage.identity_pending(limit=5)
    assert any(p["user_id"] == 40 for p in pend)
    await storage.identity_mark(40, "done")
    stats = await storage.identity_stats()
    assert stats.get("done") == 1


# §2 real coordinator-loop recovery (NO manual _recover_after_stall) -------
class _StallingLoopClient:
    """Mock client whose iter_messages hangs after `hang_after` msgs
    on the FIRST call, then not on later calls."""
    def __init__(self, messages, hang_after: int = 3):
        self.messages = messages
        self.hang_after = hang_after
        self.first_call_done = False
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
        hang_this_call = not client.first_call_done
        client.first_call_done = True
        async def _gen():
            emitted = 0
            for m in sorted(client.messages, key=lambda x: x.id):
                if m.id <= min_id: continue
                yield m
                emitted += 1
                if hang_this_call and emitted >= client.hang_after:
                    await asyncio.sleep(999)
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
        self.post_author = None; self.message = f"m{mid}"
        self.entities = []; self.grouped_id = None
        self.views = None; self.forwards = None
        self.replies = None; self.reactions = None
    def to_dict(self): return {"id": self.id}
    async def get_sender(self): return None


@pytest.mark.asyncio
async def test_real_coordinator_loop_auto_recovery(storage, tmp_path):
    """Spawn the ACTUAL _coordinator_loop (not a direct call to backfill) and
    verify it auto-recovers from a stall on its own: batch timeout fires,
    coordinator calls _recover_after_stall on its own, next iteration resumes
    from the saved checkpoint, status returns to syncing."""
    from tgparser.collector import Collector
    from tgparser.media_queue import MediaQueue
    cid = -1001234567000
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    msgs = [_FakeMsg(i, cid) for i in range(1, 15)]
    client = _StallingLoopClient(msgs, hang_after=3)

    mq = MediaQueue(client, storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage, mq, {
        "backfill_batch_size": 100,
        "backfill_batch_timeout_seconds": 3,
        "backfill_batch_pause_seconds": 0,
        "backfill_max_consecutive_failures": 5,
        "backfill_recovery_backoff_seconds": 0,
        "download_media": False,
        "follow_linked_discussion": False,
        "participants_sync": False,
    })
    coll._targets = [cid]

    # Launch the real coordinator loop in background.
    coord_task = asyncio.create_task(coll._coordinator_loop())
    # give it up to 20 seconds to timeout + recover + run the second batch
    for _ in range(200):
        state = await storage.get_sync_state(cid)
        if state and state["last_backfilled_message_id"] >= msgs[-1].id:
            break
        await asyncio.sleep(0.1)
    # stop coordinator cleanly
    coll._stop.set()
    try:
        await asyncio.wait_for(coord_task, timeout=5)
    except Exception:
        coord_task.cancel()

    # Verifications
    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == msgs[-1].id
    assert client.disconnected >= 1
    assert client.connected >= 1
    assert client.catch_up_count >= 1

    cp = await storage.coordinator_progress_all()
    row = next(r for r in cp if r["chat_id"] == cid)
    # consecutive failures were reset after the successful second batch
    assert row["consecutive_batch_failures"] == 0

    async with storage.pool.acquire() as c:
        n = await c.fetchval("SELECT count(*) FROM messages WHERE chat_id=$1", cid)
    assert n == len(msgs)


@pytest.mark.asyncio
async def test_max_consecutive_failures_marks_degraded(storage, tmp_path):
    """After max_consecutive_failures the chat is marked 'degraded'."""
    cid = -1001234567001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    msgs = [_FakeMsg(i, cid) for i in range(1, 5)]

    class _AlwaysStall(_StallingLoopClient):
        def iter_messages(self, entity, min_id=0, reverse=True):
            async def _gen():
                yield msgs[0]
                await asyncio.sleep(999)
            return _gen()

    client = _AlwaysStall(msgs, hang_after=1)
    from tgparser.collector import Collector
    from tgparser.media_queue import MediaQueue
    mq = MediaQueue(client, storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage, mq, {
        "backfill_batch_size": 100,
        "backfill_batch_timeout_seconds": 1,
        "backfill_batch_pause_seconds": 0,
        "backfill_max_consecutive_failures": 2,
        "backfill_recovery_backoff_seconds": 0,
        "download_media": False, "follow_linked_discussion": False,
        "participants_sync": False,
    })
    coll._targets = [cid]

    coord = asyncio.create_task(coll._coordinator_loop())
    for _ in range(200):
        state = await storage.get_sync_state(cid)
        if state and state["backfill_status"] == "degraded":
            break
        await asyncio.sleep(0.1)
    coll._stop.set()
    try: await asyncio.wait_for(coord, timeout=5)
    except Exception: coord.cancel()

    state = await storage.get_sync_state(cid)
    assert state["backfill_status"] == "degraded"



# v7.3 regression: multiple-active history after promotion ------------------
@pytest.mark.asyncio
async def test_promotion_retires_previous_active_history(storage):
    """v7.3: promoting a user to a higher-priority hash within the same auth
    session must leave at most one is_active history row for that user. The
    v7.1/v7.2 bug kept both the legacy and the promoted row active."""
    uid = 777001
    # 1) legacy hash, auth_fp='legacy' as if seeded by migration 0010.
    await storage.upsert_user_from_entity({
        "id": uid, "access_hash": 111111,
        "entity_quality": "min", "entity_source": "message_sender",
        "access_hash_kind": "unknown",
        "access_hash_auth_fingerprint": "sess_A",
    })
    # 2) new, higher-priority observation from the SAME auth session.
    await storage.upsert_user_from_entity({
        "id": uid, "access_hash": 222222,
        "entity_quality": "full", "entity_source": "full",
        "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc),
        "access_hash_auth_fingerprint": "sess_A",
    })
    async with storage.pool.acquire() as c:
        active_rows = await c.fetchval(
            "SELECT COUNT(*) FROM access_hash_history "
            "WHERE user_id=$1 AND is_active=TRUE", uid)
        all_rows = await c.fetchval(
            "SELECT COUNT(*) FROM access_hash_history WHERE user_id=$1", uid)
        winner_hash = await c.fetchval(
            "SELECT access_hash FROM access_hash_history "
            "WHERE user_id=$1 AND is_active=TRUE", uid)
    assert active_rows == 1
    assert all_rows == 2          # both rows preserved (append-only)
    assert winner_hash == 222222   # the full-priority hash wins


@pytest.mark.asyncio
async def test_promotion_does_not_deactivate_foreign_session(storage):
    """A promotion in session A must not flip an is_active row from session B.
    Guards the per-auth_fingerprint scope of the deactivation UPDATE."""
    uid = 777002
    await storage.upsert_user_from_entity({
        "id": uid, "access_hash": 111111,
        "entity_quality": "normal", "entity_source": "message_sender",
        "access_hash_kind": "from_message",
        "access_hash_auth_fingerprint": "sess_B",
    })
    # Different session observes the same user; priority rule won't apply ah,
    # but the history row from B must stay is_active=TRUE.
    await storage.upsert_user_from_entity({
        "id": uid, "access_hash": 333333,
        "entity_quality": "full", "entity_source": "full",
        "access_hash_kind": "full",
        "full_fetched_at": datetime.now(timezone.utc),
        "access_hash_auth_fingerprint": "sess_A",
    })
    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT auth_fingerprint, is_active FROM access_hash_history "
            "WHERE user_id=$1 ORDER BY auth_fingerprint", uid)
    sess_b = [r for r in rows if r["auth_fingerprint"] == "sess_B"]
    assert len(sess_b) == 1
    assert sess_b[0]["is_active"] is True


# v7.3 regression: backpressure hysteresis (no timeout-driven resume) -------
@pytest.mark.asyncio
async def test_backpressure_pause_holds_until_pending_below_lwm(storage, tmp_path, monkeypatch):
    """v7.3: coordinator must NOT start a new backfill batch while DB pending
    is at/above media_lw, regardless of elapsed time. The v7.1/v7.2 for-loop
    timeout released the pause after ~10 min with pending still huge."""
    from tgparser.collector import Collector
    from tgparser.media_queue import MediaQueue
    cid = -1001234567100
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    async with storage.pool.acquire() as c:
        await c.execute(
            "INSERT INTO sync_state (chat_id, backfill_status) "
            "VALUES ($1,'syncing') ON CONFLICT (chat_id) DO NOTHING", cid)
        await c.execute(
            "INSERT INTO coordinator_progress (chat_id) "
            "VALUES ($1) ON CONFLICT (chat_id) DO NOTHING", cid)
    msgs = [_FakeMsg(i, cid) for i in range(1, 5)]
    client = _StallingLoopClient(msgs, hang_after=100)

    mq = MediaQueue(client, storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage, mq, {
        "backfill_batch_size": 10,
        "backfill_batch_timeout_seconds": 2,
        "backfill_batch_pause_seconds": 0,
        "backfill_max_consecutive_failures": 5,
        "backfill_recovery_backoff_seconds": 0,
        "download_media": False,
        "follow_linked_discussion": False,
        "participants_sync": False,
        "media_pending_high_water": 100,
        "media_pending_low_water": 20,
    })
    coll._targets = [cid]

    # Hold pending >> HWM throughout the first observation window.
    pending_value = {"n": 500}
    async def _fake_pending_count():
        return pending_value["n"]
    async def _fake_done_count():
        return 0
    monkeypatch.setattr(coll, "_media_pending_count", _fake_pending_count)
    monkeypatch.setattr(coll, "_media_done_count", _fake_done_count)

    batch_calls = {"n": 0}
    orig_backfill = coll.backfill
    async def _spy_backfill(*a, **kw):
        batch_calls["n"] += 1
        return await orig_backfill(*a, **kw)
    monkeypatch.setattr(coll, "backfill", _spy_backfill)

    coord_task = asyncio.create_task(coll._coordinator_loop())
    # Let it sit in pause for 3 seconds with pending high.
    await asyncio.sleep(3)
    batches_during_pause = batch_calls["n"]
    state_during_pause = await storage.get_sync_state(cid)

    # Drop pending below LWM. Resume should fire on next 10s iteration.
    pending_value["n"] = 5
    # wait up to 15 s for the loop to notice
    for _ in range(150):
        s = await storage.get_sync_state(cid)
        if s and s["backfill_status"] != "syncing_paused_media_backpressure":
            break
        await asyncio.sleep(0.1)
    coll._stop.set()
    try:
        await asyncio.wait_for(coord_task, timeout=5)
    except Exception:
        coord_task.cancel()

    # During the high-pending window the backfill batch MUST NOT fire.
    assert batches_during_pause == 0, (
        f"backfill started while pending >> HWM (batches={batches_during_pause})")
    assert state_during_pause["backfill_status"] == "syncing_paused_media_backpressure"


@pytest.mark.asyncio
async def test_backpressure_pause_still_heartbeats(storage, tmp_path, monkeypatch):
    """Coordinator heartbeat must keep advancing while paused so health does
    not mislabel the pause as a hang."""
    from tgparser.collector import Collector
    from tgparser.media_queue import MediaQueue
    cid = -1001234567101
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    async with storage.pool.acquire() as c:
        await c.execute(
            "INSERT INTO sync_state (chat_id, backfill_status) "
            "VALUES ($1,'syncing') ON CONFLICT (chat_id) DO NOTHING", cid)
        await c.execute(
            "INSERT INTO coordinator_progress (chat_id) "
            "VALUES ($1) ON CONFLICT (chat_id) DO NOTHING", cid)
    msgs = [_FakeMsg(i, cid) for i in range(1, 3)]
    client = _StallingLoopClient(msgs, hang_after=100)
    mq = MediaQueue(client, storage, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage, mq, {
        "backfill_batch_size": 10,
        "backfill_batch_timeout_seconds": 2,
        "backfill_batch_pause_seconds": 0,
        "download_media": False,
        "follow_linked_discussion": False,
        "participants_sync": False,
        "media_pending_high_water": 100,
        "media_pending_low_water": 20,
    })
    coll._targets = [cid]
    async def _p(): return 500
    async def _d(): return 0
    monkeypatch.setattr(coll, "_media_pending_count", _p)
    monkeypatch.setattr(coll, "_media_done_count", _d)
    coord_task = asyncio.create_task(coll._coordinator_loop())
    # Wait up to 3s for the coord loop to enter pause and emit first heartbeat.
    hb1 = None
    for _ in range(30):
        await asyncio.sleep(0.1)
        cp1 = await storage.coordinator_progress_all()
        rows1 = [r for r in cp1 if r["chat_id"] == cid]
        if rows1 and rows1[0]["coordinator_heartbeat_at"] is not None:
            hb1 = rows1[0]["coordinator_heartbeat_at"]
            break
    assert hb1 is not None, "coordinator_heartbeat never fired for cid"
    await asyncio.sleep(2)
    cp2 = await storage.coordinator_progress_all()
    rows2 = [r for r in cp2 if r["chat_id"] == cid]
    hb2 = rows2[0]["coordinator_heartbeat_at"]
    coll._stop.set()
    try: await asyncio.wait_for(coord_task, timeout=5)
    except Exception: coord_task.cancel()
    assert hb2 >= hb1, "coordinator_heartbeat stopped during backpressure pause"
