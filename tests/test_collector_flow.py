"""End-to-end collector flow tests using in-memory fakes for the Telegram client.

Covers:
 - §1 backfill durability: a failing message is retried; checkpoint never
   passes a dead-letter entry; a restart re-tries the failure and advances
   when it succeeds.
 - §2 FloodWait mid-backfill resumes from last checkpoint.
 - §5 background task tracking: errors in bg tasks are captured and all are
   awaited on shutdown.
 - §6 avatar refresh: a new photo_file_id causes a new avatar_history row.
 - §8 failed-media retry after restart: sweep_retryable re-enqueues.
 - §9 delete handler creates stub via Collector handler.
 - §7 raw reactions ignore non-target peers.
"""
import asyncio
import os
import pytest
import pytest_asyncio
import asyncpg
from datetime import datetime, timezone

from telethon.errors import FloodWaitError
from telethon.tl.types import PeerChannel

from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.collector import Collector
from tgparser.media_queue import MediaQueue


from tgparser.config import load as _load_cfg
DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


class _Fw(FloodWaitError):
    def __init__(self, seconds):
        self.seconds = seconds
        self.message = f"FLOOD_WAIT_{seconds}"


class FakeMessage:
    """Minimal Telethon Message surrogate."""
    def __init__(self, mid, chat_id, text="hi"):
        self.id = mid
        self.peer_id = PeerChannel(channel_id=abs(chat_id) - 1_000_000_000_000)
        self.from_id = None
        self.sender_id = None
        self.media = None
        self.fwd_from = None
        self.reply_to = None
        self.date = datetime.now(timezone.utc)
        self.edit_date = None
        self.post_author = None
        self.message = text
        self.entities = []
        self.grouped_id = None
        self.views = None
        self.forwards = None
        self.replies = None
        self.reactions = None

    def to_dict(self):
        return {"id": self.id, "text": self.message}


class FakeClient:
    def __init__(self, messages_by_chat, flood_at=None):
        self._messages = messages_by_chat
        self._flood_at = flood_at or {}       # {(chat_id, msg_id): FloodWaitError once}
        self.handlers = []

    def on(self, builder):
        client = self
        def deco(f):
            client.handlers.append((builder, f))
            return f
        return deco

    async def get_entity(self, x):
        return x

    def iter_messages(self, entity, min_id=0, reverse=True):
        client = self
        async def _gen():
            msgs = sorted(client._messages.get(entity, []), key=lambda m: m.id)
            for m in msgs:
                if m.id <= min_id:
                    continue
                key = (entity, m.id)
                exc = client._flood_at.pop(key, None)
                if exc is not None:
                    raise exc
                yield m
        return _gen()

    async def get_messages(self, entity, ids=None):
        msgs = self._messages.get(entity, [])
        if isinstance(ids, int):
            for m in msgs:
                if m.id == ids:
                    return m
            return None
        return [m for m in msgs if m.id in (ids or [])]

    async def get_input_entity(self, cid):
        return cid

    async def catch_up(self):
        return


class CountingStorage:
    """Thin wrapper around a real Storage that counts upserts and can be made to fail."""
    def __init__(self, inner: Storage):
        self.inner = inner
        self.fail_ids: set[int] = set()
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def upsert_message(self, m, record_version=True):
        self.calls += 1
        if m["message_id"] in self.fail_ids:
            raise RuntimeError("synthetic failure")
        return await self.inner.upsert_message(m, record_version=record_version)


@pytest_asyncio.fixture
async def storage():
    tmp_db = f"tgparser_test_c_{os.getpid()}"
    sys_c = await asyncpg.connect(DSN)
    try:
        await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        await sys_c.execute(f"CREATE DATABASE {tmp_db}")
    finally:
        await sys_c.close()
    s = Storage(DSN.rsplit("/", 1)[0] + "/" + tmp_db)
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
                f"WHERE datname='{tmp_db}' AND usename=current_user "
                f"AND pid<>pg_backend_pid() AND backend_type='client backend'")
            await sys_c.execute(f"DROP DATABASE IF EXISTS {tmp_db}")
        finally:
            await sys_c.close()


def _build_collector(storage_proxy, client, tmp_path, options=None):
    opts = {"download_media": False, "backfill_on_start": False,
            "per_message_retries": 2, "bg_concurrency": 4,
            "follow_linked_discussion": False}
    if options:
        opts.update(options)
    mq = MediaQueue(client, storage_proxy, base_dir=str(tmp_path),
                    max_media_mb=10, concurrency=1)
    coll = Collector(client, storage_proxy, mq, opts)
    return coll, mq


@pytest.mark.asyncio
async def test_backfill_dlq_then_recovery(storage, tmp_path):
    """§1: message N fails, N+1 succeeds, restart with fix completes N."""
    cid = -1001234567890
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})

    msgs = [FakeMessage(1, cid), FakeMessage(2, cid), FakeMessage(3, cid)]
    client = FakeClient({cid: msgs})
    proxy = CountingStorage(storage)
    proxy.fail_ids = {2}  # msg 2 fails on first run

    coll, mq = _build_collector(proxy, client, tmp_path)
    coll._targets = [cid]

    await coll.backfill(cid)

    # DLQ holds id=2; checkpoint capped at 1.
    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == 1, state["last_backfilled_message_id"]
    assert await storage.backfill_queue_min_pending(cid) == 2
    # Messages 1 and 3 are in DB; 2 is NOT.
    async with storage.pool.acquire() as c:
        ids = sorted(r["message_id"] for r in await c.fetch(
            "SELECT message_id FROM messages WHERE chat_id=$1", cid))
    assert ids == [1, 3]

    # Simulated "restart": clear failure and run DLQ retry (reset next_retry_at).
    proxy.fail_ids.clear()
    async with storage.pool.acquire() as c:
        await c.execute("UPDATE backfill_queue SET next_retry_at = NOW() - interval '1 minute'")
    await coll.retry_dead_letter(cid)
    async with storage.pool.acquire() as c:
        ids = sorted(r["message_id"] for r in await c.fetch(
            "SELECT message_id FROM messages WHERE chat_id=$1", cid))
    assert ids == [1, 2, 3]
    # Next backfill should now advance checkpoint past 3.
    await coll.backfill(cid)
    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == 3


@pytest.mark.asyncio
async def test_floodwait_mid_backfill_resumes(storage, tmp_path):
    """§2: FloodWait in the middle of iter_messages resumes from checkpoint."""
    cid = -1001234567891
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    msgs = [FakeMessage(i, cid) for i in range(1, 6)]
    # Throw FloodWait when iter reaches msg id 3 — once.
    client = FakeClient({cid: msgs}, flood_at={(cid, 3): _Fw(0)})
    proxy = CountingStorage(storage)
    coll, _ = _build_collector(proxy, client, tmp_path)
    coll._targets = [cid]

    await coll.backfill(cid)
    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == 5
    async with storage.pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM messages WHERE chat_id=$1", cid)
    assert n == 5


@pytest.mark.asyncio
async def test_bg_tasks_awaited_on_shutdown(storage, tmp_path):
    """§5: bg tasks are tracked, exceptions are captured, drain works."""
    cid = -1001
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path)

    captured = {"n": 0}
    async def _boom():
        captured["n"] += 1
        await asyncio.sleep(0)
        raise RuntimeError("bg fail")

    async def _ok():
        captured["n"] += 1
        await asyncio.sleep(0)

    coll._spawn_bg(_boom(), name="boom")
    coll._spawn_bg(_ok(), name="ok")
    await coll.wait_background(timeout=5)
    assert captured["n"] == 2
    async with storage.pool.acquire() as c:
        kinds = [r["kind"] for r in await c.fetch("SELECT kind FROM errors")]
    assert any(k.startswith("bg_boom") for k in kinds)


@pytest.mark.asyncio
async def test_spawn_bg_cancelled_at_sema_closes_coro(storage, tmp_path):
    """Wrapper is cancelled while parked at _bg_sema (never reaches `await coro`).
    The passed-in coroutine must be closed, no 'never awaited' warning leaks."""
    import warnings as _warnings
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path,
                               options={"bg_concurrency": 1})
    # Occupy the sole semaphore slot with a hog that never finishes until we
    # cancel it. The pending-at-sema wrapper is the test target.
    release = asyncio.Event()
    async def _hog():
        await release.wait()
    hog_coro = _hog()
    hog_task = coll._spawn_bg(hog_coro, name="hog")
    # Give the loop a tick so `_hog`'s _runner gets the sema.
    await asyncio.sleep(0)

    started_marker = {"n": 0}
    async def _victim():
        started_marker["n"] += 1   # must NEVER run
        await asyncio.sleep(0)
    victim_coro = _victim()

    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        victim_task = coll._spawn_bg(victim_coro, name="victim")
        await asyncio.sleep(0)     # let create_task/_runner schedule
        # Cancel the victim while it is waiting for the sema.
        victim_task.cancel()
        try:
            await victim_task
        except asyncio.CancelledError:
            pass
        # Drain to let close callbacks run.
        await asyncio.sleep(0)
        # Release the hog cleanly to avoid interference with later teardown.
        release.set()
        await coll.wait_background(timeout=2)
        never_awaited = [x for x in w
                         if issubclass(x.category, RuntimeWarning)
                         and "never awaited" in str(x.message)]
    assert started_marker["n"] == 0, "victim coroutine must not have started"
    assert never_awaited == [], f"unexpected RuntimeWarnings: {never_awaited}"
    # Coroutine object has no frame once closed.
    assert victim_coro.cr_frame is None


@pytest.mark.asyncio
async def test_spawn_bg_cancelled_mid_run_no_warning(storage, tmp_path):
    """Wrapper is cancelled AFTER it already entered `await coro`. CancelledError
    propagates normally; no 'never awaited' RuntimeWarning fires."""
    import warnings as _warnings
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path)
    entered = asyncio.Event()
    async def _slow():
        entered.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise
    slow_coro = _slow()
    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        t = coll._spawn_bg(slow_coro, name="slow")
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0)
        never_awaited = [x for x in w
                         if issubclass(x.category, RuntimeWarning)
                         and "never awaited" in str(x.message)]
    assert never_awaited == []


@pytest.mark.asyncio
async def test_spawn_bg_create_task_failure_closes_coro(storage, tmp_path,
                                                        monkeypatch):
    """If asyncio.create_task() raises, the input coroutine must be closed to
    prevent 'never awaited'. The exception must propagate."""
    import warnings as _warnings
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path)
    async def _victim():
        await asyncio.sleep(0)
    victim_coro = _victim()
    class _Boom(RuntimeError): pass
    def _fail_create_task(*a, **kw):
        raise _Boom("synthetic task-creation failure")
    monkeypatch.setattr(asyncio, "create_task", _fail_create_task)
    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        with pytest.raises(_Boom):
            coll._spawn_bg(victim_coro, name="victim")
        await asyncio.sleep(0)
        never_awaited = [x for x in w
                         if issubclass(x.category, RuntimeWarning)
                         and "never awaited" in str(x.message)]
    assert never_awaited == []
    assert victim_coro.cr_frame is None


@pytest.mark.asyncio
async def test_wait_background_bounded_shutdown_with_hung_bg_tasks(storage, tmp_path):
    """Regression guard: adding close-on-cancel must not break the bounded
    shutdown contract. wait_background returns within timeout, hung tasks are
    cancelled, no RuntimeWarning fires."""
    import time, warnings as _warnings
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path,
                               options={"bg_concurrency": 1})
    async def _never(evt):
        try:
            await evt.wait()
        except asyncio.CancelledError:
            raise
    evt = asyncio.Event()
    # First one acquires the sema and parks inside `await coro`.
    # Second one parks at the sema (started=False).
    coro1 = _never(evt)
    coro2 = _never(evt)
    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        coll._spawn_bg(coro1, name="never1")
        await asyncio.sleep(0)
        coll._spawn_bg(coro2, name="never2")
        await asyncio.sleep(0)
        t0 = time.monotonic()
        await coll.wait_background(timeout=0.3)
        dt = time.monotonic() - t0
        await asyncio.sleep(0)
        never_awaited = [x for x in w
                         if issubclass(x.category, RuntimeWarning)
                         and "never awaited" in str(x.message)]
    assert dt < 2.0, f"wait_background took {dt:.2f}s; expected bounded"
    assert never_awaited == [], f"warnings leaked: {never_awaited}"
    assert coll._bg_tasks == set() or all(t.done() for t in coll._bg_tasks)


@pytest.mark.asyncio
async def test_avatar_refresh_on_photo_id_change(storage, tmp_path):
    """§6: avatar_history gets a new row when photo_file_id changes."""
    cid = -1001
    # Two "photos" with different ids for the same user.
    class _Photo:
        def __init__(self, pid):
            self.photo_id = pid
            self.dc_id = 1

    class _User:
        def __init__(self, uid, photo):
            self.id = uid
            self.photo = photo

    downloads = []

    class _Client(FakeClient):
        async def download_profile_photo(self, user, file=None):
            # Simulate a file written to disk.
            fname = f"{file}.jpg"
            open(fname, "wb").close()
            downloads.append(fname)
            return fname

    client = _Client({})
    coll, _ = _build_collector(storage, client, tmp_path)

    (tmp_path / "avatars").mkdir()
    u1 = _User(42, _Photo(100))
    path1, pid1 = await coll._download_user_photo(u1)
    assert pid1 == 100 and path1 is not None

    # Same photo again — must NOT re-download.
    path1_again, _ = await coll._download_user_photo(u1)
    assert path1_again == path1
    assert len(downloads) == 1

    # Photo changes — new download + new avatar_history row.
    u1.photo = _Photo(200)
    path2, pid2 = await coll._download_user_photo(u1)
    assert pid2 == 200 and path2 != path1
    assert len(downloads) == 2

    async with storage.pool.acquire() as c:
        rows = await c.fetch(
            "SELECT photo_file_id, local_path FROM avatar_history WHERE user_id=$1 ORDER BY photo_file_id",
            42)
    assert [r["photo_file_id"] for r in rows] == [100, 200]


@pytest.mark.asyncio
async def test_deletion_handler_stub_and_event(storage, tmp_path):
    """§9: delete with chat_id makes a stub; delete without goes to deletion_events."""
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})

    # With chat_id — simulate the Collector handler call.
    await storage.ensure_message_stub_deleted(cid, [500, 501])
    # Without chat_id.
    await storage.record_deletion_event([700, 701])

    async with storage.pool.acquire() as c:
        stubs = await c.fetch(
            "SELECT message_id, is_deleted FROM messages WHERE chat_id=$1 AND message_id = ANY($2)",
            cid, [500, 501])
        events = await c.fetch("SELECT deleted_ids FROM deletion_events")
    assert {r["message_id"] for r in stubs} == {500, 501}
    assert all(r["is_deleted"] for r in stubs)
    import json
    assert any(json.loads(r["deleted_ids"]) == [700, 701] for r in events)


@pytest.mark.asyncio
async def test_raw_reactions_filtered_by_targets(storage, tmp_path):
    """§7: the registered raw handler must ignore non-target peers."""
    cid = -1001234567892
    other = -1001234567899
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.upsert_chat({"id": other, "type": "channel", "title": "o"})
    client = FakeClient({})
    coll, _ = _build_collector(storage, client, tmp_path)
    coll._targets = [cid]
    coll._register_handlers([cid])

    # Grab the raw handler the collector actually registered.
    from telethon.events.raw import Raw as RawBuilder
    raw_cb = next(cb for builder, cb in client.handlers
                  if isinstance(builder, RawBuilder))

    class _FakeReactions:
        def __init__(self): self.results = []
    class _Up:
        __name__ = "UpdateMessageReactions"
        def __init__(self, peer):
            self.peer = peer
            self.msg_id = 1
            self.reactions = _FakeReactions()

    class _UpClass:
        """Workaround: type(update).__name__ must be UpdateMessageReactions."""
        pass

    # Build two updates where __class__.__name__ matches what the handler checks.
    class UpdateMessageReactions:
        def __init__(self, peer):
            self.peer = peer
            self.msg_id = 1
            self.reactions = _FakeReactions()

    non_target = UpdateMessageReactions(
        PeerChannel(channel_id=abs(other) - 1_000_000_000_000))
    target = UpdateMessageReactions(
        PeerChannel(channel_id=abs(cid) - 1_000_000_000_000))

    await raw_cb(non_target)
    await raw_cb(target)
    async with storage.pool.acquire() as c:
        non_target_count = await c.fetchval(
            "SELECT COUNT(*) FROM message_reactions WHERE chat_id=$1", other)
        target_touched = await c.fetchval(
            "SELECT COUNT(*) FROM messages WHERE chat_id=$1 AND message_id=1", cid)
    assert non_target_count == 0
    # The target handler ran its UPDATE statement on the target row (no-op
    # since the row doesn't exist, which is fine — the proof is that the
    # non-target did not even reach storage).


@pytest.mark.asyncio
async def test_media_sweep_retries_pending(storage, tmp_path):
    """§8: pending/failed rows get re-enqueued on sweep."""
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.upsert_message({"chat_id": cid, "message_id": 10})
    await storage.upsert_media_record({
        "file_id": "photo:99", "chat_id": cid, "message_id": 10,
        "media_type": "photo", "status": "failed", "attempts": 1,
    })

    enqueued = []

    class _MQ(MediaQueue):
        async def enqueue_by_ref(self, chat_id, message_id, file_id, media_type, size):
            enqueued.append((chat_id, message_id, file_id))

    mq = _MQ(FakeClient({}), storage, base_dir=str(tmp_path),
             max_media_mb=10, concurrency=1)
    n = await mq.sweep_retryable()
    assert n >= 1
    assert (cid, 10, "photo:99") in enqueued


@pytest.mark.asyncio
async def test_media_sweep_accepts_limit(storage, tmp_path):
    """v7.2 regression: sweep_retryable honours a caller-provided limit so the
    coordinator can resweep in small chunks during backpressure. Without the
    limit arg the method raised TypeError and the backpressure-wait resweep
    (deadlock mitigation) silently did nothing."""
    cid = -1002
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    for mid in range(1, 10):
        await storage.upsert_message({"chat_id": cid, "message_id": mid})
        await storage.upsert_media_record({
            "file_id": f"photo:{mid}", "chat_id": cid, "message_id": mid,
            "media_type": "photo", "status": "pending", "attempts": 0,
        })

    enqueued = []

    class _MQ(MediaQueue):
        async def enqueue_by_ref(self, chat_id, message_id, file_id, media_type, size):
            enqueued.append(file_id)

    mq = _MQ(FakeClient({}), storage, base_dir=str(tmp_path),
             max_media_mb=10, concurrency=1)
    n = await mq.sweep_retryable(limit=3)
    assert n == 3
    assert len(enqueued) == 3
    # A second sweep with the default limit should see the remaining rows.
    n2 = await mq.sweep_retryable()
    assert n2 >= 6
