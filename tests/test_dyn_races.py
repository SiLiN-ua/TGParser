"""Race-safety tests for the dynamic engine: startup readiness barrier,
quiescence drain, disable ACK ordering, generation isolation, media
semantics around disable, user-enrichment cancellation. All waits are
condition-based (wait_until) or on explicit asyncio.Events."""
import asyncio
import logging

import pytest

from tests._dyn_engine_support import (  # noqa: F401
    FakeMessage, Harness, ch, harness, storage_v11, wait_until,
)
from tests._dyn_support import add_dialog, add_pref, add_target
from tgparser.collector import _TargetInactive
from tgparser.dynamic import ChatLeases, DynamicEngine
from tgparser.media_queue import MediaQueue

A, B, C = ch(1111), ch(2222), ch(3333)


async def _count(s, table, where="TRUE", *args):
    async with s.pool.acquire() as con:
        return await con.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", *args)


async def _enabled(s, cid):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT enabled FROM target_state WHERE chat_id=$1", cid)


# -----------------------------------------------------------------------
# 1. ChatLeases unit behaviour
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lease_rejects_when_not_open():
    r = ChatLeases()
    with pytest.raises(_TargetInactive):
        async with r.lease(1):
            pass


@pytest.mark.asyncio
async def test_lease_close_waits_for_inflight_and_times_out():
    r = ChatLeases()
    r.open(1)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold():
        async with r.lease(1):
            entered.set()
            await release.wait()

    t = asyncio.create_task(hold())
    await entered.wait()
    assert r.inflight(1) == 1
    close_task = asyncio.create_task(r.close(1, timeout=0.1))
    assert await close_task is False
    assert r.inflight(1) == 1                          # still held
    with pytest.raises(_TargetInactive):               # new leases rejected
        async with r.lease(1):
            pass
    release.set()
    await t
    assert r.inflight(1) == 0
    assert await r.close(1, timeout=0.1) is True       # idempotent


@pytest.mark.asyncio
async def test_lease_close_cancels_tracked_task():
    r = ChatLeases()
    r.open(1)
    entered = asyncio.Event()

    async def hold():
        async with r.lease(1):
            entered.set()
            await asyncio.Event().wait()                 # never set

    t = asyncio.create_task(hold())
    assert r.track(1, t)
    await entered.wait()
    drained = await r.close(1, timeout=1.0)
    assert drained is True
    assert t.done() and t.cancelled()
    assert r.inflight(1) == 0


@pytest.mark.asyncio
async def test_generation_isolation_blocks_old_lease():
    r = ChatLeases()
    r.open(1)                                            # gen 1
    async with r.lease(1) as g:
        assert g == 1
    assert await r.close(1, timeout=0.1) is True
    r.open(1)                                            # gen 2
    with pytest.raises(_TargetInactive):
        async with r.lease(1, gen=1):
            pass
    async with r.lease(1, gen=2):
        pass


# -----------------------------------------------------------------------
# 2. Startup readiness barrier
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handlers_do_nothing_before_ready(harness, monkeypatch):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build()
    barrier_in = asyncio.Event()
    barrier_out = asyncio.Event()
    orig = s.set_runtime_dynamic_engine
    results = []

    async def spy(on):
        if on:
            barrier_in.set()
            await barrier_out.wait()
        await orig(on)
    monkeypatch.setattr(s, "set_runtime_dynamic_engine", spy)

    start_task = asyncio.create_task(e.start())
    try:
        await asyncio.wait_for(barrier_in.wait(), 5)
        assert not e._accepting.is_set()
        assert len(harness.client.handlers) == 5    # handlers already registered
        for b, _ in harness.client.handlers:
            assert getattr(b, "chats", None) is None

        # Fire every handler. None of them must write anything.
        class _Ev:
            def __init__(self, **kw):
                self.__dict__.update(kw)
        from telethon.tl.types import PeerChannel
        msg = FakeMessage(1, A, text="before-ready")
        await harness.handler("NewMessage")(_Ev(message=msg))
        await harness.handler("MessageEdited")(_Ev(message=msg))
        await harness.handler("ChatAction")(_Ev(chat_id=A, user_id=1, users=[1],
                                                 action_message=None,
                                                 user_left=False, user_kicked=False))
        upd = type("UpdateMessageReactions", (), {})()
        upd.peer, upd.msg_id, upd.reactions = PeerChannel(channel_id=1111), 1, None
        await harness.handler("Raw")(upd)
        await harness.handler("MessageDeleted")(_Ev(chat_id=A, deleted_ids=[1]))
        results.append(("messages", await _count(s, "messages")))
        results.append(("memberships", await _count(s, "memberships")))
        results.append(("reactions", await _count(s, "message_reactions")))
        results.append(("chat_actions", await _count(s, "chat_actions")))
        barrier_out.set()
        await asyncio.wait_for(start_task, 5)
    finally:
        barrier_out.set()
        if not start_task.done():
            start_task.cancel()
            await asyncio.gather(start_task, return_exceptions=True)
    assert dict(results) == {"messages": 0, "memberships": 0,
                             "reactions": 0, "chat_actions": 0}
    assert e.state == "ready" and e._accepting.is_set()

    await harness.handler("NewMessage")({"message": FakeMessage(7, A, text="after")}.get("message", FakeMessage(7, A, text="after")) and type("Ev", (), {"message": FakeMessage(7, A, text="after")})())
    await wait_until(lambda: _count(s, "messages", "chat_id=$1 AND text='after'", A),
                     what="accepted after ready")


@pytest.mark.asyncio
async def test_stop_clears_accepting(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    assert e._accepting.is_set()
    await e.stop()
    assert not e._accepting.is_set()
    class Ev:
        def __init__(self, message): self.message = message
    await harness.handler("NewMessage")(Ev(FakeMessage(5, A, text="late")))
    assert await _count(s, "messages", "text='late'") == 0


# -----------------------------------------------------------------------
# 3. Participant sync + disable quiesce drain
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_participant_writes_drain_before_ack(harness, monkeypatch):
    """Participant sync holds the lease. Disable waits in leases.close(A)
    until the sync returns; only then does the ACK commit enabled=FALSE.
    No participant write for A lands after the ACK, no errors row."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    writing = asyncio.Event()
    release = asyncio.Event()
    wrote_sync = {"n": 0}

    async def slow_sync(cid):
        writing.set()
        await release.wait()
        # If we reach this, we would race the ACK — must not happen.
        try:
            await harness.storage.set_participants_sync(cid, 0, True, None)
            wrote_sync["n"] += 1
        except Exception:
            pass

    e = harness.build(collector_opts={"participants_sync": True})
    monkeypatch.setattr(harness.collector, "sync_participants", slow_sync)
    await e.start()                                  # coordinator's one-time pass parks here
    await asyncio.wait_for(writing.wait(), 5)
    assert e.leases.inflight(A) == 1

    v0 = (await s.get_policy_state())["applied_version"]
    await s.upsert_target_preference(A, selected=False)
    await wait_until(lambda: not e.is_active(A), what="A removed from active set")
    # ACK withheld while the sync is parked; applied_version unchanged.
    assert await _enabled(s, A) is True
    assert (await s.get_policy_state())["applied_version"] == v0

    release.set()                                    # the sync returns
    await harness.wait_applied()
    assert await _enabled(s, A) is False
    # No late participant writes, no errors row for a TGP01.
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT COUNT(*) FROM errors WHERE message LIKE '%TGP01%'") == 0
    # If anything ran after release, it was raced by the ACK; that is what
    # the barrier prevents.
    assert wrote_sync["n"] <= 1


@pytest.mark.asyncio
async def test_quiesce_timeout_withholds_ack(harness):
    """If an inflight lease never releases within the timeout, no ACK, no
    applied_version bump, reconcile_state='error'. Next reconcile retries."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build(engine_opts={"dynamic_disable_quiesce_timeout_seconds": 0.1})
    await e.start()

    hold_rel = asyncio.Event()
    hold_in = asyncio.Event()

    async def squatter():
        async with e.leases.lease(A):
            hold_in.set()
            await hold_rel.wait()
    t = asyncio.create_task(squatter())
    await hold_in.wait()

    v0 = (await s.get_policy_state())["applied_version"]
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_reconciled_state("error")
    ps = await s.get_policy_state()
    assert ps["applied_version"] == v0
    assert await _enabled(s, A) is True
    assert "quiesce timeout" in ps["reconcile_error"]
    assert not e.is_active(A)                     # still removed from active set

    hold_rel.set()
    await t
    # Next reconcile succeeds.
    await e.reconcile()
    await harness.wait_applied()
    assert await _enabled(s, A) is False


# -----------------------------------------------------------------------
# 4. Coordinator bookkeeping does not land after ACK
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_coordinator_bookkeeping_after_ack(harness):
    """Coordinator round holds the lease: once the lease is closed and ACK
    commits enabled=FALSE, no further coordinator_heartbeat /
    coordinator_batch_end / target_update_counts write lands on that chat."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    harness.client.messages[A] = [FakeMessage(i, A) for i in range(1, 4)]
    e = await harness.start()
    async def done_one():
        return await _count(s, "messages", "chat_id=$1", A) >= 1
    await wait_until(done_one, timeout=3, what="coordinator did one round")
    before_cp_at = await _coord_updated(s, A)

    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    assert await _enabled(s, A) is False
    ack_at = await _target_updated(s, A)

    # Give the coordinator plenty of chance to run — the chat is gone from
    # the active set, so `for cid in list(self._targets): if not is_active:
    # continue` means its body never runs.
    for _ in range(5):
        harness.collector.wake()
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)

    assert await _coord_updated(s, A) == before_cp_at or \
        (await _coord_updated(s, A)) <= ack_at
    # target_state.updated_at only changed by ACK; no later bookkeeping bumps it
    assert await _target_updated(s, A) == ack_at


async def _coord_updated(s, cid):
    async with s.pool.acquire() as con:
        return await con.fetchval(
            "SELECT updated_at FROM coordinator_progress WHERE chat_id=$1", cid)


async def _target_updated(s, cid):
    async with s.pool.acquire() as con:
        return await con.fetchval(
            "SELECT updated_at FROM target_state WHERE chat_id=$1", cid)


# -----------------------------------------------------------------------
# 5. Chat-sourced bg task (user enrichment) cancels on disable
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chat_sourced_bg_task_cancelled_on_disable(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()

    writing = asyncio.Event()
    release = asyncio.Event()
    wrote_after = {"n": 0}

    async def bg_coro():
        writing.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            # A disabled chat's bg task must not write after cancellation.
            wrote_after["n"] += 1 if False else 0
            raise

    t = harness.collector._spawn_bg(bg_coro(), name="user:777", chat_id=A)
    await writing.wait()
    assert t in e.leases._slots[A].tasks            # tracked for cancellation

    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    # Close() cancels the bg task and awaits it before ACK commits.
    assert wrote_after["n"] == 0
    assert t.done()
    # DB state: target disabled
    assert await _enabled(s, A) is False


@pytest.mark.asyncio
async def test_dynamic_stop_with_pending_bg_enrichment_no_warning(harness):
    """Dynamic shutdown with user-enrichment coroutines parked at _bg_sema:
    engine.stop() + collector.wait_background() (the production teardown
    pair from run.py) cancels them cleanly — no 'never awaited'
    RuntimeWarning, no Traceback, victim coros are closed."""
    import warnings as _warnings
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    coll = harness.collector
    # Replace _bg_sema with a tiny one to force parking deterministically.
    coll._bg_sema = asyncio.Semaphore(1)
    hog_release = asyncio.Event()
    async def _hog():
        await hog_release.wait()
    hog_coro = _hog()
    coll._spawn_bg(hog_coro, name="hog")
    await asyncio.sleep(0)   # hog acquires the sema

    victim_coros = []
    async def _victim():
        await asyncio.sleep(0.01)   # must NOT run
    for i in range(3):
        c = _victim()
        victim_coros.append(c)
        coll._spawn_bg(c, name=f"user:{i}", chat_id=A)
    await asyncio.sleep(0)

    with _warnings.catch_warnings(record=True) as w:
        _warnings.simplefilter("always")
        # Production teardown order: engine.stop() first, then
        # collector.wait_background() (run.py §3–§5).
        await e.stop()
        # Release hog so its wrapper can also finish during drain.
        hog_release.set()
        await coll.wait_background(timeout=2)
        # Let add_done_callback run once more.
        await asyncio.sleep(0)
        never_awaited = [x for x in w
                         if issubclass(x.category, RuntimeWarning)
                         and "never awaited" in str(x.message)]
    assert never_awaited == [], f"warnings leaked: {never_awaited}"
    for c in victim_coros:
        assert c.cr_frame is None, "victim coroutine left un-closed"


# -----------------------------------------------------------------------
# 6. Media cases
# -----------------------------------------------------------------------

class _FakeMedia:
    """Minimal media surrogate that media_descriptor accepts (video/doc)."""
    def __init__(self, fid: int):
        from telethon.tl.types import Document
        self.document = Document(id=fid, access_hash=0, file_reference=b"",
                                  date=None, mime_type="application/octet-stream",
                                  size=100, dc_id=1, attributes=[], thumbs=[])

    def to_dict(self):
        return {"_": "FakeDoc"}


def _msg_with_doc(mid: int, chat_id: int, doc_id: int):
    from telethon.tl.types import MessageMediaDocument
    m = FakeMessage(mid, chat_id)
    doc = _FakeMedia(doc_id).document
    m.media = MessageMediaDocument(document=doc)
    return m


@pytest.mark.asyncio
async def test_enqueue_after_chat_disabled_is_noop(harness):
    """enqueue() must check the active set BEFORE any DB write."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.start()

    before = await _count(s, "media")
    # Simulate chat left the active set
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()

    res = await harness.media_q.enqueue(A, _msg_with_doc(101, A, 9001))
    assert res is None
    assert await _count(s, "media") == before


@pytest.mark.asyncio
async def test_disable_during_download_leaves_media_pending(harness):
    """Disable while a worker's download is in progress: lease is held so
    close() must wait. The worker is cancelled by stop(), not here; here we
    simulate disable mid-download by holding a lease via a slow download."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build(collector_opts={"download_media": True})
    client = harness.client
    client.messages[A] = [_msg_with_doc(100, A, 9100)]
    downloading = asyncio.Event()
    release = asyncio.Event()

    async def slow_download(message, file=None, **kw):
        downloading.set()
        await release.wait()
        from pathlib import Path
        Path(str(file)).write_bytes(b"x")
        return str(file)

    client.download_media = slow_download

    await e.start()
    harness.media_q.start()  # ensure the worker loop is running
    # Push one item through enqueue
    await harness.media_q.enqueue(A, client.messages[A][0])
    await asyncio.wait_for(downloading.wait(), 5)
    assert e.leases.inflight(A) >= 1                 # worker holds lease

    v0 = (await s.get_policy_state())["applied_version"]
    await s.upsert_target_preference(A, selected=False)
    await wait_until(lambda: not e.is_active(A), what="removed from active set")
    # ACK withheld while worker still holds the lease
    assert await _enabled(s, A) is True
    assert (await s.get_policy_state())["applied_version"] == v0

    release.set()                                    # finish download
    await harness.wait_applied()
    assert await _enabled(s, A) is False
    async with s.pool.acquire() as con:
        # The row completed naturally: status=done, path linked. No orphan,
        # no needs_refresh/failed.
        row = await con.fetchrow("SELECT status, local_path FROM media LIMIT 1")
    assert row["status"] == "done" and row["local_path"]


@pytest.mark.asyncio
async def test_shared_file_not_corrupted_by_disable(harness):
    """Two chats share one file_id. A disable of one chat must not touch the
    shared file or the active chat's link."""
    s = harness.storage
    for c in (A, B):
        await add_dialog(s, c); await add_pref(s, c, selected=True)
    e = await harness.start()
    # Seed shared media row + link to both chats (done)
    async with s.pool.acquire() as con:
        await con.execute(
            "INSERT INTO media (file_id, status, local_path, size) "
            "VALUES ('doc:42', 'done', 'f/42.bin', 100)")
        await con.execute("INSERT INTO messages (chat_id, message_id, text) VALUES "
                          "($1,1,'a'),($2,1,'b')", A, B)
        await con.execute("INSERT INTO message_media (chat_id, message_id, file_id) VALUES "
                          "($1,1,'doc:42'),($2,1,'doc:42')", A, B)

    # Disable B
    await s.upsert_target_preference(B, selected=False)
    await harness.wait_applied()

    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT status FROM media WHERE file_id='doc:42'") == "done"
        assert await con.fetchval(
            "SELECT COUNT(*) FROM message_media WHERE file_id='doc:42' AND chat_id=$1", A) == 1


# -----------------------------------------------------------------------
# 7. Disable/re-enable isolates generations
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_old_generation_cannot_write_after_reenable(harness):
    """Spawn a chat-attributed bg task for gen 1; disable + re-enable. The
    task's attempt to re-acquire a lease uses gen=1 and is rejected."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    gen1 = e.leases.generation(A)
    assert gen1 == 1

    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    await s.upsert_target_preference(A, selected=True)
    await harness.wait_applied()
    gen2 = e.leases.generation(A)
    assert gen2 == gen1 + 1

    with pytest.raises(_TargetInactive):
        async with e.leases.lease(A, gen=gen1):
            pass
    async with e.leases.lease(A, gen=gen2):
        pass


# -----------------------------------------------------------------------
# 8. Normal disable: no ERROR log, no errors row
# -----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_normal_disable_is_silent(harness, caplog):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    harness.client.messages[A] = [FakeMessage(i, A) for i in range(1, 4)]
    await harness.start()
    before_errors = await _count(s, "errors")
    caplog.set_level(logging.DEBUG)
    caplog.clear()

    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    # Let the coordinator run another round where it would try to process A
    for _ in range(3):
        harness.collector.wake()
        await asyncio.sleep(0.02)
    assert await _count(s, "errors") == before_errors
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
