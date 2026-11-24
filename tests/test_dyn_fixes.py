"""State-machine regression tests for the five lease defects.

Each test fails against the pre-fix code and passes against the current
implementation. All waits are condition-based or on asyncio.Events."""
import asyncio
import contextlib
import warnings

import pytest

from tests._dyn_engine_support import (  # noqa: F401
    FakeMessage, Harness, ch, harness, storage_v11, wait_until,
)
from tests._dyn_support import add_dialog, add_pref, add_target
from tgparser.collector import _TargetInactive
from tgparser.dynamic import ChatLeases

A, B = ch(1111), ch(2222)


async def _enabled(s, cid):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT enabled FROM target_state WHERE chat_id=$1", cid)


async def _count(s, table, where="TRUE", *args):
    async with s.pool.acquire() as con:
        return await con.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", *args)


# ---------------------------------------------------------------------------
# Defect 1: initial startup with pre-enabled legacy targets
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_initial_startup_arms_pre_enabled_target(harness):
    """Legacy -> dynamic transition: target_state.enabled=TRUE AND policy
    desires the chat. The chat is neither in to_enable nor to_disable; the
    initial reconcile must open a lease for it anyway."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await add_target(s, A, enabled=True)
    async with s.pool.acquire() as con:
        await con.execute(
            "INSERT INTO sync_state (chat_id, last_backfilled_message_id, "
            "  first_message_id) VALUES ($1, 500, 1)", A)

    e = await harness.start()
    assert e.is_active(A)                           # was False before the fix
    assert e.leases.is_active(A)
    assert e.leases.generation(A) == 1

    # Checkpoint and target_state label/enabled are untouched.
    async with s.pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT enabled, (SELECT last_backfilled_message_id FROM sync_state "
            "                 WHERE chat_id=target_state.chat_id) AS ckpt "
            "FROM target_state WHERE chat_id=$1", A)
    assert row["enabled"] is True and row["ckpt"] == 500

    # Live event is accepted and written.
    class Ev:
        def __init__(self, message): self.message = message
    await harness.handler("NewMessage")(Ev(FakeMessage(900, A, text="live")))
    assert await _count(s, "messages", "chat_id=$1 AND text='live'", A) == 1


@pytest.mark.asyncio
async def test_initial_startup_arms_held_targets(harness):
    """all-mode + no complete discovery yet: known enabled chats are held
    (awaiting_discovery). They still get a lease so they can keep working."""
    s = harness.storage
    await add_target(s, A, enabled=True)
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await add_target(s, B, enabled=True)             # enabled, unknown to policy
    async with s.pool.acquire() as con:
        await con.execute("UPDATE collection_config SET mode='all'")

    e = await harness.start()
    assert e.last_reconcile["state"] == "awaiting_discovery"
    assert e.is_active(A) and e.is_active(B)
    assert e.leases.generation(A) == 1
    assert e.leases.generation(B) == 1


@pytest.mark.asyncio
async def test_steady_reconcile_does_not_bump_generation(harness):
    """A reconcile that changes nothing in the active set must not bump
    the per-chat generation (would invalidate inflight lease holders)."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    g0 = e.leases.generation(A)
    await e.reconcile()
    await e.reconcile()
    assert e.leases.generation(A) == g0


# ---------------------------------------------------------------------------
# Defect 2: repeated close after quiescence timeout does not yield a false ACK
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_repeated_close_waits_for_same_inflight():
    """Direct ChatLeases test: timeout leaves slot still-draining; a
    second close() keeps waiting on the SAME inflight generation, it does
    NOT return True while inflight > 0."""
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

    assert await r.close(1, timeout=0.05) is False
    assert r.inflight(1) == 1                       # still held
    # The defect was: here the second close returned True (false ACK).
    assert await r.close(1, timeout=0.05) is False
    assert r.inflight(1) == 1
    with pytest.raises(_TargetInactive):            # new leases still rejected
        async with r.lease(1):
            pass

    release.set()
    await t
    assert await r.close(1, timeout=0.05) is True   # idempotent once drained


@pytest.mark.asyncio
async def test_repeat_reconcile_does_not_ack_until_release(harness):
    """End-to-end: squatter holds the lease across TWO reconciles; the
    second reconcile must NOT commit enabled=FALSE. Only after the
    squatter releases can a third reconcile apply the ACK."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build(engine_opts={"dynamic_disable_quiesce_timeout_seconds": 0.05,
                                   "dynamic_poll_seconds": 3600})
    await e.start()
    held_in, release = asyncio.Event(), asyncio.Event()

    async def squatter():
        async with e.leases.lease(A):
            held_in.set()
            await release.wait()
    t = asyncio.create_task(squatter())
    await held_in.wait()

    v0 = (await s.get_policy_state())["applied_version"]
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_reconciled_state("error")
    assert await _enabled(s, A) is True
    assert (await s.get_policy_state())["applied_version"] == v0

    # Second reconcile while the squatter is still holding: must NOT ACK.
    await e.reconcile()
    assert await _enabled(s, A) is True
    assert (await s.get_policy_state())["applied_version"] == v0

    release.set()
    await t
    await e.reconcile()                             # now the drain succeeds
    assert await _enabled(s, A) is False
    assert (await s.get_policy_state())["applied_version"] == v0 + 1


# ---------------------------------------------------------------------------
# Defect 3: policy reversal while a previous disable is still draining
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_policy_reversal_blocks_until_old_generation_drains(harness):
    """Target was enabled -> policy made it undesired -> close timed out ->
    policy makes it desired again BEFORE the stale operation finishes. The
    chat must stay inactive (no leases.is_active, excluded from _active)
    until the old generation drains; only then a new generation opens."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build(engine_opts={"dynamic_disable_quiesce_timeout_seconds": 0.05,
                                   "dynamic_poll_seconds": 3600})
    await e.start()
    gen_initial = e.leases.generation(A)
    held_in, release = asyncio.Event(), asyncio.Event()

    async def squatter():
        async with e.leases.lease(A):
            held_in.set()
            await release.wait()
    t = asyncio.create_task(squatter())
    await held_in.wait()

    # 1. policy -> undesired; reconcile close() times out.
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_reconciled_state("error")
    assert not e.is_active(A) and not e.leases.is_active(A)
    assert await _enabled(s, A) is True

    # 2. policy -> desired again BEFORE old operation finishes.
    await s.upsert_target_preference(A, selected=True)
    await harness.wait_reconciled_state("error")   # cannot reactivate yet

    # Chat is NOT reported active, no new generation opened.
    assert not e.is_active(A) and not e.leases.is_active(A)
    assert e.leases.generation(A) is None
    class Ev:
        def __init__(self, message): self.message = message
    await harness.handler("NewMessage")(Ev(FakeMessage(5, A, text="early")))
    assert await _count(s, "messages", "text='early'") == 0

    # 3. Release old squatter; next reconcile resurrects the chat with a
    #    FRESH generation.
    release.set()
    await t
    await e.reconcile()
    await harness.wait_applied()
    assert e.is_active(A) and e.leases.is_active(A)
    assert e.leases.generation(A) == gen_initial + 1
    await harness.handler("NewMessage")(Ev(FakeMessage(6, A, text="after")))
    assert await _count(s, "messages", "text='after'") == 1


# ---------------------------------------------------------------------------
# Defect 4: single deadline for both tracked-task cancel-await and inflight
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_close_deadline_covers_stubborn_task():
    """A tracked task that swallows the first CancelledError and loops
    cleanup must not block close() past its timeout. The task remains
    tracked so the next close() will cancel and await it again."""
    r = ChatLeases()
    r.open(1)
    phase = {"swallowed": False, "exited": False, "allow_exit": asyncio.Event()}

    started = asyncio.Event()

    async def stubborn():
        task = asyncio.current_task()
        started.set()
        while not phase["allow_exit"].is_set():
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                phase["swallowed"] = True
                task.uncancel()                     # defeat the first cancel
        phase["exited"] = True
    t = asyncio.create_task(stubborn())
    assert r.track(1, t)
    await started.wait()
    # Let the task reach its first await.
    await asyncio.sleep(0)

    import time as _time
    t0 = _time.monotonic()
    drained = await r.close(1, timeout=0.1)
    elapsed = _time.monotonic() - t0
    assert drained is False
    assert 0.08 < elapsed < 0.6                      # bounded by the deadline
    assert phase["swallowed"] is True
    assert not phase["exited"]
    # Task is still tracked (not cleared).
    assert t in r._slots[1].tasks

    # Allow the task to exit; the next close drains the slot.
    phase["allow_exit"].set()
    await asyncio.wait_for(t, 2)
    assert phase["exited"] is True
    assert await r.close(1, timeout=0.5) is True


@pytest.mark.asyncio
async def test_close_deadline_covers_inflight_too():
    r = ChatLeases()
    r.open(1)
    entered = asyncio.Event()

    async def hold():
        async with r.lease(1):
            entered.set()
            await asyncio.Event().wait()            # never set
    t = asyncio.create_task(hold())
    await entered.wait()

    import time as _time
    t0 = _time.monotonic()
    assert await r.close(1, timeout=0.1) is False
    assert _time.monotonic() - t0 < 0.5
    t.cancel()
    await asyncio.gather(t, return_exceptions=True)


# ---------------------------------------------------------------------------
# Defect 5: no coroutine is created before lease acquisition succeeds
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_unawaited_coroutine_when_lease_fails(harness, monkeypatch):
    """is_active(cid) is True, so the handler proceeds to lease acquisition.
    Monkeypatch lease() to raise _TargetInactive on entry (simulates the
    chat going inactive between the active-set check and the lease).
    Before the fix the handler called self.collector._handle_message(msg)
    eagerly and the coroutine was never awaited → RuntimeWarning."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()

    calls = {"handle_message": 0, "handle_action": 0,
             "handle_reactions_update": 0, "mark_deleted": 0}

    async def marker(name):
        calls[name] += 1

    monkeypatch.setattr(harness.collector, "_handle_message",
                        lambda msg: marker("handle_message"))
    monkeypatch.setattr(harness.collector, "_handle_action",
                        lambda ev: marker("handle_action"))
    monkeypatch.setattr(harness.collector, "_handle_reactions_update",
                        lambda upd, cid: marker("handle_reactions_update"))
    monkeypatch.setattr(harness.storage, "mark_deleted",
                        lambda cid, ids: marker("mark_deleted"))

    @contextlib.asynccontextmanager
    async def inactive(*a, **k):
        raise _TargetInactive(A)
        yield  # unreachable
    monkeypatch.setattr(e.leases, "lease", inactive)

    class Ev:
        def __init__(self, **kw): self.__dict__.update(kw)

    from telethon.tl.types import PeerChannel
    msg = FakeMessage(1, A)
    upd = type("UpdateMessageReactions", (), {})()
    upd.peer, upd.msg_id, upd.reactions = PeerChannel(channel_id=1111), 1, None

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        await harness.handler("NewMessage")(Ev(message=msg))
        await harness.handler("MessageEdited")(Ev(message=msg))
        await harness.handler("ChatAction")(Ev(chat_id=A, user_id=1, users=[1],
                                                action_message=None, user_left=False,
                                                user_kicked=False))
        await harness.handler("Raw")(upd)
        await harness.handler("MessageDeleted")(Ev(chat_id=A, deleted_ids=[1]))
    bad = [w for w in warns
           if issubclass(w.category, RuntimeWarning)
           and "never awaited" in str(w.message)]
    assert not bad, f"unawaited coroutines: {[str(w.message) for w in bad]}"
    # None of the collector/storage write paths were even entered.
    assert calls == {"handle_message": 0, "handle_action": 0,
                     "handle_reactions_update": 0, "mark_deleted": 0}
    assert await _count(s, "messages") == 0
