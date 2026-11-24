"""Coordinator/backfill under the dynamic engine: activity checks between
messages and in backpressure waits, checkpoint safety, TGP01 handling."""
import asyncio

import pytest

from tests._dyn_engine_support import (  # noqa: F401
    FakeMessage, Harness, ch, harness, storage_v11, wait_until,
)
from tests._dyn_support import add_dialog, add_pref, add_target

A = ch(1111)


async def _count(s, table, where="TRUE", *args):
    async with s.pool.acquire() as con:
        return await con.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", *args)


async def _ckpt(s, cid):
    st = await s.get_sync_state(cid)
    return st["last_backfilled_message_id"] if st else None


@pytest.mark.asyncio
async def test_backfill_of_inactive_target_opens_nothing(harness):
    harness.build()
    harness.collector._dynamic = True
    harness.collector._active_check = lambda cid: False
    res = await harness.collector.backfill(A)
    assert res == (0, "inactive", False)
    assert harness.client.get_entity_calls == [] and harness.client.iter_messages_calls == []


@pytest.mark.asyncio
async def test_disable_mid_backfill_blocks_ack_then_drains(harness, monkeypatch):
    """Backfill holds the lease. A disable must NOT ACK while the batch is
    parked; once the batch resumes it observes _is_active=False, returns
    'inactive', releases the lease, and only then ACK + applied_version
    advance."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    c = harness.client
    c.messages[A] = [FakeMessage(i, A) for i in range(1, 6)]
    reached, release = asyncio.Event(), asyncio.Event()

    async def hook(chat, m):
        if m.id == 3:
            reached.set()
            await release.wait()
    c.message_hook = hook
    e = harness.build()
    col = harness.collector
    results = []
    orig = col.backfill

    async def recording(cid, *a, **k):
        r = await orig(cid, *a, **k)
        results.append(r)
        return r
    monkeypatch.setattr(col, "backfill", recording)
    await e.start()
    await asyncio.wait_for(reached.wait(), 5)        # parked before message 3
    assert e.leases.inflight(A) >= 1                 # coordinator round holds lease
    v0 = (await s.get_policy_state())["applied_version"]

    # Trigger disable; reconcile removes A from the active set but waits in
    # leases.close(A) because the parked coordinator round still holds it.
    await s.upsert_target_preference(A, selected=False)
    await wait_until(lambda: not e.is_active(A),
                     what="A removed from active set")
    # ACK must not have happened yet.
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT enabled FROM target_state WHERE chat_id=$1", A) is True
    assert (await s.get_policy_state())["applied_version"] == v0

    release.set()                                    # resume parked batch
    await wait_until(lambda: results, what="parked batch returned")
    assert results == [(2, "inactive", False)]
    # Now the lease drains and the reconciler's leases.close() returns True,
    # the ACK is sent, applied_version advances.
    await harness.wait_applied()
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT enabled FROM target_state WHERE chat_id=$1", A) is False
    assert await _count(s, "messages", "chat_id=$1", A) == 2
    assert await _ckpt(s, A) == 2
    assert await _count(s, "backfill_queue") == 0
    assert await _count(s, "errors") == 0


@pytest.mark.asyncio
async def test_tgp01_in_backfill_is_not_dead_lettered(harness):
    s = harness.storage
    await add_target(s, A, enabled=False)
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO sync_state (chat_id, last_backfilled_message_id) VALUES ($1, 10)", A)
    await s.set_runtime_dynamic_engine(True)
    harness.build()
    col = harness.collector
    col._dynamic = True
    col._active_check = lambda cid: True             # stale view: DB says disabled
    harness.client.messages[A] = [FakeMessage(11, A), FakeMessage(12, A)]
    res = await col.backfill(A)
    assert res == (0, "inactive", False)
    assert await _count(s, "backfill_queue") == 0
    assert await _count(s, "messages") == 0
    assert await _ckpt(s, A) == 10


@pytest.mark.asyncio
async def test_guarded_checkpoint_refuses_disabled_target(harness):
    s = harness.storage
    await add_target(s, A, enabled=True)
    await s.init_sync_state(A)
    assert await s.set_backfill_checkpoint_if_enabled(A, 5)
    assert await _ckpt(s, A) == 5
    assert await s.set_backfill_checkpoint_if_enabled(A, 3)       # GREATEST keeps 5
    assert await _ckpt(s, A) == 5
    await s.disable_target_ack(A, "test")
    assert not await s.set_backfill_checkpoint_if_enabled(A, 9)
    assert await _ckpt(s, A) == 5


@pytest.mark.asyncio
async def test_backpressure_wait_ends_on_disable(harness, monkeypatch):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build(collector_opts={"media_pending_high_water": 10,
                                      "media_pending_low_water": 5})
    col = harness.collector
    calls = {"backfill": 0}

    async def huge():
        return 10**6
    monkeypatch.setattr(col, "_media_pending_count", huge)
    orig_backfill = col.backfill

    async def spy_backfill(cid, *a, **k):
        calls["backfill"] += 1
        return await orig_backfill(cid, *a, **k)
    monkeypatch.setattr(col, "backfill", spy_backfill)
    idles = []
    orig_idle = col._idle

    async def spy_idle(seconds):
        idles.append(seconds)
        await orig_idle(seconds)
    monkeypatch.setattr(col, "_idle", spy_idle)
    await e.start()

    async def paused():
        st = await s.get_sync_state(A)
        return st and st["backfill_status"] == "syncing_paused_media_backpressure"
    await wait_until(paused, what="backpressure pause")
    assert idles and set(idles) == {10}               # parked inside the 10 s wait
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    await wait_until(lambda: any(x in (60, 2) for x in idles), timeout=3,
                     what="coordinator left the backpressure wait")
    assert calls["backfill"] == 0
    assert harness.client.iter_messages_calls == []
