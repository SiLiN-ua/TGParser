"""Stage 0: backfill() and the coordinator honour Collector._stop.

All tests use the in-memory FakeClient (no Telegram) and the per-test
throwaway database from tests/test_collector_flow.py (never the working DB).
"""
import asyncio
import logging
import time

import pytest

from tests.test_collector_flow import (  # noqa: F401  (`storage` is a fixture)
    FakeClient, FakeMessage, CountingStorage, _Fw, _build_collector, storage,
)


class CountingClient(FakeClient):
    """FakeClient that counts get_entity calls and iterator (re)opens."""
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.iter_calls = 0
        self.get_entity_calls = 0

    async def get_entity(self, x):
        self.get_entity_calls += 1
        return await super().get_entity(x)

    def iter_messages(self, entity, min_id=0, reverse=True):
        self.iter_calls += 1
        return super().iter_messages(entity, min_id=min_id, reverse=reverse)


class StopAfterStorage(CountingStorage):
    """Sets collector._stop right after message `stop_after` has been stored."""
    def __init__(self, inner, stop_after):
        super().__init__(inner)
        self.stop_after = stop_after
        self.coll = None
        self.stored: list[int] = []

    async def upsert_message(self, m, record_version=True):
        r = await super().upsert_message(m, record_version=record_version)
        self.stored.append(m["message_id"])
        if m["message_id"] == self.stop_after:
            self.coll._stop.set()
        return r


async def _setup_chat(storage, cid):
    await storage.upsert_chat({"id": cid, "type": "channel", "title": "t"})
    await storage.init_sync_state(cid)


async def _stored_ids(storage, cid):
    async with storage.pool.acquire() as c:
        return sorted(r["message_id"] for r in await c.fetch(
            "SELECT message_id FROM messages WHERE chat_id=$1", cid))


async def _wait_checkpoint(storage, cid, value, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = await storage.get_sync_state(cid)
        if st and st["last_backfilled_message_id"] == value:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"checkpoint never reached {value}")


def _assert_not_complete(st):
    assert st["backfill_status"] != "complete", st["backfill_status"]
    assert not st["backfill_complete"]


def _assert_no_error_logs(caplog):
    bad = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not bad, [f"{r.name}: {r.getMessage()}" for r in bad]


@pytest.mark.asyncio
async def test_stop_between_messages_keeps_checkpoint(storage, tmp_path, caplog):
    """A: N processed (checkpoint=N), stop set, N+1 never processed."""
    caplog.set_level(logging.INFO)
    cid = -1001234567801
    await _setup_chat(storage, cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 6)]})
    proxy = StopAfterStorage(storage, stop_after=2)
    coll, _ = _build_collector(proxy, client, tmp_path)
    proxy.coll = coll
    coll._targets = [cid]

    added, status, more = await coll.backfill(cid)

    assert (added, status, more) == (2, "stopped", False)
    assert proxy.stored == [1, 2]              # message 3 was never started
    assert await _stored_ids(storage, cid) == [1, 2]
    st = await storage.get_sync_state(cid)
    assert st["last_backfilled_message_id"] == 2
    _assert_not_complete(st)
    assert client.iter_calls == 1
    _assert_no_error_logs(caplog)


@pytest.mark.asyncio
async def test_stop_during_floodwait_returns_fast(storage, tmp_path, caplog):
    """B: a 600 s FloodWait is interrupted by stop; iterator not re-opened."""
    caplog.set_level(logging.INFO)
    cid = -1001234567802
    await _setup_chat(storage, cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 6)]},
                            flood_at={(cid, 3): _Fw(600)})
    coll, _ = _build_collector(CountingStorage(storage), client, tmp_path)
    coll._targets = [cid]

    task = asyncio.create_task(coll.backfill(cid))
    await _wait_checkpoint(storage, cid, 2)
    await asyncio.sleep(0.2)                   # now inside the FloodWait wait
    assert not task.done()

    t0 = time.monotonic()
    coll._stop.set()
    added, status, more = await asyncio.wait_for(task, timeout=3)
    assert time.monotonic() - t0 < 1.0

    assert (added, status, more) == (2, "stopped", False)
    assert client.iter_calls == 1
    assert await _stored_ids(storage, cid) == [1, 2]
    st = await storage.get_sync_state(cid)
    assert st["last_backfilled_message_id"] == 2
    _assert_not_complete(st)
    _assert_no_error_logs(caplog)


@pytest.mark.asyncio
async def test_stop_during_transient_retry_sleep(storage, tmp_path, caplog):
    """C: the 5-7 s transient-error retry sleep is interrupted by stop."""
    caplog.set_level(logging.INFO)
    cid = -1001234567803
    await _setup_chat(storage, cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 6)]},
                            flood_at={(cid, 3): ConnectionError("net down")})
    coll, _ = _build_collector(CountingStorage(storage), client, tmp_path)
    coll._targets = [cid]

    task = asyncio.create_task(coll.backfill(cid))
    await _wait_checkpoint(storage, cid, 2)
    await asyncio.sleep(0.2)
    assert not task.done()

    t0 = time.monotonic()
    coll._stop.set()
    added, status, more = await asyncio.wait_for(task, timeout=3)
    assert time.monotonic() - t0 < 1.0

    assert (added, status, more) == (2, "stopped", False)
    assert client.iter_calls == 1              # no re-open after stop
    st = await storage.get_sync_state(cid)
    assert st["last_backfilled_message_id"] == 2
    _assert_not_complete(st)
    _assert_no_error_logs(caplog)


@pytest.mark.asyncio
async def test_stop_before_start_opens_nothing(storage, tmp_path):
    """Stop already set: no get_entity, no iterator, checkpoint untouched."""
    cid = -1001234567804
    await _setup_chat(storage, cid)
    await storage.set_backfill_checkpoint(cid, 7, 1)
    before = await storage.get_sync_state(cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 10)]})
    coll, _ = _build_collector(CountingStorage(storage), client, tmp_path)
    coll._targets = [cid]
    coll._stop.set()

    assert await coll.backfill(cid) == (0, "stopped", False)
    assert client.get_entity_calls == 0
    assert client.iter_calls == 0
    assert await _stored_ids(storage, cid) == []
    after = await storage.get_sync_state(cid)
    assert after["last_backfilled_message_id"] == 7
    assert after["first_message_id"] == before["first_message_id"]
    assert after["backfill_status"] == before["backfill_status"]
    assert after["updated_at"] == before["updated_at"]


@pytest.mark.asyncio
async def test_normal_floodwait_still_resumes(storage, tmp_path):
    """No stop: FloodWait wait is still honoured and backfill completes."""
    cid = -1001234567805
    await _setup_chat(storage, cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 6)]},
                            flood_at={(cid, 3): _Fw(0)})   # wait = 0 + 2 s
    coll, _ = _build_collector(CountingStorage(storage), client, tmp_path)
    coll._targets = [cid]

    t0 = time.monotonic()
    added, status, more = await coll.backfill(cid)
    assert time.monotonic() - t0 >= 1.9           # delay unchanged in normal run
    assert status == "complete" and more is False
    assert client.iter_calls == 2
    st = await storage.get_sync_state(cid)
    assert st["last_backfilled_message_id"] == 5


@pytest.mark.asyncio
async def test_stop_coordinator_exits_without_forced_cancel(storage, tmp_path, caplog):
    """D: coordinator mid-backfill (in FloodWait) stops cleanly, well before
    the 5 s forced-cancel fallback, and without an ERROR traceback."""
    caplog.set_level(logging.INFO)
    cid = -1001234567806
    await _setup_chat(storage, cid)
    client = CountingClient({cid: [FakeMessage(i, cid) for i in range(1, 6)]},
                            flood_at={(cid, 3): _Fw(600)})
    coll, _ = _build_collector(CountingStorage(storage), client, tmp_path,
                               options={"participants_sync": False})
    coll._targets = [cid]

    await coll.start_coordinator()
    task = coll._coordinator_task
    await _wait_checkpoint(storage, cid, 2)
    await asyncio.sleep(0.2)
    assert not task.done()

    t0 = time.monotonic()
    await coll.stop_coordinator()
    assert time.monotonic() - t0 < 2.0

    assert task.done() and not task.cancelled()
    assert task.exception() is None
    assert any(r.getMessage() == "coordinator stopped" for r in caplog.records)
    st = await storage.get_sync_state(cid)
    assert st["last_backfilled_message_id"] == 2
    _assert_not_complete(st)
    _assert_no_error_logs(caplog)
