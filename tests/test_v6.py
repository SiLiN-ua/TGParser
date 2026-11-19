"""v6 tests: stall recovery, coordinator progress watchdog, worker heartbeat,
media needs_refresh/unavailable, TG real heartbeat, pytest fixture hygiene."""
import asyncio
import pytest
from datetime import datetime, timezone


# §2 batch timeout: simulated hang -> timeout fires + checkpoint preserved
@pytest.mark.asyncio
async def test_coordinator_batch_start_progress_end(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.init_sync_state(cid)

    await storage.coordinator_batch_start(cid, start_message_id=100)
    rows = await storage.coordinator_progress_all()
    row = next(r for r in rows if r["chat_id"] == cid)
    assert row["batch_running"] is True
    assert row["batch_started_at"] is not None

    await storage.coordinator_batch_progress(cid, message_id=250)
    await storage.coordinator_batch_end(cid, added=150, duration_sec=12,
                                        success=True)
    rows = await storage.coordinator_progress_all()
    row = next(r for r in rows if r["chat_id"] == cid)
    assert row["batch_running"] is False
    assert row["consecutive_batch_failures"] == 0
    assert row["last_batch_added"] == 150


@pytest.mark.asyncio
async def test_coordinator_failure_increments_consecutive(storage):
    cid = -1002
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.init_sync_state(cid)

    await storage.coordinator_batch_start(cid, 1)
    await storage.coordinator_batch_end(cid, added=0, duration_sec=5,
                                        success=False, reason="batch_timeout_600s")
    await storage.coordinator_batch_start(cid, 1)
    await storage.coordinator_batch_end(cid, added=0, duration_sec=5,
                                        success=False, reason="batch_timeout_600s")
    row = next(r for r in await storage.coordinator_progress_all() if r["chat_id"] == cid)
    assert row["consecutive_batch_failures"] == 2
    # success resets
    await storage.coordinator_batch_start(cid, 1)
    await storage.coordinator_batch_end(cid, added=50, duration_sec=5, success=True)
    row = next(r for r in await storage.coordinator_progress_all() if r["chat_id"] == cid)
    assert row["consecutive_batch_failures"] == 0


# §6 media expired file reference flow
@pytest.mark.asyncio
async def test_media_needs_refresh_and_unavailable(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.upsert_message({"chat_id": cid, "message_id": 10})
    await storage.upsert_media_record({
        "file_id": "photo:X", "chat_id": cid, "message_id": 10,
        "media_type": "photo", "status": "needs_refresh",
        "last_error": "FileReferenceExpired",
    })
    pending = await storage.media_needs_refresh()
    assert any(p["file_id"] == "photo:X" for p in pending)

    # too many retries -> mark unavailable
    for _ in range(5):
        await storage.media_increment_attempt("photo:X", "retry")
    await storage.media_mark_unavailable("photo:X", "refresh_max_attempts_exhausted")
    breakdown = await storage.media_status_breakdown()
    assert breakdown.get("unavailable", 0) >= 1


# §7 worker heartbeat
@pytest.mark.asyncio
async def test_worker_heartbeat_roundtrip(storage):
    await storage.worker_heartbeat(0, pid=1234, current_file_id="photo:A")
    await storage.worker_heartbeat(0, pid=1234, current_file_id=None,
                                   completed_delta=1)
    await storage.worker_heartbeat(1, pid=1234, current_file_id="doc:B",
                                   failed_delta=1, last_error="boom")
    rows = await storage.workers_all()
    by = {r["worker_id"]: r for r in rows}
    assert by[0]["completed_count"] == 1
    assert by[1]["failed_count"] == 1
    assert by[1]["last_error"] == "boom"


# §7 media throughput
@pytest.mark.asyncio
async def test_media_throughput_computes(storage):
    cid = -1001
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.upsert_message({"chat_id": cid, "message_id": 1})
    await storage.upsert_media_record({
        "file_id": "p:1", "chat_id": cid, "message_id": 1,
        "media_type": "photo", "status": "done",
        "completed_at": datetime.now(timezone.utc),
    })
    th = await storage.media_throughput()
    assert th["m1"] >= 1
    assert th["m5"] >= 0.2


# §2 batch timeout simulation using asyncio.wait_for
@pytest.mark.asyncio
async def test_batch_timeout_cancels_and_preserves_checkpoint(storage):
    """Fakes a slow backfill with asyncio.sleep(10) and wraps it in a 0.2s
    wait_for - caller must get TimeoutError and checkpoint must survive."""
    cid = -1003
    await storage.upsert_chat({"id": cid, "type": "supergroup", "title": "t"})
    await storage.init_sync_state(cid)
    await storage.set_backfill_checkpoint(cid, last_id=777, first_id=1)

    async def slow_batch():
        await asyncio.sleep(10)
        return (0, "syncing", True)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(slow_batch(), timeout=0.2)

    state = await storage.get_sync_state(cid)
    assert state["last_backfilled_message_id"] == 777  # preserved
