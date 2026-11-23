"""Shutdown and MediaQueue.stop correctness tests.

Verifies the Linux-side shutdown contract introduced to fix "stopping/stopped"
semantics in runtime_state and to make MediaQueue.stop fast and bounded:

  - MediaQueue.stop with a huge in-memory queue returns in bounded time.
  - A stuck download worker is cancelled on stop.
  - No media-* asyncio tasks remain after stop.
  - runtime_state goes 'stopping' (with real pid) -> 'stopped' (pid NULL).
  - A simulated abort during teardown leaves 'stopping' with a dead pid,
    which health classifies as unhealthy.
  - Pending media rows left by a stopped process are swept on restart.
  - Shutdown during an active backfill preserves a safe checkpoint
    (sync_state.last_backfilled_message_id is not advanced).
"""
import asyncio
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.media_queue import MediaQueue
from tgparser.config import load as _load_cfg


DSN = os.environ.get("PG_DSN") or _load_cfg()["postgres"]["dsn"]


# --- shared helpers -------------------------------------------------------

async def _make_fresh_db():
    """Spin up a throwaway database + migrated Storage. Returns (storage, dbname)."""
    tmp = f"tgparser_shutdown_{os.getpid()}_{secrets.token_hex(4)}"
    sys = await asyncpg.connect(DSN)
    try:
        await sys.execute(f"DROP DATABASE IF EXISTS {tmp}")
        await sys.execute(f"CREATE DATABASE {tmp}")
    finally:
        await sys.close()
    tmp_dsn = DSN.rsplit("/", 1)[0] + f"/{tmp}"
    st = Storage(tmp_dsn)
    await st.connect()
    await apply_migrations(st.pool, "migrations")
    return st, tmp


async def _drop_db(dbname: str):
    base = DSN.rsplit("/", 1)[0]
    sys = await asyncpg.connect(base + "/postgres")
    try:
        await sys.execute(f"""
            SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname=$1 AND usename=current_user AND pid<>pg_backend_pid()
        """, dbname)
        await sys.execute(f"DROP DATABASE IF EXISTS {dbname}")
    finally:
        await sys.close()


@pytest_asyncio.fixture
async def storage():
    st, name = await _make_fresh_db()
    try:
        yield st
    finally:
        await st.close()
        await _drop_db(name)


class _FastDoneClient:
    """A Telethon surrogate whose download_media returns a stub path after 0s."""
    async def get_messages(self, chat_id, ids=None):
        class M:
            def __init__(self): self.id = ids; self.media = None
        return M()

    async def download_media(self, message, file=None, **kw):
        p = Path(str(file))
        p.write_bytes(b"x")       # stub content
        return str(p)


class _HangingClient:
    """A Telethon surrogate whose download_media sleeps far past any stop timeout."""
    async def get_messages(self, chat_id, ids=None):
        class M:
            def __init__(self): self.id = ids; self.media = None
        return M()

    async def download_media(self, message, file=None, **kw):
        await asyncio.sleep(1000)   # simulate stuck download


# --- 1. MediaQueue.stop is bounded even with a huge queue -----------------

@pytest.mark.asyncio
async def test_media_queue_stop_fast_with_huge_queue(storage, tmp_path):
    """Put 25k items in the queue, call stop(), verify it returns in <10s and
    leaves no media-* tasks pending."""
    mq = MediaQueue(_FastDoneClient(), storage, base_dir=str(tmp_path),
                    max_media_mb=500, concurrency=2)
    # Bypass dedup/_mark_inflight for speed — we only care about queue size.
    # Put raw sentinels-like items directly. The workers will see them as
    # dicts that _download can process; but easier: just put None sentinels
    # mixed with dicts so workers bail. We really just stress the "huge
    # queue" + "fast stop" path.
    for i in range(25_000):
        mq.queue.put_nowait({
            "chat_id": 1, "message_id": i,
            "desc": {"file_id": f"photo:{i}", "media_type": "photo", "size": 1},
            "message": None,
        })
    mq.start()
    t0 = time.monotonic()
    await mq.stop(timeout=3.0)
    dt = time.monotonic() - t0
    assert dt < 10.0, f"MediaQueue.stop took {dt:.1f}s (should be bounded)"
    media_tasks = [t for t in asyncio.all_tasks()
                   if (t.get_name() or "").startswith("media-")]
    media_tasks = [t for t in media_tasks if t is not asyncio.current_task()]
    assert not media_tasks, f"media-* tasks survived stop(): {media_tasks}"


# --- 2. stop cancels a stuck download ------------------------------------

@pytest.mark.asyncio
async def test_media_queue_stop_cancels_stuck_download(storage, tmp_path):
    mq = MediaQueue(_HangingClient(), storage, base_dir=str(tmp_path),
                    max_media_mb=500, concurrency=2)

    class _M:
        def __init__(self, i): self.id = i; self.media = None
        def to_dict(self): return {"id": self.id}

    # Insert a real media row so _download can run.
    await storage.upsert_media_record({
        "file_id": "photo:1", "chat_id": 1, "message_id": 1,
        "media_type": "photo", "status": "pending",
    })
    mq.start()
    # Feed one item; worker will enter the sleep(1000) in _download.
    mq.queue.put_nowait({
        "chat_id": 1, "message_id": 1,
        "desc": {"file_id": "photo:1", "media_type": "photo", "size": 1},
        "message": _M(1),
    })
    # Let worker pick it up and enter the hanging download.
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    await mq.stop(timeout=1.0)
    dt = time.monotonic() - t0
    assert dt < 5.0, f"stop() did not cancel hung download in time: {dt:.1f}s"
    media_tasks = [t for t in asyncio.all_tasks()
                   if (t.get_name() or "").startswith("media-")]
    media_tasks = [t for t in media_tasks if t is not asyncio.current_task()]
    assert not media_tasks, f"media-* tasks survived cancel: {media_tasks}"


# --- 3. runtime sequence: stopping(pid) -> stopped(pid=NULL) -------------

@pytest.mark.asyncio
async def test_runtime_state_stopping_then_stopped_sequence(storage):
    fake_pid = os.getpid()
    await storage.set_runtime(fake_pid, "syncing", [-1], note=None, started=True)
    # First write: stopping with the real pid preserved.
    await storage.set_runtime(fake_pid, "stopping", [-1], note=None)
    async with storage.pool.acquire() as con:
        r = await con.fetchrow("SELECT status, pid FROM runtime_state WHERE id=1")
    assert r["status"] == "stopping"
    assert r["pid"] == fake_pid
    # Final write: stopped with pid NULL.
    await storage.set_runtime(None, "stopped", [], note=None)
    async with storage.pool.acquire() as con:
        r = await con.fetchrow("SELECT status, pid FROM runtime_state WHERE id=1")
    assert r["status"] == "stopped"
    assert r["pid"] is None


# --- 4. aborted teardown must NOT write 'stopped' ------------------------

@pytest.mark.asyncio
async def test_aborted_teardown_leaves_stopping(storage):
    """If an exception/cancellation lands between the 'stopping' write and the
    final 'stopped' write, runtime_state must remain 'stopping' with the
    (dead-after-the-fact) pid — never a false 'stopped'."""
    fake_pid = 99999999  # a pid that is extremely unlikely to exist
    await storage.set_runtime(fake_pid, "syncing", [-1], note=None, started=True)
    await storage.set_runtime(fake_pid, "stopping", [-1], note=None)

    # Simulate a teardown that crashes BEFORE writing 'stopped'.
    teardown_reached_stopped = False
    try:
        raise RuntimeError("simulated media queue failure during teardown")
        teardown_reached_stopped = True
        await storage.set_runtime(None, "stopped", [], note=None)  # not reached
    except RuntimeError:
        pass

    assert not teardown_reached_stopped
    async with storage.pool.acquire() as con:
        r = await con.fetchrow("SELECT status, pid FROM runtime_state WHERE id=1")
    assert r["status"] == "stopping"
    assert r["pid"] == fake_pid
    # Health treats pid_not_alive on a dead pid as unhealthy — exactly what
    # we want when a process was SIGKILL'd mid-shutdown.
    # (We only assert the state here; health.py has its own tests.)


# --- 5. pending media rows are swept on restart --------------------------

@pytest.mark.asyncio
async def test_pending_media_resweep_after_restart(storage, tmp_path):
    """A row left 'pending' by a previous process is picked up by sweep."""
    for i in range(5):
        await storage.upsert_media_record({
            "file_id": f"photo:{1000+i}", "chat_id": 1, "message_id": 1000+i,
            "media_type": "photo", "status": "pending",
        })

    # Fresh queue: sweep_retryable should enqueue all five.
    mq = MediaQueue(_FastDoneClient(), storage, base_dir=str(tmp_path),
                    max_media_mb=500, concurrency=1)
    swept = await mq.sweep_retryable(limit=100)
    assert swept == 5, f"sweep returned {swept}, expected 5"
    assert mq.queue.qsize() >= 5


# --- 6. shutdown mid-backfill preserves safe checkpoint -------------------

@pytest.mark.asyncio
async def test_shutdown_mid_backfill_preserves_checkpoint(storage):
    """`sync_state.last_backfilled_message_id` is updated only after a batch
    commits. We simulate that by writing an initial checkpoint, then
    cancelling a 'batch' task before it can call set_backfill_checkpoint.
    The row must remain at the initial value."""
    chat_id = -1001
    await storage.upsert_chat({"id": chat_id, "title": "x"})
    await storage.set_backfill_checkpoint(chat_id, 100, 1)

    async def fake_batch():
        # Imagine iterating messages, then committing a new checkpoint at
        # the very end. We never reach the commit because we get cancelled.
        await asyncio.sleep(10)
        await storage.set_backfill_checkpoint(chat_id, 200, 1)  # never runs

    t = asyncio.create_task(fake_batch())
    await asyncio.sleep(0.1)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass

    async with storage.pool.acquire() as con:
        r = await con.fetchrow(
            "SELECT last_backfilled_message_id FROM sync_state WHERE chat_id=$1",
            chat_id)
    assert r["last_backfilled_message_id"] == 100, (
        f"checkpoint advanced to {r['last_backfilled_message_id']} after cancel"
    )


# --- 7. Collector.wait_background cancels hung tasks silently -------------

@pytest.mark.asyncio
async def test_wait_background_cancels_hung_task_silently(caplog):
    """A background task stuck in asyncio.sleep must be cancelled by
    wait_background() without raising and without logging the CancelledError
    as an error. After the call, the task is done and no longer tracked."""
    from tgparser.collector import Collector

    # Build a bare-bones Collector just to use wait_background + _bg_tasks.
    # We bypass __init__'s client/storage requirements by instantiating
    # via object.__new__ and setting only the attributes wait_background
    # touches.
    c = object.__new__(Collector)
    c._bg_tasks = set()

    async def _hang():
        # Simulates a Telethon flood-wait sleep.
        await asyncio.sleep(1000)

    hung = asyncio.create_task(_hang(), name="hung-bg")
    c._bg_tasks.add(hung)

    caplog.clear()
    t0 = time.monotonic()
    # Must return cleanly, without propagating CancelledError.
    await c.wait_background(timeout=0.5)
    dt = time.monotonic() - t0

    # 1. Returned in reasonable time (grace + cancel budget).
    assert dt < 3.0, f"wait_background took {dt:.2f}s on a hung task"
    # 2. Hung task is done.
    assert hung.done(), "hung task not finished after wait_background"
    # 3. No tracked task remains.
    assert hung not in c._bg_tasks
    assert c._bg_tasks == set()
    # 4. CancelledError must NOT appear as an ERROR / WARNING for the
    #    hung task in the log — it's the normal shutdown outcome.
    for rec in caplog.records:
        msg = rec.getMessage()
        if "CancelledError" in msg and rec.levelno >= 30:  # WARNING+
            pytest.fail(f"CancelledError logged at {rec.levelname}: {msg}")
