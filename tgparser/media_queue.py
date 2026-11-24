import asyncio
import contextlib
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, Photo, Document,
    DocumentAttributeAudio, DocumentAttributeVideo, DocumentAttributeFilename,
)
from telethon import errors

from .collector import _TargetInactive as _TargetInactiveLocal
from .retry import retry_async

log = logging.getLogger("tgparser.media")


def media_descriptor(message) -> dict | None:
    """Describe a telethon message.media for storage.

    Downloadable media (photo, document and their variants) get a stable
    `file_id` derived from the real Telegram media id: `photo:<id>` or
    `doc:<id>`. One Telegram file can appear in many messages — the
    many-to-many link lives in `message_media`, keyed by `file_id`.

    Non-downloadable media (webpage preview, geo, contact, poll, unsupported)
    do NOT have a shareable Telegram file. To avoid collisions where every
    webpage or every poll gets the same `other:...` id and clobbers each
    other, we derive `file_id` from `(chat_id, message_id, tl_type)`, which is
    only known in the enqueue path. `enqueue()` fills this in; the raw
    descriptor here returns `file_id=None` to signal "needs chat/message
    scoping" and `is_downloadable=False`.
    """
    m = getattr(message, "media", None)
    if m is None:
        return None
    if isinstance(m, MessageMediaPhoto) and isinstance(m.photo, Photo):
        return {
            "file_id": f"photo:{m.photo.id}",
            "media_type": "photo",
            "size": None, "mime_type": "image/jpeg", "file_name": None,
            "duration": None, "width": None, "height": None,
            "is_downloadable": True,
        }
    if isinstance(m, MessageMediaDocument) and isinstance(m.document, Document):
        doc = m.document
        mime = doc.mime_type or ""
        file_name = None
        duration = None
        width = None
        height = None
        for a in (doc.attributes or []):
            if isinstance(a, DocumentAttributeFilename):
                file_name = a.file_name
            elif isinstance(a, DocumentAttributeAudio):
                duration = a.duration
            elif isinstance(a, DocumentAttributeVideo):
                duration = a.duration
                width = a.w
                height = a.h
        if mime.startswith("video/"):
            t = "video"
        elif mime.startswith("audio/"):
            t = "voice" if any(
                isinstance(a, DocumentAttributeAudio) and getattr(a, "voice", False)
                for a in (doc.attributes or [])) else "audio"
        elif mime.startswith("image/"):
            t = "sticker" if "webp" in mime or (file_name or "").endswith(".tgs") else "image"
        elif mime == "application/x-tgsticker":
            t = "sticker"
        else:
            t = "document"
        return {
            "file_id": f"doc:{doc.id}",
            "media_type": t,
            "size": doc.size,
            "mime_type": mime or None,
            "file_name": file_name,
            "duration": duration, "width": width, "height": height,
            "is_downloadable": True,
        }
    # Non-downloadable: webpage preview, geo, contact, poll, etc.
    return {
        "file_id": None,
        "media_type": type(m).__name__.lower().replace("messagemedia", ""),
        "size": None, "mime_type": None, "file_name": None,
        "duration": None, "width": None, "height": None,
        "is_downloadable": False,
    }


class MediaQueue:
    """Bounded in-flight download queue with:

    - Per-file_id dedup (never queue the same pending file twice concurrently).
    - Retry-on-failure via `media.attempts`/`last_error` persisted to Postgres.
    - On resume, sweep pending/failed rows and re-enqueue those with a real
      `(chat_id, message_id)` pointer.
    - Graceful shutdown: stop accepting, drain in-flight within timeout.
    """

    def __init__(self, client, storage, base_dir: str,
                 max_media_mb: int, concurrency: int = 3,
                 min_free_mb: int = 500, max_attempts: int = 5):
        self.client = client
        self.storage = storage
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_media_mb) * 1024 * 1024
        self.min_free_bytes = int(min_free_mb) * 1024 * 1024
        self.concurrency = max(1, int(concurrency))
        self.max_attempts = max_attempts
        self.queue: asyncio.Queue = asyncio.Queue()
        self._workers: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._accepting = True
        self._inflight: set[str] = set()         # file_ids currently queued or downloading
        self._inflight_lock = asyncio.Lock()
        # Dynamic engine: callable() -> frozenset of active chat ids. None =>
        # legacy behaviour (every chat).
        self.active_ids = None
        # Dynamic engine: async contextmanager factory `lease(chat_id)`. Held
        # across enqueue DB writes and across the worker's download+finalize
        # so a disable barrier cannot land between a finished file and its
        # message link. None => legacy behaviour (null context).
        self.chat_lease = None

    def _chat_active(self, chat_id) -> bool:
        return self.active_ids is None or chat_id in self.active_ids()

    def _lease_ctx(self, chat_id):
        if self.chat_lease is None:
            return contextlib.nullcontext()
        return self.chat_lease(chat_id)

    def start(self):
        for i in range(self.concurrency):
            self._workers.append(asyncio.create_task(self._worker(i), name=f"media-{i}"))

    async def stop(self, timeout: float = 5.0):
        """Fast, bounded shutdown.

        The queue is durable: every enqueued `media` row is persisted in
        Postgres with status='pending' before it ever enters the in-memory
        asyncio.Queue. Draining tens of thousands of in-memory items on
        shutdown is both unnecessary and unsafe — on restart, sweep_retryable
        re-enqueues every pending/failed row that still has a message pointer.

        Strategy:
          1. Stop accepting new items (enqueue/enqueue_by_ref reject).
          2. Short grace period (`timeout`) so recently-started downloads
             can write their 'done'/'failed' row naturally.
          3. Cancel every worker still running (idle on queue.get(), mid
             download, inside a retry sleep, inside the heartbeat ticker).
          4. Collect worker tasks via asyncio.gather(return_exceptions=True)
             so CancelledError and download exceptions are absorbed without
             tracebacks propagating.
          5. Drop in-memory items on the floor: the asyncio.Queue is not
             persisted. The DB rows keep status='pending' and are swept up
             on next start.

        Guaranteed to return within ~`timeout` + a small cancel budget.
        No worker task is left pending.
        """
        self._accepting = False
        self._stop.set()

        # Poke workers blocked on queue.get() so the first one in each slot
        # can wake and observe _stop / the None sentinel.
        for _ in self._workers:
            try:
                self.queue.put_nowait(None)
            except Exception:
                pass

        # Grace period: wait for workers to finish their current item or
        # notice the sentinel. Polling avoids queue.join() against a huge
        # pending queue.
        loop = asyncio.get_event_loop()
        deadline = loop.time() + max(0.0, float(timeout))
        while loop.time() < deadline:
            if all(t.done() for t in self._workers):
                break
            await asyncio.sleep(0.1)

        # Cancel whatever is still running.
        for t in self._workers:
            if not t.done():
                t.cancel()

        # Collect; return_exceptions=True absorbs CancelledError + any raised
        # download exception so stop() itself cannot propagate.
        results = await asyncio.gather(*self._workers, return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                log.debug("media worker exited with exception: %r", r)
        self._workers.clear()

    def _chat_dir(self, chat_id: int) -> Path:
        d = self.base / str(chat_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _has_free_space(self) -> bool:
        try:
            _, _, free = shutil.disk_usage(self.base)
            return free > self.min_free_bytes
        except Exception:
            return True

    async def _mark_inflight(self, file_id: str) -> bool:
        async with self._inflight_lock:
            if file_id in self._inflight:
                return False
            self._inflight.add(file_id)
            return True

    async def _release_inflight(self, file_id: str):
        async with self._inflight_lock:
            self._inflight.discard(file_id)

    async def enqueue(self, chat_id: int, message):
        if not self._accepting:
            return None
        # Dynamic engine: reject if the chat has already left the active set.
        # The lease is then held across every DB write below so a disable
        # barrier cannot land between upsert_media_record and link_message_media.
        if not self._chat_active(chat_id):
            return None
        try:
            async with self._lease_ctx(chat_id):
                return await self._enqueue_locked(chat_id, message)
        except _TargetInactiveLocal:
            return None

    async def _enqueue_locked(self, chat_id: int, message):
        desc = media_descriptor(message)
        if desc is None:
            return None

        # Non-downloadable media: stable id scoped to (chat, message, type).
        # Record as metadata_only, link, return without queuing a download.
        if not desc.get("is_downloadable", True) or desc.get("file_id") is None:
            file_id = f"meta:{chat_id}:{message.id}:{desc['media_type']}"
            raw = None
            try:
                raw = message.media.to_dict()
            except Exception:
                pass
            await self.storage.media_set_metadata_only(
                file_id, desc["media_type"], chat_id, message.id, raw=raw)
            return (desc["media_type"], file_id, None)

        file_id = desc["file_id"]

        # Persist descriptor first (keeps attempts/last_error accurate).
        await self.storage.upsert_media_record({
            "file_id": file_id,
            "chat_id": chat_id,
            "message_id": message.id,
            "media_type": desc["media_type"],
            "mime_type": desc["mime_type"],
            "size": desc["size"],
            "file_name": desc["file_name"],
            "duration": desc["duration"],
            "width": desc["width"],
            "height": desc["height"],
            "status": "pending",
        })
        # Link this message to the file (M2M).
        await self.storage.link_message_media(chat_id, message.id, file_id)

        status, path = await self.storage.media_existing_path(file_id)
        if status == "done" and path:
            # Still make sure the current message points at the file.
            await self.storage.attach_media_to_message(
                chat_id, message.id, desc["media_type"], file_id, path)
            return (desc["media_type"], file_id, path)

        if desc["size"] and desc["size"] > self.max_bytes:
            await self.storage.upsert_media_record({
                "file_id": file_id,
                "status": "skipped",
                "last_error": f"size {desc['size']} > max_media_bytes {self.max_bytes}",
            })
            return (desc["media_type"], file_id, None)

        if not await self._mark_inflight(file_id):
            # Already queued / downloading. The link to this message is already
            # saved; the worker will attach the final path when done.
            return (desc["media_type"], file_id, None)

        await self.queue.put({
            "chat_id": chat_id,
            "message_id": message.id,
            "desc": desc,
            "message": message,
        })
        return (desc["media_type"], file_id, None)

    async def enqueue_by_ref(self, chat_id: int, message_id: int, file_id: str,
                             media_type: str, size: int | None):
        """Enqueue a retry for a file we already know about but need to fetch again."""
        if not self._accepting:
            return
        if not self._chat_active(chat_id):
            return
        try:
            async with self._lease_ctx(chat_id):
                await self._enqueue_by_ref_locked(
                    chat_id, message_id, file_id, media_type, size)
        except _TargetInactiveLocal:
            return

    async def _enqueue_by_ref_locked(self, chat_id, message_id, file_id,
                                     media_type, size):
        if not await self._mark_inflight(file_id):
            return
        # We need the real message object to call download_media; fetch on demand.
        try:
            msg = await self.client.get_messages(chat_id, ids=message_id)
        except Exception as e:
            await self._release_inflight(file_id)
            log.warning("retry fetch msg (%s,%s) failed: %s", chat_id, message_id, e)
            await self.storage.media_increment_attempt(file_id, str(e)[:500])
            return
        if msg is None:
            # Message gone — mark skipped so we stop retrying.
            await self._release_inflight(file_id)
            await self.storage.upsert_media_record({
                "file_id": file_id, "status": "skipped",
                "last_error": "source message gone",
            })
            return
        await self.queue.put({
            "chat_id": chat_id, "message_id": message_id,
            "desc": {"file_id": file_id, "media_type": media_type, "size": size},
            "message": msg,
        })

    async def sweep_retryable(self, limit: int = 500):
        """Re-enqueue pending/failed media.

        Called once at startup and, since v7.2, again from the coordinator's
        backpressure wait when workers have run dry while DB still holds
        pending rows (otherwise `sweep_retryable`'s startup cap of 500 lets a
        larger backlog — accumulated across process crashes while items were
        in the in-memory asyncio.Queue — starve workers and deadlock).
        """
        if self.active_ids is not None:
            rows = await self.storage.media_get_retryable(
                limit=limit, chat_ids=list(self.active_ids()))
        else:
            rows = await self.storage.media_get_retryable(limit=limit)
        for r in rows:
            await self.enqueue_by_ref(r["chat_id"], r["message_id"],
                                      r["file_id"], r["media_type"], r["size"])
        return len(rows)

    async def _worker(self, idx: int):
        import os as _os
        pid = _os.getpid()
        try:
            await self.storage.worker_heartbeat(idx, pid=pid, current_file_id=None)
        except Exception:
            pass
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                return
            file_id = item["desc"]["file_id"]
            chat_id = item.get("chat_id")
            if not self._chat_active(chat_id):
                # Target left the active set between enqueue and dequeue:
                # leave the media row pending in Postgres (swept again if
                # the target is re-enabled).
                await self._release_inflight(file_id)
                self.queue.task_done()
                continue
            # Dynamic engine: hold the per-chat lease for the WHOLE download
            # + final writes. A disable barrier cannot land between a finished
            # file and its message link; if the chat left the set between the
            # _chat_active check and lease acquisition, _TargetInactiveLocal
            # drops the item back to pending in the DB.
            try:
                _lease = self._lease_ctx(chat_id)
                try:
                    await _lease.__aenter__()
                except _TargetInactiveLocal:
                    await self._release_inflight(file_id)
                    self.queue.task_done()
                    continue
            except Exception:
                await self._release_inflight(file_id)
                self.queue.task_done()
                continue
            try:
                await self.storage.worker_heartbeat(
                    idx, pid=pid, current_file_id=file_id)
            except Exception:
                pass
            # v7: background heartbeat-ticker while download is in progress
            stop_hb = asyncio.Event()
            async def _hb_ticker():
                while not stop_hb.is_set():
                    try:
                        await asyncio.wait_for(stop_hb.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        pass
                    if not stop_hb.is_set():
                        try:
                            await self.storage.worker_heartbeat(
                                idx, pid=pid, current_file_id=file_id)
                        except Exception:
                            pass
            hb_task = asyncio.create_task(_hb_ticker(), name=f"media-hb-{idx}")
            try:
                await self._download(item)
                try:
                    await self.storage.worker_heartbeat(
                        idx, pid=pid, current_file_id=None, completed_delta=1)
                except Exception:
                    pass
            except asyncio.CancelledError:
                # Worker is being shut down. Make sure the heartbeat task
                # dies with us so MediaQueue.stop can report zero media-*
                # pending tasks. No DB bookkeeping here — the media row stays
                # 'pending' and will be swept on the next start.
                stop_hb.set()
                if not hb_task.done():
                    hb_task.cancel()
                try:
                    await hb_task
                except BaseException:
                    pass
                raise
            except Exception as e:
                if getattr(e, "sqlstate", None) == "TGP01":
                    # Disabled mid-download (dynamic engine): the media row
                    # keeps whatever state it reached; not a failure.
                    log.info("media %s: target disabled during download", file_id)
                    continue
                # v6: classify expired file reference as 'needs_refresh' so the
                # coordinator refreshes the Message object instead of looping.
                err_s = str(e)
                try:
                    from telethon.errors import FileReferenceExpiredError
                    expired = isinstance(e, FileReferenceExpiredError)
                except Exception:
                    expired = False
                if expired or "file reference" in err_s.lower() \
                        or "FileReferenceExpired" in err_s:
                    log.info("media %s: file reference expired -> needs_refresh",
                             file_id)
                    try:
                        await self.storage.upsert_media_record({
                            "file_id": file_id, "status": "needs_refresh",
                            "last_error": "FileReferenceExpired",
                        })
                        await self.storage.media_increment_attempt(
                            file_id, "FileReferenceExpired")
                    except Exception:
                        pass
                else:
                    log.warning("media worker %d failed on %s: %s", idx, file_id, e)
                    try:
                        await self.storage.upsert_media_record({
                            "file_id": file_id, "status": "failed",
                            "last_error": err_s[:500],
                        })
                        await self.storage.media_increment_attempt(file_id, err_s[:500])
                    except Exception:
                        pass
                try:
                    await self.storage.worker_heartbeat(
                        idx, pid=pid, current_file_id=None,
                        failed_delta=1, last_error=err_s[:300])
                except Exception:
                    pass
            finally:
                try:
                    stop_hb.set()
                    await hb_task
                except Exception:
                    pass
                try:
                    await _lease.__aexit__(None, None, None)
                except Exception:
                    pass
                await self._release_inflight(file_id)
                self.queue.task_done()

    async def _download(self, item):
        desc = item["desc"]
        chat_id = item["chat_id"]
        message = item["message"]
        file_id = desc["file_id"]

        if not self._has_free_space():
            await self.storage.upsert_media_record({
                "file_id": file_id, "status": "failed", "last_error": "disk full",
            })
            log.error("disk full under %s", self.base)
            return

        chat_dir = self._chat_dir(chat_id)
        file_id_safe = file_id.replace(":", "_")

        # If a file exists on disk already from a previous run, adopt it only
        # after a size check (reject obvious truncation).
        existing = [p for p in chat_dir.glob(f"{file_id_safe}.*")
                    if not p.name.endswith(".part")]
        if existing:
            p = existing[0]
            disk_size = p.stat().st_size
            if disk_size > 0:
                local = str(p.relative_to(self.base))
                await self.storage.upsert_media_record({
                    "file_id": file_id, "status": "done",
                    "local_path": local, "size": disk_size,
                    "completed_at": datetime.now(timezone.utc),
                })
                await self.storage.attach_media_to_message(
                    chat_id, message.id, desc["media_type"], file_id, local)
                return
            # zero-byte leftover, delete and continue to redownload
            try:
                p.unlink()
            except Exception:
                pass

        # Record attempt start (increments even if download fails).
        await self.storage.media_increment_attempt(file_id, None)

        # Download to a sibling ".part" file, then atomically rename after
        # size verification. This guarantees that a visible file is never
        # marked 'done' in the DB unless it exists on disk with its size.
        prefix = chat_dir / file_id_safe
        part_target = chat_dir / f".tmp_{file_id_safe}"
        try:
            path = await retry_async(
                self.client.download_media, message, file=str(part_target),
                max_tries=3, base=2.0, cap=30.0)
        except errors.FloodWaitError:
            raise
        if not path:
            await self.storage.upsert_media_record({
                "file_id": file_id, "status": "failed",
                "last_error": "download returned None",
            })
            return

        src = Path(path)
        if not src.exists() or src.stat().st_size == 0:
            await self.storage.upsert_media_record({
                "file_id": file_id, "status": "failed",
                "last_error": "downloaded file missing or empty",
            })
            try:
                src.unlink()
            except Exception:
                pass
            return

        # Rename to final name preserving extension telethon chose.
        suffix = src.suffix if src.suffix else ".bin"
        final = chat_dir / f"{file_id_safe}{suffix}"
        try:
            os.replace(src, final)
        except Exception as e:
            await self.storage.upsert_media_record({
                "file_id": file_id, "status": "failed",
                "last_error": f"atomic rename failed: {e}"[:300],
            })
            return
        size = final.stat().st_size

        # Optional SHA-256 to detect silent corruption on future verify.
        sha = None
        try:
            import hashlib
            h = hashlib.sha256()
            with open(final, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
            sha = h.hexdigest()
        except Exception:
            pass

        rel = str(final.relative_to(self.base))
        await self.storage.upsert_media_record({
            "file_id": file_id, "status": "done",
            "local_path": rel, "size": size, "sha256": sha,
            "completed_at": datetime.now(timezone.utc),
        })
        await self.storage.attach_media_to_message(
            chat_id, message.id, desc["media_type"], file_id, rel)
