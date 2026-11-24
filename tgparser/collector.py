import asyncio
import contextlib
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from telethon import events, errors
from telethon.tl.types import (
    User, Channel, Chat, PeerUser, PeerChannel, PeerChat,
    MessageFwdHeader, MessageService,
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaContact,
    MessageMediaPoll, MessageMediaWebPage,
    ChannelParticipantCreator, ChannelParticipantAdmin,
    ChannelParticipantBanned, ChannelParticipantLeft,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetMessageReactionsListRequest

from .ids import canonical, is_user_peer, is_channel_peer, peer_kind
from .reactions import reactions_total_and_rows, reaction_key, peer_id_and_kind

log = logging.getLogger("tgparser.collector")


def _peer_id_signed(peer):
    if isinstance(peer, PeerUser):
        return peer.user_id
    if isinstance(peer, PeerChannel):
        return int(f"-100{peer.channel_id}")
    if isinstance(peer, PeerChat):
        return -peer.chat_id
    return None


def _ent_to_dict(e):
    try:
        return e.to_dict()
    except Exception:
        return {"_": type(e).__name__}


def _to_dict_safe(entity):
    try:
        return entity.to_dict()
    except Exception:
        return {}


def is_inactive_write(exc: BaseException) -> bool:
    """The v11 chat write guard rejected the write (target not enabled)."""
    return getattr(exc, "sqlstate", None) == "TGP01"


class _TargetInactive(Exception):
    """Raised inside backfill when the target stopped being active."""


class Collector:
    def __init__(self, client, storage, media_queue, options):
        self.client = client
        self.storage = storage
        self.media_q = media_queue
        self.options = options
        self._targets: list[int] = []
        self._user_locks: dict[int, asyncio.Lock] = {}
        self._user_profile_ttl = int(options.get("user_profile_ttl_hours", 24))
        self._fetch_reaction_authors = bool(options.get("reaction_authors", False))
        self._reaction_authors_limit = int(options.get("reaction_authors_limit", 100))
        self._participants_sync = bool(options.get("participants_sync", True))
        self._per_msg_retry = int(options.get("per_message_retries", 3))
        self._bg_tasks: set[asyncio.Task] = set()
        self._bg_sema = asyncio.Semaphore(int(options.get("bg_concurrency", 10)))
        self._react_authors_sema = asyncio.Semaphore(
            int(options.get("reaction_authors_concurrency", 2)))

        # Per-chat in-memory set of message_ids that are currently queued in
        # backfill_queue (so we never advance checkpoint past them).
        self._pending_floor: dict[int, int | None] = {}

        # Degraded-state tracking: targets that failed to resolve.
        self.failed_targets: list[tuple[str, str]] = []
        self.ok_targets: list[int] = []

        # v5: batch coordinator + user profile queue dedup
        self.batch_size = int(options.get("backfill_batch_size",
                              options.get("backfill_limit") or 2000))
        self.batch_pause = int(options.get("backfill_batch_pause_seconds", 2))
        self.batch_concurrency = int(options.get("backfill_max_concurrency", 1))
        # v6: stall recovery
        self.batch_timeout = int(options.get("backfill_batch_timeout_seconds", 600))
        self.progress_timeout = int(options.get("backfill_progress_timeout_seconds", 180))
        self.recovery_backoff = int(options.get("backfill_recovery_backoff_seconds", 30))
        self.max_consecutive_failures = int(options.get(
            "backfill_max_consecutive_failures", 5))
        # v6: media backpressure
        self.media_hw = int(options.get("media_pending_high_water", 25_000))
        self.media_lw = int(options.get("media_pending_low_water", 5_000))
        # v6: expired reference retry
        self.media_refresh_max = int(options.get("media_refresh_max_attempts", 3))

        self._user_profile_inflight: set[int] = set()
        self._user_profile_lock = asyncio.Lock()
        self._coordinator_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # Per-PID batch counter (acceptance proof: >=2 batches per target same PID)
        self.batches_this_pid: dict[int, int] = {}
        # Dynamic engine hooks. None => legacy behaviour, unchanged.
        self._active_check = None          # callable(chat_id) -> bool
        self._dynamic = False
        # Async context-manager factory for per-chat work; dynamic engine
        # supplies its lease(chat_id) here. Legacy stays on nullcontext.
        self._chat_work_ctx = None
        # Hook called after a chat-attributed bg task is spawned; dynamic
        # engine registers the task in its per-chat lease registry so a
        # disable can cancel chat-sourced user/profile/reaction follow-ups.
        self._bg_spawn_hook = None         # callable(chat_id, task) -> None
        self._wake = asyncio.Event()       # set when the target set changes

    def _is_active(self, chat_id: int) -> bool:
        return self._active_check is None or bool(self._active_check(chat_id))

    def _begin_chat_work(self, chat_id: int):
        """Enter the per-chat work context. Legacy is a null context; the
        dynamic engine returns its per-chat lease, whose __aenter__ raises
        _TargetInactive if the chat is no longer active, and which blocks
        a disable-ACK quiesce from completing while held."""
        if self._chat_work_ctx is None:
            return contextlib.nullcontext()
        return self._chat_work_ctx(chat_id)

    def wake(self):
        self._wake.set()

    async def _idle(self, seconds: float):
        """Idle wait that ends on stop or (dynamic) on a target-set change."""
        if self._stop.is_set():
            return
        stop_t = asyncio.ensure_future(self._stop.wait())
        wake_t = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({stop_t, wake_t}, timeout=seconds,
                               return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (stop_t, wake_t):
                t.cancel()
            await asyncio.gather(stop_t, wake_t, return_exceptions=True)
            self._wake.clear()

    # -- public lifecycle ----------------------------------------------------
    async def start(self, targets: list[str]) -> int:
        """Resolve targets, register handlers. Return count of resolved targets.
        failed_targets is populated for any that didn't resolve.

        Supports: @username, numeric chat_id, canonical -100id, and private
        invite links (t.me/+hash or t.me/joinchat/hash) via a dialog-scan
        fallback — the user must already be a member of the private group.
        """
        resolved_ids: set[int] = set()
        for t in targets:
            entity = None
            try:
                entity = await self.client.get_entity(t)
            except Exception as e:
                entity = await self._resolve_via_invite_or_dialogs(t)
                if entity is None:
                    log.error("cannot resolve target %s: %s", t, e)
                    self.failed_targets.append((str(t), str(e)[:200]))
                    try:
                        await self.storage.record_error(
                            "resolve_target", None, None, None, e)
                    except Exception:
                        pass
                    continue
            cid = canonical(entity)
            await self._save_chat(entity)
            await self.storage.init_sync_state(cid)
            await self.storage.target_upsert(cid, str(t), "running")
            resolved_ids.add(cid)
            if self.options.get("follow_linked_discussion", True) and isinstance(entity, Channel):
                linked = await self._get_linked(entity)
                if linked is not None:
                    lc = canonical(linked)
                    await self._save_chat(linked)
                    await self.storage.init_sync_state(lc)
                    await self.storage.target_upsert(
                        lc, f"linked:{t}", "running")
                    resolved_ids.add(lc)

        self._targets = sorted(resolved_ids)
        self.ok_targets = list(self._targets)
        self._register_handlers(self._targets)
        # Lifecycle: disable targets that are no longer in config.
        for row in await self.storage.targets_all():
            if row["chat_id"] not in resolved_ids and row.get("enabled"):
                await self.storage.target_disable(
                    row["chat_id"], reason="removed_from_config")
                log.info("target %s disabled (not in config)", row["chat_id"])
            elif row["chat_id"] in resolved_ids and not row.get("enabled", True):
                await self.storage.target_enable(row["chat_id"])
        return len(self._targets)

    # v5: background coordinator - keeps running batches until natural end
    async def start_coordinator(self):
        """Spawn the background backfill coordinator task.

        The coordinator keeps iterating batches for each active target whose
        backfill status is `running` or `syncing`, pausing between batches so
        live updates and reaction handlers keep flowing. It never blocks the
        main loop or creates a second TelegramClient."""
        if self._coordinator_task is not None:
            return
        self._coordinator_task = asyncio.create_task(
            self._coordinator_loop(), name="backfill-coordinator")

    async def stop_coordinator(self):
        # backfill() advances the checkpoint after EACH successfully processed
        # message. It checks self._stop before starting the next message and
        # before re-opening the iterator, and its FloodWait / transient-retry
        # waits are stop-aware, so a normal stop returns quickly with status
        # 'stopped' (never 'complete'). The forced cancel below stays as a
        # fallback for a network call that does not return within 5s; a
        # message interrupted by that cancel did not advance the checkpoint.
        self._stop.set()
        if self._coordinator_task:
            try:
                await asyncio.wait_for(self._coordinator_task, timeout=5)
            except Exception:
                self._coordinator_task.cancel()
                try:
                    await self._coordinator_task
                except BaseException:
                    pass

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep up to `seconds`; return True as soon as _stop is set."""
        if self._stop.is_set():
            return True
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _coordinator_loop(self):
        log.info(
            "coordinator started (batch_size=%s pause=%ss timeout=%ss "
            "progress_timeout=%ss hw/lw=%s/%s)",
            self.batch_size, self.batch_pause, self.batch_timeout,
            self.progress_timeout, self.media_hw, self.media_lw)
        try:
            if self._participants_sync:
                for cid in list(self._targets):
                    if self._stop.is_set():
                        return
                    if not self._is_active(cid):
                        continue
                    try:
                        async with self._begin_chat_work(cid):
                            await self.sync_participants(cid)
                    except _TargetInactive:
                        continue
                    except asyncio.CancelledError:
                        continue
                    except Exception as e:
                        log.warning("participants sync %s: %s", cid, e)

            while not self._stop.is_set():
                any_progress = False
                for cid in list(self._targets):
                    if self._stop.is_set():
                        break
                    if not self._is_active(cid):
                        continue
                    try:
                        async with self._begin_chat_work(cid):
                            progressed = await self._coordinator_chat_round(cid)
                        if progressed:
                            any_progress = True
                    except _TargetInactive:
                        continue
                    except asyncio.CancelledError:
                        continue
                    except Exception as e:
                        log.exception("coordinator round failed on %s", cid)
                        await self.storage.record_error(
                            "coordinator_round", cid, None, None, e)

                # Shutdown requested during the target loop: skip the
                # Shutdown requested during the target loop: skip the
                # post-round maintenance (it makes Telegram calls) and exit.
                if self._stop.is_set():
                    break

                # v5: user profile queue drain
                try:
                    await self._drain_user_profile_queue()
                except Exception as e:
                    log.warning("user_profile_queue drain: %s", e)

                # v6: refresh media with expired file references
                try:
                    await self._refresh_media_needing_refresh()
                except Exception as e:
                    log.warning("media refresh: %s", e)

                # v7.1: only long-idle sleep when everything is naturally
                # complete AND no failures. Otherwise short sleep so recovery
                # can retry quickly.
                any_more = False
                for _c in self._targets:
                    _s = await self.storage.get_sync_state(_c)
                    if _s and _s["backfill_status"] in (
                        "syncing", "running", "stalled", "degraded",
                        "syncing_paused_media_backpressure"):
                        any_more = True
                        break
                if not any_progress and not any_more:
                    await self._idle(60)
                elif not any_progress:
                    await self._idle(2)
        finally:
            log.info("coordinator stopped")

    async def _coordinator_chat_round(self, cid: int) -> bool:
        """One pass of the coordinator over a single chat: heartbeat,
        backpressure handling, one backfill batch, DLQ sweep. Runs inside
        the caller's per-chat work context (dynamic: holds the lease),
        so every write below is attributable to the chat. Returns True
        if the batch made progress."""
        any_progress = False
        await self.storage.coordinator_heartbeat(cid)

        # v7.3: media backpressure - true hysteresis.
        # Invariant: no new backfill batch starts while DB pending
        # is >= media_lw. The v7.2 variant (60-iteration cap then
        # unconditional resume) could kick off a new 2000-msg batch
        # with pending still far above HWM, growing the backlog.
        # Here we stay paused until pending actually drops below
        # LWM or _stop is set, but keep heartbeating, re-sweeping
        # the in-memory queue, and watching for silently-dead
        # workers (done_count stops changing while q_depth > 0).
        pending = await self._media_pending_count()
        if pending > self.media_hw:
            log.info("backpressure: media pending %s > hw %s - pausing backfill",
                     pending, self.media_hw)
            await self.storage.set_backfill_status(
                cid, "syncing_paused_media_backpressure")
            last_done_check_at = datetime.now(timezone.utc)
            last_done_count = await self._media_done_count()
            while (pending >= self.media_lw and not self._stop.is_set()
                   and self._is_active(cid)):
                await self._idle(10)
                if self._stop.is_set() or not self._is_active(cid):
                    break
                pending = await self._media_pending_count()
                await self.storage.coordinator_heartbeat(cid)
                # resweep if in-memory queue has drained
                try:
                    q_depth = (self.media_q.queue.qsize()
                               + len(self.media_q._inflight))
                    if q_depth < 50:
                        swept = await self.media_q.sweep_retryable(
                            limit=50)
                        if swept:
                            log.info(
                                "backpressure: resweep queued %s "
                                "items (pending=%s q_depth=%s)",
                                swept, pending, q_depth)
                except Exception:
                    log.exception("backpressure resweep failed")
                # periodic liveness check for media workers:
                # if done_count hasn't moved for 5 min while q_depth
                # >0 we log a warning (don't force degraded to
                # avoid false positives on legitimately slow
                # downloads, e.g. one large video).
                now = datetime.now(timezone.utc)
                if (now - last_done_check_at).total_seconds() >= 300:
                    cur_done = await self._media_done_count()
                    if (cur_done == last_done_count
                            and q_depth > 0
                            and not self._stop.is_set()):
                        log.warning(
                            "backpressure: media workers made no "
                            "progress for 5 min (done=%s, q_depth=%s); "
                            "coordinator remains paused and will keep "
                            "re-sweeping, but media pipeline may be "
                            "stuck - inspect media worker heartbeats.",
                            cur_done, q_depth)
                    last_done_count = cur_done
                    last_done_check_at = now
            if self._stop.is_set() or not self._is_active(cid):
                return any_progress
            # pending < LWM -> hysteresis resume
            log.info("backpressure: resume (pending=%s < lw=%s)",
                     pending, self.media_lw)
            await self.storage.set_backfill_status(cid, "syncing")

        # v6: batch with hard timeout + progress watchdog
        batch_start = datetime.now(timezone.utc)
        added = 0; status = "syncing"; more = False
        reason = None; ok = True
        try:
            added, status, more = await asyncio.wait_for(
                self.backfill(cid), timeout=self.batch_timeout)
            if added:
                any_progress = True
        except asyncio.TimeoutError:
            ok = False
            reason = f"batch_timeout_{self.batch_timeout}s"
            log.warning("batch TIMEOUT on %s after %ss - recovering",
                        cid, self.batch_timeout)
            await self._recover_after_stall(cid, reason)
        except Exception as e:
            ok = False
            reason = f"{type(e).__name__}: {e}"[:300]
            log.exception("coordinator batch failed on %s", cid)
            await self.storage.record_error(
                "coordinator_batch", cid, None, None, e)
            await self._recover_after_stall(cid, reason)

        dur = int((datetime.now(timezone.utc) - batch_start).total_seconds())
        await self.storage.coordinator_batch_end(
            cid, added, dur, ok, reason)

        if not ok:
            if await self._sleep_or_stop(self.recovery_backoff):
                return any_progress
            prog = await self._coord_progress_row(cid)
            if prog and prog.get("consecutive_batch_failures", 0) >= self.max_consecutive_failures:
                log.error("target %s DEGRADED after %s consecutive failures",
                          cid, prog["consecutive_batch_failures"])
                await self.storage.set_backfill_status(cid, "degraded")
                # longer sleep before next attempt
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self.recovery_backoff * 10)
                except asyncio.TimeoutError:
                    pass

        if ok and not more and status in ("complete", "complete_with_errors"):
            await self.retry_dead_letter(cid)

        if ok and more:
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.batch_pause)
            except asyncio.TimeoutError:
                pass
        return any_progress

    async def _media_pending_count(self) -> int:
        if self._dynamic:
            return await self.storage.media_pending_count_for(list(self._targets))
        async with self.storage.pool.acquire() as con:
            return await con.fetchval(
                "SELECT COUNT(*) FROM media WHERE status='pending'")

    async def _media_done_count(self) -> int:
        async with self.storage.pool.acquire() as con:
            return await con.fetchval(
                "SELECT COUNT(*) FROM media WHERE status='done'")

    async def _coord_progress_row(self, chat_id: int) -> dict | None:
        async with self.storage.pool.acquire() as con:
            r = await con.fetchrow(
                "SELECT * FROM coordinator_progress WHERE chat_id=$1", chat_id)
            return dict(r) if r else None

    async def _recover_after_stall(self, chat_id: int, reason: str):
        """Called after batch timeout or exception.

        1. Mark target stalled so health picks it up — UNLESS it's already
           'degraded' (max consecutive failures reached), which must stick.
        2. Disconnect+reconnect Telegram (handles Windows sleep / dropped socket).
        3. catch_up live updates.
        """
        try:
            cur = await self.storage.get_sync_state(chat_id)
            if not cur or cur["backfill_status"] != "degraded":
                await self.storage.set_backfill_status(chat_id, "stalled")
        except Exception:
            pass
        try:
            log.warning("TG recovery: disconnect+reconnect+catch_up (reason=%s)", reason)
            try:
                if self.client.is_connected():
                    await self.client.disconnect()
            except Exception:
                pass
            # Brief pause, then reconnect
            await asyncio.sleep(2)
            await self.client.connect()
            try:
                await self.client.catch_up()
            except Exception as e:
                log.info("catch_up during recovery soft-fail: %s", e)
        except Exception as e:
            log.warning("TG recovery failed: %s", e)

    async def _refresh_media_needing_refresh(self):
        """Re-fetch messages whose file_reference expired, then let the normal
        media queue re-download them with the fresh reference."""
        if self._dynamic:
            rows = await self.storage.media_needs_refresh(
                limit=20, chat_ids=list(self._targets))
        else:
            rows = await self.storage.media_needs_refresh(limit=20)
        if not rows:
            return
        for r in rows:
            fid = r["file_id"]
            attempts = r["attempts"] or 0
            if attempts >= self.media_refresh_max:
                await self.storage.media_mark_unavailable(
                    fid, "refresh_max_attempts_exhausted")
                continue
            try:
                entity = await self.client.get_entity(r["chat_id"])
                msg = await self.client.get_messages(entity, ids=r["message_id"])
            except Exception as e:
                await self.storage.media_increment_attempt(fid, f"refresh_fetch: {e}"[:300])
                continue
            if msg is None or getattr(msg, "media", None) is None:
                await self.storage.media_mark_unavailable(
                    fid, "source_message_gone_or_no_media")
                continue
            # Put the refreshed message back into the media queue (bypass dedup
            # by releasing its in-flight marker).
            try:
                self.media_q._inflight.discard(fid)
            except Exception:
                pass
            await self.media_q.enqueue(r["chat_id"], msg)

    # kept for backward-compatibility with older tests that import this name

    async def _drain_user_profile_queue(self, limit: int = 50):
        max_attempts = int(self.options.get("user_profile_retry_max_attempts", 5))
        pending = await self.storage.user_profile_queue_pending(
            limit=limit, max_attempts=max_attempts)
        processed = 0
        for row in pending:
            uid = row["user_id"]
            try:
                await self._ensure_user(uid)
                await self.storage.user_profile_queue_mark_done(uid)
                processed += 1
            except Exception as e:
                if row["attempts"] + 1 >= max_attempts:
                    await self.storage.user_profile_queue_mark_dead(uid)
                else:
                    await self.storage.user_profile_queue_upsert(
                        uid, f"{type(e).__name__}: {e}"[:400],
                        retry_in_minutes=int(self.options.get(
                            "user_profile_retry_pause_minutes", 10)))
        if processed:
            log.info("user_profile_queue drained=%d", processed)

    async def run_backfill_and_sync(self) -> dict:
        """Backfill every target, retry the dead-letter queue, sync participants.

        Returns a summary dict with per-target status so the caller can decide
        the overall runtime status (healthy / degraded / unhealthy).
        """
        summary = {"total": len(self._targets), "failed": [], "degraded": []}
        if self.options.get("backfill_on_start", True):
            for cid in self._targets:
                try:
                    await self.backfill(cid)
                except Exception as e:
                    log.exception("backfill of %s failed", cid)
                    await self.storage.record_error("backfill", cid, None, None, e)
                    await self.storage.target_upsert(
                        cid, str(cid), "incomplete", last_error=str(e)[:500])
                    summary["failed"].append(cid)
        for cid in self._targets:
            try:
                await self.retry_dead_letter(cid)
            except Exception as e:
                log.exception("retry_dead_letter of %s failed", cid)
                await self.storage.record_error("retry_dlq", cid, None, None, e)
        if self._participants_sync:
            for cid in self._targets:
                try:
                    await self.sync_participants(cid)
                except Exception as e:
                    log.exception("sync_participants of %s failed", cid)
                    await self.storage.record_error("participants", cid, None, None, e)
        # Classify degraded targets.
        for row in await self.storage.targets_all():
            if row["backfill_status"] in ("blocked", "complete_with_errors",
                                          "incomplete"):
                summary["degraded"].append(row["chat_id"])
        return summary

    async def wait_background(self, timeout: float = 20.0):
        """Wait for all tracked background tasks with a timeout, then cancel.

        Shutdown contract:
          - Tasks that finish naturally within `timeout` are kept as-is.
          - Tasks still pending at `timeout` are cancelled and awaited via
            asyncio.gather(return_exceptions=True), so one hung task cannot
            hold up shutdown.
          - asyncio.CancelledError from cancelled tasks is treated as the
            NORMAL shutdown outcome and is NOT logged.
          - Any other exception a background task raised is logged at
            WARNING, once per task, without leaking out of this method.
          - The method itself must never raise CancelledError at shutdown:
            that would short-circuit the rest of teardown in run.py.
          - Completed tasks are removed from self._bg_tasks.
        """
        if not self._bg_tasks:
            return
        pending = list(self._bg_tasks)
        done, not_done = await asyncio.wait(pending, timeout=timeout)
        for t in not_done:
            t.cancel()
        # Gather with return_exceptions=True absorbs CancelledError and any
        # task-raised exception. Nothing leaves this method.
        results = await asyncio.gather(*not_done, return_exceptions=True)
        for t, r in zip(not_done, results):
            if isinstance(r, asyncio.CancelledError):
                continue  # expected on shutdown
            if isinstance(r, BaseException):
                log.warning("bg task %r raised during shutdown: %r",
                            t.get_name(), r)
        # Also inspect tasks that finished before the timeout for real errors.
        for t in done:
            if t.cancelled():
                continue
            exc = t.exception()
            if exc is not None:
                log.warning("bg task %r finished with exception: %r",
                            t.get_name(), exc)
        # Prune finished tasks from the tracked set.
        self._bg_tasks = {t for t in self._bg_tasks if not t.done()}

    # -- handlers ------------------------------------------------------------
    def _register_handlers(self, chat_ids: list[int]):
        target_set = set(chat_ids)

        @self.client.on(events.NewMessage(chats=chat_ids))
        async def _new(ev):
            try:
                await self._handle_message(ev.message)
            except Exception as e:
                log.exception("new_message error")
                await self.storage.record_error(
                    "new_message", canonical(ev.message.peer_id), ev.message.id, None, e)

        @self.client.on(events.MessageEdited(chats=chat_ids))
        async def _edited(ev):
            try:
                await self._handle_message(ev.message)
            except Exception as e:
                log.exception("edited error")
                await self.storage.record_error(
                    "edited", canonical(ev.message.peer_id), ev.message.id, None, e)

        @self.client.on(events.MessageDeleted(chats=chat_ids))
        async def _deleted(ev):
            try:
                ids = list(ev.deleted_ids)
                raw_payload = None
                try:
                    raw_payload = ev.original_update.to_dict() if getattr(
                        ev, "original_update", None) else None
                except Exception:
                    raw_payload = None
                update_type = (type(ev.original_update).__name__
                               if getattr(ev, "original_update", None)
                               else type(ev).__name__)
                if ev.chat_id is not None:
                    cid = canonical(ev.chat_id)
                    # Mark existing rows and auto-stub missing ones in one call.
                    await self.storage.ensure_message_stub_deleted(cid, ids)
                    # Opportunistically close any unresolved event that named
                    # this channel and one of these ids.
                    try:
                        await self.storage.resolve_deletion_events(cid, ids)
                    except Exception:
                        pass
                else:
                    # Telegram did not send a chat_id — cannot associate.
                    log.info("deletion without chat_id, ids=%d; logged as event",
                             len(ids))
                    await self.storage.record_deletion_event(
                        ids, channel_id=None, update_type=update_type,
                        raw_payload=raw_payload)
            except Exception as e:
                log.exception("deleted error")
                await self.storage.record_error("deleted", None, None, None, e)

        @self.client.on(events.ChatAction(chats=chat_ids))
        async def _action(ev):
            try:
                await self._handle_action(ev)
            except Exception as e:
                log.exception("chat_action error")
                await self.storage.record_error("chat_action", None, None, None, e)

        @self.client.on(events.Raw())
        async def _raw(update):
            name = type(update).__name__
            if name not in ("UpdateMessageReactions", "UpdateBotMessageReactions"):
                return
            peer = getattr(update, "peer", None)
            if peer is None:
                return
            try:
                cid = canonical(peer)
            except Exception:
                return
            if cid not in target_set:
                return
            try:
                await self._handle_reactions_update(update, cid)
            except Exception as e:
                log.exception("reactions update error")
                await self.storage.record_error("reactions_update", cid, None, None, e)

    # -- backfill ------------------------------------------------------------
    async def backfill(self, chat_id: int, batch_size: int | None = None):
        """Durable, resumable, FloodWait-tolerant backfill - ONE BATCH.

        Processes up to `batch_size` messages (default: self.batch_size).
        Returns truth about the batch so a coordinator can decide whether to
        schedule another one. Checkpoint invariant: last_backfilled_message_id
        never advances past any message_id currently in backfill_queue with
        status='pending'.

        Returns (added:int, status:str, more:bool). `more` is True iff the
        batch was capped by batch_size (implying more messages likely remain).
        """
        # Shutdown already requested: start no Telegram call and no DB query.
        if self._stop.is_set():
            log.info("backfill chat=%s skipped: shutdown in progress", chat_id)
            return (0, "stopped", False)
        if not self._is_active(chat_id):
            return (0, "inactive", False)
        entity = await self.client.get_entity(chat_id)
        limit = batch_size if batch_size is not None else self.batch_size
        count_total = 0
        stopped_on_limit = False
        stopped = False
        inactive = False
        last_id = None
        first_id = None
        # (Re-)read pending floor from DB.
        pending_floor = await self.storage.backfill_queue_min_pending(chat_id)
        self._pending_floor[chat_id] = pending_floor

        while True:
            # Never (re-)open the iterator once shutdown has been requested.
            if self._stop.is_set():
                stopped = True
                break
            if not self._is_active(chat_id):
                inactive = True
                break
            state = await self.storage.get_sync_state(chat_id)
            min_id = int(state["last_backfilled_message_id"] or 0) if state else 0
            first_id = state["first_message_id"] if state else None

            # If a pending entry blocks us, don't re-iterate past it indefinitely.
            # We still iterate from checkpoint; the per-advance cap enforces the floor.
            log.info("backfill start chat=%s from>%s limit=%s pending_floor=%s",
                     chat_id, min_id, limit, pending_floor)
            await self.storage.coordinator_batch_start(chat_id, min_id)
            run_count = 0
            last_id = min_id
            try:
                async for msg in self.client.iter_messages(entity, min_id=min_id, reverse=True):
                    # Checked before starting the next message: a message that
                    # was not started is not processed and not checkpointed.
                    if self._stop.is_set():
                        stopped = True
                        break
                    if not self._is_active(chat_id):
                        inactive = True
                        break
                    ok = await self._process_backfill_message(chat_id, msg)
                    if ok:
                        last_id = max(last_id, msg.id)
                        count_total += 1
                        run_count += 1
                        if first_id is None or msg.id < first_id:
                            first_id = msg.id
                        await self._advance_checkpoint(chat_id, last_id, first_id)
                        # v6: progress watchdog signal every message
                        if run_count % 25 == 0:
                            await self.storage.coordinator_batch_progress(
                                chat_id, last_id)
                    if run_count and run_count % 100 == 0:
                        if await self._sleep_or_stop(0.3):
                            stopped = True
                            break
                    if limit and count_total >= limit:
                        stopped_on_limit = True
                        break
                # natural end of iterator (or stop / limit break above)
                break
            except _TargetInactive:
                inactive = True
                break
            except errors.FloodWaitError as e:
                wait = min(int(e.seconds) + 2, 3600)
                log.warning("backfill FloodWait %ss on %s; resuming after sleep", wait, chat_id)
                # Checkpoint whatever we accrued, then sleep and re-open the iterator.
                await self._advance_checkpoint(chat_id, last_id, first_id)
                if await self._sleep_or_stop(wait):
                    stopped = True
                    break
                continue
            except (errors.RPCError, errors.ServerError, ConnectionError, OSError) as e:
                log.warning("backfill transient %s on %s; retry in 5s", type(e).__name__, chat_id)
                await self._advance_checkpoint(chat_id, last_id, first_id)
                if await self._sleep_or_stop(5 + random.uniform(0, 2)):
                    stopped = True
                    break
                continue

            if stopped_on_limit:
                break

        if inactive:
            # Target left the active set (dynamic engine): every processed
            # message already advanced its checkpoint; nothing is classified
            # and nothing more is written for this chat.
            log.info("backfill chat=%s stopped: target inactive; processed=%s",
                     chat_id, count_total)
            return (count_total, "inactive", False)

        if stopped or self._stop.is_set():
            # Shutdown: keep the checkpoint of the last successfully processed
            # message, do NOT classify the batch (never 'complete'), do NOT
            # touch backfill_status/target_state, and let the coordinator exit.
            if last_id is not None:
                await self._advance_checkpoint(chat_id, last_id, first_id)
            log.info("backfill chat=%s stopped by shutdown; processed=%s, checkpoint kept",
                     chat_id, count_total)
            return (count_total, "stopped", False)

        await self._advance_checkpoint(chat_id, last_id, first_id)
        pending_floor = await self.storage.backfill_queue_min_pending(chat_id)
        await self._advance_checkpoint(chat_id, last_id, first_id)
        # TRUTHFUL completion: only 'complete' when iteration ended naturally
        # AND no unresolved pending/dead queue entries remain.
        stats = await self.storage.backfill_queue_stats(chat_id)
        if stopped_on_limit:
            status = "incomplete"
        elif pending_floor is not None:
            status = "blocked"
        elif stats.get("dead_unacknowledged", 0) > 0:
            status = "complete_with_errors"
        else:
            status = "complete"
        # v5: `incomplete` is reclassified as `syncing` when more batches
        # are likely (batch size cap hit AND no pending/dead issues).
        if status == "incomplete":
            status = "syncing"
        await self.storage.set_backfill_status(
            chat_id, status, mark_complete=(status == "complete"))
        await self.storage.target_update_counts(
            chat_id, pending=stats.get("pending", 0),
            dead=stats.get("dead_unacknowledged", 0))
        await self.storage.record_batch_progress(chat_id)
        self.batches_this_pid[chat_id] = self.batches_this_pid.get(chat_id, 0) + 1
        # Propagate truthful status to target_state so health sees it.
        target_label = str(chat_id)
        try:
            async with self.storage.pool.acquire() as con:
                cur = await con.fetchval(
                    "SELECT target FROM target_state WHERE chat_id=$1",
                    chat_id)
                if cur:
                    target_label = cur
        except Exception:
            pass
        await self.storage.target_upsert(chat_id, target_label, status)
        more = stopped_on_limit and pending_floor is None
        log.info("backfill batch chat=%s added=%s status=%s more=%s pid_batches=%d",
                 chat_id, count_total, status, more,
                 self.batches_this_pid[chat_id])
        return (count_total, status, more)

    async def _process_backfill_message(self, chat_id: int, msg) -> bool:
        """Process a single message with bounded retry. On final failure, record in
        backfill_queue and return False so the caller does not advance checkpoint
        past this id."""
        last_err: Exception | None = None
        for attempt in range(1, self._per_msg_retry + 1):
            try:
                await self._handle_message(msg)
                # Clear any prior dead-letter row.
                await self.storage.backfill_queue_mark_done(chat_id, msg.id)
                return True
            except errors.FloodWaitError:
                raise
            except Exception as e:
                if is_inactive_write(e):
                    raise _TargetInactive() from None
                last_err = e
                await asyncio.sleep(min(10, 0.5 * (2 ** (attempt - 1))))
        # Retries exhausted: dead-letter and keep going; checkpoint will be
        # capped by this pending floor.
        err_s = f"{type(last_err).__name__}: {last_err}"
        log.warning("backfill msg %s/%s failed after retries: %s", chat_id, msg.id, err_s)
        next_retry = datetime.now(timezone.utc) + timedelta(minutes=5)
        await self.storage.backfill_queue_upsert(
            chat_id, msg.id, err_s, attempts=self._per_msg_retry,
            next_retry_at=next_retry, status="pending")
        await self.storage.record_error("backfill_msg", chat_id, msg.id, None, last_err)
        # Update in-memory floor.
        pf = self._pending_floor.get(chat_id)
        self._pending_floor[chat_id] = msg.id if pf is None else min(pf, msg.id)
        return False

    async def _advance_checkpoint(self, chat_id: int, candidate_last: int,
                                  first_id: int | None):
        """Advance last_backfilled_message_id but never past the pending floor - 1."""
        floor = self._pending_floor.get(chat_id)
        target = candidate_last
        if floor is not None:
            target = min(target, floor - 1)
        if target <= 0:
            return
        if self._dynamic:
            await self.storage.set_backfill_checkpoint_if_enabled(chat_id, target, first_id)
            return
        await self.storage.set_backfill_checkpoint(chat_id, target, first_id)

    async def retry_dead_letter(self, chat_id: int):
        """Walk the backfill_queue and attempt to process pending items again."""
        if not self._is_active(chat_id):
            return
        entity = await self.client.get_entity(chat_id)
        total = 0
        succeeded = 0
        while True:
            pending = await self.storage.backfill_queue_get_pending(chat_id, limit=50)
            if not pending:
                break
            if not self._is_active(chat_id):
                return
            for row in pending:
                if not self._is_active(chat_id):
                    return
                total += 1
                mid = row["message_id"]
                try:
                    msg = await self.client.get_messages(entity, ids=mid)
                except errors.FloodWaitError as e:
                    await asyncio.sleep(min(int(e.seconds) + 2, 3600))
                    continue
                except Exception as e:
                    log.warning("dlq fetch (%s,%s) err: %s", chat_id, mid, e)
                    await self.storage.backfill_queue_upsert(
                        chat_id, mid, f"fetch: {e}"[:500],
                        attempts=row["attempts"] + 1,
                        next_retry_at=datetime.now(timezone.utc) + timedelta(minutes=10))
                    continue
                if msg is None:
                    # Source message gone from Telegram — stop retrying, mark dead.
                    await self.storage.backfill_queue_upsert(
                        chat_id, mid, "source message gone", attempts=row["attempts"] + 1,
                        next_retry_at=datetime.now(timezone.utc) + timedelta(days=365),
                        status="dead")
                    continue
                try:
                    await self._handle_message(msg)
                    await self.storage.backfill_queue_mark_done(chat_id, mid)
                    succeeded += 1
                except Exception as e:
                    if is_inactive_write(e):
                        return
                    log.warning("dlq handle (%s,%s) err: %s", chat_id, mid, e)
                    await self.storage.backfill_queue_upsert(
                        chat_id, mid, f"handle: {e}"[:500],
                        attempts=row["attempts"] + 1,
                        next_retry_at=datetime.now(timezone.utc) + timedelta(minutes=30),
                        status=("dead" if row["attempts"] + 1 >= 10 else "pending"))
            if len(pending) < 50:
                break  # no more

        # Recompute floor after DLQ pass.
        self._pending_floor[chat_id] = await self.storage.backfill_queue_min_pending(chat_id)
        # Recompute backfill_status now that the queue may have drained.
        stats = await self.storage.backfill_queue_stats(chat_id)
        state = await self.storage.get_sync_state(chat_id)
        current_status = state["backfill_status"] if state else "running"
        new_status = current_status
        if current_status in ("blocked", "complete_with_errors", "complete"):
            if self._pending_floor[chat_id] is None:
                new_status = ("complete" if stats.get("dead_unacknowledged", 0) == 0
                              else "complete_with_errors")
                await self.storage.set_backfill_status(
                    chat_id, new_status, mark_complete=(new_status == "complete"))
        await self.storage.target_update_counts(
            chat_id, pending=stats.get("pending", 0),
            dead=stats.get("dead_unacknowledged", 0))
        # Mirror onto target_state.
        try:
            async with self.storage.pool.acquire() as con:
                target_label = await con.fetchval(
                    "SELECT target FROM target_state WHERE chat_id=$1",
                    chat_id) or str(chat_id)
            await self.storage.target_upsert(chat_id, target_label, new_status)
        except Exception:
            pass
        if total:
            log.info("DLQ pass chat=%s tried=%s ok=%s floor=%s",
                     chat_id, total, succeeded, self._pending_floor[chat_id])

    # -- message handling ----------------------------------------------------
    async def _handle_message(self, msg):
        if msg is None:
            return
        cid = canonical(msg.peer_id)

        sender_user_id = None
        sender_chat_id = None
        sender_entity = None
        try:
            sender_entity = await msg.get_sender()
        except Exception:
            sender_entity = None

        if msg.from_id is not None:
            if is_user_peer(msg.from_id):
                sender_user_id = msg.from_id.user_id
            elif is_channel_peer(msg.from_id):
                sender_chat_id = _peer_id_signed(msg.from_id)
        elif msg.sender_id and msg.sender_id > 0:
            sender_user_id = msg.sender_id
        elif msg.sender_id and msg.sender_id < 0:
            sender_chat_id = msg.sender_id

        # v7: persist the sender entity immediately (even if `min`), record
        # an observation, then spawn full-profile enrichment if needed.
        if sender_user_id and sender_entity is not None and hasattr(sender_entity, "id"):
            await self._store_sender_entity(
                sender_entity, cid, msg.id, source_type="message_sender")
        elif sender_user_id:
            # No cached entity yet (common for historical backfill). Still
            # record the observation so we can prove per-chat coverage, and
            # queue enrichment via the usual path.
            try:
                await self.storage.record_user_observation(
                    user_id=sender_user_id, chat_id=cid, message_id=msg.id,
                    source_type="message_sender", entity_quality="min",
                    username_at_time=None, display_name_at_time=None,
                    access_hash_present=False)
            except Exception:
                pass
        if sender_user_id:
            self._spawn_bg(
                self._safe_ensure_user(sender_user_id, sender=sender_entity,
                                       hint_chat=cid, hint_msg=msg.id),
                name=f"user:{sender_user_id}", chat_id=cid)

        # Descriptor first (pure function): we need media_type/file_id in the
        # messages row BEFORE anything else touches message_media (FK).
        media_type = None
        media_file_id = None
        media_path = None
        media_desc = None
        if msg.media is not None:
            media_desc = media_descriptor_or_none(msg)
            if media_desc:
                media_type = media_desc["media_type"]
                # For non-downloadable we scope the file_id below after upsert.
                if media_desc.get("is_downloadable", True):
                    media_file_id = media_desc["file_id"]

        fwd_user, fwd_chat, fwd_name, fwd_msg_id, fwd_date, fwd_post = (None,) * 6
        if isinstance(msg.fwd_from, MessageFwdHeader):
            fwd_date = msg.fwd_from.date
            fwd_name = msg.fwd_from.from_name
            fwd_msg_id = msg.fwd_from.channel_post
            fwd_post = msg.fwd_from.post_author
            if msg.fwd_from.from_id is not None:
                if is_user_peer(msg.fwd_from.from_id):
                    fwd_user = msg.fwd_from.from_id.user_id
                elif is_channel_peer(msg.fwd_from.from_id):
                    fwd_chat = _peer_id_signed(msg.fwd_from.from_id)

        reply_mid = None
        reply_top = None
        reply_peer = None
        thread_id = None
        if msg.reply_to:
            reply_mid = getattr(msg.reply_to, "reply_to_msg_id", None)
            reply_top = getattr(msg.reply_to, "reply_to_top_id", None)
            rp = getattr(msg.reply_to, "reply_to_peer_id", None)
            if rp is not None:
                reply_peer = _peer_id_signed(rp)
            if getattr(msg.reply_to, "forum_topic", False):
                thread_id = reply_top or reply_mid
            elif reply_top:
                thread_id = reply_top

        is_service = isinstance(msg, MessageService)
        service_action = type(msg.action).__name__ if is_service else None
        service_payload = None
        if is_service:
            try:
                service_payload = msg.action.to_dict()
            except Exception:
                service_payload = {"_": service_action}

        geo = None
        contact = None
        poll = None
        webpage = None
        if isinstance(msg.media, (MessageMediaGeo, MessageMediaGeoLive)):
            try:
                geo = msg.media.to_dict()
            except Exception:
                geo = {"_": type(msg.media).__name__}
        elif isinstance(msg.media, MessageMediaContact):
            try:
                contact = msg.media.to_dict()
            except Exception:
                contact = {"_": "contact"}
        elif isinstance(msg.media, MessageMediaPoll):
            try:
                poll = msg.media.to_dict()
            except Exception:
                poll = {"_": "poll"}
        elif isinstance(msg.media, MessageMediaWebPage):
            try:
                webpage = msg.media.to_dict()
            except Exception:
                webpage = {"_": "webpage"}

        total_reactions, reaction_rows = reactions_total_and_rows(msg.reactions)

        try:
            raw = msg.to_dict()
        except Exception:
            raw = {"_": "unserializable"}

        await self.storage.upsert_message({
            "chat_id": cid,
            "message_id": msg.id,
            "sender_user_id": sender_user_id,
            "sender_chat_id": sender_chat_id,
            "date": msg.date,
            "edit_date": msg.edit_date,
            "reply_to_message_id": reply_mid,
            "reply_to_top_id": reply_top,
            "reply_to_peer_id": reply_peer,
            "thread_id": thread_id,
            "post_author": getattr(msg, "post_author", None),
            "text": msg.message if hasattr(msg, "message") else None,
            "entities": [_ent_to_dict(e) for e in (msg.entities or [])] if hasattr(msg, "entities") else [],
            "media_type": media_type,
            "media_path": media_path,
            "media_file_id": media_file_id,
            "media_group_id": getattr(msg, "grouped_id", None),
            "fwd_from_user_id": fwd_user,
            "fwd_from_chat_id": fwd_chat,
            "fwd_from_name": fwd_name,
            "fwd_from_msg_id": fwd_msg_id,
            "fwd_date": fwd_date,
            "fwd_channel_post": fwd_post,
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
            "replies_count": (msg.replies.replies if getattr(msg, "replies", None) else None),
            "reactions_total": total_reactions,
            "is_service": is_service,
            "service_action": service_action,
            "service_payload": service_payload,
            "geo": geo, "contact": contact, "poll": poll, "webpage": webpage,
            "raw": raw,
        })
        if reaction_rows:
            await self.storage.replace_reactions(cid, msg.id, reaction_rows)
            if self._fetch_reaction_authors:
                self._spawn_bg(self._safe_fetch_reaction_authors(cid, msg.id),
                               name=f"react_auth:{cid}:{msg.id}", chat_id=cid)

        # Media enqueue AFTER message upsert so FK(message_media -> messages)
        # has a parent row to point at.
        if media_desc is not None and self.options.get("download_media", True):
            try:
                res = await self.media_q.enqueue(cid, msg)
                if res:
                    _, mfid, mpath = res
                    # For metadata_only (scoped id) or already-cached file,
                    # record the final file_id on the message row.
                    if mfid and mfid != media_file_id:
                        async with self.storage.pool.acquire() as con:
                            await con.execute(
                                "UPDATE messages SET media_file_id=$3, "
                                "media_path=COALESCE(NULLIF($4,''),media_path) "
                                "WHERE chat_id=$1 AND message_id=$2",
                                cid, msg.id, mfid, mpath or "")
                        await self.storage.link_message_media(cid, msg.id, mfid)
            except Exception as e:
                log.warning("enqueue media (%s,%s) soft-fail: %s",
                            cid, msg.id, e)

    # -- live reactions ------------------------------------------------------
    async def _handle_reactions_update(self, update, cid: int):
        msg_id = getattr(update, "msg_id", None)
        reactions = getattr(update, "reactions", None)
        if msg_id is None or reactions is None:
            return
        total, rows = reactions_total_and_rows(reactions)
        await self.storage.replace_reactions(cid, msg_id, rows)
        try:
            async with self.storage.pool.acquire() as con:
                await con.execute(
                    "UPDATE messages SET reactions_total=$3 WHERE chat_id=$1 AND message_id=$2",
                    cid, msg_id, total)
        except Exception:
            pass
        if self._fetch_reaction_authors:
            self._spawn_bg(self._safe_fetch_reaction_authors(cid, msg_id),
                           name=f"react_auth_live:{cid}:{msg_id}", chat_id=cid)

    async def _safe_fetch_reaction_authors(self, chat_id: int, msg_id: int):
        async with self._react_authors_sema:
            # Soft rate limit: stagger to avoid tripping admin-only endpoints hard.
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                entity = await self.client.get_input_entity(chat_id)
            except Exception as e:
                log.info("cannot resolve entity for reaction authors %s: %s", chat_id, e)
                return
            rows: list[dict] = []
            offset = ""
            fetched = 0
            is_complete = False
            try:
                while fetched < self._reaction_authors_limit:
                    try:
                        res = await self.client(GetMessageReactionsListRequest(
                            peer=entity, id=msg_id, reaction=None, offset=offset,
                            limit=min(100, self._reaction_authors_limit - fetched)))
                    except errors.FloodWaitError as e:
                        await asyncio.sleep(min(int(e.seconds) + 2, 3600))
                        continue
                    except errors.ChatAdminRequiredError:
                        log.info("reaction authors require admin in %s", chat_id)
                        return
                    except (errors.MessageIdInvalidError,
                            errors.MsgIdInvalidError):
                        return
                    for r in res.reactions:
                        pid, kind = peer_id_and_kind(r.peer_id)
                        rk = reaction_key(r.reaction)
                        if pid is None or rk is None:
                            continue
                        rows.append({
                            "reaction": rk, "peer_id": pid, "peer_kind": kind,
                            "reacted_at": getattr(r, "date", None),
                        })
                    fetched += len(res.reactions)
                    if not res.next_offset:
                        is_complete = True
                        break
                    offset = res.next_offset
                await self.storage.replace_reaction_authors(
                    chat_id, msg_id, rows, is_complete=is_complete)
            except Exception as e:
                log.info("reaction authors (%s,%s) soft-fail: %s", chat_id, msg_id, e)

    # -- chat actions / memberships ------------------------------------------
    async def _handle_action(self, ev):
        payload = {}
        try:
            payload = ev.action_message.to_dict() if ev.action_message else {}
        except Exception:
            pass
        action = type(ev.action_message.action).__name__ if ev.action_message else "unknown"
        try:
            cid = canonical(ev.chat_id)
        except Exception:
            cid = ev.chat_id
        actor = getattr(ev, "user_id", None)
        await self.storage.record_action(cid, ev.action_message.id if ev.action_message else None,
                                         actor, action, payload)
        try:
            users_added = getattr(ev, "users", None)
            if users_added:
                for u in (users_added if isinstance(users_added, (list, tuple)) else [users_added]):
                    uid = u if isinstance(u, int) else getattr(u, "id", None)
                    if uid:
                        await self.storage.upsert_membership(
                            cid, uid, "member",
                            joined_at=datetime.now(timezone.utc),
                            source="chat_action")
            if getattr(ev, "user_left", False) or getattr(ev, "user_kicked", False):
                uid = actor
                if uid:
                    await self.storage.upsert_membership(
                        cid, uid, "left" if getattr(ev, "user_left", False) else "kicked",
                        left_at=datetime.now(timezone.utc),
                        source="chat_action")
        except Exception as e:
            log.info("chat action membership update soft-fail: %s", e)

    async def sync_participants(self, chat_id: int):
        """Resumable participant sync for groups/supergroups.

        Uses raw `GetParticipantsRequest` with explicit offset/limit, saves
        `participants_offset` after each successful page, and resumes from the
        saved offset on restart. Honors admin-required and FloodWait.
        """
        if not self._participants_sync:
            return
        state = await self.storage.get_sync_state(chat_id)
        if state and state["participants_synced"] and state["participants_is_complete"]:
            return
        try:
            entity = await self.client.get_entity(chat_id)
        except Exception as e:
            await self.storage.set_participants_sync(
                chat_id, 0, False, f"resolve: {e}"[:200])
            return
        if not isinstance(entity, (Channel, Chat)):
            await self.storage.set_participants_sync(
                chat_id, 0, True, "not_a_group")
            return
        if isinstance(entity, Chat):
            # Basic groups: telethon.iter_participants is fine and small.
            return await self._sync_participants_basic(chat_id, entity)
        # Channel / supergroup: raw API pagination.
        return await self._sync_participants_channel(
            chat_id, entity, resume_offset=(state["participants_offset"] or 0))

    async def _sync_participants_basic(self, chat_id: int, entity):
        count = 0
        limitation: str | None = None
        is_complete = False
        try:
            async for p in self.client.iter_participants(entity):
                uid = getattr(p, "id", None)
                if uid is None:
                    continue
                await self.storage.upsert_membership(
                    chat_id, uid, self._participant_role(p),
                    source="initial_sync",
                    is_bot=getattr(p, "bot", None))
                count += 1
            is_complete = True
        except errors.ChatAdminRequiredError:
            limitation = "admin_required"
        except errors.FloodWaitError as e:
            limitation = f"flood_wait:{int(e.seconds)}"
        except Exception as e:
            limitation = f"error:{type(e).__name__}"
        await self.storage.set_participants_sync(
            chat_id, count, is_complete, limitation)

    async def _sync_participants_channel(self, chat_id: int, entity,
                                         resume_offset: int = 0):
        from telethon.tl.functions.channels import GetParticipantsRequest
        from telethon.tl.types import ChannelParticipantsRecent
        page_size = int(self.options.get("participants_page_size", 200))
        offset = int(resume_offset)
        total_available: int | None = None
        limitation: str | None = None
        is_complete = False
        synced = 0
        try:
            while True:
                try:
                    res = await self.client(GetParticipantsRequest(
                        channel=entity, filter=ChannelParticipantsRecent(),
                        offset=offset, limit=page_size, hash=0))
                except errors.FloodWaitError as e:
                    limitation = f"flood_wait:{int(e.seconds)}"
                    break
                except errors.ChatAdminRequiredError:
                    limitation = "admin_required"
                    break
                except errors.ChannelPrivateError:
                    limitation = "channel_private"
                    break
                if total_available is None:
                    total_available = getattr(res, "count", None)
                users_map = {u.id: u for u in res.users}
                page_participants = res.participants
                if not page_participants:
                    is_complete = True
                    break
                for p in page_participants:
                    uid = getattr(p, "user_id", None) or getattr(p, "peer", None)
                    if uid is None:
                        continue
                    if not isinstance(uid, int):
                        uid = getattr(uid, "user_id", None)
                    if uid is None:
                        continue
                    u = users_map.get(uid)
                    await self.storage.upsert_membership(
                        chat_id, uid, self._participant_role(p),
                        source="initial_sync",
                        is_bot=getattr(u, "bot", None) if u else None)
                    synced += 1
                offset += len(page_participants)
                # Checkpoint after every successful page.
                await self.storage.set_participants_sync(
                    chat_id, offset, False, None)
                if total_available is not None and offset >= total_available:
                    is_complete = True
                    break
        except Exception as e:
            limitation = f"error:{type(e).__name__}"
            log.info("participants sync %s soft-fail: %s", chat_id, e)
        await self.storage.set_participants_sync(
            chat_id, offset, is_complete, limitation)
        log.info("participants sync chat=%s synced=%s offset=%s "
                 "total=%s complete=%s limit=%s",
                 chat_id, synced, offset, total_available, is_complete, limitation)

    def _participant_role(self, p):
        try:
            pp = getattr(p, "participant", p)
            if isinstance(pp, ChannelParticipantCreator):
                return "creator"
            if isinstance(pp, ChannelParticipantAdmin):
                return "admin"
            if isinstance(pp, ChannelParticipantBanned):
                return "banned"
            if isinstance(pp, ChannelParticipantLeft):
                return "left"
        except Exception:
            pass
        return "member"

    # -- v7: entity ingestion from messages ---------------------------------
    async def _store_sender_entity(self, ent, chat_id: int, msg_id: int,
                                   source_type: str):
        """Save a Telethon User entity (even `min`) so we later have the
        access_hash required to build InputPeerUser, plus a per-chat
        observation row."""
        from telethon.tl.types import User as TUser
        if not isinstance(ent, TUser):
            return
        is_min = bool(getattr(ent, "min", False))
        quality = "min" if is_min else "normal"
        access_hash = getattr(ent, "access_hash", None)
        data = {
            "id": ent.id,
            "access_hash": access_hash if access_hash else None,
            "username": getattr(ent, "username", None),
            "first_name": getattr(ent, "first_name", None),
            "last_name": getattr(ent, "last_name", None),
            "phone": getattr(ent, "phone", None),
            "is_bot": getattr(ent, "bot", None),
            "is_premium": getattr(ent, "premium", None),
            "is_scam": getattr(ent, "scam", None),
            "is_fake": getattr(ent, "fake", None),
            "is_verified": getattr(ent, "verified", None),
            "is_deleted": getattr(ent, "deleted", None),
            "is_contact": getattr(ent, "contact", None),
            "is_mutual_contact": getattr(ent, "mutual_contact", None),
            "is_close_friend": getattr(ent, "close_friend", None),
            "is_min": is_min,
            "lang_code": getattr(ent, "lang_code", None),
            "photo_dc": getattr(getattr(ent, "photo", None), "dc_id", None),
            "photo_file_id": getattr(getattr(ent, "photo", None), "photo_id", None),
            "entity_source": source_type,
            "entity_quality": quality,
            "source_chat_id": chat_id,
            "source_message_id": msg_id,
        }
        try:
            data["raw"] = ent.to_dict()
        except Exception:
            data["raw"] = None
        try:
            await self.storage.upsert_user_from_entity(data)
        except Exception as e:
            log.info("store_sender_entity soft-fail uid=%s: %s", ent.id, e)
            return
        # Observation row (no text, no raw bio/username values beyond what
        # already lives in users)
        try:
            display = " ".join(x for x in [data.get("first_name"),
                                           data.get("last_name")] if x) or None
            await self.storage.record_user_observation(
                user_id=ent.id, chat_id=chat_id, message_id=msg_id,
                source_type=source_type, entity_quality=quality,
                username_at_time=data.get("username"),
                display_name_at_time=display,
                access_hash_present=bool(access_hash),
                raw_meta=None)
        except Exception:
            pass

    # -- users / avatars -----------------------------------------------------
    async def _safe_ensure_user(self, user_id: int, sender=None,
                                hint_chat: int | None = None,
                                hint_msg: int | None = None):
        # Dedup: skip if already in-flight or already queued.
        async with self._user_profile_lock:
            if user_id in self._user_profile_inflight:
                return
            self._user_profile_inflight.add(user_id)
        try:
            await self._ensure_user(user_id, sender=sender)
        except ValueError as e:
            # Telethon throws ValueError("Could not find the input entity ...")
            # Record once in queue, dedupe — don't spam errors.log.
            await self.storage.user_profile_queue_upsert(
                user_id, f"input_entity: {e}"[:300],
                hint_chat_id=hint_chat, hint_message_id=hint_msg)
        except Exception as e:
            await self.storage.user_profile_queue_upsert(
                user_id, f"{type(e).__name__}: {e}"[:300],
                hint_chat_id=hint_chat, hint_message_id=hint_msg)
        finally:
            async with self._user_profile_lock:
                self._user_profile_inflight.discard(user_id)

    async def _ensure_user(self, user_id: int, sender=None):
        lock = self._user_locks.setdefault(user_id, asyncio.Lock())
        if lock.locked():
            return
        async with lock:
            needs = await self.storage.user_needs_refresh(user_id, self._user_profile_ttl)
            if not needs:
                return
            # v7: prefer the sender entity (carries access_hash), otherwise
            # try to build InputPeerUser from the access_hash we already
            # persisted, otherwise fall back to Telethon's own resolve.
            peer = None
            if sender is not None:
                try:
                    peer = await self.client.get_input_entity(sender)
                except Exception:
                    peer = None
            if peer is None:
                ah = await self.storage.user_access_hash(user_id)
                if ah is not None:
                    from telethon.tl.types import InputPeerUser
                    peer = InputPeerUser(user_id=user_id, access_hash=ah)
            if peer is None:
                try:
                    peer = await self.client.get_input_entity(user_id)
                except Exception:
                    peer = None
            if peer is None:
                # No InputPeer → cannot call GetFullUser. Queue + bail.
                raise ValueError(f"no InputPeer for {user_id}")
            try:
                full = await self.client(GetFullUserRequest(peer))
            except (errors.UserIdInvalidError, errors.PeerIdInvalidError):
                return
            except errors.FloodWaitError as e:
                await asyncio.sleep(min(int(e.seconds) + 2, 3600))
                return
            user = next((u for u in full.users if u.id == user_id), None)
            if user is None:
                return
            bio = full.full_user.about if full.full_user else None
            photo_path, photo_fid = await self._download_user_photo(user)
            try:
                raw = user.to_dict()
            except Exception:
                raw = {}
            await self.storage.upsert_user_from_entity({
                "id": user.id,
                "access_hash": getattr(user, "access_hash", None),
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "phone": user.phone,
                "bio": bio,
                "is_bot": user.bot,
                "is_premium": getattr(user, "premium", False),
                "is_scam": getattr(user, "scam", False),
                "is_fake": getattr(user, "fake", False),
                "is_verified": getattr(user, "verified", False),
                "is_deleted": getattr(user, "deleted", False),
                "is_contact": getattr(user, "contact", None),
                "is_mutual_contact": getattr(user, "mutual_contact", None),
                "is_close_friend": getattr(user, "close_friend", None),
                "is_min": bool(getattr(user, "min", False)),
                "lang_code": getattr(user, "lang_code", None),
                "photo_path": photo_path,
                "photo_dc": getattr(getattr(user, "photo", None), "dc_id", None),
                "photo_file_id": photo_fid,
                "entity_source": "full",
                "entity_quality": "full",
                "raw": raw,
                "full_fetched_at": datetime.now(timezone.utc),
            })

    async def _download_user_photo(self, user) -> tuple[str | None, int | None]:
        """Download the current avatar if we have not yet seen this photo_file_id.

        Returns (local_path, photo_file_id). Preserves history in avatar_history;
        old files are not deleted.
        """
        photo = getattr(user, "photo", None)
        photo_fid = getattr(photo, "photo_id", None) if photo is not None else None
        if photo is None or photo_fid is None:
            return None, None
        existing = await self.storage.avatar_lookup(user.id, photo_fid)
        if existing:
            return existing, photo_fid
        d = self.media_q.base / "avatars"
        d.mkdir(parents=True, exist_ok=True)
        prefix = d / f"{user.id}_{photo_fid}"
        try:
            path = await self.client.download_profile_photo(user, file=str(prefix))
        except errors.FloodWaitError:
            raise
        except Exception:
            return None, photo_fid
        if path:
            rel = str(Path(path).relative_to(self.media_q.base))
            await self.storage.avatar_record(user.id, photo_fid, rel)
            return rel, photo_fid
        return None, photo_fid

    async def _save_chat(self, entity):
        rec = {"id": canonical(entity), "raw": _to_dict_safe(entity)}
        if isinstance(entity, Channel):
            rec["type"] = "channel" if entity.broadcast else "supergroup"
            rec["title"] = entity.title
            rec["username"] = entity.username
            rec["is_forum"] = getattr(entity, "forum", False)
            rec["is_broadcast"] = bool(entity.broadcast)
            try:
                full = await self.client(GetFullChannelRequest(entity))
                rec["about"] = full.full_chat.about
                rec["participants_count"] = full.full_chat.participants_count
                lc = full.full_chat.linked_chat_id
                if lc:
                    for c in full.chats:
                        if c.id == lc:
                            rec["linked_chat_id"] = canonical(c)
                            break
                    else:
                        rec["linked_chat_id"] = int(f"-100{lc}")
            except Exception as e:
                log.info("full channel fetch failed for %s: %s", entity.id, e)
        elif isinstance(entity, Chat):
            rec["type"] = "group"
            rec["title"] = entity.title
            rec["is_broadcast"] = False
        elif isinstance(entity, User):
            rec["type"] = "user"
            rec["title"] = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            rec["username"] = entity.username
            rec["is_broadcast"] = False
        await self.storage.upsert_chat(rec)

    async def _get_linked(self, channel):
        try:
            full = await self.client(GetFullChannelRequest(channel))
            lc = full.full_chat.linked_chat_id
            if lc:
                for c in full.chats:
                    if c.id == lc:
                        return c
        except Exception:
            pass
        return None

    async def catch_up(self):
        try:
            await self.client.catch_up()
        except Exception as e:
            log.warning("catch_up failed: %s", e)

    async def _resolve_via_invite_or_dialogs(self, target: str):
        """Fallback for private invite links: `t.me/+hash` or `t.me/joinchat/h`.

        Strategy:
          1. If the link carries an invite hash, call CheckChatInviteRequest;
             when the user is a member Telegram returns `ChatInviteAlready`
             which wraps the Chat.
          2. Otherwise scan iter_dialogs() and match by a trailing identifier.
        """
        import re
        from telethon.tl.functions.messages import CheckChatInviteRequest
        from telethon.tl.types import ChatInviteAlready, ChatInvitePeek

        m = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", target or "")
        if m:
            invite_hash = m.group(1)
            try:
                res = await self.client(CheckChatInviteRequest(hash=invite_hash))
                if isinstance(res, (ChatInviteAlready, ChatInvitePeek)):
                    return res.chat
            except Exception as e:
                log.info("CheckChatInviteRequest(%s) failed: %s", invite_hash, e)
        # Final fallback: find in dialogs by matching title substring from URL
        try:
            async for d in self.client.iter_dialogs():
                if d.entity and target and str(getattr(d.entity, "id", "")) in target:
                    return d.entity
        except Exception:
            pass
        return None

    # -- background tasks ----------------------------------------------------
    def _spawn_bg(self, coro, name: str, chat_id: int | None = None):
        async def _runner():
            async with self._bg_sema:
                try:
                    await coro
                except asyncio.CancelledError:
                    # Expected on chat disable / shutdown.
                    raise
                except Exception as e:
                    if is_inactive_write(e):
                        log.debug("bg %s: target %s disabled during write", name, chat_id)
                        return
                    log.warning("bg %s failed: %s", name, e)
                    try:
                        await self.storage.record_error("bg_" + name, chat_id, None, None, e)
                    except Exception:
                        pass
        t = asyncio.create_task(_runner(), name=name)
        self._bg_tasks.add(t)
        t.add_done_callback(self._bg_tasks.discard)
        if chat_id is not None and self._bg_spawn_hook is not None:
            self._bg_spawn_hook(chat_id, t)
        return t


def media_descriptor_or_none(msg):
    from .media_queue import media_descriptor
    return media_descriptor(msg)
