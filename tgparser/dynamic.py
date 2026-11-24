"""Dynamic collection engine (schema v11 + deployment gate on).

Runs inside the single run.py process, on the same TelegramClient, Storage,
Collector and MediaQueue as the legacy engine. The legacy engine is selected
whenever schema < 11 or the gate is off; the choice is made once at startup.

Ownership: every task started here lives in self._tasks or self._chat_tasks
and is cancelled and awaited by stop(). Telethon handlers are registered once,
without a static `chats=` filter, and consult both the readiness barrier and
the per-chat lease registry before any write.

Readiness barrier:
    _accepting is cleared until start() finishes. Handlers return early while
    it is clear. The barrier is set ONLY after runtime_state.dyn_engine=TRUE
    is confirmed by the database. stop() clears it immediately so no new live
    event enters the collector after shutdown begins.

Per-chat lease registry (ChatLeases):
    Every operation attributable to a chat — live write, backfill batch,
    participant sync, coordinator round, media download, chat-sourced bg
    task — takes a short lease on the chat. Disable closes the lease: new
    leases are rejected, tracked cancellable tasks are cancelled, inflight
    operations are awaited with a bounded timeout. Only after the lease is
    fully drained does the reconciler acquire the DB exclusive advisory lock
    and COMMIT enabled=FALSE; COMMIT is the ACK. If the drain times out,
    enabled stays TRUE, applied_version is not advanced, reconcile_state is
    recorded as 'error', and the next reconcile retries.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import datetime, timedelta, timezone

import asyncpg
from telethon import errors, events

from .collector import _TargetInactive, is_inactive_write
from .ids import canonical

log = logging.getLogger("tgparser.dynamic")

_TRANSIENT = (errors.FloodWaitError, errors.ServerError, errors.RpcCallFailError,
              ConnectionError, OSError, asyncio.TimeoutError, TimeoutError)


class DynamicStartupError(Exception):
    pass


def classify_entity(entity) -> str:
    """Telethon entity -> discovered_dialogs.category."""
    from telethon.tl.types import Channel, ChannelForbidden, Chat, ChatForbidden, User
    if isinstance(entity, (ChatForbidden, ChannelForbidden)):
        return "forbidden"
    if isinstance(entity, Channel):
        if getattr(entity, "broadcast", False):
            return "channel"
        if getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False):
            return "megagroup"
        return "unknown"
    if isinstance(entity, Chat):
        return "group"
    if isinstance(entity, User):
        if getattr(entity, "is_self", False):
            return "saved"
        return "bot" if getattr(entity, "bot", False) else "private"
    return "unknown"


class _ChatSlot:
    __slots__ = ("gen", "active", "closing", "inflight", "zero", "tasks")

    def __init__(self):
        self.gen = 0
        self.active = False
        self.closing = False
        self.inflight = 0
        self.zero = asyncio.Event()
        self.zero.set()
        self.tasks: set[asyncio.Task] = set()


class ChatLeases:
    """Per-chat runtime activity registry.

    All mutations are synchronous (no await between read and write), so a
    single-threaded event loop needs no asyncio.Lock. close() only yields
    while cancelling tracked tasks and while waiting for the inflight-zero
    event; concurrent open()/close() is serialised by the reconciler lock
    at the engine level.
    """

    def __init__(self):
        self._slots: dict[int, _ChatSlot] = {}

    def is_active(self, chat_id) -> bool:
        s = self._slots.get(chat_id)
        return s is not None and s.active and not s.closing

    def generation(self, chat_id) -> int | None:
        s = self._slots.get(chat_id)
        return s.gen if s and s.active and not s.closing else None

    def inflight(self, chat_id) -> int:
        s = self._slots.get(chat_id)
        return s.inflight if s is not None else 0

    def open(self, chat_id: int) -> int:
        s = self._slots.get(chat_id)
        if s is None:
            s = _ChatSlot()
            self._slots[chat_id] = s
        s.gen += 1
        s.active = True
        s.closing = False
        if s.inflight == 0:
            s.zero.set()
        return s.gen

    @contextlib.asynccontextmanager
    async def lease(self, chat_id: int, *, gen: int | None = None):
        """Acquire a short-lived lease on the chat. Raises _TargetInactive
        if the chat is not active or if `gen` does not match the current
        generation. The yielded value is the generation held."""
        s = self._slots.get(chat_id)
        if s is None or not s.active or s.closing:
            raise _TargetInactive(chat_id)
        if gen is not None and gen != s.gen:
            raise _TargetInactive(chat_id)
        this_gen = s.gen
        s.inflight += 1
        s.zero.clear()
        try:
            yield this_gen
        finally:
            s.inflight -= 1
            if s.inflight == 0:
                s.zero.set()

    def track(self, chat_id: int, task: asyncio.Task) -> bool:
        """Register a cancellable task that belongs to the chat's current
        generation. Returns False if the chat is not active (caller then
        cancels the task itself)."""
        s = self._slots.get(chat_id)
        if s is None or not s.active or s.closing:
            return False
        s.tasks.add(task)
        task.add_done_callback(s.tasks.discard)
        return True

    async def close(self, chat_id: int, timeout: float) -> bool:
        """Close the chat's generation and drain it.

        State machine:
          * active and/or closing are set here; they are NOT reset on a
            timeout. A subsequent close() with the SAME slot continues to
            wait for the same inflight generation to drain.
          * A single deadline `now + timeout` bounds the whole call:
            task-cancellation await AND the inflight-zero wait share it.
          * Tracked tasks that are still running are cancelled on every
            entry (idempotent via Task.cancel()). They stay in `s.tasks`
            until their done-callback removes them, so a timed-out close
            leaves them tracked for the next close to wait on.
          * Returns True iff all tracked tasks are done AND inflight == 0.
            After a True return, `closing` is cleared so a later `open()`
            can resurrect the slot with a fresh generation.
          * A slot that has never been opened (`s is None`) is already
            drained: returns True immediately.
          * A slot that is already fully drained (no active, no inflight,
            no running tasks) also returns True immediately, so repeated
            closes are idempotent."""
        s = self._slots.get(chat_id)
        if s is None:
            return True
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout)
        s.closing = True
        s.active = False

        # Cancel any tracked task that is still running. Idempotent.
        for t in list(s.tasks):
            if not t.done():
                t.cancel()

        # Wait for tracked tasks to finish, bounded by the remaining budget.
        # done-callbacks remove tasks from s.tasks; we never clear it here.
        while True:
            pending = [t for t in s.tasks if not t.done()]
            if not pending:
                break
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            # asyncio.wait never raises on timeout; it returns (done, pending).
            _done, still = await asyncio.wait(pending, timeout=remaining)
            if still:
                # One or more tasks did not respond within the budget.
                return False

        # Wait for synchronous lease holders (coordinator rounds, backfill
        # batches, media worker items) to release.
        if s.inflight > 0:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            try:
                await asyncio.wait_for(s.zero.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return False
        if s.inflight != 0 or any(not t.done() for t in s.tasks):
            return False

        s.closing = False
        return True


class DynamicEngine:
    def __init__(self, client, storage, collector, media_q,
                 yaml_targets: list[str] | None = None, options: dict | None = None):
        o = dict(options or {})
        self.client = client
        self.storage = storage
        self.collector = collector
        self.media_q = media_q
        self.yaml_targets = list(yaml_targets or [])
        self.poll_seconds = float(o.get("dynamic_poll_seconds", 15))
        self.discovery_poll_seconds = float(o.get("dynamic_discovery_poll_seconds", 5))
        self.discovery_page = int(o.get("dynamic_discovery_page", 100))
        self.yaml_poll_seconds = float(o.get("dynamic_yaml_poll_seconds", 60))
        self.yaml_batch = int(o.get("dynamic_yaml_batch", 5))
        self.yaml_max_attempts = int(o.get("dynamic_yaml_max_attempts", 10))
        self.ready_timeout = float(o.get("dynamic_ready_timeout_seconds", 30))
        self.listener_backoff_max = float(o.get("dynamic_listener_backoff_max", 30))
        self.disable_lock_timeout_ms = int(o.get("dynamic_disable_lock_timeout_ms", 30_000))
        self.disable_quiesce_timeout = float(
            o.get("dynamic_disable_quiesce_timeout_seconds", 30))

        self.leases = ChatLeases()
        self._active: frozenset[int] = frozenset()
        self._accepting = asyncio.Event()
        self._stopping = asyncio.Event()
        self._tasks: dict[str, asyncio.Task] = {}
        self._chat_tasks: set[asyncio.Task] = set()
        self._reconcile_event = asyncio.Event()
        self._reconcile_lock = asyncio.Lock()
        self._listener_ready = asyncio.Event()
        self._listener_con: asyncpg.Connection | None = None
        self._discovery_failures = 0
        self.state = "init"
        self.listener_connects = 0
        self.notifications = 0
        self.reconcile_count = 0
        self.last_reconcile: dict = {}
        self.handlers: list = []

    # --- active set / lease hooks --------------------------------------------
    def is_active(self, chat_id) -> bool:
        return chat_id in self._active and self.leases.is_active(chat_id)

    def active_ids(self) -> frozenset[int]:
        return self._active

    def _publish_active(self, new_ids) -> None:
        new = frozenset(int(x) for x in new_ids)
        if new != self._active:
            self._active = new                       # single atomic rebind
            self.collector._targets = sorted(new)
            self.collector.wake()

    # --- chat-attributed task registration -----------------------------------
    def _spawn_chat_task(self, cid: int, coro, name: str) -> asyncio.Task:
        """Spawn a chat-attributed task: tracked in the lease registry for
        cancellation on disable and in self._chat_tasks for engine shutdown."""
        t = asyncio.create_task(coro, name=name)
        self._chat_tasks.add(t)
        t.add_done_callback(self._chat_tasks.discard)
        if not self.leases.track(cid, t):
            t.cancel()
        return t

    def _chat_bg_hook(self, cid: int, task: asyncio.Task) -> None:
        """Collector hook: a background task spawned from a chat-attributed
        write path must cancel if the chat is no longer active."""
        if not self.leases.track(cid, task):
            task.cancel()

    # --- lifecycle -----------------------------------------------------------
    def _spawn(self, name: str, coro) -> asyncio.Task:
        t = asyncio.create_task(coro, name=name)
        self._tasks[name] = t
        return t

    async def start(self) -> None:
        try:
            for raw in self.yaml_targets:
                try:
                    await self.storage.yaml_import_upsert(str(raw))
                except ValueError:
                    log.warning("ignoring malformed YAML target entry")
            res = await self.reconcile(initial=True)
            if res["state"] == "error":
                raise DynamicStartupError(f"initial reconcile failed: {res['error']}")
            c = self.collector
            c._dynamic = True
            c._active_check = self.is_active
            c._chat_work_ctx = self.leases.lease
            c._bg_spawn_hook = self._chat_bg_hook
            self.media_q.active_ids = self.active_ids
            self.media_q.chat_lease = self.leases.lease
            c._targets = sorted(self._active)
            self._register_handlers()
            self._spawn("dyn-listener", self._listener_loop())
            self._spawn("dyn-reconciler", self._reconciler_loop())
            self._spawn("dyn-discovery", self._discovery_loop())
            self._spawn("dyn-yaml", self._yaml_loop())
            await c.start_coordinator()
            try:
                await asyncio.wait_for(self._listener_ready.wait(), self.ready_timeout)
            except asyncio.TimeoutError:
                raise DynamicStartupError("policy listener not ready")
            for name, t in self._tasks.items():
                if t.done():
                    raise DynamicStartupError(f"task {name} ended during startup")
            if c._coordinator_task is None or c._coordinator_task.done():
                raise DynamicStartupError("coordinator not running")
            # The DB flip happens AFTER everything is up but BEFORE handlers
            # start accepting work — so a live event during startup never
            # bypasses the write guard and never writes without a lease.
            await self.storage.set_runtime_dynamic_engine(True)
            self._accepting.set()
            self.state = "ready"
            log.info("dynamic engine ready: %d active target(s), policy v%s",
                     len(self._active), self.last_reconcile.get("version"))
        except BaseException:
            self._accepting.clear()
            self.state = "failed"
            await self.stop()
            try:
                await self.storage.set_runtime_dynamic_engine(False)
            except Exception:
                log.warning("could not reset dyn_engine after failed startup")
            raise

    async def stop(self) -> None:
        """Stop accepting work (handlers return early immediately), cancel
        and await every engine task plus every chat-attributed task, close
        the listener connection. The TelegramClient is left to run.py."""
        self._accepting.clear()
        self._stopping.set()
        if self.state != "failed":
            self.state = "stopping"
        tasks = list(self._tasks.values()) + list(self._chat_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for t, r in zip(tasks, results):
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    log.warning("engine task %s ended with %s",
                                t.get_name(), type(r).__name__)
        self._tasks.clear()
        self._chat_tasks.clear()
        await self._close_listener()
        if self.state != "failed":
            self.state = "stopped"

    async def _wait_stop(self, seconds: float, *events_: asyncio.Event) -> None:
        waiters = [asyncio.ensure_future(self._stopping.wait())]
        waiters += [asyncio.ensure_future(e.wait()) for e in events_]
        try:
            await asyncio.wait(waiters, timeout=max(0.0, seconds),
                               return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    # --- reconcile -----------------------------------------------------------
    async def reconcile(self, initial: bool = False) -> dict:
        async with self._reconcile_lock:
            try:
                res = await self._reconcile_once(initial)
            except Exception as e:
                res = {"state": "error", "error": type(e).__name__,
                       "version": self.last_reconcile.get("version")}
                log.warning("reconcile failed: %s", type(e).__name__)
                try:
                    ps = await self.storage.get_policy_state()
                    await self.storage.record_reconcile(
                        int(ps["requested_version"]), "error", type(e).__name__)
                except Exception:
                    pass
            self.reconcile_count += 1
            if res.get("state") != self.last_reconcile.get("state"):
                log.info("reconcile v%s -> %s", res.get("version"), res.get("state"))
            self.last_reconcile = res
            return res

    async def _reconcile_once(self, initial: bool = False) -> dict:
        for _ in range(3):
            mg = await self.storage.evaluate_mass_gone_block()
            snap = await self.storage.policy_snapshot()
            if snap["requested_version"] == mg["version"]:
                break
        else:
            raise RuntimeError("policy version kept moving")
        ver = snap["requested_version"]
        rows = {r["chat_id"]: r for r in snap["rows"]}
        desired = {cid for cid, r in rows.items() if r["desired"]}
        enabled = set(snap["enabled"])
        awaiting = snap["mode"] == "all" and not snap["any_complete_discovery"]

        to_enable = sorted(desired - enabled)
        to_disable, held = [], []
        for cid in sorted(enabled - desired):
            reason = rows[cid]["reason"] if cid in rows else "not_selected"
            if awaiting:
                held.append(cid)
            elif reason == "dialog_gone" and mg["state"] == "blocked":
                held.append(cid)
            else:
                to_disable.append((cid, reason))

        errors_: list[str] = []
        newly: list[int] = []
        held_set = set(held)
        disabling = {cid for cid, _ in to_disable}

        # 1. DB enable for new targets; open their lease generation right
        #    after a successful INSERT/UPDATE, so step 2 can see them.
        for cid in to_enable:
            r = rows.get(cid) or {}
            try:
                await self.storage.enable_dynamic_target(
                    cid, r.get("title") or r.get("username") or str(cid))
                self.leases.open(cid)
                newly.append(cid)
            except Exception as e:
                errors_.append(f"enable {cid}: {type(e).__name__}")

        # 2. Arm a lease for every chat that should end up in the active set.
        #    Covers three cases:
        #      (a) existing DB-enabled + desired on initial startup
        #          (legacy->dynamic transition; lease was never opened);
        #      (b) existing DB-enabled + held
        #          (awaiting_discovery, or mass-gone block holds it);
        #      (c) policy reversal: a previous close() timed out and left
        #          the slot inactive with inflight > 0; policy now wants
        #          the chat active again. We first drain the OLD generation
        #          completely, then open() a new one. If the drain still
        #          times out, the chat stays inactive and we record an
        #          error — the next reconcile will try again.
        #    Already-open slots are a no-op (no generation bump).
        should_be_active = ((enabled | set(newly)) - disabling) | held_set
        leased: set[int] = set()
        for cid in should_be_active:
            if self.leases.is_active(cid):
                leased.add(cid)
                continue
            drained = await self.leases.close(
                cid, timeout=self.disable_quiesce_timeout)
            if not drained:
                errors_.append(f"activate {cid}: quiesce timeout")
                log.warning(
                    "activate %s withheld: previous generation did not drain",
                    cid)
                continue
            self.leases.open(cid)
            leased.add(cid)

        # 3. Publish the active set: only chats whose lease is actually open
        #    reach handlers and the coordinator. Chats whose activation
        #    stalled are omitted and silently retried next reconcile.
        self._publish_active(leased)

        # 4. For every disabling target: close the per-chat lease (cancel
        #    tracked tasks, await them, wait for inflight=0 with a bounded
        #    timeout). If the drain does not complete, do NOT send the ACK:
        #    enabled stays TRUE, applied_version is not advanced, state=error.
        for cid, reason in to_disable:
            drained = await self.leases.close(cid, timeout=self.disable_quiesce_timeout)
            if not drained:
                errors_.append(f"quiesce timeout: {cid}")
                log.warning("disable %s: quiesce timeout; ACK withheld", cid)
                continue
            try:
                await self.storage.disable_target_ack(
                    cid, f"policy:{reason}",
                    lock_timeout_ms=self.disable_lock_timeout_ms)
            except Exception as e:
                errors_.append(f"disable {cid}: {type(e).__name__}")

        # 5. For newly enabled targets (non-initial): spawn participant sync
        #    as a chat-attributed task. On the initial reconcile the
        #    coordinator's own one-time pass handles it.
        if not initial:
            for cid in newly:
                if cid in leased:
                    self._spawn_chat_task(cid, self._participants_job(cid),
                                          name=f"participants:{cid}")
        self.collector.wake()

        if errors_:
            err = "; ".join(errors_)[:500]
            await self.storage.record_reconcile(ver, "error", err)
            return {"state": "error", "error": err, "version": ver,
                    "enabled": newly, "disabled": [c for c, _ in to_disable],
                    "held": held}
        if held:
            state = "awaiting_discovery" if awaiting else "mass_gone_blocked"
            await self.storage.record_reconcile(ver, state)
            return {"state": state, "version": ver, "enabled": newly,
                    "disabled": [c for c, _ in to_disable], "held": held}
        await self.storage.apply_policy_version(ver)
        if mg["state"] == "confirmed":
            await self.storage.clear_mass_gone_block(ver, mg["fingerprint"])
        await self.storage.record_reconcile(ver, "ok")
        return {"state": "ok", "version": ver, "enabled": newly,
                "disabled": [c for c, _ in to_disable], "held": []}

    async def _participants_job(self, chat_id: int):
        try:
            async with self.leases.lease(chat_id):
                if self._stopping.is_set():
                    return
                await self.collector.sync_participants(chat_id)
        except _TargetInactive:
            return
        except asyncio.CancelledError:
            return
        except Exception as e:
            if is_inactive_write(e):
                return
            log.warning("participants job %s: %s", chat_id, type(e).__name__)

    async def _reconciler_loop(self):
        while not self._stopping.is_set():
            await self._wait_stop(self.poll_seconds, self._reconcile_event)
            if self._stopping.is_set():
                return
            hinted = self._reconcile_event.is_set()
            self._reconcile_event.clear()
            try:
                ps = await self.storage.get_policy_state()
                behind = ps["requested_version"] > ps["applied_version"]
            except Exception as e:
                log.warning("reconciler poll failed: %s", type(e).__name__)
                continue
            if behind or hinted:
                await self.reconcile()

    # --- LISTEN/NOTIFY accelerator -------------------------------------------
    async def _close_listener(self):
        con, self._listener_con = self._listener_con, None
        if con is not None and not con.is_closed():
            try:
                await asyncio.wait_for(con.close(), timeout=3)
            except BaseException:
                con.terminate()

    def _on_notify(self, *_args):
        self.notifications += 1
        self._reconcile_event.set()

    async def _listener_loop(self):
        backoff = 1.0
        while not self._stopping.is_set():
            lost = asyncio.Event()
            try:
                con = await asyncpg.connect(self.storage.dsn)
                self._listener_con = con
                con.add_termination_listener(lambda _c: lost.set())
                await con.add_listener("tgp_policy", self._on_notify)
                self.listener_connects += 1
                backoff = 1.0
                self._listener_ready.set()
                self._reconcile_event.set()      # notifies may have been missed
                while not self._stopping.is_set() and not lost.is_set():
                    await self._wait_stop(self.poll_seconds, lost)
                    if con.is_closed():
                        lost.set()
            except asyncio.CancelledError:
                await self._close_listener()
                raise
            except Exception as e:
                log.warning("policy listener: %s; retry in %.0fs", type(e).__name__, backoff)
            await self._close_listener()
            if self._stopping.is_set():
                return
            if lost.is_set():
                log.info("policy listener connection lost; reconnecting")
            await self._wait_stop(backoff)
            backoff = min(backoff * 2, self.listener_backoff_max)

    # --- handlers ------------------------------------------------------------
    def _register_handlers(self):
        c = self.client

        async def _new(ev):
            await self._on_message(ev, "new_message")

        async def _edited(ev):
            await self._on_message(ev, "edited")

        async def _deleted(ev):
            await self._on_deleted(ev)

        async def _action(ev):
            await self._on_action(ev)

        async def _raw(update):
            await self._on_raw(update)

        for builder, fn in ((events.NewMessage(), _new), (events.MessageEdited(), _edited),
                            (events.MessageDeleted(), _deleted), (events.ChatAction(), _action),
                            (events.Raw(), _raw)):
            c.on(builder)(fn)
            self.handlers.append((builder, fn))

    def _reject_until_ready(self) -> bool:
        return not self._accepting.is_set() or self._stopping.is_set()

    async def _guarded_in_lease(self, kind: str, cid: int, mid, factory):
        """Take the per-chat lease then call `factory()` to build and await
        the coroutine. `factory` is a zero-arg callable so no coroutine is
        created when lease acquisition fails — avoids the `coroutine was
        never awaited` warning. Swallows the write guard's TGP01 silently."""
        try:
            async with self.leases.lease(cid):
                await factory()
        except _TargetInactive:
            return
        except Exception as e:
            if is_inactive_write(e):
                log.debug("%s for chat %s dropped: target disabled", kind, cid)
                return
            log.exception("%s error", kind)
            try:
                await self.storage.record_error(kind, cid, mid, None, e)
            except Exception:
                pass

    async def _on_message(self, ev, kind: str):
        if self._reject_until_ready():
            return
        msg = getattr(ev, "message", None)
        if msg is None:
            return
        try:
            cid = canonical(msg.peer_id)
        except Exception:
            return
        if not self.is_active(cid):
            return
        await self._guarded_in_lease(
            kind, cid, msg.id,
            lambda: self.collector._handle_message(msg))

    async def _on_action(self, ev):
        if self._reject_until_ready() or ev.chat_id is None:
            return
        try:
            cid = canonical(ev.chat_id)
        except Exception:
            return
        if not self.is_active(cid):
            return
        await self._guarded_in_lease(
            "chat_action", cid, None,
            lambda: self.collector._handle_action(ev))

    async def _on_raw(self, update):
        if self._reject_until_ready():
            return
        if type(update).__name__ not in ("UpdateMessageReactions", "UpdateBotMessageReactions"):
            return
        peer = getattr(update, "peer", None)
        if peer is None:
            return
        try:
            cid = canonical(peer)
        except Exception:
            return
        if not self.is_active(cid):
            return
        await self._guarded_in_lease(
            "reactions_update", cid, None,
            lambda: self.collector._handle_reactions_update(update, cid))

    async def _on_deleted(self, ev):
        if self._reject_until_ready():
            return
        ids = [int(i) for i in (getattr(ev, "deleted_ids", None) or [])]
        if not ids:
            return
        if ev.chat_id is not None:
            try:
                cid = canonical(ev.chat_id)
            except Exception:
                return
            if not self.is_active(cid):
                return
            # Existing rows only; resolve_deletion_events is skipped — it
            # creates stub rows, which the dynamic engine never does.
            await self._guarded_in_lease(
                "deleted", cid, None,
                lambda: self.storage.mark_deleted(cid, ids))
            return
        candidates = [c for c in self._active if c > -10**12 and self.leases.is_active(c)]
        if not candidates:
            return
        try:
            res = await self.storage.mark_deleted_existing_unique(candidates, ids)
        except Exception as e:
            if not is_inactive_write(e):
                log.warning("deletion without chat id failed: %s", type(e).__name__)
            return
        if res["ambiguous"]:
            log.info("deletion without chat id: %d ambiguous id(s) left untouched",
                     len(res["ambiguous"]))

    # --- discovery -----------------------------------------------------------
    def _dialog_item(self, d, archived: bool) -> dict | None:
        e = getattr(d, "entity", None)
        if e is None:
            return None
        try:
            cid = canonical(e)
        except Exception:
            return None
        return {"chat_id": cid, "category": classify_entity(e),
                "title": getattr(e, "title", None) or getattr(d, "name", None),
                "username": getattr(e, "username", None),
                "is_archived": archived,
                "participants_count": getattr(e, "participants_count", None)}

    async def _discovery_loop(self):
        first = True
        next_due = 0.0
        while not self._stopping.is_set():
            try:
                ctl = await self.storage.get_discovery_control()
                trigger = None
                if ctl["requested_seq"] > ctl["consumed_seq"]:
                    trigger = "request"
                elif time.monotonic() >= next_due:
                    trigger = "startup" if first else "scheduled"
                if trigger:
                    rid = await self.storage.claim_discovery_run(trigger)
                    if rid is not None:
                        first = False
                        ok, retry_s = await self.run_discovery(rid)
                        cfg = await self.storage.get_collection_config()
                        interval = int(cfg.get("discovery_interval_sec") or 300)
                        next_due = time.monotonic() + (interval if ok else retry_s)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("discovery loop: %s", type(e).__name__)
            await self._wait_stop(self.discovery_poll_seconds)

    async def run_discovery(self, run_id: int) -> tuple[bool, float]:
        """Both passes into staging, then the atomic publish. Returns
        (ok, retry_after_seconds)."""
        try:
            for archived in (False, True):
                batch: list[dict] = []
                async for d in self.client.iter_dialogs(archived=archived):
                    if self._stopping.is_set():
                        raise asyncio.CancelledError()
                    item = self._dialog_item(d, archived)
                    if item:
                        batch.append(item)
                    if len(batch) >= self.discovery_page:
                        await self.storage.stage_discovery_items(run_id, batch)
                        batch = []
                if batch:
                    await self.storage.stage_discovery_items(run_id, batch)
                await self.storage.mark_discovery_pass_done(
                    run_id, "archived" if archived else "normal")
            res = await self.storage.finish_discovery_run_success(run_id)
            self._discovery_failures = 0
            if res["applied"]:
                log.info("discovery run %s published: seen=%s gone=%s",
                         run_id, res["seen"], res["gone"])
                self._reconcile_event.set()
            return True, 0.0
        except asyncio.CancelledError:
            await asyncio.shield(self.storage.finish_discovery_run_fail(
                run_id, "cancelled: shutdown", cancelled=True))
            raise
        except errors.FloodWaitError as e:
            wait = float(min(int(getattr(e, "seconds", 60)) + 5, 3600))
            await self.storage.finish_discovery_run_fail(
                run_id, "FloodWaitError", retry_after_sec=int(wait))
            log.warning("discovery run %s FloodWait; next attempt in %.0fs", run_id, wait)
            return False, wait
        except Exception as e:
            self._discovery_failures += 1
            wait = float(min(3600, 60 * 2 ** (self._discovery_failures - 1)))
            await self.storage.finish_discovery_run_fail(
                run_id, type(e).__name__, retry_after_sec=int(wait))
            log.warning("discovery run %s failed (%s); next attempt in %.0fs",
                        run_id, type(e).__name__, wait)
            return False, wait

    # --- YAML import worker --------------------------------------------------
    async def _yaml_loop(self):
        while not self._stopping.is_set():
            try:
                for row in await self.storage.yaml_import_due(self.yaml_batch):
                    if self._stopping.is_set():
                        break
                    await self.resolve_yaml(row)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("yaml import loop: %s", type(e).__name__)
            await self._wait_stop(self.yaml_poll_seconds)

    async def resolve_yaml(self, row) -> str:
        """Returns 'selected' | 'retry' | 'terminal'."""
        raw = row["raw_target"]
        attempts = int(row["attempts"] or 0)
        entity, err = None, None
        try:
            entity = await self.client.get_entity(raw)
        except Exception as e:
            err = e
        if entity is None and not isinstance(err, _TRANSIENT):
            try:
                entity = await self.collector._resolve_via_invite_or_dialogs(raw)
            except Exception:
                entity = None
        if entity is not None:
            cid = canonical(entity)
            try:
                await self.collector._save_chat(entity)
            except Exception:
                pass
            await self.storage.yaml_import_resolved(raw, cid)
            return "selected"
        cls = type(err).__name__ if err is not None else "NotResolved"
        if isinstance(err, _TRANSIENT) and attempts + 1 < self.yaml_max_attempts:
            if isinstance(err, errors.FloodWaitError):
                delay = min(int(getattr(err, "seconds", 60)) + 5, 3600)
            else:
                delay = min(3600, 60 * 2 ** attempts)
            await self.storage.yaml_import_record_error(
                raw, cls, next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=delay))
            return "retry"
        if isinstance(err, _TRANSIENT):
            cls = "MaxAttempts"
        await self.storage.yaml_import_mark_terminal(raw, cls)
        return "terminal"
