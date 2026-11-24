import asyncio
import logging
import os
import signal
import sys
import traceback

from tgparser.config import load as load_config
from tgparser.logging_setup import setup as setup_logging
from tgparser.lock import ProcessLock
from tgparser.client import make_client
from tgparser.storage import Storage
from tgparser.migrations import apply as apply_migrations
from tgparser.media_queue import MediaQueue
from tgparser.collector import Collector
from tgparser.dynamic import DynamicEngine
from tgparser.qr_login import qr_login


EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LOCKED = 2
EXIT_NO_TARGETS = 3


class StartupError(Exception):
    pass


async def _tg_heartbeat_loop(client, storage, stop: asyncio.Event,
                             interval: int = 30):
    """v6: hits an actual network RPC (GetState) so the cached get_me shortcut
    can't paint a hung socket as healthy. Also detects large wall-clock jumps
    (Windows sleep/resume) and triggers an explicit reconnect+catch_up."""
    import time as _time
    from telethon.tl.functions.updates import GetStateRequest

    last_mono = _time.monotonic()
    while not stop.is_set():
        now_mono = _time.monotonic()
        # If we observe a gap much larger than our sleep interval, assume the
        # host slept and the Telegram socket is probably dead.
        if now_mono - last_mono > interval * 3:
            logging.getLogger("tgparser").warning(
                "heartbeat: monotonic jump %.0fs > %ss - reconnecting Telegram",
                now_mono - last_mono, interval * 3)
            try:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(1)
                await client.connect()
                try:
                    await client.catch_up()
                except Exception:
                    pass
            except Exception as e:
                logging.getLogger("tgparser").warning("post-wake reconnect failed: %s", e)
        last_mono = now_mono

        try:
            await asyncio.wait_for(client(GetStateRequest()), timeout=20)
            await storage.record_tg_ok()
        except Exception as e:
            try:
                await storage.record_tg_error(str(e)[:400])
            except Exception:
                pass
            # also try a reconnect best-effort
            try:
                if not client.is_connected():
                    await client.connect()
            except Exception:
                pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def _db_heartbeat_loop(storage, stop: asyncio.Event, interval: int = 15):
    while not stop.is_set():
        try:
            await storage.heartbeat()
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def _select_engine(storage, log: logging.Logger) -> str:
    """Runs before any Telegram connect or collector write; returns 'legacy'
    or 'dynamic'. The choice holds for the whole process lifetime.

      schema < 11              -> legacy, nothing v11-related is touched
      schema >= 11, gate off   -> legacy
      schema >= 11, gate on    -> dynamic

    With schema v11 dyn_engine=FALSE is recorded first in BOTH cases, so a
    stale TRUE from a previous dynamic process never survives this start; the
    dynamic engine sets TRUE itself only once it is fully ready. Failure to
    record FALSE is fatal."""
    try:
        version = await storage.schema_version()
    except Exception as e:
        raise StartupError(f"schema version unavailable: {type(e).__name__}")
    if version < 11:
        return "legacy"
    engine = "dynamic" if await storage.dynamic_gate_enabled() else "legacy"
    try:
        await storage.set_runtime_dynamic_engine(False)
    except Exception as e:
        log.error("could not record dyn_engine=false: %s", type(e).__name__)
        raise StartupError("dyn_engine=false not recorded")
    try:
        n = await storage.recover_stale_discovery_runs()
        if n:
            log.warning("marked %s stale discovery run(s) failed", n)
    except Exception as e:
        log.warning("stale discovery recovery skipped: %s", type(e).__name__)
    log.info("engine selected: %s (schema v%s)", engine, version)
    return engine


async def _main_inner(log: logging.Logger, cfg: dict,
                      lock: ProcessLock, stop: asyncio.Event) -> int:
    storage = Storage(cfg["postgres"]["dsn"])
    client = None
    media_q = None
    collector = None
    engine = None
    heartbeat_task: asyncio.Task | None = None
    tg_heartbeat_task: asyncio.Task | None = None
    try:
        try:
            await storage.connect()
        except Exception as e:
            log.error("postgres connect failed: %s", e)
            raise StartupError("postgres unavailable")
        await apply_migrations(storage.pool, "migrations")

        engine_kind = await _select_engine(storage, log)

        client = make_client(cfg)
        try:
            await client.connect()
        except Exception as e:
            log.error("telegram connect failed: %s", e)
            raise StartupError("telegram unavailable")
        await qr_login(client)
        me = await client.get_me()
        log.info("signed in as @%s (id=%s)", me.username or "?", me.id)

        media_q = MediaQueue(
            client, storage,
            base_dir=cfg["storage"]["media_dir"],
            max_media_mb=cfg["storage"].get("max_media_mb", 500),
            concurrency=cfg["storage"].get("media_concurrency", 3),
        )
        media_q.start()

        opts = dict(cfg.get("options", {}))
        opts["download_media"] = cfg["storage"].get("download_media", True)
        collector = Collector(client, storage, media_q, opts)

        requested = list(cfg.get("targets") or [])

        if engine_kind == "dynamic":
            # Same client, storage, collector and media queue; collector.start
            # (legacy `chats=` handlers + YAML-driven disable) is never called.
            engine = DynamicEngine(client, storage, collector, media_q,
                                   yaml_targets=requested, options=opts)
            try:
                await engine.start()
            except Exception as e:
                log.error("dynamic engine startup failed: %s", e)
                raise StartupError("dynamic engine startup failed")
            await storage.set_runtime(os.getpid(), "healthy",
                                      sorted(engine.active_ids()),
                                      note="engine=dynamic", started=True)
            heartbeat_task = asyncio.create_task(
                _db_heartbeat_loop(storage, stop, interval=15), name="db-hb")
            tg_heartbeat_task = asyncio.create_task(
                _tg_heartbeat_loop(client, storage, stop, interval=30), name="tg-hb")
            try:
                swept = await media_q.sweep_retryable()
                if swept:
                    log.info("media sweep re-enqueued %s files", swept)
            except Exception as e:
                log.warning("media sweep failed: %s", e)
            await collector.catch_up()
            await storage.set_runtime(os.getpid(), "syncing",
                                      sorted(engine.active_ids()), note="engine=dynamic")
            log.info("running (dynamic engine); live updates on. Ctrl+C to stop.")
            await stop.wait()
            return EXIT_OK

        resolved = await collector.start(requested)

        if resolved == 0:
            log.error("no targets resolved (requested=%s). Exiting.", requested)
            await storage.set_runtime(
                os.getpid(), "unhealthy", [],
                note=f"no targets resolved: {collector.failed_targets}",
                started=True)
            return EXIT_NO_TARGETS

        initial_status = "healthy" if not collector.failed_targets else "degraded"
        await storage.set_runtime(
            os.getpid(), initial_status, collector.ok_targets,
            note=(f"failed_targets={collector.failed_targets}"
                  if collector.failed_targets else None),
            started=True)
        if collector.failed_targets:
            log.warning("starting DEGRADED: %s/%s targets failed: %s",
                        len(collector.failed_targets),
                        len(collector.failed_targets) + resolved,
                        collector.failed_targets)

        # Start DB + Telegram heartbeats before the (potentially long) backfill.
        heartbeat_task = asyncio.create_task(
            _db_heartbeat_loop(storage, stop, interval=15), name="db-hb")
        tg_heartbeat_task = asyncio.create_task(
            _tg_heartbeat_loop(client, storage, stop, interval=30), name="tg-hb")

        # Resume incomplete media from previous runs.
        try:
            swept = await media_q.sweep_retryable()
            if swept:
                log.info("media sweep re-enqueued %s files", swept)
        except Exception as e:
            log.warning("media sweep failed: %s", e)

        # v5: background coordinator runs batches in this very process while
        # live handlers keep receiving updates.
        await collector.start_coordinator()
        await collector.catch_up()
        await storage.set_runtime(
            os.getpid(), "syncing", collector.ok_targets, note=None)
        log.info("running; coordinator active; live updates on. Ctrl+C to stop.")

        await stop.wait()
        return EXIT_OK

    finally:
        log.info("shutting down")
        # The whole teardown runs as a separate task guarded by asyncio.shield,
        # so even if the outer main task has been cancelled (SIGINT delivered
        # mid-await, asyncio.Runner cancelling on second Ctrl-C, etc.) the
        # teardown runs to completion and the final 'stopped' row lands in
        # Postgres before we exit. Each step catches BaseException (not just
        # Exception) because asyncio.CancelledError is a BaseException and
        # must not short-circuit shutdown.
        async def _teardown():
            # 1) Mark 'stopping' with the REAL pid. 'stopped' is written only
            #    at the very end, so a SIGKILL or crash between here and the
            #    final write leaves the row at 'stopping' with a dead pid,
            #    which health.py correctly flags as unhealthy.
            try:
                if engine:
                    _targets = sorted(engine.active_ids())
                else:
                    _targets = list(collector.ok_targets) if collector else []
                await storage.set_runtime(os.getpid(), "stopping",
                                          _targets, note=None)
            except BaseException:
                log.exception("set_runtime(stopping) failed")

            # 2) Cancel heartbeat tasks BEFORE any further teardown so they
            #    cannot race with the final status writes.
            for t in (heartbeat_task, tg_heartbeat_task):
                if t:
                    t.cancel()
                    try:
                        await t
                    except BaseException:
                        pass

            # 2b) Dynamic engine: stop discovery/reconciler/listener/YAML
            #     tasks before the coordinator and the client they use.
            if engine:
                try:
                    await engine.stop()
                except BaseException:
                    log.exception("dynamic engine stop failed")
                log.info("dynamic engine stopped")

            # 3) Coordinator (~5s budget). Batches advance checkpoint only
            #    after a successful commit, so cancelling mid-batch leaves
            #    sync_state.last_backfilled_message_id untouched.
            if collector:
                try:
                    await collector.stop_coordinator()
                except BaseException:
                    log.exception("coordinator stop failed")
                log.info("coordinator stopped (run.py)")
                # Background tasks: collector.wait_background cancels anything
                # still running past 5s, logs real exceptions internally, and
                # never raises on normal CancelledError-driven shutdown. We
                # still guard with except BaseException + log.exception in
                # case of a genuine programming error inside wait_background.
                try:
                    await collector.wait_background(timeout=5)
                except BaseException:
                    log.exception("wait_background raised unexpectedly")
                log.info("background tasks stopped")

            # 4) MediaQueue: fast stop. Pending `media` rows stay in Postgres
            #    and are re-enqueued by sweep_retryable() at next start.
            if media_q:
                try:
                    await media_q.stop(timeout=5)
                except BaseException:
                    log.exception("media queue stop failed")
                log.info("media stopped")

            # 5) Telegram client disconnect — only log success after the
            #    await returns without raising.
            if client:
                disconnected = False
                try:
                    await client.disconnect()
                    disconnected = True
                except BaseException:
                    log.exception("client disconnect failed")
                if disconnected:
                    log.info("client disconnected")

            # 6a) Dynamic engine is no longer running: every writer is down.
            if engine:
                try:
                    await storage.set_runtime_dynamic_engine(False)
                except BaseException:
                    log.exception("could not reset dyn_engine on shutdown")

            # 6) Final 'stopped' + pid=NULL.
            try:
                await storage.set_runtime(None, "stopped", [], note=None)
            except BaseException:
                log.exception("set_runtime(stopped) failed")

            # 7) Close pool, log bye.
            try:
                await storage.close()
            except BaseException:
                pass
            log.info("bye")

        _td_task = asyncio.ensure_future(_teardown())
        try:
            await asyncio.shield(_td_task)
        except asyncio.CancelledError:
            # Outer cancellation hit the shield; the teardown task keeps
            # running. Wait for it in a cancellation-safe way.
            while not _td_task.done():
                try:
                    await asyncio.shield(_td_task)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
        except BaseException:
            try:
                await _td_task
            except BaseException:
                pass


async def _main(log: logging.Logger, cfg: dict) -> int:
    lock = ProcessLock("./tgparser.lock")
    try:
        lock.acquire()
    except RuntimeError as e:
        # Quiet exit when the holder is clearly a healthy TGParser - this
        # happens every minute under the task's repetition trigger and must
        # not spam the log.
        quiet = False
        try:
            storage = Storage(cfg["postgres"]["dsn"])
            await storage.connect()
            rt = await storage.get_runtime()
            await storage.close()
            if rt and rt["pid"] and rt["heartbeat_at"]:
                from datetime import datetime, timezone
                age = (datetime.now(timezone.utc) - rt["heartbeat_at"]).total_seconds()
                if age < 120:
                    quiet = True
        except Exception:
            pass
        if quiet:
            log.debug("lock held by live instance; exiting silently")
        else:
            log.error("startup aborted: %s", e)
        return EXIT_LOCKED

    stop = asyncio.Event()

    def _sig(*_):
        log.info("signal received, shutting down")
        stop.set()

    try:
        signal.signal(signal.SIGINT, _sig)
        signal.signal(signal.SIGTERM, _sig)
    except Exception:
        pass

    try:
        return await _main_inner(log, cfg, lock, stop)
    finally:
        try:
            lock.release()
        except Exception:
            pass


def main() -> int:
    cfg = load_config()
    log = setup_logging(cfg)
    log.info("TGParser starting (pid=%s)", os.getpid())
    try:
        return asyncio.run(_main(log, cfg))
    except (KeyboardInterrupt, asyncio.CancelledError):
        return EXIT_OK
    except Exception as e:
        log.critical("fatal: %s\n%s", e, traceback.format_exc())
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
