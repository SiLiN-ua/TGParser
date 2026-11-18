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
from tgparser.qr_login import qr_login


EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LOCKED = 2
EXIT_NO_TARGETS = 3


class StartupError(Exception):
    pass


async def _tg_heartbeat_loop(client, storage, stop: asyncio.Event,
                             interval: int = 30):
    """Periodically poke Telegram with a cheap authenticated call and
    persist the result for health. Does NOT create a second client."""
    while not stop.is_set():
        try:
            await client.get_me()
            await storage.record_tg_ok()
        except Exception as e:
            try:
                await storage.record_tg_error(str(e)[:400])
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


async def _main_inner(log: logging.Logger, cfg: dict,
                      lock: ProcessLock, stop: asyncio.Event) -> int:
    storage = Storage(cfg["postgres"]["dsn"])
    client = None
    media_q = None
    collector = None
    heartbeat_task: asyncio.Task | None = None
    tg_heartbeat_task: asyncio.Task | None = None
    try:
        try:
            await storage.connect()
        except Exception as e:
            log.error("postgres connect failed: %s", e)
            raise StartupError("postgres unavailable")
        await apply_migrations(storage.pool, "migrations")

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

        try:
            summary = await collector.run_backfill_and_sync()
            if summary["failed"] and len(summary["failed"]) == summary["total"]:
                await storage.set_runtime(
                    os.getpid(), "unhealthy", collector.ok_targets,
                    note=f"all targets failed: {summary['failed']}")
            elif summary["degraded"] or summary["failed"]:
                await storage.set_runtime(
                    os.getpid(), "degraded", collector.ok_targets,
                    note=f"degraded: failed={summary['failed']} "
                         f"deg={summary['degraded']}")
            else:
                await storage.set_runtime(
                    os.getpid(), "healthy", collector.ok_targets,
                    note=None)
        except Exception as e:
            log.exception("run_backfill_and_sync failed")
            await storage.record_error("backfill_cycle", None, None, None, e)
            await storage.set_runtime(
                os.getpid(), "degraded", collector.ok_targets,
                note=f"backfill_cycle exception: {e}"[:300])

        await collector.catch_up()
        log.info("running; waiting for live updates. Ctrl+C to stop.")

        await stop.wait()
        return EXIT_OK

    finally:
        log.info("shutting down")
        for t in (heartbeat_task, tg_heartbeat_task):
            if t:
                t.cancel()
                try:
                    await t
                except BaseException:
                    pass
        if collector:
            try:
                await collector.wait_background(timeout=15)
            except Exception:
                log.exception("bg task drain failed")
        if media_q:
            try:
                await media_q.stop(timeout=30)
            except Exception:
                log.exception("media queue stop failed")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        try:
            await storage.set_runtime(None, "stopped", [], note=None)
        except Exception:
            pass
        try:
            await storage.close()
        except Exception:
            pass
        log.info("bye")


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
