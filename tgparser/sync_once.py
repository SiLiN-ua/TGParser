"""One-shot Telegram dialog sync for the UI.

Runs the SAME dynamic-engine discovery + yaml-resolver passes that
``run.py`` runs continuously, but exactly once, with no live update
handlers, no coordinator, no backfill and no media downloads. Exits
when done.

It is the only path that reaches Telegram on behalf of the UI; the
UI itself never opens a Telegram session. The main ``tgparser.service``
must be stopped — this process acquires the SAME on-disk session file
through ``make_client`` and refuses to run if the service is still up.

Usage::

    python -m tgparser.sync_once [--timeout SECONDS]

Prints a single JSON line with counts and exits 0 on success, non-zero
on failure.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time

from tgparser.client import make_client
from tgparser.collector import Collector
from tgparser.config import load as load_config
from tgparser.dynamic import DynamicEngine
from tgparser.lock import ProcessLock
from tgparser.media_queue import MediaQueue
from tgparser.storage import Storage


log = logging.getLogger("tgparser.sync_once")


EXIT_OK = 0
EXIT_ERROR = 1
EXIT_LOCKED = 2
EXIT_THROTTLED = 3


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


async def _reserve_runtime(storage: Storage) -> bool:
    """Reserve runtime_state.pid for this process under a FOR UPDATE lock.
    Refuses if another TGParser process already owns it. ``True`` on
    success — caller MUST release in the matching finally branch."""
    pid = os.getpid()
    async with storage.pool.acquire() as con:
        async with con.transaction():
            row = await con.fetchrow(
                "SELECT pid, status FROM runtime_state WHERE id=1 FOR UPDATE")
            if row and row["pid"] and row["pid"] != pid and _pid_alive(row["pid"]):
                # A live process owns it — probably tgparser.service.
                return False
            # Otherwise the pid is ours, empty, or left by a dead process
            # (we already hold tgparser.lock, so no live TGParser can exist).
            await con.execute(
                "UPDATE runtime_state SET pid=$1, status='syncing_dialogs', "
                "started_at=NOW(), heartbeat_at=NOW() WHERE id=1",
                pid)
    return True


async def _release_runtime(storage: Storage) -> None:
    pid = os.getpid()
    try:
        async with storage.pool.acquire() as con:
            await con.execute(
                "UPDATE runtime_state SET pid=NULL, status='stopped', "
                "heartbeat_at=NOW() WHERE id=1 AND pid=$1",
                pid)
    except Exception:
        log.exception("could not release runtime_state")


async def _run(timeout: float) -> dict:
    cfg = load_config()
    storage = Storage(cfg["postgres"]["dsn"])
    await storage.connect()
    client = make_client(cfg)

    summary = {
        "ok": False, "exit_code": EXIT_ERROR,
        "duration_s": 0.0,
        "discovery": None,
        "resolver": {"attempted": 0, "selected": 0, "retry": 0,
                     "terminal": 0, "errors": 0},
        "message": "",
    }
    t0 = time.monotonic()

    reserved = False
    try:
        ok = await _reserve_runtime(storage)
        if not ok:
            summary["message"] = "runtime_state already owned by another process"
            summary["exit_code"] = EXIT_LOCKED
            return summary
        reserved = True

        schema = await storage.schema_version()
        if schema < 11:
            summary["message"] = f"schema v{schema}; v11 required"
            return summary

        await client.connect()
        if not await client.is_user_authorized():
            summary["message"] = "Telegram session not authorized"
            return summary

        # Build a Collector only so DynamicEngine can call its helpers
        # (resolve_via_invite_or_dialogs, _save_chat). We do NOT start it
        # and never register any handlers. The config keeps media settings
        # under "storage", not "media" — mirror run.py to stay consistent.
        storage_cfg = cfg.get("storage", {})
        media_q = MediaQueue(client, storage,
                             base_dir=storage_cfg["media_dir"],
                             max_media_mb=storage_cfg.get("max_media_mb", 500),
                             concurrency=0)  # zero worker tasks; never started
        coll = Collector(client, storage, media_q,
                         options={"download_media": False,
                                  "backfill_on_start": False,
                                  "per_message_retries": 1,
                                  "bg_concurrency": 1,
                                  "follow_linked_discussion": False})
        engine = DynamicEngine(client, storage, coll, media_q,
                               yaml_targets=[], options={})

        # --- discovery pass (one claim, one iter_dialogs sweep) ---
        # We hold the session lock and runtime_state, so any 'running' run
        # left behind belongs to a dead process (same rule as run.py startup).
        await storage.recover_stale_discovery_runs()

        async def _active_ids() -> set[int]:
            async with storage.pool.acquire() as con:
                rows = await con.fetch(
                    "SELECT chat_id FROM discovered_dialogs WHERE gone_at IS NULL")
            return {int(r["chat_id"]) for r in rows}

        before = await _active_ids()
        ctl = await storage.get_discovery_control()
        trigger = ("request" if ctl and ctl["requested_seq"] > ctl["consumed_seq"]
                   else "scheduled")
        rid = await storage.claim_discovery_run(trigger)
        if rid is None:
            ctl = await storage.get_discovery_control()
            wait_s = 0
            if ctl and ctl["next_allowed_at"]:
                async with storage.pool.acquire() as con:
                    wait_s = int(await con.fetchval(
                        "SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM ($1 - NOW()))))",
                        ctl["next_allowed_at"]))
            summary["discovery"] = {"ran": False, "reason": "throttled",
                                    "wait_s": wait_s}
            summary["exit_code"] = EXIT_THROTTLED
            summary["message"] = "throttled"
            return summary
        ok_disc, retry_s = await asyncio.wait_for(
            engine.run_discovery(rid), timeout=max(10.0, timeout * 0.6))
        if not ok_disc:
            summary["discovery"] = {"ran": True, "run_id": rid, "ok": False,
                                    "retry_s": retry_s}
            summary["message"] = "discovery failed"
            return summary
        after = await _active_ids()
        summary["discovery"] = {"ran": True, "run_id": rid, "ok": True,
                                "seen": len(after),
                                "new": len(after - before),
                                "gone": len(before - after)}

        # --- yaml resolver loop until empty or time-cap ---
        resolver_deadline = t0 + timeout
        while time.monotonic() < resolver_deadline:
            rows = await storage.yaml_import_due(5)
            if not rows:
                break
            for row in rows:
                if time.monotonic() >= resolver_deadline:
                    break
                summary["resolver"]["attempted"] += 1
                try:
                    verdict = await engine.resolve_yaml(row)
                    summary["resolver"][verdict] = \
                        summary["resolver"].get(verdict, 0) + 1
                except Exception as e:
                    log.warning("resolver %s: %s",
                                row.get("raw_target"), type(e).__name__)
                    summary["resolver"]["errors"] += 1

        summary["ok"] = True
        summary["exit_code"] = EXIT_OK
        summary["message"] = "sync complete"
        return summary

    except asyncio.TimeoutError:
        summary["message"] = "sync_once timed out"
        return summary
    except Exception as e:
        log.exception("sync_once failed")
        summary["message"] = f"{type(e).__name__}: {e}"[:300]
        return summary
    finally:
        summary["duration_s"] = round(time.monotonic() - t0, 2)
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass
        if reserved:
            await _release_runtime(storage)
        await storage.close()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    p = argparse.ArgumentParser(prog="tgparser.sync_once")
    p.add_argument("--timeout", type=float, default=120.0,
                   help="Hard cap for the whole sync in seconds (default 120)")
    args = p.parse_args(argv)

    # Cross-process lock on the shared session file: identical mechanism to
    # run.py so there is zero chance of two Telethon clients opening the
    # same session concurrently.
    lock = ProcessLock("./tgparser.lock")
    result: dict
    try:
        try:
            lock.acquire()
        except RuntimeError as e:
            result = {"ok": False, "exit_code": EXIT_LOCKED,
                      "duration_s": 0.0,
                      "message": f"session lock held: {e}"[:300]}
            print(json.dumps(result, ensure_ascii=False, default=str))
            return EXIT_LOCKED
        try:
            async def _driver():
                return await asyncio.wait_for(
                    _run(args.timeout), timeout=args.timeout + 10)
            result = asyncio.run(_driver())
        except asyncio.TimeoutError:
            result = {"ok": False, "exit_code": EXIT_ERROR,
                      "duration_s": args.timeout + 10,
                      "message": "wall-clock timeout"}
        except Exception as e:
            result = {"ok": False, "exit_code": EXIT_ERROR,
                      "duration_s": 0.0,
                      "message": f"{type(e).__name__}: {e}"[:300]}
    finally:
        lock.release()
    print(json.dumps(result, ensure_ascii=False, default=str))
    return int(result.get("exit_code", EXIT_ERROR))


if __name__ == "__main__":
    sys.exit(main())
