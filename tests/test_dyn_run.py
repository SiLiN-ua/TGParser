"""run.py engine selection and full process lifecycle with a fake client:

  schema 10              -> legacy
  schema 11, gate off    -> legacy (dyn_engine=false)
  schema 11, gate on     -> dynamic (no legacy handlers, one client)
"""
import asyncio
import json
import logging

import asyncpg
import pytest

import run
from tests._dyn_engine_support import ENGINE_OPTS, FakeClient, ch, wait_until
from tests._dyn_support import add_dialog, add_pref, storage_v10, storage_v11  # noqa: F401
from tgparser.collector import Collector
from tgparser.storage import Storage

A = ch(1111)
OWNED = ("dyn-", "backfill-coordinator", "media-", "participants:", "db-hb", "tg-hb")


class RunClient(FakeClient):
    async def is_user_authorized(self):
        return True

    async def get_me(self):
        class Me:
            username, id = "test", 1
        return Me()


def _install(monkeypatch):
    made = []

    def make_client(cfg):
        c = RunClient()
        made.append(c)
        return c
    monkeypatch.setattr(run, "make_client", make_client)
    return made


def _cfg(dsn, tmp_path, targets):
    return {"postgres": {"dsn": dsn},
            "storage": {"media_dir": str(tmp_path), "download_media": False,
                        "media_concurrency": 1},
            "options": {**ENGINE_OPTS, "participants_sync": False,
                        "follow_linked_discussion": False},
            "targets": targets}


async def _runtime(dsn):
    con = await asyncpg.connect(dsn)
    try:
        return await con.fetchrow("SELECT * FROM runtime_state WHERE id=1")
    finally:
        await con.close()


def _owned_alive():
    return [t.get_name() for t in asyncio.all_tasks()
            if t.get_name().startswith(OWNED) and not t.done()]


async def _run_until_syncing(dsn, tmp_path, targets, check):
    stop = asyncio.Event()
    main = asyncio.create_task(run._main_inner(
        logging.getLogger("tgparser.test_run"), _cfg(dsn, tmp_path, targets),
        lock=None, stop=stop))

    async def syncing():
        if main.done():
            return True
        r = await _runtime(dsn)
        return r and r["status"] == "syncing"
    await wait_until(syncing, timeout=10, what="runtime syncing")
    if not main.done():
        await check()
    stop.set()
    return await asyncio.wait_for(main, 30)


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", ["v10", "v11_gate_off"])
async def test_legacy_engine_selected(storage_v10, storage_v11, schema, tmp_path,
                                      monkeypatch, caplog):
    s = storage_v10 if schema == "v10" else storage_v11
    made = _install(monkeypatch)

    def no_dynamic(*a, **k):
        raise AssertionError("dynamic engine must not be constructed")
    monkeypatch.setattr(run, "DynamicEngine", no_dynamic)
    starts = []
    orig_start = Collector.start

    async def spy_start(self, targets):
        starts.append(list(targets))
        return await orig_start(self, targets)
    monkeypatch.setattr(Collector, "start", spy_start)

    async def check():
        c = made[0]
        assert len(c.handlers) == 5
        assert all(getattr(b, "chats", None) == [A] for b, _ in c.handlers
                   if type(b).__name__ != "Raw")
        if schema == "v11_gate_off":
            assert (await _runtime(s.dsn))["dyn_engine"] is False
    assert await _run_until_syncing(s.dsn, tmp_path, [A], check) == run.EXIT_OK
    assert starts == [[A]] and len(made) == 1
    assert _owned_alive() == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.asyncio
async def test_dynamic_engine_selected_and_clean_shutdown(storage_v11, tmp_path,
                                                          monkeypatch, caplog):
    s = storage_v11
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    async with s.pool.acquire() as con:
        await con.execute("UPDATE deployment_gates SET enabled=TRUE")
    made = _install(monkeypatch)

    async def no_legacy(self, targets):
        raise AssertionError("legacy Collector.start must not run")
    monkeypatch.setattr(Collector, "start", no_legacy)

    async def check():
        c = made[0]
        assert len(c.handlers) == 5
        assert all(getattr(b, "chats", None) is None for b, _ in c.handlers)
        rt = await _runtime(s.dsn)
        assert rt["dyn_engine"] is True and rt["note"] == "engine=dynamic"
        assert json.loads(rt["targets"]) == [A]
        alive = _owned_alive()
        for name in ("dyn-listener", "dyn-reconciler", "dyn-discovery", "dyn-yaml",
                     "backfill-coordinator"):
            assert name in alive
    caplog.set_level(logging.INFO)
    assert await _run_until_syncing(s.dsn, tmp_path, ["@from_yaml"], check) == run.EXIT_OK
    assert len(made) == 1
    rt = await _runtime(s.dsn)
    assert rt["dyn_engine"] is False and rt["status"] == "stopped" and rt["pid"] is None
    assert _owned_alive() == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    text = caplog.text
    order = [text.index(m) for m in ("dynamic engine stopped", "coordinator stopped (run.py)",
                                     "media stopped", "client disconnected", "bye")]
    assert order == sorted(order)
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT status FROM dialog_discovery_runs ORDER BY id LIMIT 1") == "cancelled"
        assert await con.fetchval(
            "SELECT COUNT(*) FROM yaml_target_imports WHERE raw_target='@from_yaml'") == 1


@pytest.mark.asyncio
async def test_dynamic_startup_failure_is_fatal(storage_v11, tmp_path, monkeypatch):
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute("UPDATE deployment_gates SET enabled=TRUE")
    made = _install(monkeypatch)

    async def boom(self):
        raise asyncpg.exceptions.InsufficientPrivilegeError("x")
    monkeypatch.setattr(Storage, "policy_snapshot", boom)

    async def no_legacy(self, targets):
        raise AssertionError("must not fall back to legacy")
    monkeypatch.setattr(Collector, "start", no_legacy)
    stop = asyncio.Event()
    with pytest.raises(run.StartupError, match="dynamic engine startup failed"):
        await run._main_inner(logging.getLogger("t"), _cfg(s.dsn, tmp_path, []),
                              lock=None, stop=stop)
    rt = await _runtime(s.dsn)
    assert rt["dyn_engine"] is False and rt["status"] == "stopped"
    assert len(made) == 1 and _owned_alive() == []


@pytest.mark.asyncio
async def test_gate_read_once_per_process(storage_v11, tmp_path, monkeypatch):
    """Flipping the gate while a legacy process runs changes nothing in it."""
    s = storage_v11
    made = _install(monkeypatch)

    async def check():
        async with s.pool.acquire() as con:
            await con.execute("UPDATE deployment_gates SET enabled=TRUE")
        await asyncio.sleep(0)
        assert (await _runtime(s.dsn))["dyn_engine"] is False
        assert all(getattr(b, "chats", None) == [A] for b, _ in made[0].handlers
                   if type(b).__name__ != "Raw")
    assert await _run_until_syncing(s.dsn, tmp_path, [A], check) == run.EXIT_OK
    assert (await _runtime(s.dsn))["dyn_engine"] is False
