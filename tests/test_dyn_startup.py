"""run.py legacy startup vs schema v11: dyn_engine=false is written before any
Telegram client exists, and failure to write it is fatal."""
import asyncio
import logging

import asyncpg
import pytest

import run
from tests._dyn_support import storage_v10, storage_v11  # noqa: F401
from tgparser.storage import Storage


class _Abort(Exception):
    pass


def _cfg(dsn):
    return {"postgres": {"dsn": dsn},
            "storage": {"media_dir": "/nonexistent", "download_media": False},
            "options": {}, "targets": []}


def _install_fake_client(monkeypatch, dsn, seen):
    class FakeClient:
        async def connect(self):
            con = await asyncpg.connect(dsn)
            try:
                has_col = await con.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                    "WHERE table_name='runtime_state' AND column_name='dyn_engine')")
                seen["dyn_engine_at_connect"] = (
                    await con.fetchval("SELECT dyn_engine FROM runtime_state WHERE id=1")
                    if has_col else "no-column")
            finally:
                await con.close()
            raise _Abort("stop after connect")

        async def disconnect(self):
            pass

    def make_client(cfg):
        seen["make_client_calls"] = seen.get("make_client_calls", 0) + 1
        return FakeClient()

    monkeypatch.setattr(run, "make_client", make_client)


async def _start(dsn):
    log = logging.getLogger("tgparser.test_startup")
    return await run._main_inner(log, _cfg(dsn), lock=None, stop=asyncio.Event())


@pytest.mark.asyncio
async def test_v11_writes_false_before_connect_even_if_stale_true(storage_v11, monkeypatch):
    s = storage_v11
    await s.set_runtime_dynamic_engine(True)            # stale value from a dead process
    seen = {}
    _install_fake_client(monkeypatch, s.dsn, seen)
    with pytest.raises(run.StartupError, match="telegram unavailable"):
        await _start(s.dsn)
    assert seen == {"make_client_calls": 1, "dyn_engine_at_connect": False}


@pytest.mark.asyncio
async def test_v11_write_failure_is_fatal_and_no_client(storage_v11, monkeypatch, caplog):
    s = storage_v11
    await s.set_runtime_dynamic_engine(True)
    seen = {}
    _install_fake_client(monkeypatch, s.dsn, seen)

    async def broken(self, on):
        raise asyncpg.exceptions.InsufficientPrivilegeError("denied")

    monkeypatch.setattr(Storage, "set_runtime_dynamic_engine", broken)
    with pytest.raises(run.StartupError, match="dyn_engine=false not recorded"):
        await _start(s.dsn)
    assert "make_client_calls" not in seen


@pytest.mark.asyncio
async def test_v11_unconfirmed_write_is_fatal(storage_v11, monkeypatch):
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute(
            "CREATE FUNCTION keep_true() RETURNS trigger AS $$ BEGIN NEW.dyn_engine := TRUE; "
            "RETURN NEW; END $$ LANGUAGE plpgsql; "
            "CREATE TRIGGER t_keep BEFORE UPDATE ON runtime_state FOR EACH ROW "
            "EXECUTE FUNCTION keep_true();")
    seen = {}
    _install_fake_client(monkeypatch, s.dsn, seen)
    with pytest.raises(run.StartupError, match="dyn_engine=false not recorded"):
        await _start(s.dsn)
    assert "make_client_calls" not in seen


@pytest.mark.asyncio
async def test_v11_recovers_stale_discovery_run(storage_v11, monkeypatch):
    s = storage_v11
    rid = await s.claim_discovery_run("startup")
    seen = {}
    _install_fake_client(monkeypatch, s.dsn, seen)
    with pytest.raises(run.StartupError):
        await _start(s.dsn)
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT status FROM dialog_discovery_runs WHERE id=$1", rid) == "failed"


@pytest.mark.asyncio
async def test_pre_v11_startup_is_plain_legacy(storage_v10, monkeypatch, caplog):
    """Exercises the schema<11 branch of _select_engine. We freeze schema at
    v10 by replacing apply_migrations with a v10-only applier, so the test
    sees run.py behave as it would on a DB that has not yet been upgraded."""
    from tests._dyn_support import apply_up_to

    async def apply_up_to_10(pool, migrations_dir="migrations"):
        s_ = Storage(s.dsn)
        s_.pool = pool
        try:
            await apply_up_to(s_, 10)
        finally:
            s_.pool = None
    monkeypatch.setattr(run, "apply_migrations", apply_up_to_10)

    s = storage_v10
    seen = {}
    _install_fake_client(monkeypatch, s.dsn, seen)
    caplog.set_level(logging.DEBUG)
    with pytest.raises(run.StartupError, match="telegram unavailable"):
        await _start(s.dsn)
    assert seen == {"make_client_calls": 1, "dyn_engine_at_connect": "no-column"}
    assert "UndefinedTable" not in caplog.text and "UndefinedColumn" not in caplog.text
    assert "dyn_engine" not in caplog.text and "discovery" not in caplog.text
    assert await s.schema_version() == 10
