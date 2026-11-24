"""Dynamic engine discovery loop: both passes, atomic publish, failures,
FloodWait, cancellation, request handling, reconcile hand-off."""
import asyncio

import pytest
from telethon import errors
from telethon.tl.types import (
    Channel, ChannelForbidden, Chat, ChatForbidden, ChatPhotoEmpty, User,
)

from tests._dyn_engine_support import (  # noqa: F401
    Dialog, Harness, ch, harness, storage_v11, wait_until,
)
from tests._dyn_support import add_dialog, set_config, version
from tgparser.dynamic import classify_entity


def mega(n, title="m"):
    return Channel(id=n, title=title, photo=ChatPhotoEmpty(), date=None, megagroup=True)


def bcast(n, title="b"):
    return Channel(id=n, title=title, photo=ChatPhotoEmpty(), date=None, broadcast=True)


async def _live(s):
    async with s.pool.acquire() as con:
        return await con.fetchval(
            "SELECT md5(COALESCE(string_agg(d::text,'|' ORDER BY chat_id),'')) FROM discovered_dialogs d")


async def _run_row(s, rid):
    async with s.pool.acquire() as con:
        return await con.fetchrow("SELECT * FROM dialog_discovery_runs WHERE id=$1", rid)


def test_classify_entity():
    assert classify_entity(bcast(1)) == "channel"
    assert classify_entity(mega(1)) == "megagroup"
    assert classify_entity(Channel(id=1, title="g", photo=ChatPhotoEmpty(), date=None,
                                   gigagroup=True)) == "megagroup"
    assert classify_entity(Chat(id=1, title="g", photo=ChatPhotoEmpty(),
                                participants_count=3, date=None, version=1)) == "group"
    assert classify_entity(ChatForbidden(id=1, title="x")) == "forbidden"
    assert classify_entity(ChannelForbidden(id=1, access_hash=1, title="x")) == "forbidden"
    assert classify_entity(User(id=1, is_self=True)) == "saved"
    assert classify_entity(User(id=2, bot=True)) == "bot"
    assert classify_entity(User(id=3)) == "private"
    assert classify_entity(object()) == "unknown"


@pytest.mark.asyncio
async def test_publish_only_after_both_passes(harness):
    s = harness.storage
    await add_dialog(s, ch(7), title="old")              # will be gone after the run
    c = harness.client
    c.dialogs = {False: [Dialog(mega(1, "A")), Dialog(bcast(2, "B"))],
                 True: [Dialog(mega(3, "C"))]}
    reached, release = asyncio.Event(), asyncio.Event()

    async def hook(archived, i):
        if archived and i == 0:
            reached.set()
            await release.wait()
    c.dialog_hook = hook
    e = harness.build()
    rid = await s.claim_discovery_run("scheduled")
    live0, v0 = await _live(s), await version(s)
    task = asyncio.create_task(e.run_discovery(rid))
    await asyncio.wait_for(reached.wait(), 5)
    r = await _run_row(s, rid)
    assert (r["normal_pass_done"], r["archived_pass_done"], r["status"]) == (True, False, "running")
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT COUNT(*) FROM dialog_discovery_items WHERE run_id=$1", rid) == 2
    assert await _live(s) == live0 and await version(s) == v0
    release.set()
    assert await task == (True, 0.0)
    async with s.pool.acquire() as con:
        rows = {r["chat_id"]: r for r in await con.fetch("SELECT * FROM discovered_dialogs")}
    assert rows[ch(1)]["category"] == "megagroup" and not rows[ch(1)]["is_archived"]
    assert rows[ch(2)]["category"] == "channel"
    assert rows[ch(3)]["is_archived"] is True
    assert rows[ch(7)]["gone_at"] is not None
    assert await version(s) == v0 + 1
    assert e._reconcile_event.is_set()


@pytest.mark.asyncio
async def test_archived_pass_failure_leaves_live_snapshot(harness):
    s = harness.storage
    await add_dialog(s, ch(7), title="old")
    c = harness.client
    c.dialogs = {False: [Dialog(mega(1))], True: [Dialog(mega(3))]}

    async def hook(archived, i):
        if archived:
            raise ConnectionError("dropped")
    c.dialog_hook = hook
    e = harness.build()
    rid = await s.claim_discovery_run("scheduled")
    live0, v0 = await _live(s), await version(s)
    ok, retry = await e.run_discovery(rid)
    assert (ok, retry) == (False, 60.0)
    r = await _run_row(s, rid)
    assert r["status"] == "failed" and r["error"] == "ConnectionError"
    assert await _live(s) == live0 and await version(s) == v0
    ctl = await s.get_discovery_control()
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT $1 > NOW() + interval '50 seconds'", ctl["next_allowed_at"])
    ok, retry = await e.run_discovery(await _reclaim(s))
    assert retry == 120.0                                       # bounded exponential


async def _reclaim(s):
    async with s.pool.acquire() as con:
        await con.execute("UPDATE discovery_control SET next_allowed_at=NULL")
    return await s.claim_discovery_run("scheduled")


@pytest.mark.asyncio
async def test_floodwait_backoff(harness):
    s = harness.storage
    c = harness.client
    c.dialogs = {False: [Dialog(mega(1))], True: []}

    async def hook(archived, i):
        raise errors.FloodWaitError(request=None, capture=300)
    c.dialog_hook = hook
    e = harness.build()
    rid = await s.claim_discovery_run("scheduled")
    assert await e.run_discovery(rid) == (False, 305.0)
    assert (await _run_row(s, rid))["error"] == "FloodWaitError"


@pytest.mark.asyncio
async def test_shutdown_cancels_running_discovery(harness):
    s = harness.storage
    await add_dialog(s, ch(7), title="old")
    c = harness.client
    reached = asyncio.Event()
    c.dialogs = {False: [Dialog(mega(1))], True: [Dialog(mega(3))]}

    async def hook(archived, i):
        if archived:
            reached.set()
            await asyncio.Event().wait()
    c.dialog_hook = hook
    live0, v0 = await _live(s), await version(s)
    e = await harness.start()
    await asyncio.wait_for(reached.wait(), 5)
    await e.stop()
    async with s.pool.acquire() as con:
        r = await con.fetchrow("SELECT status, error FROM dialog_discovery_runs")
    assert tuple(r) == ("cancelled", "cancelled: shutdown")
    assert await _live(s) == live0 and await version(s) == v0


@pytest.mark.asyncio
async def test_request_triggers_run_and_publish_triggers_reconcile(harness):
    s = harness.storage
    c = harness.client
    c.dialogs = {False: [Dialog(mega(1, "A"))], True: []}
    await set_config(s, mode="all")
    e = await harness.start()

    async def runs(status="complete"):
        async with s.pool.acquire() as con:
            return await con.fetchval(
                "SELECT COUNT(*) FROM dialog_discovery_runs WHERE status=$1", status)
    await wait_until(lambda: runs(), what="startup run")
    await harness.wait_applied()
    assert e.active_ids() == {ch(1)}                 # all-mode reconcile after publish
    c.dialogs = {False: [Dialog(mega(1, "A")), Dialog(bcast(2, "B"))], True: []}
    async with s.pool.acquire() as con:
        await con.execute("UPDATE discovery_control SET next_allowed_at=NULL")
    await s.request_discovery("ui")
    await wait_until(lambda: e.active_ids() == {ch(1), ch(2)}, what="new dialog collected")
    ctl = await s.get_discovery_control()
    assert ctl["requested_seq"] == ctl["consumed_seq"] == 1
    async with s.pool.acquire() as con:
        kinds = [r["trigger_kind"] for r in await con.fetch(
            "SELECT trigger_kind FROM dialog_discovery_runs ORDER BY id")]
    assert kinds == ["startup", "request"]


@pytest.mark.asyncio
async def test_request_waits_while_a_run_is_in_progress(harness):
    s = harness.storage
    c = harness.client
    gate = asyncio.Event()
    c.dialogs = {False: [Dialog(mega(1))], True: []}

    async def hook(archived, i):
        await gate.wait()
    c.dialog_hook = hook
    await harness.start()
    await wait_until(lambda: _running(s), what="startup run running")
    await s.request_discovery("ui")
    ctl = await s.get_discovery_control()
    assert ctl["consumed_seq"] == 0                     # cannot be claimed concurrently
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs") == 1
    c.dialog_hook = None
    gate.set()
    async with s.pool.acquire() as con:
        await wait_until(lambda: _complete(s), what="startup complete")
        await con.execute("UPDATE discovery_control SET next_allowed_at=NULL")

    async def consumed():
        return (await s.get_discovery_control())["consumed_seq"] == 1
    await wait_until(consumed, what="request consumed")


async def _running(s):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs WHERE status='running'")


async def _complete(s):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs WHERE status='complete'")
