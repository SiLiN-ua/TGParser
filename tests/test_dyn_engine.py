"""Dynamic engine: readiness, reconcile transitions, live-event filtering,
deletions, NOTIFY/polling, disable ACK, mass-gone, YAML worker, tasks."""
import asyncio
from datetime import datetime, timezone

import asyncpg
import pytest
from telethon import errors
from telethon.tl.types import PeerChannel

from tests._dyn_engine_support import (  # noqa: F401
    Dialog, FakeMessage, Harness, ch, harness, storage_v11, wait_until,
)
from tests._dyn_support import add_dialog, add_pref, add_target, set_config

DATA_TABLES = ("messages", "message_versions", "message_media", "message_reactions",
               "message_reaction_authors", "memberships", "chat_actions",
               "user_observations", "users", "user_profile_queue", "deletion_events",
               "backfill_queue", "errors")


async def data_snapshot(s, inactive=()):
    """Row hashes of every data table, plus the state rows of the given
    (inactive) chats. Engine/coordinator state of ACTIVE targets legitimately
    changes while the engine runs and is excluded."""
    out = {}
    async with s.pool.acquire() as con:
        for t in DATA_TABLES:
            out[t] = await con.fetchval(
                f"SELECT md5(COALESCE(string_agg(x::text,'|' ORDER BY x::text),'')) FROM {t} x")
        for t in ("sync_state", "target_state", "coordinator_progress"):
            out[t] = await con.fetchval(
                f"SELECT md5(COALESCE(string_agg(x::text,'|' ORDER BY x::text),'')) "
                f"FROM {t} x WHERE chat_id = ANY($1::bigint[])", list(inactive))
    return out

A, B, C = ch(1111), ch(2222), ch(3333)


async def _enabled(s):
    async with s.pool.acquire() as con:
        return {r["chat_id"] for r in await con.fetch(
            "SELECT chat_id FROM target_state WHERE enabled")}


async def _count(s, table, where="TRUE", *args):
    async with s.pool.acquire() as con:
        return await con.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", *args)


# --- startup / readiness ----------------------------------------------------------

@pytest.mark.asyncio
async def test_ready_only_after_everything_is_up(harness, monkeypatch):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = harness.build()
    seen = {}
    orig = s.set_runtime_dynamic_engine

    async def spy(on):
        if on:
            seen.update(
                active=set(e.active_ids()),
                enabled=await _enabled(s),
                handlers=len(harness.client.handlers),
                tasks={n: not t.done() for n, t in e._tasks.items()},
                coordinator=not harness.collector._coordinator_task.done(),
                listener=e.listener_connects)
        await orig(on)
    monkeypatch.setattr(s, "set_runtime_dynamic_engine", spy)
    await harness.start()
    assert seen == {"active": {A}, "enabled": {A}, "handlers": 5,
                    "tasks": {"dyn-listener": True, "dyn-reconciler": True,
                              "dyn-discovery": True, "dyn-yaml": True},
                    "coordinator": True, "listener": 1}
    assert (await s.get_runtime())["dyn_engine"] is True
    assert e.state == "ready"


@pytest.mark.asyncio
async def test_handlers_have_no_static_chat_filter(harness):
    await harness.start()
    kinds = sorted(type(b).__name__ for b, _ in harness.client.handlers)
    assert kinds == ["ChatAction", "MessageDeleted", "MessageEdited", "NewMessage", "Raw"]
    for b, _ in harness.client.handlers:
        assert getattr(b, "chats", None) is None


@pytest.mark.asyncio
async def test_initial_reconcile_failure_aborts_and_leaves_flag_false(harness, monkeypatch):
    s = harness.storage
    await s.set_runtime_dynamic_engine(False)

    async def boom():
        raise asyncpg.exceptions.InsufficientPrivilegeError("x")
    monkeypatch.setattr(s, "policy_snapshot", boom)
    e = harness.build()
    with pytest.raises(Exception, match="initial reconcile failed"):
        await e.start()
    assert (await s.get_runtime())["dyn_engine"] is False
    assert e.state == "failed" and not e._tasks and not harness.client.handlers


@pytest.mark.asyncio
async def test_listener_failure_aborts_startup(harness, monkeypatch):
    import tgparser.dynamic as dyn
    s = harness.storage

    async def no_connect(*a, **k):
        raise OSError("refused")
    monkeypatch.setattr(dyn.asyncpg, "connect", no_connect)
    e = harness.build(engine_opts={"dynamic_ready_timeout_seconds": 0.5})
    with pytest.raises(Exception, match="listener not ready"):
        await e.start()
    assert (await s.get_runtime())["dyn_engine"] is False
    assert all(t.done() for t in asyncio.all_tasks() if t.get_name().startswith("dyn-"))


@pytest.mark.asyncio
async def test_zero_targets_idle_then_added_without_restart(harness):
    s = harness.storage
    e = await harness.start()
    assert e.active_ids() == frozenset()
    assert not harness.collector._coordinator_task.done()
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.wait_applied()
    assert e.active_ids() == {A} and await _enabled(s) == {A}
    assert all(not t.done() for t in e._tasks.values())


# --- transitions --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_selected_all_category_exclude_transitions(harness):
    s = harness.storage
    from telethon.tl.types import Channel, ChatPhotoEmpty
    harness.client.dialogs = {False: [
        Dialog(Channel(id=1111, title="Alpha", photo=ChatPhotoEmpty(), date=None, megagroup=True)),
        Dialog(Channel(id=2222, title="Casino news", photo=ChatPhotoEmpty(), date=None, broadcast=True)),
        Dialog(Channel(id=3333, title="Gamma", photo=ChatPhotoEmpty(), date=None, megagroup=True))],
        True: []}
    e = await harness.start()
    await wait_until(lambda: _count(s, "dialog_discovery_runs", "status='complete'"),
                     what="startup discovery")
    await add_pref(s, A, selected=True)
    await harness.wait_applied()
    assert e.active_ids() == {A}
    await set_config(s, mode="all")
    await harness.wait_applied()
    assert e.active_ids() == {A, B, C}
    await set_config(s, category_channel=False)
    await harness.wait_applied()
    assert e.active_ids() == {A, C}
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO collection_title_rules (action,pattern) VALUES ('exclude','gamma')")
    await harness.wait_applied()
    assert e.active_ids() == {A}
    await s.upsert_target_preference(A, selected=False, excluded=True)
    await harness.wait_applied()
    assert e.active_ids() == frozenset() and await _enabled(s) == set()
    await set_config(s, mode="selected")
    await s.upsert_target_preference(A, excluded=False, selected=True)
    await harness.wait_applied()
    assert e.active_ids() == {A}


@pytest.mark.asyncio
async def test_all_mode_awaiting_discovery_keeps_known_targets(harness):
    s = harness.storage
    await add_target(s, A, True)
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await add_target(s, B, True)           # enabled, unknown to policy
    await set_config(s, mode="all")
    e = await harness.start()
    assert e.active_ids() == {A, B}
    assert e.last_reconcile["state"] == "awaiting_discovery"
    assert set(e.last_reconcile["held"]) == {B}
    ps = await s.get_policy_state()
    assert ps["applied_version"] < ps["requested_version"]
    assert ps["reconcile_state"] == "awaiting_discovery"
    assert await _enabled(s) == {A, B}


@pytest.mark.asyncio
async def test_disable_reenable_preserves_checkpoint(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await add_target(s, A, True)
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO sync_state (chat_id, last_backfilled_message_id, "
                          "first_message_id) VALUES ($1, 50, 1)", A)
    e = await harness.start()
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    assert e.active_ids() == frozenset()
    st = await s.get_sync_state(A)
    assert st["last_backfilled_message_id"] == 50
    async with s.pool.acquire() as con:
        r = await con.fetchrow("SELECT enabled, disabled_reason FROM target_state WHERE chat_id=$1", A)
    assert r["enabled"] is False and r["disabled_reason"] == "policy:not_selected"
    harness.client.messages[A] = [FakeMessage(i, A) for i in (49, 50, 51, 52)]
    await s.upsert_target_preference(A, selected=True)
    await harness.wait_applied()
    await wait_until(lambda: s.get_sync_state(A), what="sync state")
    await wait_until(lambda: _count(s, "messages", "chat_id=$1", A), what="backfill")

    async def at52():
        return (await s.get_sync_state(A))["last_backfilled_message_id"] == 52
    await wait_until(at52, what="checkpoint 52")
    assert await _count(s, "messages", "chat_id=$1 AND message_id<=50", A) == 0


# --- live events ------------------------------------------------------------------------

class _Ev:
    def __init__(self, **kw):
        self.__dict__.update(kw)


@pytest.mark.asyncio
async def test_inactive_live_events_write_nothing(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.start()
    before = await data_snapshot(s, inactive=[B])
    counts = {t: await _count(s, t) for t in ("users", "user_observations", "user_profile_queue",
                                              "memberships", "chat_actions", "message_reactions",
                                              "messages", "sync_state", "errors")}
    msg = FakeMessage(7, B, from_user=777)
    await harness.handler("NewMessage")(_Ev(message=msg))
    await harness.handler("MessageEdited")(_Ev(message=msg))
    await harness.handler("ChatAction")(_Ev(chat_id=B, user_id=777, users=[777],
                                             action_message=None, user_left=False,
                                             user_kicked=False))
    upd = type("UpdateMessageReactions", (), {})()
    upd.peer, upd.msg_id, upd.reactions = PeerChannel(channel_id=2222), 7, None
    await harness.handler("Raw")(upd)
    await harness.handler("MessageDeleted")(_Ev(chat_id=B, deleted_ids=[7]))
    await harness.collector.wait_background(timeout=1)
    assert await data_snapshot(s, inactive=[B]) == before
    assert {t: await _count(s, t) for t in counts} == counts


@pytest.mark.asyncio
async def test_active_live_message_is_stored(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.start()
    await harness.handler("NewMessage")(_Ev(message=FakeMessage(9, A, text="live")))
    assert await _count(s, "messages", "chat_id=$1 AND text='live'", A) == 1


@pytest.mark.asyncio
async def test_live_write_after_disable_is_rejected_quietly(harness, caplog):
    """An event that passed the active-set check before the ACK hits the guard
    afterwards: TGP01 is swallowed (no ERROR, no errors row)."""
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_applied()
    e._active = frozenset({A})        # simulate the stale pre-ACK view
    await harness.handler("NewMessage")(_Ev(message=FakeMessage(3, A)))
    assert await _count(s, "messages") == 0
    assert await _count(s, "errors") == 0
    assert not [r for r in caplog.records if r.levelname == "ERROR"]


# --- MessageDeleted -------------------------------------------------------------------------

P1, P2, P3 = 501, 502, 503        # private chats (positive ids)


async def _msg(s, chat, mid):
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO messages (chat_id, message_id, text) VALUES ($1,$2,'x')",
                          chat, mid)


async def _deleted(s):
    async with s.pool.acquire() as con:
        return {(r["chat_id"], r["message_id"]) for r in await con.fetch(
            "SELECT chat_id, message_id FROM messages WHERE is_deleted")}


@pytest.mark.asyncio
async def test_deleted_with_chat_id(harness):
    s = harness.storage
    for c in (A, B):
        await add_dialog(s, c)
    await add_pref(s, A, selected=True)
    for c in (A, B):
        await _msg(s, c, 1)          # written before the guard is armed
    await harness.start()
    total = await _count(s, "messages")
    await harness.handler("MessageDeleted")(_Ev(chat_id=A, deleted_ids=[1, 99]))
    await harness.handler("MessageDeleted")(_Ev(chat_id=B, deleted_ids=[1]))
    assert await _deleted(s) == {(A, 1)}
    assert await _count(s, "messages") == total             # no stub for 99
    assert await _count(s, "deletion_events") == 0


@pytest.mark.asyncio
async def test_deleted_without_chat_id(harness):
    s = harness.storage
    for c in (P1, P2, P3, A):
        await add_dialog(s, c, category="private" if c > 0 else "megagroup")
    for c in (P1, P2, A):
        await add_pref(s, c, selected=True)
    await _msg(s, P1, 10)          # unique in an active private chat
    await _msg(s, P1, 20); await _msg(s, P2, 20)   # ambiguous between two active chats
    await _msg(s, P3, 30)          # only in an INACTIVE private chat
    await _msg(s, A, 40)           # channel ids are never matched without chat id
    await harness.start()
    total = await _count(s, "messages")
    await harness.handler("MessageDeleted")(_Ev(chat_id=None, deleted_ids=[10, 20, 30, 40, 50]))
    assert await _deleted(s) == {(P1, 10)}
    assert await _count(s, "messages") == total
    assert await _count(s, "deletion_events") == 0


@pytest.mark.asyncio
async def test_deleted_without_chat_id_no_private_targets(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await _msg(s, P3, 1)
    await harness.start()
    before = await data_snapshot(s, inactive=[P3])
    await harness.handler("MessageDeleted")(_Ev(chat_id=None, deleted_ids=[1]))
    assert await data_snapshot(s, inactive=[P3]) == before


# --- NOTIFY / polling / listener ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_notify_accelerates_reconcile(harness):
    s = harness.storage
    e = await harness.start(engine_opts={"dynamic_poll_seconds": 30})
    n0 = e.notifications
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.wait_applied(timeout=5)       # far below the 30 s poll
    assert e.notifications > n0 and e.active_ids() == {A}


@pytest.mark.asyncio
async def test_lost_notify_compensated_by_polling(harness):
    s = harness.storage
    e = await harness.start(engine_opts={"dynamic_poll_seconds": 0.1})
    await e._listener_con.remove_listener("tgp_policy", e._on_notify)
    n0 = e.notifications
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.wait_applied()
    assert e.notifications == n0 and e.active_ids() == {A}


@pytest.mark.asyncio
async def test_listener_reconnects_after_backend_killed(harness):
    s = harness.storage
    e = await harness.start(engine_opts={"dynamic_poll_seconds": 0.1})
    pid = e._listener_con.get_server_pid()
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT pg_terminate_backend($1)", pid)
    await wait_until(lambda: e.listener_connects >= 2, what="listener reconnect")
    assert e._listener_con.get_server_pid() != pid
    n0 = e.notifications
    await add_pref(s, A, selected=True)
    await wait_until(lambda: e.notifications > n0, what="notify after reconnect")


# --- partial reconcile ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_partial_reconcile_does_not_advance_applied(harness, monkeypatch):
    s = harness.storage
    for c in (A, B):
        await add_dialog(s, c); await add_pref(s, c, selected=True)
    e = await harness.start()
    applied0 = (await s.get_policy_state())["applied_version"]
    real = s.disable_target_ack
    fail = {"left": 1}

    async def flaky(cid, reason, lock_timeout_ms=30000):
        if fail["left"]:
            fail["left"] -= 1
            raise asyncpg.exceptions.LockNotAvailableError("lock timeout")
        await real(cid, reason, lock_timeout_ms=lock_timeout_ms)
    monkeypatch.setattr(s, "disable_target_ack", flaky)
    async with s.pool.acquire() as con:
        await con.execute("UPDATE target_preferences SET selected=FALSE")   # one statement
    await harness.wait_applied()
    ps = await s.get_policy_state()
    assert ps["applied_version"] == ps["requested_version"] == applied0 + 1
    assert e.active_ids() == frozenset() and await _enabled(s) == set()
    assert e.reconcile_count >= 2


@pytest.mark.asyncio
async def test_partial_reconcile_state_visible(harness, monkeypatch):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()

    async def broken(*a, **k):
        raise asyncpg.exceptions.LockNotAvailableError("lock timeout")
    monkeypatch.setattr(s, "disable_target_ack", broken)
    await s.upsert_target_preference(A, selected=False)
    await harness.wait_reconciled_state("error")
    ps = await s.get_policy_state()
    assert ps["applied_version"] < ps["requested_version"]
    assert ps["reconcile_state"] == "error"
    assert "LockNotAvailableError" in ps["reconcile_error"]
    assert e.active_ids() == frozenset()                 # removed from the active set first
    assert await _enabled(s) == {A}                      # but no ACK yet
    res = await s.policy_dry_run()
    assert res["reconcile"]["state"] == "error"


# --- disable ACK ----------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_disable_ack_waits_for_inflight_write_then_blocks(harness):
    s = harness.storage
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    e = await harness.start()
    writer = await asyncpg.connect(s.dsn)
    tx = writer.transaction()
    await tx.start()
    await writer.execute("INSERT INTO messages (chat_id, message_id) VALUES ($1, 1)", A)
    try:
        await s.upsert_target_preference(A, selected=False)

        async def waiting():
            async with s.pool.acquire() as con:
                return await con.fetchval(
                    "SELECT COUNT(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted")
        await wait_until(waiting, what="disable waiting on advisory lock")
        assert not e.is_active(A)                      # step 1 done before the ACK
        assert await _enabled(s) == {A}                # ACK not given yet
    finally:
        await tx.commit()
    await harness.wait_applied()
    assert await _enabled(s) == set()
    assert await _count(s, "messages", "chat_id=$1", A) == 1   # in-flight write survived
    with pytest.raises(asyncpg.exceptions.PostgresError) as exc:
        await writer.execute("INSERT INTO messages (chat_id, message_id) VALUES ($1, 2)", A)
    assert exc.value.sqlstate == "TGP01"
    await writer.close()


# --- mass-gone ------------------------------------------------------------------------------------

async def _ten_targets_three_gone(s):
    ids = [ch(9000 + i) for i in range(10)]
    for c in ids:
        await add_dialog(s, c); await add_pref(s, c, selected=True)
    return ids


@pytest.mark.asyncio
async def test_mass_gone_blocked_until_exact_confirmation(harness):
    s = harness.storage
    ids = await _ten_targets_three_gone(s)
    e = await harness.start()
    assert e.active_ids() == set(ids)
    async with s.pool.acquire() as con:
        await con.execute("UPDATE discovered_dialogs SET gone_at=NOW() WHERE chat_id = ANY($1)", ids[:3])
    await add_pref(s, ch(1), selected=False)                 # any bump triggers reconcile
    await harness.wait_reconciled_state("mass_gone_blocked")
    assert e.active_ids() == set(ids) and await _enabled(s) == set(ids)
    ps = await s.get_policy_state()
    assert ps["applied_version"] < ps["requested_version"]
    ver, fp = ps["blocked_version"], ps["blocked_fingerprint"]

    # Stale confirmation: the admin confirms (ver, fp) and the policy moves in
    # the same instant (one transaction), before any reconcile sees it.
    async with s.pool.acquire() as con:
        async with con.transaction():
            await con.execute(
                "UPDATE policy_state SET confirmed_version=$1, confirmed_fingerprint=$2, "
                "confirmed_at=NOW(), confirmed_by='admin_cli'", ver, fp)
            await con.execute(
                "INSERT INTO target_preferences (chat_id, selected) VALUES ($1, FALSE)", ch(2))
    await harness.wait_reconciled_state("mass_gone_blocked")
    assert (await s.get_policy_state())["blocked_version"] == ver + 1
    assert await _enabled(s) == set(ids)

    ps = await s.get_policy_state()
    assert await s.confirm_mass_gone(ps["blocked_version"], ps["blocked_fingerprint"])
    await harness.wait_applied()
    assert e.active_ids() == set(ids[3:]) and await _enabled(s) == set(ids[3:])
    ps = await s.get_policy_state()
    assert ps["blocked_version"] is None and ps["confirmed_version"] is not None


# --- YAML worker ------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_yaml_worker_semantics(harness):
    s = harness.storage
    from telethon.tl.types import Channel, ChatPhotoEmpty

    def chan(n, title):
        return Channel(id=n, title=title, photo=ChatPhotoEmpty(), date=None,
                       megagroup=True, access_hash=1)
    fw = errors.FloodWaitError(request=None, capture=120)
    harness.client.dialogs = {False: [], True: []}
    harness.client.resolve = {
        "@ok": chan(4401, "Ok"), "@manual": chan(4402, "Manual"),
        "@flaky": ConnectionError("reset"), "@flood": fw,
        "@bad": ValueError("No user has \"bad\" as username"),
    }
    await add_pref(s, ch(4402), excluded=True)              # user's later manual choice
    e = harness.build(engine_opts={"dynamic_yaml_poll_seconds": 3600, "dynamic_yaml_batch": 10},
                      yaml_targets=["@ok", "@manual", "@flaky", "@flood", "@bad"])
    rows = {}
    for raw in ("@ok", "@manual", "@flaky", "@flood", "@bad"):
        await s.yaml_import_upsert(raw)
    for r in await s.yaml_import_due(10):
        rows[r["raw_target"]] = await e.resolve_yaml(r)
    assert rows == {"@ok": "selected", "@manual": "selected", "@flaky": "retry",
                    "@flood": "retry", "@bad": "terminal"}
    async with s.pool.acquire() as con:
        y = {r["raw_target"]: r for r in await con.fetch("SELECT * FROM yaml_target_imports")}
        p = {r["chat_id"]: r for r in await con.fetch("SELECT * FROM target_preferences")}
        delay = await con.fetchval(
            "SELECT EXTRACT(EPOCH FROM next_retry_at - NOW()) FROM yaml_target_imports "
            "WHERE raw_target='@flood'")
    assert p[ch(4401)]["selected"] is True and p[ch(4401)]["updated_by"] == "yaml_import"
    assert p[ch(4402)]["excluded"] is True and p[ch(4402)]["selected"] is False
    assert y["@flaky"]["attempts"] == 1 and y["@flaky"]["last_error_class"] == "ConnectionError"
    assert 100 < delay <= 125
    assert y["@bad"]["status"] == "terminal" and y["@bad"]["last_error_class"] == "ValueError"
    assert [r["raw_target"] for r in await s.yaml_import_due(10)] == []      # nothing due yet

    # re-import on next start does not resurrect anything
    await e.start()
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT COUNT(*) FROM yaml_target_imports") == 5
        assert await con.fetchval(
            "SELECT excluded FROM target_preferences WHERE chat_id=$1", ch(4402)) is True


@pytest.mark.asyncio
async def test_yaml_max_attempts_becomes_terminal(harness):
    s = harness.storage
    harness.client.resolve = {"@flaky": ConnectionError("reset")}
    harness.client.dialogs = {False: [], True: []}
    e = harness.build(engine_opts={"dynamic_yaml_max_attempts": 2})
    await s.yaml_import_upsert("@flaky")
    row = (await s.yaml_import_due(5))[0]
    assert await e.resolve_yaml(row) == "retry"
    async with s.pool.acquire() as con:
        await con.execute("UPDATE yaml_target_imports SET next_retry_at=NOW()")
    row = (await s.yaml_import_due(5))[0]
    assert await e.resolve_yaml(row) == "terminal"
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT last_error_class FROM yaml_target_imports") == "MaxAttempts"


@pytest.mark.asyncio
async def test_yaml_loop_respects_batch_limit(harness):
    s = harness.storage
    harness.client.dialogs = {False: [], True: []}
    for i in range(7):
        await s.yaml_import_upsert(f"@t{i}")
    e = await harness.start(engine_opts={"dynamic_yaml_batch": 3, "dynamic_yaml_poll_seconds": 3600})

    async def three_done():
        async with s.pool.acquire() as con:
            return await con.fetchval(
                "SELECT COUNT(*) FROM yaml_target_imports WHERE status='terminal'") == 3
    await wait_until(three_done, what="first yaml batch")
    async with s.pool.acquire() as con:
        assert await con.fetchval(
            "SELECT COUNT(*) FROM yaml_target_imports WHERE status='pending'") == 4
    assert e.state == "ready"


# --- tracked side jobs / shutdown -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_participant_sync_is_tracked_and_cancelled(harness, monkeypatch):
    s = harness.storage
    started = asyncio.Event()

    async def slow_sync(cid):
        started.set()
        await asyncio.Event().wait()
    e = harness.build(collector_opts={"participants_sync": True})
    monkeypatch.setattr(harness.collector, "sync_participants", slow_sync)
    await e.start()
    await add_dialog(s, A); await add_pref(s, A, selected=True)
    await harness.wait_applied()
    await asyncio.wait_for(started.wait(), 5)
    names = {t.get_name() for t in e._chat_tasks}
    assert f"participants:{A}" in names
    assert harness.engine.leases.inflight(A) >= 1
    await harness.close()
    harness.engine = harness.collector = harness.media_q = None
    assert not [t for t in asyncio.all_tasks()
                if t.get_name().startswith(("participants:", "dyn-", "backfill-coordinator"))
                and not t.done()]


@pytest.mark.asyncio
async def test_stop_is_idempotent_and_leaves_no_tasks(harness, caplog):
    e = await harness.start()
    await e.stop()
    await e.stop()
    assert e.state == "stopped" and not e._tasks
    assert e._listener_con is None
    assert not [r for r in caplog.records if r.levelname in ("ERROR", "CRITICAL")]
