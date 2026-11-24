"""v11 draft: crash-safe single-run claim, staged atomic snapshot, mass-gone."""
import asyncio

import asyncpg
import pytest

from tests._dyn_support import (  # noqa: F401
    NotifyCounter, add_dialog, add_target, policy, snapshot, storage_v11,
    unthrottle, version,
)
from tgparser.storage import Storage


async def _running_count(s):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs WHERE status='running'")


async def _runs(s):
    async with s.pool.acquire() as con:
        return await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs")


async def _live(s):
    async with s.pool.acquire() as con:
        return await con.fetchval(
            "SELECT md5(COALESCE(string_agg(d::text,'|' ORDER BY chat_id),'')) FROM discovered_dialogs d")


async def _full_run(s, items, trigger="scheduled"):
    rid = await s.claim_discovery_run(trigger)
    assert rid is not None
    await s.stage_discovery_items(rid, items)
    assert await s.mark_discovery_pass_done(rid, "normal")
    assert await s.mark_discovery_pass_done(rid, "archived")
    return rid


# --- claim ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_scheduled_sets_spacing_not_seq(storage_v11):
    s = storage_v11
    rid = await s.claim_discovery_run("scheduled")
    ctl = await s.get_discovery_control()
    assert ctl["last_run_id"] == rid and ctl["consumed_seq"] == 0
    assert ctl["next_allowed_at"] is not None


@pytest.mark.asyncio
async def test_request_consumed_once(storage_v11):
    s = storage_v11
    assert await s.claim_discovery_run("request") is None       # nothing requested
    await s.request_discovery("ui")
    rid = await s.claim_discovery_run("request")
    assert rid is not None
    await s.finish_discovery_run_fail(rid, "x")
    await unthrottle(s)
    assert await s.claim_discovery_run("request") is None       # same seq already consumed
    ctl = await s.get_discovery_control()
    assert (ctl["requested_seq"], ctl["consumed_seq"]) == (1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kinds", [
    ["request"] * 8,
    ["scheduled"] * 8,
    ["request", "scheduled"] * 4,
    ["startup", "request", "scheduled", "startup"] * 2,
])
async def test_concurrent_claims_single_run(storage_v11, kinds):
    s = storage_v11
    await s.request_discovery("ui")
    res = await asyncio.gather(*[s.claim_discovery_run(k) for k in kinds])
    assert len([r for r in res if r is not None]) == 1
    assert await _runs(s) == 1


@pytest.mark.asyncio
async def test_stale_running_blocks_until_recovery(storage_v11):
    s = storage_v11
    rid = await s.claim_discovery_run("startup")      # process "crashes" here
    await unthrottle(s)
    for k in ("scheduled", "startup"):
        assert await s.claim_discovery_run(k) is None
    await s.request_discovery("ui")
    assert await s.claim_discovery_run("request") is None
    assert (await s.get_discovery_control())["consumed_seq"] == 0
    assert await s.recover_stale_discovery_runs() == 1
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT status FROM dialog_discovery_runs WHERE id=$1", rid) == "failed"
    rid2 = await s.claim_discovery_run("request")
    assert rid2 is not None and rid2 != rid
    assert (await s.get_discovery_control())["consumed_seq"] == 1


@pytest.mark.asyncio
async def test_insert_failure_does_not_consume(storage_v11):
    s = storage_v11
    await s.request_discovery("ui")
    async with s.pool.acquire() as con:
        await con.execute(
            "CREATE FUNCTION boom() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$ "
            "LANGUAGE plpgsql;"
            "CREATE TRIGGER t_boom BEFORE INSERT ON dialog_discovery_runs "
            "FOR EACH ROW EXECUTE FUNCTION boom();")
    with pytest.raises(asyncpg.exceptions.RaiseError):
        await s.claim_discovery_run("request")
    ctl = await s.get_discovery_control()
    assert ctl["consumed_seq"] == 0 and ctl["last_run_id"] is None and ctl["next_allowed_at"] is None
    async with s.pool.acquire() as con:
        await con.execute("DROP TRIGGER t_boom ON dialog_discovery_runs")
    assert await s.claim_discovery_run("request") is not None


@pytest.mark.asyncio
async def test_db_allows_only_one_running_row(storage_v11):
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO dialog_discovery_runs (trigger_kind) VALUES ('scheduled')")
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await con.execute("INSERT INTO dialog_discovery_runs (trigger_kind) VALUES ('request')")


@pytest.mark.asyncio
async def test_throttle_after_finish(storage_v11):
    s = storage_v11
    rid = await _full_run(s, [])
    await s.finish_discovery_run_success(rid)
    assert await s.claim_discovery_run("scheduled") is None
    await unthrottle(s)
    assert await s.claim_discovery_run("scheduled") is not None


# --- staging / atomic snapshot ---------------------------------------------------

@pytest.mark.asyncio
async def test_partial_run_then_fail_leaves_live_snapshot_identical(storage_v11):
    s = storage_v11
    await add_dialog(s, -1, title="live")
    live0, v0 = await _live(s), await version(s)
    rid = await s.claim_discovery_run("scheduled")
    assert await s.stage_discovery_items(rid, [
        {"chat_id": -1, "category": "channel", "title": "renamed"},
        {"chat_id": -2, "category": "megagroup", "title": "new"}]) == 2
    assert await s.mark_discovery_pass_done(rid, "normal")
    assert await _live(s) == live0
    assert await policy(s, -2) is None                       # new dialog invisible
    assert await s.finish_discovery_run_fail(rid, "archived pass: FloodWait")
    assert await _live(s) == live0
    assert await version(s) == v0


@pytest.mark.asyncio
async def test_cancel_after_staging_no_bump(storage_v11):
    s = storage_v11
    await add_dialog(s, -1)
    live0, v0 = await _live(s), await version(s)
    rid = await _full_run(s, [{"chat_id": -9, "category": "group"}])
    assert await s.finish_discovery_run_fail(rid, "shutdown", cancelled=True)
    assert await _live(s) == live0 and await version(s) == v0
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT status FROM dialog_discovery_runs WHERE id=$1", rid) == "cancelled"


@pytest.mark.asyncio
async def test_success_requires_both_passes(storage_v11):
    s = storage_v11
    v0 = await version(s)
    rid = await s.claim_discovery_run("scheduled")
    await s.stage_discovery_items(rid, [{"chat_id": -1, "category": "group"}])
    await s.mark_discovery_pass_done(rid, "normal")
    res = await s.finish_discovery_run_success(rid)
    assert res["applied"] is False and res["reason"] == "pass_incomplete"
    assert await version(s) == v0 and await _running_count(s) == 1
    assert await policy(s, -1) is None


@pytest.mark.asyncio
async def test_reappeared_dialog_stays_gone_until_success(storage_v11):
    s = storage_v11
    await add_dialog(s, -3, gone=True)
    await add_dialog(s, -4)                                   # will vanish
    await add_target(s, -3, True)
    rid = await _full_run(s, [{"chat_id": -3, "category": "megagroup", "title": "back"},
                              {"chat_id": -5, "category": "channel", "title": "fresh"}])
    assert (await policy(s, -3))["reason"] == "dialog_gone"
    assert await policy(s, -5) is None
    v0 = await version(s)
    async with NotifyCounter(s.dsn) as n:
        res = await s.finish_discovery_run_success(rid)
        assert await n.settle() == 1
    assert res == {"applied": True, "reason": "complete", "requested_version": v0 + 1,
                   "seen": 2, "gone": 1}
    async with s.pool.acquire() as con:
        g = {r["chat_id"]: r["gone_at"] is not None for r in await con.fetch(
            "SELECT chat_id, gone_at FROM discovered_dialogs")}
        assert g == {-3: False, -4: True, -5: False}
        assert await con.fetchval("SELECT title FROM discovered_dialogs WHERE chat_id=-3") == "back"
        assert await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_items WHERE run_id=$1", rid) == 0
        r = await con.fetchrow("SELECT status, dialogs_seen, gone_detected FROM dialog_discovery_runs WHERE id=$1", rid)
        assert tuple(r) == ("complete", 2, 1)


@pytest.mark.asyncio
async def test_repeat_success_is_noop(storage_v11):
    s = storage_v11
    rid = await _full_run(s, [{"chat_id": -1, "category": "group"}])
    first = await s.finish_discovery_run_success(rid)
    live1, v1 = await _live(s), await version(s)
    second = await s.finish_discovery_run_success(rid)
    assert first["applied"] and not second["applied"]
    assert second["reason"] == "status_complete"
    assert await _live(s) == live1 and await version(s) == v1
    assert not await s.finish_discovery_run_fail(rid, "late")      # cannot fail a complete run
    assert await s.stage_discovery_items(rid, [{"chat_id": -2, "category": "group"}]) == 0


@pytest.mark.asyncio
async def test_failed_run_staging_retention(storage_v11):
    s = storage_v11
    ids = []
    for i in range(5):
        await unthrottle(s)
        rid = await s.claim_discovery_run("scheduled")
        await s.stage_discovery_items(rid, [{"chat_id": -100 - i, "category": "group"}])
        await s.finish_discovery_run_fail(rid, "x")
        ids.append(rid)
    async with s.pool.acquire() as con:
        kept = {r["run_id"] for r in await con.fetch("SELECT DISTINCT run_id FROM dialog_discovery_items")}
    assert kept == set(ids[-Storage.STAGING_RETAIN_FAILED_RUNS:])


@pytest.mark.asyncio
async def test_staging_sanitises_input(storage_v11):
    s = storage_v11
    rid = await s.claim_discovery_run("scheduled")
    await s.stage_discovery_items(rid, [{"chat_id": -1, "category": "weird",
                                         "title": "t" * 999, "username": "u" * 99}])
    async with s.pool.acquire() as con:
        r = await con.fetchrow("SELECT * FROM dialog_discovery_items")
    assert r["category"] == "unknown" and len(r["title"]) == 256 and len(r["username"]) == 64


# --- mass-gone ------------------------------------------------------------------------

@pytest.mark.parametrize("active,gone,expected", [
    (0, 0, False), (0, 10, False),
    (1, 1, True),        # percent branch
    (10, 2, False),      # exactly 20%, count 2
    (10, 3, True),       # percent branch
    (100, 5, False),
    (100, 6, True),      # count branch
    (100, 20, True),     # count branch (20 > 5)
    (100, 21, True),
])
def test_mass_gone_formula(active, gone, expected):
    assert Storage._should_mass_block(gone, active) is expected


async def _targets_with_gone(s, total, gone):
    for i in range(total):
        await add_target(s, -1000 - i, True)
        await add_dialog(s, -1000 - i, gone=i < gone)


@pytest.mark.asyncio
async def test_mass_gone_block_confirm_and_invalidate(storage_v11):
    s = storage_v11
    await _targets_with_gone(s, 10, 3)
    ev = await s.evaluate_mass_gone_block()
    assert ev["state"] == "blocked" and ev["gone"] == 3 and ev["active"] == 10
    assert await s.evaluate_mass_gone_block() == ev            # idempotent, no new history
    assert not await s.confirm_mass_gone(ev["version"], "0" * 64)
    assert not await s.confirm_mass_gone(ev["version"] + 1, ev["fingerprint"])
    with pytest.raises(ValueError):
        await s.confirm_mass_gone(ev["version"], ev["fingerprint"], confirmed_by="ui")
    assert await s.confirm_mass_gone(ev["version"], ev["fingerprint"])
    assert (await s.evaluate_mass_gone_block())["state"] == "confirmed"
    assert Storage.mass_gone_state(await s.get_policy_state()) == "confirmed"

    # Restart (new pool) does not lift anything.
    s2 = Storage(s.dsn)
    await s2.connect()
    try:
        assert Storage.mass_gone_state(await s2.get_policy_state()) == "confirmed"
    finally:
        await s2.close()

    # Any policy change (new version) invalidates the confirmation.
    await s.upsert_target_preference(-1000, note=None, selected=True)
    ev2 = await s.evaluate_mass_gone_block()
    assert ev2["state"] == "blocked" and ev2["version"] == ev["version"] + 1
    ps = await s.get_policy_state()
    assert ps["confirmed_version"] == ev["version"]          # history kept
    assert Storage.mass_gone_state(ps) == "blocked"
    async with s.pool.acquire() as con:
        events = [r["event"] for r in await con.fetch("SELECT event FROM policy_block_history ORDER BY id")]
    assert events == ["blocked", "confirmed", "blocked"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    "INSERT INTO target_preferences (chat_id, selected) VALUES (-1000, TRUE)",
    "UPDATE collection_config SET category_private=TRUE",
])
async def test_confirmation_invalid_immediately_after_policy_change(storage_v11, change):
    s = storage_v11
    await _targets_with_gone(s, 10, 3)
    ev = await s.evaluate_mass_gone_block()
    assert ev["state"] == "blocked"
    assert await s.confirm_mass_gone(ev["version"], ev["fingerprint"])
    assert Storage.mass_gone_state(await s.get_policy_state()) == "confirmed"
    assert (await s.policy_dry_run())["block"]["state"] == "confirmed"

    async with s.pool.acquire() as con:
        await con.execute(change)
    # no evaluate_mass_gone_block() here
    ps = await s.get_policy_state()
    assert ps["requested_version"] == ev["version"] + 1
    assert Storage.mass_gone_state(ps) == "blocked"
    assert (await s.policy_dry_run())["block"]["state"] == "blocked"
    assert ps["confirmed_version"] == ev["version"]
    assert ps["confirmed_fingerprint"] == ev["fingerprint"]
    assert ps["confirmed_by"] == "admin_cli"

    # evaluator: set still needs a block -> refreshed for the new version
    ev2 = await s.evaluate_mass_gone_block()
    assert ev2["state"] == "blocked" and ev2["version"] == ev["version"] + 1
    ps = await s.get_policy_state()
    assert ps["blocked_version"] == ev2["version"]
    assert Storage.mass_gone_state(ps) == "blocked"

    # evaluator: set no longer needs a block -> current block cleared
    async with s.pool.acquire() as con:
        await con.execute("UPDATE discovered_dialogs SET gone_at=NULL")
    assert (await s.evaluate_mass_gone_block())["state"] == "none"
    ps = await s.get_policy_state()
    assert ps["blocked_version"] is None and ps["blocked_fingerprint"] is None
    assert ps["confirmed_version"] == ev["version"]
    assert Storage.mass_gone_state(ps) == "none"


@pytest.mark.asyncio
async def test_mass_gone_clear(storage_v11):
    s = storage_v11
    await _targets_with_gone(s, 100, 20)
    ev = await s.evaluate_mass_gone_block()
    assert ev["state"] == "blocked"                           # 20 gone of 100: count branch
    assert await s.confirm_mass_gone(ev["version"], ev["fingerprint"])
    assert not await s.clear_mass_gone_block(ev["version"], "f" * 64)
    assert await s.clear_mass_gone_block(ev["version"], ev["fingerprint"])
    ps = await s.get_policy_state()
    assert ps["blocked_version"] is None and ps["confirmed_version"] == ev["version"]
    assert Storage.mass_gone_state(ps) == "none"


@pytest.mark.asyncio
async def test_mass_gone_auto_clears_when_rule_no_longer_holds(storage_v11):
    s = storage_v11
    await _targets_with_gone(s, 10, 3)
    assert (await s.evaluate_mass_gone_block())["state"] == "blocked"
    async with s.pool.acquire() as con:
        await con.execute("UPDATE discovered_dialogs SET gone_at=NULL WHERE chat_id IN (-1000,-1001)")
    assert (await s.evaluate_mass_gone_block())["state"] == "none"
    ps = await s.get_policy_state()
    assert ps["blocked_version"] is None
    res = await s.policy_dry_run()
    assert res["block"]["state"] == "none"
