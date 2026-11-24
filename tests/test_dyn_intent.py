"""v11 draft: intent writes bump policy_state.requested_version once per
statement that changes policy fields; rollback leaves nothing."""
import pytest

from tests._dyn_support import NotifyCounter, storage_v11, version  # noqa: F401


async def _exec_and_count(s, *sqls):
    v0 = await version(s)
    async with NotifyCounter(s.dsn) as n:
        async with s.pool.acquire() as con:
            for q in sqls:
                await con.execute(q)
        notes = await n.settle()
    return await version(s) - v0, notes


@pytest.mark.asyncio
@pytest.mark.parametrize("sql", [
    "UPDATE collection_config SET mode='all'",
    "UPDATE collection_config SET category_private=TRUE",
    "UPDATE collection_config SET include_archived=TRUE",
    "UPDATE collection_config SET mode='all', category_bot=TRUE, include_archived=TRUE",
])
async def test_config_change_bumps_once(storage_v11, sql):
    assert await _exec_and_count(storage_v11, sql) == (1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("sql", [
    "UPDATE collection_config SET mode='selected'",
    "UPDATE collection_config SET discovery_interval_sec=900",
    "UPDATE collection_config SET updated_at=NOW(), updated_by='ui'",
    "UPDATE collection_config SET mode='all' WHERE id=2",
])
async def test_config_noop_does_not_bump(storage_v11, sql):
    assert await _exec_and_count(storage_v11, sql) == (0, 0)


@pytest.mark.asyncio
async def test_preference_insert_update_delete(storage_v11):
    s = storage_v11
    assert await _exec_and_count(
        s, "INSERT INTO target_preferences (chat_id, selected) VALUES (-1, TRUE)") == (1, 1)
    assert await _exec_and_count(
        s, "UPDATE target_preferences SET selected=FALSE, excluded=TRUE WHERE chat_id=-1") == (1, 1)
    assert await _exec_and_count(
        s, "UPDATE target_preferences SET note='hi', updated_at=NOW() WHERE chat_id=-1") == (0, 0)
    assert await _exec_and_count(
        s, "UPDATE target_preferences SET excluded=TRUE WHERE chat_id=-1") == (0, 0)
    assert await _exec_and_count(
        s, "DELETE FROM target_preferences WHERE chat_id=-1") == (1, 1)
    assert await _exec_and_count(
        s, "DELETE FROM target_preferences WHERE chat_id=-1") == (0, 0)
    assert await _exec_and_count(
        s, "INSERT INTO target_preferences (chat_id) VALUES (-2) ON CONFLICT DO NOTHING",
        "INSERT INTO target_preferences (chat_id) VALUES (-2) ON CONFLICT DO NOTHING") == (1, 1)


@pytest.mark.asyncio
async def test_storage_upsert_preference_single_bump(storage_v11):
    s = storage_v11
    v0 = await version(s)
    await s.upsert_target_preference(-3, selected=True)
    assert await version(s) == v0 + 1
    await s.upsert_target_preference(-3, note="only a note")
    assert await version(s) == v0 + 1
    await s.upsert_target_preference(-3, selected=False)
    assert await version(s) == v0 + 2


@pytest.mark.asyncio
async def test_rules_insert_update_delete(storage_v11):
    s = storage_v11
    assert await _exec_and_count(
        s, "INSERT INTO collection_title_rules (action, pattern) VALUES ('exclude','casino')") == (1, 1)
    assert await _exec_and_count(
        s, "UPDATE collection_title_rules SET created_by='ui'") == (0, 0)
    assert await _exec_and_count(
        s, "UPDATE collection_title_rules SET pattern='betting'") == (1, 1)
    assert await _exec_and_count(s, "DELETE FROM collection_title_rules") == (1, 1)


@pytest.mark.asyncio
async def test_bulk_statement_bumps_once(storage_v11):
    s = storage_v11
    assert await _exec_and_count(
        s, "INSERT INTO target_preferences (chat_id, selected) "
           "SELECT -g, TRUE FROM generate_series(1,100) g") == (1, 1)
    assert await _exec_and_count(
        s, "UPDATE target_preferences SET selected=FALSE") == (1, 1)
    assert await _exec_and_count(s, "DELETE FROM target_preferences") == (1, 1)


@pytest.mark.asyncio
async def test_rollback_leaves_no_bump_or_notify(storage_v11):
    s = storage_v11
    v0 = await version(s)
    async with NotifyCounter(s.dsn) as n:
        with pytest.raises(RuntimeError):
            async with s.pool.acquire() as con:
                async with con.transaction():
                    await con.execute("UPDATE collection_config SET mode='all'")
                    await con.execute("INSERT INTO target_preferences (chat_id) VALUES (-4)")
                    raise RuntimeError("abort")
        assert await n.settle() == 0
    assert await version(s) == v0


@pytest.mark.asyncio
async def test_policy_state_update_does_not_recurse(storage_v11):
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute("UPDATE policy_state SET applied_version=0")
    assert await version(s) == 0
