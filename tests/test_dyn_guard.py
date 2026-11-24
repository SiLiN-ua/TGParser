"""v11 draft: chat write guard gated on runtime_state.dyn_engine."""
import asyncpg
import pytest

from tests._dyn_support import add_target, storage_v11  # noqa: F401
from tgparser.storage import _RETRYABLE_PG

CASES = [
    ("messages", "INSERT INTO messages (chat_id, message_id) VALUES ($1, 1)"),
    ("message_versions", "INSERT INTO message_versions (chat_id, message_id) VALUES ($1, 1)"),
    ("message_media", "INSERT INTO message_media (chat_id, message_id, file_id) VALUES ($1, 1, 'f')"),
    ("message_reactions", "INSERT INTO message_reactions (chat_id, message_id, reaction, count) "
                          "VALUES ($1, 1, 'x', 1)"),
    ("message_reaction_authors", "INSERT INTO message_reaction_authors "
                                 "(chat_id, message_id, reaction, peer_id) VALUES ($1, 1, 'x', 2)"),
    ("memberships", "INSERT INTO memberships (chat_id, user_id) VALUES ($1, 3)"),
    ("chat_actions", "INSERT INTO chat_actions (chat_id, action) VALUES ($1, 'a')"),
    ("user_observations", "INSERT INTO user_observations (user_id, chat_id) VALUES (5, $1)"),
]


async def _dyn(s, on):
    await s.set_runtime_dynamic_engine(on)


async def _legacy_message_flow(s, chat_id):
    """The real Storage write path of one message, as legacy collector does it."""
    await s.upsert_message({"chat_id": chat_id, "message_id": 1, "text": "hi",
                            "sender_user_id": 7, "media_file_id": "doc:1"})
    await s.upsert_message({"chat_id": chat_id, "message_id": 1, "text": "edited",
                            "sender_user_id": 7, "media_file_id": "doc:1"})
    await s.upsert_media_record({"file_id": "doc:1", "chat_id": chat_id, "message_id": 1,
                                 "media_type": "document", "status": "pending"})
    await s.link_message_media(chat_id, 1, "doc:1")
    await s.replace_reactions(chat_id, 1, [{"reaction": "👍", "count": 2}])
    await s.record_user_observation(7, chat_id, 1, "message_sender", "normal",
                                    None, None, False)
    await s.upsert_membership(chat_id, 7, "member")


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "disabled"])
async def test_legacy_engine_writes_pass(storage_v11, state):
    s = storage_v11
    if state == "disabled":
        await add_target(s, -1001, enabled=False)
    await _legacy_message_flow(s, -1001)
    async with s.pool.acquire() as con:
        assert await con.fetchval("SELECT text FROM messages WHERE chat_id=-1001") == "edited"
        assert await con.fetchval("SELECT COUNT(*) FROM message_reactions WHERE chat_id=-1001") == 1


@pytest.mark.asyncio
async def test_legacy_engine_does_not_touch_target_state(storage_v11):
    """Gate-off path never reaches target_state nor the advisory lock: it works
    with target_state dropped, and no advisory lock is held afterwards."""
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute("DROP VIEW v_target_policy")
        await con.execute("DROP TABLE target_state")
        await con.execute("INSERT INTO media (file_id) VALUES ('f')")  # FK parent
        async with con.transaction():
            for _, sql in CASES:
                await con.execute(sql, -2001)
            held = await con.fetchval(
                "SELECT COUNT(*) FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()")
        assert held == 0


@pytest.mark.asyncio
async def test_dynamic_enabled_passes_and_holds_shared_lock(storage_v11):
    s = storage_v11
    await add_target(s, -3001, True)
    await _dyn(s, True)
    async with s.pool.acquire() as con:
        async with con.transaction():
            await con.execute("INSERT INTO messages (chat_id, message_id) VALUES (-3001, 1)")
            mode = await con.fetchval(
                "SELECT mode FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()")
        assert mode == "ShareLock"
        await con.execute("UPDATE messages SET text='x' WHERE chat_id=-3001")
    await _legacy_message_flow(s, -3001)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "disabled"])
async def test_dynamic_rejects_every_table(storage_v11, state):
    s = storage_v11
    await _dyn(s, True)
    for i, (tbl, sql) in enumerate(CASES):
        cid = -4000 - i
        if state == "disabled":
            await add_target(s, cid, enabled=False)
        with pytest.raises(asyncpg.exceptions.PostgresError) as exc:
            async with s.pool.acquire() as con:
                await con.execute(sql, cid)
        assert exc.value.sqlstate == "TGP01", tbl


@pytest.mark.asyncio
async def test_dynamic_update_after_disable_rejected(storage_v11):
    s = storage_v11
    await add_target(s, -5001, True)
    await _dyn(s, True)
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO messages (chat_id, message_id) VALUES (-5001, 1)")
        async with con.transaction():
            await con.execute("SELECT pg_advisory_xact_lock(-5001::bigint)")
            await con.execute("UPDATE target_state SET enabled=FALSE WHERE chat_id=-5001")
        with pytest.raises(asyncpg.exceptions.PostgresError) as exc:
            await con.execute("UPDATE messages SET text='y' WHERE chat_id=-5001")
    assert exc.value.sqlstate == "TGP01"


@pytest.mark.asyncio
async def test_dynamic_null_chat_action_passes(storage_v11):
    s = storage_v11
    await _dyn(s, True)
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO chat_actions (chat_id, action) VALUES (NULL, 'sys')")


@pytest.mark.asyncio
async def test_tgp01_is_not_retried(storage_v11, caplog):
    s = storage_v11
    await _dyn(s, True)
    with pytest.raises(asyncpg.exceptions.PostgresError) as exc:
        await s.upsert_message({"chat_id": -6001, "message_id": 1})   # @_with_db_retry
    assert exc.value.sqlstate == "TGP01"
    assert not isinstance(exc.value, _RETRYABLE_PG)
    assert "transient" not in caplog.text


@pytest.mark.asyncio
async def test_gate_alone_does_not_arm_guard(storage_v11):
    s = storage_v11
    async with s.pool.acquire() as con:
        await con.execute("UPDATE deployment_gates SET enabled=TRUE")
        await con.execute("INSERT INTO messages (chat_id, message_id) VALUES (-7001, 1)")


@pytest.mark.asyncio
async def test_no_session_bypass(storage_v11):
    s = storage_v11
    await _dyn(s, True)
    async with s.pool.acquire() as con:
        await con.execute("SET tgparser.allow_inactive_write = 'on'")
        with pytest.raises(asyncpg.exceptions.PostgresError) as exc:
            await con.execute("INSERT INTO messages (chat_id, message_id) VALUES (-8001, 1)")
        await con.execute("RESET tgparser.allow_inactive_write")
    assert exc.value.sqlstate == "TGP01"
