"""v11 draft: shape, idempotency, seed, constraints, audit."""
import asyncpg
import pytest

from tests._dyn_support import (  # noqa: F401  (fixtures)
    DRAFT, NotifyCounter, add_target, apply_draft, snapshot, storage_v10,
    storage_v11, version,
)

GUARDED = ("messages", "message_versions", "message_media", "message_reactions",
           "message_reaction_authors", "memberships", "chat_actions",
           "user_observations")


def test_draft_is_not_in_auto_migrations_dir():
    from pathlib import Path
    assert DRAFT.exists()
    assert not list(Path("migrations").glob("0011*"))


@pytest.mark.asyncio
async def test_v10_fixture_has_no_v11_objects(storage_v10):
    assert await storage_v10.schema_version() == 10
    async with storage_v10.pool.acquire() as con:
        assert await con.fetchval("SELECT to_regclass('policy_state')") is None


@pytest.mark.asyncio
async def test_draft_shape(storage_v11):
    s = storage_v11
    assert await s.schema_version() == 11
    async with s.pool.acquire() as con:
        for t in ("deployment_gates", "collection_config", "collection_title_rules",
                  "target_preferences", "yaml_target_imports", "dialog_discovery_runs",
                  "dialog_discovery_items", "discovered_dialogs", "discovery_control",
                  "policy_state", "policy_block_history", "ui_audit_log"):
            assert await con.fetchval("SELECT to_regclass($1) IS NOT NULL", t), t
        assert await con.fetchval("SELECT to_regclass('v_target_policy') IS NOT NULL")
        for t in GUARDED:
            assert await con.fetchval(
                "SELECT COUNT(*) FROM information_schema.triggers "
                "WHERE event_object_table=$1 AND trigger_name='trg_tgp_guard'", t) == 2, t
        assert await con.fetchval(
            "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'") is False
        assert await con.fetchval("SELECT dyn_engine FROM runtime_state WHERE id=1") is False
        assert await con.fetchval("SELECT requested_version FROM policy_state") == 0


@pytest.mark.asyncio
async def test_0011_seed_from_existing_enabled_target(storage_v10):
    s = storage_v10
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO chats (id,type,title,is_broadcast) VALUES "
                          "(-1001000000001,'supergroup','Alpha',FALSE),"
                          "(-1002,'channel','News',TRUE)")
    await add_target(s, -1001000000001, True)
    await add_target(s, -1002, True)
    await add_target(s, -1003, False)
    await apply_draft(s)
    async with s.pool.acquire() as con:
        rows = {r["chat_id"]: r["category"] for r in await con.fetch(
            "SELECT chat_id, category FROM discovered_dialogs")}
        assert rows == {-1001000000001: "megagroup", -1002: "channel"}
        prefs = await con.fetch("SELECT chat_id, selected, excluded FROM target_preferences")
        assert {r["chat_id"] for r in prefs} == {-1001000000001, -1002}
        assert all(r["selected"] and not r["excluded"] for r in prefs)
    assert await version(s) == 1


@pytest.mark.asyncio
async def test_0011_seed_without_enabled_targets_does_not_bump(storage_v10):
    await add_target(storage_v10, -5, False)
    await apply_draft(storage_v10)
    assert await version(storage_v10) == 0


@pytest.mark.asyncio
async def test_0011_reapply_preserves_policy_version_and_timestamps(storage_v10):
    s = storage_v10
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO chats (id,type,title) VALUES (-77,'supergroup','A')")
    await add_target(s, -77, True)
    await apply_draft(s)
    v_before = await version(s)
    assert v_before == 1
    before = await snapshot(s)
    async with s.pool.acquire() as con:
        counts_before = {t: await con.fetchval(f"SELECT COUNT(*) FROM {t}")
                         for t in ("discovered_dialogs", "target_preferences",
                                   "collection_config", "discovery_control",
                                   "policy_state", "deployment_gates")}
        ts_before = await con.fetchrow(
            "SELECT (SELECT updated_at FROM collection_config) cc, "
            "(SELECT updated_at FROM target_preferences WHERE chat_id=-77) tp, "
            "(SELECT last_seen_at FROM discovered_dialogs WHERE chat_id=-77) dd, "
            "(SELECT requested_at FROM policy_state) ps")
    async with NotifyCounter(s.dsn) as n:
        await apply_draft(s, register=False)
        await apply_draft(s, register=False)
        assert await n.settle() == 0
    assert await version(s) == v_before
    assert await snapshot(s) == before
    async with s.pool.acquire() as con:
        counts_after = {t: await con.fetchval(f"SELECT COUNT(*) FROM {t}")
                        for t in counts_before}
        ts_after = await con.fetchrow(
            "SELECT (SELECT updated_at FROM collection_config) cc, "
            "(SELECT updated_at FROM target_preferences WHERE chat_id=-77) tp, "
            "(SELECT last_seen_at FROM discovered_dialogs WHERE chat_id=-77) dd, "
            "(SELECT requested_at FROM policy_state) ps")
    assert counts_after == counts_before
    assert dict(ts_after) == dict(ts_before)


# --- constraints -------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("sql", [
    "INSERT INTO collection_title_rules (action, pattern) VALUES ('include','spam')",
    "INSERT INTO collection_title_rules (action, pattern) VALUES ('exclude','x')",
    "INSERT INTO collection_title_rules (action, pattern) VALUES ('exclude', repeat('a',121))",
    "INSERT INTO collection_title_rules (action, pattern) VALUES ('exclude', E'ab\\ncd')",
    "INSERT INTO target_preferences (chat_id, selected, excluded) VALUES (-1,TRUE,TRUE)",
    "INSERT INTO target_preferences (chat_id, note) VALUES (-1, repeat('n',501))",
    "UPDATE collection_config SET discovery_interval_sec=86401",
    "UPDATE collection_config SET discovery_interval_sec=59",
    "UPDATE collection_config SET mode='everything'",
    "INSERT INTO yaml_target_imports (raw_target) VALUES (repeat('y',301))",
    "INSERT INTO yaml_target_imports (raw_target) VALUES ('')",
    "INSERT INTO yaml_target_imports (raw_target, status) VALUES ('@a','selected')",
    "INSERT INTO dialog_discovery_runs (trigger_kind, error) VALUES ('scheduled', repeat('e',501))",
    "INSERT INTO dialog_discovery_runs (trigger_kind, status) VALUES ('scheduled','complete')",
    "INSERT INTO ui_audit_log (actor, action, outcome) VALUES ('hacker','x.y','ok')",
    "INSERT INTO ui_audit_log (actor, action, outcome) VALUES ('ui','Bad Action','ok')",
    "INSERT INTO ui_audit_log (actor, action, outcome) VALUES ('ui','prefs.update','maybe')",
    "INSERT INTO ui_audit_log (actor, action, outcome, detail) VALUES ('ui','a.b','ok','[1]')",
    "INSERT INTO ui_audit_log (actor, action, outcome, detail) VALUES "
    "('ui','a.b','ok', jsonb_build_object('k', repeat(md5(random()::text), 200)))",
    "INSERT INTO ui_audit_log (actor, action, outcome, user_agent) VALUES ('ui','a.b','ok', repeat('u',201))",
    "INSERT INTO ui_audit_log (actor, action, outcome, error) VALUES ('ui','a.b','ok', repeat('e',501))",
    "INSERT INTO ui_audit_log (actor, action, outcome, client_ip) VALUES ('ui','a.b','ok','not-an-ip')",
])
async def test_constraints_reject(storage_v11, sql):
    with pytest.raises((asyncpg.exceptions.CheckViolationError,
                        asyncpg.exceptions.InvalidTextRepresentationError)):
        async with storage_v11.pool.acquire() as con:
            await con.execute(sql)


@pytest.mark.asyncio
async def test_ui_audit_whitelist(storage_v11):
    s = storage_v11
    await s.ui_audit("ui", "prefs.update", "ok",
                     {"chat_id": -1, "field": "selected", "old": False, "new": True},
                     client_ip="127.0.0.1", user_agent="ua")
    for bad in ({"body": "{...}"}, {"token": "x"}, {"reason": "postgres://u:p@h/db"},
                {"reason": "x" * 121}, {"old": [1, 2]}, {"new": "+380501234567"}):
        with pytest.raises(ValueError):
            await s.ui_audit("ui", "prefs.update", "ok", bad)
    with pytest.raises(ValueError):
        await s.ui_audit("root", "prefs.update", "ok")
    with pytest.raises(ValueError):
        await s.ui_audit("ui", "prefs.update", "ok", client_ip="evil")
    async with s.pool.acquire() as con:
        rows = await con.fetch("SELECT actor, client_ip::text ip, detail FROM ui_audit_log")
    assert len(rows) == 1
    assert rows[0]["ip"] == "127.0.0.1/32"


@pytest.mark.asyncio
async def test_yaml_import_lifecycle(storage_v11):
    s = storage_v11
    rid = await s.yaml_import_upsert("@foo")
    assert await s.yaml_import_upsert("@foo") == rid
    await s.yaml_import_record_error("@foo", "Resolve Error!")
    await s.yaml_import_mark_terminal("@foo", "UserRemovedInUI")
    await s.yaml_import_upsert("@foo")             # YAML still lists it
    await s.yaml_import_mark_selected("@foo", -9)  # must not resurrect
    async with s.pool.acquire() as con:
        r = await con.fetchrow("SELECT * FROM yaml_target_imports WHERE raw_target='@foo'")
    assert r["status"] == "terminal"
    assert r["attempts"] == 1
    assert r["last_error_class"] == "UserRemovedInUI"
    with pytest.raises(ValueError):
        await s.yaml_import_upsert("bad\ntarget")
