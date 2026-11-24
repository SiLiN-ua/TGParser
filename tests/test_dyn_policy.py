"""v11 draft: strict v_target_policy truth table and read-only dry-run."""
import json

import pytest

from tests._dyn_support import (  # noqa: F401
    NotifyCounter, add_dialog, add_pref, add_target, policy, set_config,
    snapshot, storage_v10, storage_v11,
)


async def _expect(s, chat_id, reason, desired):
    r = await policy(s, chat_id)
    assert r is not None, f"{chat_id} missing from v_target_policy"
    assert (r["reason"], r["desired"]) == (reason, desired), dict(r)


# --- selected mode ------------------------------------------------------------

@pytest.mark.asyncio
async def test_selected_mode_table(storage_v11):
    s = storage_v11
    await add_dialog(s, -1); await add_pref(s, -1, selected=True)
    await add_dialog(s, -2)
    await add_dialog(s, -3); await add_pref(s, -3, excluded=True)
    await add_dialog(s, -4, gone=True); await add_pref(s, -4, selected=True)
    await add_dialog(s, -5, category="forbidden"); await add_pref(s, -5, selected=True)
    await add_dialog(s, -6, category="saved"); await add_pref(s, -6, selected=True)
    await add_dialog(s, -7, category="megagroup", is_archived=True); await add_pref(s, -7, selected=True)
    await add_dialog(s, -8, title="Casino"); await add_pref(s, -8, selected=True)
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO collection_title_rules (action,pattern) VALUES ('exclude','casino')")
    await set_config(s, category_megagroup=False)
    await _expect(s, -1, "selected", True)           # category toggle ignored in selected
    await _expect(s, -2, "not_selected", False)
    await _expect(s, -3, "excluded", False)
    await _expect(s, -4, "dialog_gone", False)
    await _expect(s, -5, "forbidden", False)
    await _expect(s, -6, "selected", True)           # manual pick of saved allowed
    await _expect(s, -7, "selected", True)           # archive policy is all-mode only
    await _expect(s, -8, "selected", True)           # exclude rules are all-mode only


@pytest.mark.asyncio
async def test_selected_without_discovery_is_on(storage_v11):
    s = storage_v11
    await add_pref(s, -11, selected=True)                       # no discovery, no target_state
    await add_target(s, -12, enabled=False); await add_pref(s, -12, selected=True)
    await _expect(s, -11, "selected", True)
    await _expect(s, -12, "selected", True)
    r = await policy(s, -11)
    assert r["discovered"] is False and r["category"] is None and r["in_target_state"] is False


@pytest.mark.asyncio
async def test_historical_disabled_target_without_discovery_is_off(storage_v11):
    s = storage_v11
    await add_target(s, -13, enabled=False)
    await _expect(s, -13, "not_selected", False)
    await set_config(s, mode="all")
    await _expect(s, -13, "not_discovered", False)


# --- all mode -----------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_mode_table(storage_v11):
    s = storage_v11
    await set_config(s, mode="all")
    await add_dialog(s, -21)                                             # in collection
    await add_dialog(s, -22); await add_pref(s, -22, selected=False)    # selected irrelevant
    await add_pref(s, -23, selected=True)                               # undiscovered
    await add_dialog(s, -24, category="saved"); await add_pref(s, -24, selected=True)
    await add_dialog(s, -25, category="unknown")
    await add_dialog(s, -26, is_archived=True)
    await add_dialog(s, -27, category="private")
    await add_dialog(s, -28, category="bot")
    await add_dialog(s, -29, title="Best CASINO wins")
    await add_dialog(s, -30, title="x", username="casino_bot_news")
    await add_dialog(s, -31, title="100%_off")      # wildcard chars are literal
    await add_dialog(s, -32); await add_pref(s, -32, excluded=True)
    await add_dialog(s, -33, gone=True)
    await add_dialog(s, -34, category="forbidden")
    await add_dialog(s, -35, category="channel")
    await add_dialog(s, -36, category="group")
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO collection_title_rules (action,pattern) VALUES "
                          "('exclude','casino'),('exclude','%_')")
    await _expect(s, -21, "in_collection", True)
    await _expect(s, -22, "in_collection", True)
    await _expect(s, -23, "not_discovered", False)
    await _expect(s, -24, "saved_or_unknown", False)
    await _expect(s, -25, "saved_or_unknown", False)
    await _expect(s, -26, "archived_policy", False)
    await _expect(s, -27, "category_off", False)
    await _expect(s, -28, "category_off", False)
    await _expect(s, -29, "exclude_rule", False)
    await _expect(s, -30, "exclude_rule", False)
    await _expect(s, -31, "exclude_rule", False)     # literal "%_" substring present
    await _expect(s, -32, "excluded", False)
    await _expect(s, -33, "dialog_gone", False)
    await _expect(s, -34, "forbidden", False)
    await _expect(s, -35, "in_collection", True)
    await _expect(s, -36, "in_collection", True)
    await set_config(s, include_archived=True, category_private=True, category_bot=True,
                     category_channel=False)
    await _expect(s, -26, "in_collection", True)
    await _expect(s, -27, "in_collection", True)
    await _expect(s, -28, "in_collection", True)
    await _expect(s, -35, "category_off", False)


@pytest.mark.asyncio
async def test_pattern_wildcards_are_literal(storage_v11):
    s = storage_v11
    await set_config(s, mode="all")
    await add_dialog(s, -41, title="plain title")
    async with s.pool.acquire() as con:
        await con.execute("INSERT INTO collection_title_rules (action,pattern) VALUES ('exclude','p%e')")
    await _expect(s, -41, "in_collection", True)


# --- dry-run ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_fields_and_seed_gap(storage_v11):
    s = storage_v11
    await add_dialog(s, -51, title="Alpha"); await add_pref(s, -51, selected=True)
    await add_target(s, -52, enabled=True)          # legacy-enabled, no pref, no discovery
    await add_pref(s, -53, selected=True)           # selected, undiscovered
    res = await s.policy_dry_run()
    for k in ("schema_version", "gate_enabled", "engine_active", "requires_restart",
              "requested_version", "applied_version", "block", "awaiting_discovery",
              "would_enable", "would_disable", "disable_suppressed_by"):
        assert k in res, k
    assert res["schema_version"] == 11
    assert res["gate_enabled"] is False and res["engine_active"] is False
    assert res["requires_restart"] is False
    assert {r["chat_id"] for r in res["would_enable"]} == {-51, -53}
    assert [r["chat_id"] for r in res["would_disable"]] == [-52]
    assert res["enabled_without_discovery"] == [-52]
    assert res["enabled_without_preference"] == [-52]
    assert res["block"]["state"] == "none"
    json.dumps(res, default=str)


@pytest.mark.asyncio
async def test_dry_run_awaiting_discovery_and_gate_flags(storage_v11):
    s = storage_v11
    await set_config(s, mode="all")
    async with s.pool.acquire() as con:
        await con.execute("UPDATE deployment_gates SET enabled=TRUE")
    res = await s.policy_dry_run()
    assert res["awaiting_discovery"] is True
    assert "awaiting_discovery" in res["disable_suppressed_by"]
    assert res["gate_enabled"] is True and res["engine_active"] is False
    assert res["requires_restart"] is True


@pytest.mark.asyncio
async def test_dry_run_writes_nothing(storage_v11):
    s = storage_v11
    await add_dialog(s, -61); await add_pref(s, -61, selected=True)
    await add_target(s, -62, enabled=True)
    before = await snapshot(s)
    async with NotifyCounter(s.dsn) as n:
        await s.policy_dry_run()
        await s.policy_dry_run()
        assert await n.settle() == 0
    assert await snapshot(s) == before


@pytest.mark.asyncio
async def test_admin_cli_json(storage_v11, monkeypatch, capsys):
    from tgparser import admin
    await add_pref(storage_v11, -71, selected=True)
    monkeypatch.setenv("PG_DSN", storage_v11.dsn)
    before = await snapshot(storage_v11)
    args = admin.build_parser().parse_args(["policy-dry-run", "--json"])
    assert await args.func(args) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["schema_version"] == 11 and out["would_enable"][0]["chat_id"] == -71
    assert await snapshot(storage_v11) == before
    args = admin.build_parser().parse_args(["policy-dry-run"])
    assert await args.func(args) == 0
    assert "requires_restart" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_admin_cli_on_v10_is_clean(storage_v10, monkeypatch, capsys):
    from tgparser import admin
    monkeypatch.setenv("PG_DSN", storage_v10.dsn)
    args = admin.build_parser().parse_args(["policy-dry-run", "--json"])
    assert await args.func(args) == 2
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False and "not applied" in out["error"]
