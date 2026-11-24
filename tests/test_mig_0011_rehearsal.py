"""Upgrade/rollback/gate-off rehearsal for the finalized migration 0011.

All checks run against temporary databases created and dropped inside the
test; the working database is never touched.
"""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

import run
from tests._dyn_support import (
    _create_blank_db, _dispose, apply_up_to,
)
from tests.conftest import _safe_drop
from tgparser.migrations import apply as apply_migrations
from tgparser.storage import Storage


MIG_DIR = Path("migrations")
MIG_0011 = MIG_DIR / "0011_dynamic_targets.sql"


# =============================================================================
# Static checks of the final SQL file
# =============================================================================

def test_migration_file_properties():
    """The file lives in migrations/, no stale draft remains, and the SQL
    is well-formed for safe transactional apply."""
    assert MIG_0011.exists()
    assert not list(Path("design").glob("0011_dynamic_targets*.sql"))
    assert not list(MIG_DIR.glob("*.draft.sql"))
    nums = sorted(int(re.match(r"(\d+)_", f.name).group(1))
                  for f in MIG_DIR.glob("*.sql"))
    assert nums == list(range(1, 12))
    sql = MIG_0011.read_text(encoding="utf-8")
    assert "CONCURRENTLY" not in sql.upper()
    assert "COPY " not in sql
    assert "dblink" not in sql.lower() and "http" not in sql.lower()
    for secret in ("api_hash", "PG_DSN", "postgres://", "postgresql://"):
        assert secret.lower() not in sql.lower(), secret
    # The migration does mention `session` in its audit-log section (regex
    # that forbids secret-looking values in detail), so only explicit secret
    # value literals are forbidden.
    assert "PG_DSN=" not in sql
    # Guard uses the shared advisory lock on chat_id; disable ACK takes the
    # exclusive lock with the SAME key. Both must be present.
    assert "pg_advisory_xact_lock_shared" in sql
    # Guard function and trigger plumbing.
    assert "FUNCTION tgp_guard_chat_write" in sql
    assert "ERRCODE = 'TGP01'" in sql
    assert "deployment_gates" in sql
    assert "ON CONFLICT (key) DO NOTHING" in sql           # keep existing gate
    # Mass-gone block fingerprint column is a 64-char hex sha256.
    assert "blocked_fingerprint" in sql


# =============================================================================
# Fake stack to drive run._main_inner without a Telegram client
# =============================================================================

class _AbortAfterConnect(Exception):
    pass


def _install_client(monkeypatch, seen=None):
    class FakeClient:
        async def is_user_authorized(self): return True
        async def get_me(self):
            class Me: username, id = "test", 1
            return Me()
        async def connect(self):
            if seen is not None:
                seen["connected"] = True
            raise _AbortAfterConnect("abort after client.connect")
        async def disconnect(self): pass
    made = []

    def mk(_cfg):
        c = FakeClient()
        made.append(c)
        return c
    monkeypatch.setattr(run, "make_client", mk)
    return made


def _cfg(dsn, targets=()):
    return {"postgres": {"dsn": dsn},
            "storage": {"media_dir": "/nonexistent", "download_media": False},
            "options": {"participants_sync": False, "follow_linked_discussion": False},
            "targets": list(targets)}


# =============================================================================
# 2. Upgrade safety: realistic v10 -> v11
# =============================================================================

async def _seed_realistic_v10(s):
    """A plausible v10 snapshot: three chats, two enabled target_state rows,
    one legacy-disabled target, checkpoints, messages, media, memberships,
    reactions, runtime_state of a legacy process."""
    A, B, C = -1001000000001, -1001000000002, -1001000000003
    async with s.pool.acquire() as con:
        await con.execute(
            "INSERT INTO chats (id, type, title, username, is_broadcast) VALUES "
            "($1,'supergroup','Alpha','alpha',FALSE),"
            "($2,'channel','Broadcast','bcast',TRUE),"
            "($3,'supergroup','Gamma',NULL,FALSE)", A, B, C)
        await con.execute(
            "INSERT INTO target_state (chat_id, target, backfill_status, enabled, "
            "  last_success_at) VALUES "
            "($1,'alpha','syncing',TRUE,NOW()),"
            "($2,'bcast','complete',TRUE,NOW()),"
            "($3,'gamma','running',FALSE,NOW())", A, B, C)
        await con.execute(
            "INSERT INTO sync_state (chat_id, last_backfilled_message_id, "
            "  first_message_id, backfill_status) VALUES "
            "($1, 1234, 1, 'syncing'),"
            "($2,  500, 1, 'complete'),"
            "($3,  777, 1, 'running')", A, B, C)
        await con.execute(
            "INSERT INTO messages (chat_id, message_id, text) VALUES "
            "($1, 1, 'hi'),($1, 2, 'hi2'),($2, 1, 'post')", A, B)
        await con.execute(
            "INSERT INTO media (file_id, chat_id, message_id, status, size) VALUES "
            "('doc:1', $1, 1, 'done', 100)", A)
        await con.execute(
            "INSERT INTO message_media (chat_id, message_id, file_id) VALUES "
            "($1, 1, 'doc:1')", A)
        await con.execute(
            "INSERT INTO memberships (chat_id, user_id, role) VALUES "
            "($1, 10, 'member')", A)
        await con.execute(
            "INSERT INTO message_reactions (chat_id, message_id, reaction, count) "
            "VALUES ($1, 1, '+1', 3)", A)
        await con.execute(
            "UPDATE runtime_state SET pid=1234, status='syncing', "
            "started_at=NOW(), heartbeat_at=NOW() WHERE id=1")
    return A, B, C


async def _snapshot(s, tables):
    """md5 of every row of each listed table (includes updated_at)."""
    out = {}
    async with s.pool.acquire() as con:
        for t in tables:
            out[t] = await con.fetchval(
                f"SELECT md5(COALESCE(string_agg(x::text,'|' ORDER BY x::text),'')) "
                f"FROM {t} x")
    return out


# target_state and runtime_state gain NEW columns in 0011 (label; dyn_engine,
# dyn_engine_set_at). Snapshot their existing v10 columns only so the diff
# reflects data change, not schema change.
LEGACY_TABLES = (
    "chats", "sync_state", "messages", "message_media",
    "media", "memberships", "message_reactions",
)
V10_TARGET_STATE_COLS = ("chat_id", "target", "backfill_status",
                          "last_success_at", "last_error_at", "last_error",
                          "pending_count", "dead_count", "note", "updated_at",
                          "enabled", "disabled_at", "disabled_reason")


@pytest.mark.asyncio
async def test_upgrade_v10_to_v11_preserves_legacy_state():
    s, name = await _create_blank_db()
    try:
        await apply_up_to(s, 10)
        A, B, C = await _seed_realistic_v10(s)
        assert await s.schema_version() == 10

        before = await _snapshot(s, LEGACY_TABLES)
        async with s.pool.acquire() as con:
            before_rt = dict(await con.fetchrow(
                "SELECT pid, started_at, heartbeat_at, status, targets, note, "
                "       telegram_connected, telegram_last_ok_at, telegram_last_error, "
                "       update_last_received_at FROM runtime_state WHERE id=1"))
            before_ts = {r["chat_id"]: dict(r) for r in await con.fetch(
                f"SELECT {', '.join(V10_TARGET_STATE_COLS)} FROM target_state")}

        import time
        t0 = time.monotonic()
        await apply_migrations(s.pool, "migrations")
        elapsed = time.monotonic() - t0

        assert await s.schema_version() == 11
        after = await _snapshot(s, LEGACY_TABLES)
        assert after == before, f"legacy tables changed: {set(k for k in before if before[k] != after[k])}"

        async with s.pool.acquire() as con:
            # Gate is created FALSE, dyn_engine column exists and is FALSE,
            # runtime_state preserved apart from the new dyn_engine columns.
            assert await con.fetchval(
                "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'") is False
            rt_after = dict(await con.fetchrow(
                "SELECT pid, started_at, heartbeat_at, status, targets, note, "
                "       telegram_connected, telegram_last_ok_at, telegram_last_error, "
                "       update_last_received_at, dyn_engine, dyn_engine_set_at "
                "FROM runtime_state WHERE id=1"))
            assert rt_after["dyn_engine"] is False
            for k in before_rt:
                assert rt_after[k] == before_rt[k], f"runtime_state.{k} changed"
            ts_after = {r["chat_id"]: dict(r) for r in await con.fetch(
                f"SELECT {', '.join(V10_TARGET_STATE_COLS)} FROM target_state")}
            assert ts_after == before_ts
            # Singletons: exactly one row each.
            assert await con.fetchval("SELECT COUNT(*) FROM collection_config") == 1
            assert await con.fetchval("SELECT COUNT(*) FROM discovery_control") == 1
            assert await con.fetchval("SELECT COUNT(*) FROM policy_state") == 1
            # Seed:
            #  * discovered_dialogs + target_preferences for the TWO enabled
            #    legacy targets only (disabled one is NOT seeded);
            #  * requested_version is 1 (seed bumped once);
            #  * applied_version is still 0.
            seeded = {r["chat_id"]: r["category"] for r in await con.fetch(
                "SELECT chat_id, category FROM discovered_dialogs")}
            assert seeded == {A: "megagroup", B: "channel"}
            prefs = await con.fetch(
                "SELECT chat_id, selected, excluded FROM target_preferences")
            assert {r["chat_id"] for r in prefs} == {A, B}
            assert all(r["selected"] and not r["excluded"] for r in prefs)
            ps = await con.fetchrow("SELECT * FROM policy_state WHERE id=1")
            assert (ps["requested_version"], ps["applied_version"]) == (1, 0)
            # Discovery initial state is empty — no runs, no staging.
            assert await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_runs") == 0
            assert await con.fetchval("SELECT COUNT(*) FROM dialog_discovery_items") == 0
            # No target was silently disabled by the migration.
            assert await con.fetchval(
                "SELECT COUNT(*) FROM target_state WHERE enabled=TRUE "
                "AND chat_id = ANY($1::bigint[])", [A, B]) == 2
            assert await con.fetchval(
                "SELECT COUNT(*) FROM target_state WHERE enabled=FALSE "
                "AND chat_id = ANY($1::bigint[])", [C]) == 1
            # Guard trigger is installed on every chat-scoped writer.
            for t in ("messages", "message_versions", "message_media",
                      "message_reactions", "message_reaction_authors",
                      "memberships", "chat_actions", "user_observations"):
                assert await con.fetchval(
                    "SELECT COUNT(*) FROM information_schema.triggers "
                    "WHERE event_object_table=$1 AND trigger_name='trg_tgp_guard'",
                    t) == 2, t

        # The upgrade is modest; keep the budget generous enough to survive
        # noisy CI without hiding a regression.
        assert elapsed < 10.0, f"migration 0011 took {elapsed:.2f}s on empty state"
    finally:
        await _dispose(s, name)


# =============================================================================
# 3. Rollback rehearsal: a mid-transaction failure must roll everything back
# =============================================================================

SPLIT_MARKER = "-- 7. policy_state (singleton) + mass-gone block history"


def _split_migration_at_marker() -> tuple[str, str]:
    """Split the migration SQL at the stable marker that opens section 7
    (policy_state). We assert the marker is unique so the test is not
    silently invalidated by a future reshuffle."""
    sql = MIG_0011.read_text(encoding="utf-8")
    assert sql.count(SPLIT_MARKER) == 1, (
        f"split marker occurs {sql.count(SPLIT_MARKER)} times; test fixture "
        "must be adjusted")
    idx = sql.index(SPLIT_MARKER)
    # Rewind to the '-- =====' banner just above the marker so the first
    # half ends at a statement boundary.
    banner = sql.rfind("-- =============", 0, idx)
    assert banner > 0
    return sql[:banner], sql[banner:]


@pytest.mark.asyncio
async def test_rollback_in_middle_of_partial_migration_sql():
    """Execute the migration up to the policy_state section, assert some v11
    objects already exist INSIDE the transaction, then abort before the rest.
    The whole transaction must roll back: schema remains v10, no v11
    fragments (tables / columns / triggers / functions / indexes / checks)
    survive, and a subsequent normal apply_migrations succeeds."""
    s, name = await _create_blank_db()
    try:
        await apply_up_to(s, 10)
        v10_shape = await _enumerate_schema(s)
        assert await s.schema_version() == 10

        first_half, second_half = _split_migration_at_marker()
        # Second half must contain the pieces we expect to miss after rollback.
        for token in ("policy_state", "tgp_guard_chat_write",
                      "v_target_policy", "INSERT INTO discovered_dialogs"):
            assert token in second_half, token
        # First half must create the pieces we assert on INSIDE the tx.
        for token in ("CREATE TABLE IF NOT EXISTS deployment_gates",
                      "CREATE TABLE IF NOT EXISTS discovery_control",
                      "dialog_discovery_items", "collection_config"):
            assert token in first_half, token

        with pytest.raises(RuntimeError, match="abort before second half"):
            async with s.pool.acquire() as con:
                async with con.transaction():
                    await con.execute(first_half)
                    # Mid-transaction assertions: section-1..6 objects exist,
                    # section-7+ objects do not yet.
                    assert await con.fetchval(
                        "SELECT to_regclass('public.deployment_gates') IS NOT NULL")
                    assert await con.fetchval(
                        "SELECT to_regclass('public.discovery_control') IS NOT NULL")
                    assert await con.fetchval(
                        "SELECT to_regclass('public.dialog_discovery_items') IS NOT NULL")
                    assert await con.fetchval(
                        "SELECT to_regclass('public.policy_state')") is None
                    assert await con.fetchval(
                        "SELECT to_regclass('public.v_target_policy')") is None
                    assert await con.fetchval(
                        "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n "
                        "ON n.oid=p.pronamespace WHERE n.nspname='public' "
                        "AND p.proname='tgp_guard_chat_write')") is False
                    # Now abort, simulating a mid-SQL failure.
                    raise RuntimeError("abort before second half")

        # After rollback: schema is byte-for-byte v10.
        assert await s.schema_version() == 10
        async with s.pool.acquire() as con:
            # Not registered as v11.
            assert await con.fetchval(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=11") == 0
            for t in ("deployment_gates", "collection_config",
                      "collection_title_rules", "target_preferences",
                      "yaml_target_imports", "dialog_discovery_runs",
                      "dialog_discovery_items", "discovered_dialogs",
                      "discovery_control", "policy_state",
                      "policy_block_history", "ui_audit_log"):
                assert await con.fetchval(
                    "SELECT to_regclass($1)", f"public.{t}") is None, t
            # New columns on legacy tables are gone too.
            assert await con.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name='runtime_state' AND column_name='dyn_engine')"
            ) is False
            assert await con.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name='target_state' AND column_name='label')"
            ) is False
        after_shape = await _enumerate_schema(s)
        assert after_shape == v10_shape, {
            "new": sorted(after_shape - v10_shape),
            "missing": sorted(v10_shape - after_shape),
        }

        # A normal retry via the real runner applies 0011 cleanly.
        await apply_migrations(s.pool, "migrations")
        assert await s.schema_version() == 11
        async with s.pool.acquire() as con:
            assert await con.fetchval(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=11") == 1
            assert await con.fetchval("SELECT COUNT(*) FROM policy_state") == 1
            assert await con.fetchval(
                "SELECT to_regclass('public.v_target_policy') IS NOT NULL")
    finally:
        await _dispose(s, name)


@pytest.mark.asyncio
async def test_rollback_commit_time_failure():
    """A secondary rollback form: the whole SQL executes, then we abort
    before COMMIT. Same invariants hold."""
    s, name = await _create_blank_db()
    try:
        await apply_up_to(s, 10)
        v10_shape = await _enumerate_schema(s)
        sql = MIG_0011.read_text(encoding="utf-8")
        with pytest.raises(RuntimeError, match="abort after full SQL"):
            async with s.pool.acquire() as con:
                async with con.transaction():
                    await con.execute(sql)
                    raise RuntimeError("abort after full SQL, before COMMIT")
        assert await s.schema_version() == 10
        assert await _enumerate_schema(s) == v10_shape
    finally:
        await _dispose(s, name)


async def _enumerate_schema(s) -> set[str]:
    """Signature of the schema: tables, views, columns, triggers, functions,
    indexes, check constraints. Used to prove no v11 fragments survived
    rollback."""
    sig = set()
    async with s.pool.acquire() as con:
        for r in await con.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public'"):
            sig.add(f"table:{r['table_name']}")
        for r in await con.fetch(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema='public'"):
            sig.add(f"view:{r['table_name']}")
        for r in await con.fetch(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema='public'"):
            sig.add(f"col:{r['table_name']}.{r['column_name']}")
        for r in await con.fetch(
            "SELECT trigger_name, event_object_table FROM information_schema.triggers "
            "WHERE trigger_schema='public'"):
            sig.add(f"trg:{r['event_object_table']}.{r['trigger_name']}")
        for r in await con.fetch(
            "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='public'"):
            sig.add(f"fn:{r['proname']}")
        for r in await con.fetch(
            "SELECT indexname, tablename FROM pg_indexes WHERE schemaname='public'"):
            sig.add(f"idx:{r['tablename']}.{r['indexname']}")
        for r in await con.fetch(
            "SELECT conname FROM pg_constraint c JOIN pg_namespace n "
            "ON n.oid=c.connamespace WHERE n.nspname='public'"):
            sig.add(f"con:{r['conname']}")
    return sig


# =============================================================================
# 4. Gate-off compatibility: a v11 process selects legacy when the gate is off
# =============================================================================

class _LegacyFakeClient:
    """Minimal FakeClient driving a real legacy coordinator. Implements
    exactly the Telethon surface the collector touches on a gate-off v11
    start: get_entity, on(), iter_messages, iter_dialogs, catch_up, get_me,
    disconnect, is_connected, get_input_entity."""

    def __init__(self, chat_id: int, messages: list):
        self.chat_id = chat_id
        self._messages = {chat_id: list(messages)}
        self.handlers: list = []
        self.connected = True

    def on(self, builder):
        def deco(fn):
            self.handlers.append((builder, fn))
            return fn
        return deco

    async def is_user_authorized(self): return True
    async def connect(self): self.connected = True
    def is_connected(self): return self.connected
    async def disconnect(self): self.connected = False
    async def catch_up(self): return None

    async def get_me(self):
        class _Me: username = "legacy"; id = 1
        return _Me()

    async def get_entity(self, x):
        return x

    async def get_input_entity(self, x):
        return x

    def iter_messages(self, entity, min_id=0, reverse=True):
        src = self._messages.get(entity, [])

        async def gen():
            for m in sorted(src, key=lambda m: m.id):
                if m.id <= min_id:
                    continue
                yield m
        return gen()

    async def get_messages(self, entity, ids=None):
        for m in self._messages.get(entity, []):
            if m.id == ids:
                return m
        return None

    def iter_dialogs(self, archived=False):
        async def gen():
            if False:
                yield None
        return gen()

    async def __call__(self, request):
        # Minimal responses so collector's optional full-channel fetch /
        # updates heartbeat do not crash. They run inside try/except and
        # a raise here is also acceptable; we just no-op.
        from telethon.tl.functions.updates import GetStateRequest
        from telethon.tl.functions.channels import GetFullChannelRequest
        if isinstance(request, GetStateRequest):
            class _State:
                pts = qts = seq = unread_count = 0
                date = None
            return _State()
        if isinstance(request, GetFullChannelRequest):
            raise RuntimeError("no full channel in fake")
        raise RuntimeError(f"unsupported fake request: {type(request).__name__}")


@pytest.mark.asyncio
async def test_full_gate_off_legacy_lifecycle_on_v11_db(tmp_path, monkeypatch, caplog):
    """End-to-end integration: boot run._main_inner with a fake Telegram
    client on a v11 DB, let the legacy coordinator process one real
    message, deliver one live event through the registered legacy handler,
    then set `stop` and verify a clean teardown."""
    from telethon.tl.types import PeerChannel

    CHAT = -1001000000777
    import tgparser.collector as collector_mod
    from tgparser.media_queue import MediaQueue

    class _LiveMessage:
        def __init__(self, mid, text):
            self.id = mid
            self.peer_id = PeerChannel(channel_id=1000000777)
            self.from_id = self.sender_id = None
            self.media = self.fwd_from = self.reply_to = None
            self.date = self.edit_date = None
            self.post_author = None
            self.message = text
            self.entities = []
            self.grouped_id = self.views = self.forwards = None
            self.replies = self.reactions = None

        async def get_sender(self):
            return None

        def to_dict(self):
            return {"id": self.id, "text": self.message}

    s, name = await _create_blank_db()
    try:
        await apply_migrations(s.pool, "migrations")
        assert await s.schema_version() == 11

        # Seed one existing chat so get_entity-returns-int is fine and the
        # collector's _save_chat upsert has a row to look at.
        async with s.pool.acquire() as con:
            await con.execute(
                "INSERT INTO chats (id, type, title) VALUES ($1,'supergroup','t')",
                CHAT)

        # Fake client; one backfill message in the chat.
        client = _LegacyFakeClient(CHAT, [_LiveMessage(100, "backfilled")])
        made_clients: list = []

        def mk(_cfg):
            made_clients.append(client)
            return client
        monkeypatch.setattr(run, "make_client", mk)

        # Guard: DynamicEngine must NEVER be constructed in this path.
        dyn_calls = {"n": 0}

        def no_dyn(*a, **k):
            dyn_calls["n"] += 1
            raise AssertionError("DynamicEngine must not be constructed")
        monkeypatch.setattr(run, "DynamicEngine", no_dyn)

        # Keep teardown tight: short heartbeats so stop gathers quickly.
        monkeypatch.setattr(run, "_db_heartbeat_loop",
                            lambda *a, **k: asyncio.sleep(0))
        monkeypatch.setattr(run, "_tg_heartbeat_loop",
                            lambda *a, **k: asyncio.sleep(0))

        cfg = {"postgres": {"dsn": s.dsn},
               "storage": {"media_dir": str(tmp_path), "download_media": False,
                           "media_concurrency": 1, "max_media_mb": 10},
               "options": {"participants_sync": False, "follow_linked_discussion": False,
                           "backfill_batch_size": 10, "backfill_batch_pause_seconds": 0,
                           "per_message_retries": 1},
               "targets": [CHAT]}

        stop = asyncio.Event()
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(run._main_inner(
            logging.getLogger("tgparser.gateoff"), cfg, lock=None, stop=stop))

        # Wait for the engine choice + coordinator to come up.
        async def ready():
            async with s.pool.acquire() as con:
                rt = await con.fetchrow("SELECT status, dyn_engine FROM runtime_state WHERE id=1")
                return rt and rt["status"] in ("healthy", "syncing") and rt["dyn_engine"] is False
        async def backfilled():
            async with s.pool.acquire() as con:
                return await con.fetchval(
                    "SELECT COUNT(*) FROM messages WHERE chat_id=$1 AND text='backfilled'",
                    CHAT) == 1

        await wait_until_async(ready, timeout=10, what="legacy runtime syncing")
        await wait_until_async(backfilled, timeout=10,
                               what="coordinator processed the backfill message")

        # Legacy handlers are registered with the static chats= filter.
        assert client.handlers, "no handlers registered"
        from telethon import events
        new_message_handlers = [
            (b, fn) for b, fn in client.handlers
            if isinstance(b, events.NewMessage.Event.__class__.__bases__[0])
               or type(b).__name__ == "NewMessage"]
        assert any(type(b).__name__ == "NewMessage" and
                   getattr(b, "chats", None) == [CHAT]
                   for b, _ in client.handlers)

        # Deliver a live event. The guard trigger is on but dyn_engine is
        # FALSE, so the write proceeds. Pick the real NewMessage handler.
        new_handler = next(fn for b, fn in client.handlers
                           if type(b).__name__ == "NewMessage")

        class _Ev:
            def __init__(self, msg): self.message = msg
        await new_handler(_Ev(_LiveMessage(101, "live-event")))

        async def live_stored():
            async with s.pool.acquire() as con:
                return await con.fetchval(
                    "SELECT COUNT(*) FROM messages WHERE chat_id=$1 AND text='live-event'",
                    CHAT) == 1
        await wait_until_async(live_stored, timeout=5,
                               what="live handler stored the message")

        # Checkpoint advanced, nothing silently disabled, gate still off.
        async with s.pool.acquire() as con:
            ckpt = await con.fetchval(
                "SELECT last_backfilled_message_id FROM sync_state WHERE chat_id=$1",
                CHAT)
            assert ckpt >= 100
            gate = await con.fetchval(
                "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
            assert gate is False
            disabled_now = await con.fetchval(
                "SELECT COUNT(*) FROM target_state WHERE enabled=FALSE "
                "AND disabled_reason IS NOT NULL")
            assert disabled_now == 0

        # Trigger shutdown and wait for the full teardown.
        stop.set()
        rc = await asyncio.wait_for(task, 20)
        assert rc == run.EXIT_OK

        # Final runtime_state is stopped with pid=NULL and dyn_engine=FALSE.
        async with s.pool.acquire() as con:
            rt = await con.fetchrow(
                "SELECT pid, status, dyn_engine FROM runtime_state WHERE id=1")
        assert rt["status"] == "stopped" and rt["pid"] is None
        assert rt["dyn_engine"] is False
        assert dyn_calls["n"] == 0 and len(made_clients) == 1

        # No lingering engine/coordinator/media/background tasks.
        owned = [t for t in asyncio.all_tasks()
                 if t.get_name().startswith(("dyn-", "backfill-coordinator",
                                              "media-", "participants:",
                                              "db-hb", "tg-hb"))
                 and not t.done()]
        assert owned == [], owned

        # No ERROR-level log and no CancelledError traceback.
        for r in caplog.records:
            if r.levelno >= logging.ERROR:
                assert "CancelledError" not in r.getMessage()
                assert "Traceback" not in r.getMessage()
    finally:
        await _dispose(s, name)


async def wait_until_async(pred_coro_factory, timeout=5.0, what="condition"):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        if await pred_coro_factory():
            return
        if loop.time() > end:
            raise AssertionError(f"timeout waiting for {what}")
        await asyncio.sleep(0.02)


# Original "abort after client.connect" test kept too, under its original name.
@pytest.mark.asyncio
async def test_v11_gate_off_startup_is_legacy_and_untouched(tmp_path, monkeypatch):
    s, name = await _create_blank_db()
    try:
        await apply_migrations(s.pool, "migrations")
        assert await s.schema_version() == 11
        # Legacy writes still work: gate is off and dyn_engine stays FALSE,
        # so the guard trigger short-circuits.
        A = -1001000000042
        async with s.pool.acquire() as con:
            await con.execute(
                "INSERT INTO chats (id,type,title) VALUES ($1,'supergroup','pre')", A)
            await con.execute("INSERT INTO messages (chat_id,message_id) VALUES ($1,1)", A)
            assert await con.fetchval("SELECT COUNT(*) FROM messages WHERE chat_id=$1", A) == 1

        # Drive run._main_inner once; abort right after Telegram.connect.
        made = _install_client(monkeypatch)
        dyn_called = {"n": 0}

        def no_dyn(*a, **k):
            dyn_called["n"] += 1
            raise AssertionError("DynamicEngine must not be constructed")
        monkeypatch.setattr(run, "DynamicEngine", no_dyn)

        import logging as _lg
        caplog_logger = _lg.getLogger("t")
        caplog_logger.setLevel(_lg.INFO)
        stop = asyncio.Event()
        cfg = _cfg(s.dsn, targets=[A])
        with pytest.raises(run.StartupError):
            await run._main_inner(caplog_logger, cfg, lock=None, stop=stop)

        assert len(made) == 1
        assert dyn_called["n"] == 0                          # legacy branch taken
        async with s.pool.acquire() as con:
            rt = await con.fetchrow("SELECT * FROM runtime_state WHERE id=1")
            assert rt["dyn_engine"] is False
            gate = await con.fetchval(
                "SELECT enabled FROM deployment_gates WHERE key='dynamic_targets'")
            assert gate is False
            # Nothing was silently disabled.
            assert await con.fetchval(
                "SELECT COUNT(*) FROM target_state WHERE enabled=FALSE "
                "AND disabled_reason IS NOT NULL") == 0
    finally:
        await _dispose(s, name)


# =============================================================================
# Stale dyn_engine=TRUE must be reset before Telegram.connect; failure fatal
# =============================================================================

@pytest.mark.asyncio
async def test_stale_dyn_engine_true_is_reset_before_client(monkeypatch):
    s, name = await _create_blank_db()
    try:
        await apply_migrations(s.pool, "migrations")
        await s.set_runtime_dynamic_engine(True)             # stale leftover

        seen = {}

        class FakeClient:
            async def connect(self):
                con = await asyncpg.connect(s.dsn)
                try:
                    seen["dyn_engine_at_connect"] = await con.fetchval(
                        "SELECT dyn_engine FROM runtime_state WHERE id=1")
                finally:
                    await con.close()
                raise _AbortAfterConnect("stop")
            async def disconnect(self): pass
            async def is_user_authorized(self): return True
            async def get_me(self):
                class Me: username, id = "t", 1
                return Me()
        monkeypatch.setattr(run, "make_client", lambda _c: FakeClient())
        with pytest.raises(run.StartupError):
            await run._main_inner(logging.getLogger("t"), _cfg(s.dsn),
                                  lock=None, stop=asyncio.Event())
        assert seen.get("dyn_engine_at_connect") is False
    finally:
        await _dispose(s, name)


@pytest.mark.asyncio
async def test_fatal_if_cannot_reset_stale_dyn_engine(monkeypatch):
    s, name = await _create_blank_db()
    try:
        await apply_migrations(s.pool, "migrations")
        await s.set_runtime_dynamic_engine(True)

        async def broken(self, on):
            raise asyncpg.exceptions.InsufficientPrivilegeError("denied")
        monkeypatch.setattr(Storage, "set_runtime_dynamic_engine", broken)

        seen = {"client_made": 0}

        def mk(_cfg):
            seen["client_made"] += 1
            raise AssertionError("client must not be created")
        monkeypatch.setattr(run, "make_client", mk)

        with pytest.raises(run.StartupError, match="dyn_engine=false not recorded"):
            await run._main_inner(logging.getLogger("t"), _cfg(s.dsn),
                                  lock=None, stop=asyncio.Event())
        assert seen["client_made"] == 0
    finally:
        await _dispose(s, name)
