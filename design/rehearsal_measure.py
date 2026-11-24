"""Measure migration 0011: duration, table sizes before/after, locks taken,
seed counts. Runs against an on-the-fly temporary database and drops it.
"""
import asyncio
import os
import secrets
import time
from pathlib import Path

import asyncpg

from tgparser.config import load
from tgparser.migrations import apply as apply_migrations
from tgparser.storage import Storage
from tests._dyn_support import apply_up_to


TABLES_V10 = [
    "chats", "users", "memberships", "messages", "message_versions",
    "message_media", "message_reactions", "message_reaction_authors",
    "media", "sync_state", "backfill_queue", "deletion_events", "chat_actions",
    "errors", "target_state", "runtime_state", "coordinator_progress",
    "media_worker_state", "user_profile_queue", "user_profile_history",
    "user_observations", "identity_enrichment_queue", "access_hash_history",
    "avatar_history",
]
TABLES_V11_NEW = [
    "deployment_gates", "collection_config", "collection_title_rules",
    "target_preferences", "yaml_target_imports", "dialog_discovery_runs",
    "dialog_discovery_items", "discovered_dialogs", "discovery_control",
    "policy_state", "policy_block_history", "ui_audit_log",
]


async def _sizes(con, tables):
    out = {}
    for t in tables:
        exists = await con.fetchval("SELECT to_regclass($1) IS NOT NULL", t)
        if exists:
            n = await con.fetchval(f"SELECT COUNT(*) FROM {t}")
            out[t] = n
    return out


async def _seed_realistic_v10(con):
    A, B, C = -1001000000001, -1001000000002, -1001000000003
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


async def main():
    cfg = load()
    base = cfg["postgres"]["dsn"]
    name = f"tgparser_test_mig0011_{secrets.token_hex(4)}_{os.getpid()}"
    adm = await asyncpg.connect(base.rsplit("/", 1)[0] + "/postgres")
    await adm.execute(f'CREATE DATABASE "{name}"')
    await adm.close()
    try:
        dsn = base.rsplit("/", 1)[0] + "/" + name
        s = Storage(dsn)
        await s.connect()
        await apply_up_to(s, 10)
        async with s.pool.acquire() as con:
            await _seed_realistic_v10(con)
            v = await con.fetchval("SELECT max(version) FROM schema_migrations")
        assert v == 10
        async with s.pool.acquire() as con:
            before = await _sizes(con, TABLES_V10)

        # Capture AccessShare/RowExclusive locks taken by the migration
        # (pg_locks is per-session; sample from a parallel connection while
        # another one runs the migration).
        probe_con = await asyncpg.connect(dsn)
        locks: set = set()

        async def _probe_locks(target_pid):
            while True:
                try:
                    rows = await probe_con.fetch(
                        "SELECT c.relname, l.mode FROM pg_locks l "
                        "JOIN pg_class c ON c.oid = l.relation "
                        "WHERE l.pid = $1 AND c.relnamespace = 'public'::regnamespace",
                        target_pid)
                    for r in rows:
                        locks.add((r["relname"], r["mode"]))
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    return

        async with s.pool.acquire() as mig_con:
            server_pid = await mig_con.fetchval("SELECT pg_backend_pid()")
            probe = asyncio.create_task(_probe_locks(server_pid))
            t0 = time.monotonic()
            sql = Path("migrations/0011_dynamic_targets.sql").read_text()
            async with mig_con.transaction():
                await mig_con.execute(sql)
                await mig_con.execute(
                    "INSERT INTO schema_migrations (version) VALUES (11)")
            elapsed = time.monotonic() - t0
            probe.cancel()
            try:
                await probe
            except Exception:
                pass
        await probe_con.close()

        async with s.pool.acquire() as con:
            # Long-blocked queries would show up in pg_stat_activity;
            # right now there is none.
            blocked = await con.fetchval(
                "SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE state='active' AND wait_event_type='Lock'")
            after_v10 = await _sizes(con, TABLES_V10)
            after_v11 = await _sizes(con, TABLES_V11_NEW)

        print(f"elapsed_s: {elapsed:.3f}")
        print(f"blocked_backends: {blocked}")
        print("locks_taken:")
        for rel, mode in sorted(locks):
            print(f"  {mode:>22s}  {rel}")
        print("v10 tables (rows before -> after):")
        for t in TABLES_V10:
            b = before.get(t, "-")
            a = after_v10.get(t, "-")
            print(f"  {t:<30s} {b!s:>6} -> {a!s:>6}")
        print("v11 tables (new, rows):")
        for t in TABLES_V11_NEW:
            print(f"  {t:<30s} {after_v11.get(t, '-')}")
        await s.close()
    finally:
        adm = await asyncpg.connect(base.rsplit("/", 1)[0] + "/postgres")
        await adm.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await adm.close()


if __name__ == "__main__":
    asyncio.run(main())
