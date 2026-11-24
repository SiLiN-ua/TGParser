"""Benchmark of the v11 draft chat write guard. Temporary databases only.

  A  v10 schema, no guard trigger
  B  v11 draft, runtime_state.dyn_engine = FALSE (legacy)
  C  v11 draft, dyn_engine = TRUE, target enabled

Measures (DB creation and migration time excluded):
  1. executemany of N plain INSERTs into messages in one transaction
     (pipelined; worst case for relative overhead), R repeats, median;
  2. INSERT ... SELECT generate_series(N) with EXPLAIN ANALYZE trigger time;
  3. the real Storage write path of one message (upsert_message x2 with a
     version, media record + link, reactions, user observation, membership),
     M messages, R repeats, median per message.

Usage: .venv/bin/python design/bench_guard_overhead.py [N] [R] [M]
"""
import asyncio
import json
import os
import re
import secrets
import statistics
import sys
import time
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tgparser.config import load  # noqa: E402
from tgparser.migrations import apply as apply_migrations  # noqa: E402
from tgparser.storage import Storage  # noqa: E402

DRAFT = Path(__file__).resolve().parents[1] / "migrations" / "0011_dynamic_targets.sql"
CHAT = -1009999


async def make(base_dsn, v11: bool):
    name = f"tgparser_test_bench_{secrets.token_hex(4)}_{os.getpid()}"
    adm = await asyncpg.connect(base_dsn.rsplit("/", 1)[0] + "/postgres")
    await adm.execute(f'CREATE DATABASE "{name}"')
    await adm.close()
    s = Storage(base_dsn.rsplit("/", 1)[0] + "/" + name)
    await s.connect()
    await apply_migrations(s.pool, str(Path(__file__).resolve().parents[1] / "migrations"))
    if v11:
        async with s.pool.acquire() as con:
            async with con.transaction():
                await con.execute(DRAFT.read_text(encoding="utf-8"))
                await con.execute("INSERT INTO schema_migrations VALUES (11)")
    return s, name


async def drop(base_dsn, s, name):
    await s.close()
    adm = await asyncpg.connect(base_dsn.rsplit("/", 1)[0] + "/postgres")
    await adm.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    await adm.close()


async def reset(con):
    await con.execute("TRUNCATE messages, message_versions, message_media, media, "
                      "message_reactions, user_observations, memberships CASCADE")
    await con.execute("VACUUM ANALYZE messages")


async def bench_executemany(s, n, r):
    out = []
    async with s.pool.acquire() as con:
        for _ in range(r):
            await reset(con)
            rows = [(CHAT, i, "x") for i in range(n)]
            t0 = time.perf_counter()
            async with con.transaction():
                await con.executemany(
                    "INSERT INTO messages (chat_id, message_id, text) VALUES ($1,$2,$3)", rows)
            out.append(time.perf_counter() - t0)
    return out


async def bench_explain(s, n, r):
    totals, trig = [], []
    async with s.pool.acquire() as con:
        for _ in range(r):
            await reset(con)
            async with con.transaction():
                plan = await con.fetchval(
                    "EXPLAIN (ANALYZE, FORMAT JSON) INSERT INTO messages (chat_id, message_id, text) "
                    f"SELECT {CHAT}, g, 'x' FROM generate_series(1,{n}) g")
                p = json.loads(plan)[0]
                totals.append(p["Execution Time"] / 1000)
                trig.append(sum(t["Time"] for t in p.get("Triggers", [])
                                if t["Trigger Name"] == "trg_tgp_guard") / 1000)
                await con.execute("ROLLBACK; BEGIN")
    return totals, trig


async def bench_message_flow(s, m, r):
    out = []
    for _ in range(r):
        async with s.pool.acquire() as con:
            await reset(con)
        t0 = time.perf_counter()
        for i in range(m):
            fid = f"doc:{i}"
            await s.upsert_message({"chat_id": CHAT, "message_id": i, "text": "hi",
                                    "sender_user_id": 7, "media_file_id": fid})
            await s.upsert_message({"chat_id": CHAT, "message_id": i, "text": "edited",
                                    "sender_user_id": 7, "media_file_id": fid})
            await s.upsert_media_record({"file_id": fid, "chat_id": CHAT, "message_id": i,
                                         "media_type": "document", "status": "pending"})
            await s.link_message_media(CHAT, i, fid)
            await s.replace_reactions(CHAT, i, [{"reaction": "+1", "count": 2}])
            await s.record_user_observation(7, CHAT, i, "message_sender", "normal",
                                            None, None, False)
            await s.upsert_membership(CHAT, 7, "member")
        out.append((time.perf_counter() - t0) / m)
    return out


async def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    m = int(sys.argv[3]) if len(sys.argv) > 3 else 1_000
    base = load()["postgres"]["dsn"]
    results = {}
    for label, v11, dyn in (("A_v10_no_trigger", False, None),
                            ("B_v11_dyn_false", True, False),
                            ("C_v11_dyn_true_enabled", True, True)):
        s, name = await make(base, v11)
        try:
            if v11:
                async with s.pool.acquire() as con:
                    await con.execute("INSERT INTO target_state (chat_id, target, enabled) "
                                      "VALUES ($1,'bench',TRUE)", CHAT)
                await s.set_runtime_dynamic_engine(dyn)
            em = await bench_executemany(s, n, r)
            ex_total, ex_trig = await bench_explain(s, n, r)
            flow = await bench_message_flow(s, m, r)
            results[label] = {
                "executemany_s_median": statistics.median(em),
                "executemany_runs": em,
                "insert_select_exec_s_median": statistics.median(ex_total),
                "guard_trigger_s_median": statistics.median(ex_trig),
                "message_flow_ms_per_msg_median": statistics.median(flow) * 1000,
                "message_flow_runs_ms": [x * 1000 for x in flow],
            }
        finally:
            await drop(base, s, name)
    a = results["A_v10_no_trigger"]
    for k in ("B_v11_dyn_false", "C_v11_dyn_true_enabled"):
        x = results[k]
        for metric in ("executemany_s_median", "insert_select_exec_s_median",
                       "message_flow_ms_per_msg_median"):
            x[metric + "_vs_A_pct"] = round((x[metric] / a[metric] - 1) * 100, 1)
    print(json.dumps({"N": n, "repeats": r, "messages": m, "results": results}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
