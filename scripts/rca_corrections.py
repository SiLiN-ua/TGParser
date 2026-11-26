"""Read-only evidence corrections for the remediation report.

Produces _corrections.json with:
  1. Tick arithmetic (gaps, batches, attribution via timestamp clustering)
  2. Media metric definitions + SQL + overlap analysis
  3. Backpressure transitions with full context
  4. Errors schema + corrected query
  5. Backfill exit condition from code reading

NOTHING is written to production tables.
"""
import asyncio, json, pathlib, datetime as dt, sys
sys.path.insert(0, r"E:\TGParser")
import asyncpg
from tgparser.config import load as cfg_load

BASE = pathlib.Path(r"E:\TGParser\reports")
TICKS = BASE / "_watch_remediation.jsonl"
OUT = BASE / "_corrections.json"

def parse_ts(s): return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))

def load_ticks():
    return [json.loads(l) for l in TICKS.read_text("utf-8").splitlines() if l.strip()]

def analyze_gaps(ticks):
    """Cluster ticks into runs separated by >90s gaps.
    Each contiguous run is likely one watcher."""
    runs = []
    cur = []
    prev_ts = None
    for t in ticks:
        ts = parse_ts(t["ts"])
        if prev_ts is not None and (ts - prev_ts).total_seconds() > 90:
            runs.append(cur); cur = []
        cur.append(t)
        prev_ts = ts
    if cur: runs.append(cur)
    return runs

def transitions(ticks):
    out = []
    prev = None
    for t in ticks:
        bf = t["sync"]["backfill_status"]
        if prev is not None and bf != prev["bf"]:
            out.append({
                "ts": t["ts"],
                "from_status": prev["bf"],
                "to_status": bf,
                "pending_before": prev["pending"],
                "pending_after": t["media_by_status"].get("pending"),
                "last_msg_before": prev["last_msg"],
                "last_msg_after": t["sync"]["last_backfilled_message_id"],
                "batches_before": prev["batches"],
                "batches_after": t["sync"]["batches_completed"],
                "batch_running_after": (t.get("coord") or {}).get("batch_running"),
            })
        prev = {
            "bf": bf,
            "pending": t["media_by_status"].get("pending"),
            "last_msg": t["sync"]["last_backfilled_message_id"],
            "batches": t["sync"]["batches_completed"],
        }
    return out

async def errors_schema(conn):
    cols = await conn.fetch("""
      SELECT column_name, data_type FROM information_schema.columns
      WHERE table_name='errors' ORDER BY ordinal_position
    """)
    return [dict(c) for c in cols]

async def errors_evidence(conn, cols, t0_utc, tN_utc):
    colnames = {c["column_name"] for c in cols}
    ts_col = None
    for cand in ("ts", "created_at", "occurred_at", "at", "when", "logged_at"):
        if cand in colnames: ts_col = cand; break
    out = {"ts_column_used": ts_col, "total_rows": None,
           "within_watch_window": None, "unresolved": None,
           "by_type_within_window": []}
    out["total_rows"] = await conn.fetchval("SELECT COUNT(*) FROM errors")
    if ts_col:
        try:
            out["within_watch_window"] = await conn.fetchval(
                f"SELECT COUNT(*) FROM errors WHERE {ts_col} >= $1 AND {ts_col} <= $2",
                parse_ts(t0_utc), parse_ts(tN_utc))
            if "resolved" in colnames or "resolved_at" in colnames:
                rc = "resolved" if "resolved" in colnames else "resolved_at IS NOT NULL"
                if rc == "resolved":
                    out["unresolved"] = await conn.fetchval(
                        "SELECT COUNT(*) FROM errors WHERE COALESCE(resolved, FALSE) = FALSE")
                else:
                    out["unresolved"] = await conn.fetchval(
                        "SELECT COUNT(*) FROM errors WHERE resolved_at IS NULL")
            kind_col = None
            for cand in ("kind", "type", "class", "category", "source"):
                if cand in colnames: kind_col = cand; break
            if kind_col:
                rows = await conn.fetch(
                    f"SELECT {kind_col} AS k, COUNT(*)::int AS n FROM errors "
                    f"WHERE {ts_col} >= $1 AND {ts_col} <= $2 GROUP BY 1 ORDER BY 2 DESC LIMIT 20",
                    parse_ts(t0_utc), parse_ts(tN_utc))
                out["by_type_within_window"] = [{"kind": r["k"], "n": r["n"]} for r in rows]
        except Exception as e:
            out["__error__"] = f"{type(e).__name__}: {str(e)[:200]}"
    return out

async def media_metric_definitions(conn):
    defs = {}
    defs["media_pending_DEF"] = "SELECT COUNT(*) FROM media WHERE status='pending'"
    defs["media_pending"] = await conn.fetchval(defs["media_pending_DEF"])
    defs["media_done_DEF"] = "SELECT COUNT(*) FROM media WHERE status='done'"
    defs["media_done"] = await conn.fetchval(defs["media_done_DEF"])
    defs["media_failed_DEF"] = "SELECT COUNT(*) FROM media WHERE status='failed'"
    defs["media_failed"] = await conn.fetchval(defs["media_failed_DEF"])
    defs["eligible_for_sweep_DEF"] = (
        "SELECT COUNT(*) FROM media WHERE status IN ('pending','failed') "
        "AND attempts < 5 AND chat_id IS NOT NULL AND message_id IS NOT NULL "
        "-- matches storage.media_get_retryable()")
    defs["eligible_for_sweep"] = await conn.fetchval(
        "SELECT COUNT(*) FROM media WHERE status IN ('pending','failed') "
        "AND attempts < 5 AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    defs["retryable_pending_only_DEF"] = (
        "SELECT COUNT(*) FROM media WHERE status='pending' AND attempts < 5 "
        "AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    defs["retryable_pending_only"] = await conn.fetchval(
        "SELECT COUNT(*) FROM media WHERE status='pending' AND attempts < 5 "
        "AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    defs["retryable_failed_only_DEF"] = (
        "SELECT COUNT(*) FROM media WHERE status='failed' AND attempts < 5 "
        "AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    defs["retryable_failed_only"] = await conn.fetchval(
        "SELECT COUNT(*) FROM media WHERE status='failed' AND attempts < 5 "
        "AND chat_id IS NOT NULL AND message_id IS NOT NULL")
    defs["pending_already_at_max_attempts"] = await conn.fetchval(
        "SELECT COUNT(*) FROM media WHERE status='pending' AND attempts >= 5")
    defs["failed_already_at_max_attempts"] = await conn.fetchval(
        "SELECT COUNT(*) FROM media WHERE status='failed' AND attempts >= 5")
    defs["in_memory_queue_depth_SOURCE"] = (
        "MediaQueue.queue.qsize() + len(MediaQueue._inflight) in collector process; "
        "not observable from DB. Recorded only in logs, not DB."
    )
    defs["note"] = (
        "eligible_for_sweep is NOT a subset of media_pending alone: it unions "
        "status IN ('pending','failed') filtered by attempts<5 and non-null chat/message. "
        "So eligible_for_sweep >= retryable_pending_only, and separately >= retryable_failed_only. "
        "At tN eligible_for_sweep = retryable_pending_only + retryable_failed_only "
        "(for attempts<5 with pointer). The earlier report's 'eligible_pending=30498' label "
        "was misleading: the value is `pending+failed` with attempts<5, not a pending-only subset."
    )
    return defs

async def access_hash_live(conn):
    r = {}
    r["A_1976_count"] = await conn.fetchval(
        "SELECT COUNT(*) FROM users WHERE access_hash IS NOT NULL AND access_hash_kind IS NULL")
    r["A_no_history"] = await conn.fetchval(
        "SELECT COUNT(*) FROM users u "
        "WHERE u.access_hash IS NOT NULL AND u.access_hash_kind IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM access_hash_history h WHERE h.user_id=u.id)")
    r["B_two_active"] = await conn.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM ("
        "  SELECT user_id FROM access_hash_history WHERE is_active=TRUE "
        "  GROUP BY user_id HAVING COUNT(*) > 1) t")
    # last_updated after watch window starts
    r["new_unclassified_since_watch_start"] = await conn.fetchval(
        "SELECT COUNT(*) FROM users WHERE access_hash IS NOT NULL "
        "AND access_hash_kind IS NULL AND last_updated >= $1",
        parse_ts("2026-11-21T21:10:01Z"))
    # new history rows since watch start
    r["new_history_rows_since_watch_start"] = await conn.fetchval(
        "SELECT COUNT(*) FROM access_hash_history WHERE first_seen >= $1",
        parse_ts("2026-11-21T21:10:01Z"))
    # new two-active cases since: approximate via became_active_at
    r["new_active_history_rows_since_watch_start"] = await conn.fetchval(
        "SELECT COUNT(*) FROM access_hash_history WHERE became_active_at >= $1 "
        "AND is_active=TRUE",
        parse_ts("2026-11-21T21:10:01Z"))
    return r

async def main():
    ticks = load_ticks()
    runs = analyze_gaps(ticks)
    cfg = cfg_load()
    conn = await asyncpg.connect(cfg["postgres"]["dsn"])
    err_cols = await errors_schema(conn)
    t0 = ticks[0]["ts"]; tN = ticks[-1]["ts"]
    err = await errors_evidence(conn, err_cols, t0, tN)
    media_defs = await media_metric_definitions(conn)
    ah = await access_hash_live(conn)
    await conn.close()

    out = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "tick_arithmetic": {
            "total_ticks": len(ticks),
            "runs_detected": len(runs),
            "runs": [
                {"index": i, "ticks": len(r),
                 "first_ts": r[0]["ts"], "last_ts": r[-1]["ts"],
                 "duration_s": (parse_ts(r[-1]["ts"]) - parse_ts(r[0]["ts"])).total_seconds(),
                 "pid_observed_in_runtime_state": r[0]["runtime"]["pid"]}
                for i, r in enumerate(runs, 1)
            ],
        },
        "max_gap_between_ticks_s": max(
            [(parse_ts(ticks[i+1]["ts"]) - parse_ts(ticks[i]["ts"])).total_seconds()
             for i in range(len(ticks)-1)] or [0]),
        "max_gap_location": None,
        "transitions": transitions(ticks),
        "errors_table_schema": err_cols,
        "errors_evidence": err,
        "media_metric_definitions": media_defs,
        "access_hash_live_recheck": ah,
    }
    # find location of max gap
    maxg = 0; loc = None
    for i in range(len(ticks)-1):
        g = (parse_ts(ticks[i+1]["ts"]) - parse_ts(ticks[i]["ts"])).total_seconds()
        if g > maxg: maxg = g; loc = (ticks[i]["ts"], ticks[i+1]["ts"])
    out["max_gap_location"] = {"from": loc[0], "to": loc[1], "gap_s": maxg} if loc else None

    OUT.write_text(json.dumps(out, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {OUT} size={OUT.stat().st_size}")
    print(f"total ticks: {len(ticks)}")
    print(f"runs: {len(runs)}")
    for i, r in enumerate(runs, 1):
        print(f"  run {i}: {len(r)} ticks, {r[0]['ts']} -> {r[-1]['ts']}")
    print(f"max gap: {maxg:.1f}s at {loc}")
    print(f"transitions: {len(out['transitions'])}")
    for tr in out["transitions"]:
        print(f"  {tr['ts']} {tr['from_status']} -> {tr['to_status']}  "
              f"pending {tr['pending_before']}->{tr['pending_after']}  "
              f"last_msg {tr['last_msg_before']}->{tr['last_msg_after']}")
    print(f"errors_table columns: {[c['column_name'] for c in err_cols]}")
    print(f"errors evidence: {err}")
    print(f"ah live: {ah}")

asyncio.run(main())
