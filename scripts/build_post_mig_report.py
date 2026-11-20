"""Build reports/TGParser_v7_1_post_migration.{html,json} from captured data.

Secret scrubber applied before write.
"""
import json, os, sys, re, html as _h, datetime as dt
PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT); sys.path.insert(0, PROJECT)

TICKS = os.path.join(PROJECT, "reports", "_post_mig_ticks.jsonl")
ACC = os.path.join(PROJECT, "reports", "_acceptance_queries.json")
OUT_JSON = os.path.join(PROJECT, "reports", "TGParser_v7_1_post_migration.json")
OUT_HTML = os.path.join(PROJECT, "reports", "TGParser_v7_1_post_migration.html")

# ---------- scrubber ----------
SECRETS_PATTERNS = [
    re.compile(r"\+?\d{10,15}"),                               # phones
    re.compile(r"[a-f0-9]{32,}", re.I),                        # api_hash / fingerprints
    re.compile(r"[A-Za-z0-9_-]{20,}:[A-Za-z0-9_-]{20,}"),      # bot tokens
    re.compile(r"postgres(?:ql)?://[^\"'\s]+"),                # DSNs
    re.compile(r"@[A-Za-z][A-Za-z0-9_]{3,31}"),                # usernames
    re.compile(r"t\.me/[^\s\"'<]+"),                           # invite links
    re.compile(r"POSTGRES_PASSWORD\s*=\s*\S+"),
    re.compile(r"TG_API_(?:ID|HASH)\s*=\s*\S+"),
]
# access_hash values are 64-bit signed integers; strip any field explicitly named
AH_FIELDS = {"access_hash", "api_hash", "phone", "bio", "username", "session"}


def scrub(obj):
    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            if k in AH_FIELDS:
                clean[k] = "<redacted>"
            else:
                clean[k] = scrub(v)
        return clean
    if isinstance(obj, list):
        return [scrub(x) for x in obj]
    if isinstance(obj, str):
        s = obj
        for p in SECRETS_PATTERNS:
            s = p.sub("<redacted>", s)
        return s
    return obj

# ---------- load ----------
ticks = []
with open(TICKS, encoding="utf-8") as f:
    for line in f:
        ticks.append(json.loads(line))
with open(ACC, encoding="utf-8") as f:
    acc = json.load(f)

baseline = ticks[0]["snapshot"]
final = ticks[-1]["snapshot"]
pids = [t["snapshot"]["runtime"]["pid"] for t in ticks if t["snapshot"].get("runtime")]
pid_stable = len(set(pids)) == 1
hb_ages = [t["snapshot"]["runtime"]["heartbeat_age_sec"] for t in ticks if t["snapshot"].get("runtime")]
max_hb = max(hb_ages) if hb_ages else None
min_hb = min(hb_ages) if hb_ages else None
tg_ages = [t["snapshot"]["runtime"]["tg_age_sec"] for t in ticks if t["snapshot"].get("runtime") and t["snapshot"]["runtime"].get("tg_age_sec") is not None]
max_tg = max(tg_ages) if tg_ages else None

def delta(key):
    return (final.get(key) or 0) - (baseline.get(key) or 0)

delta_msgs = delta("messages")
delta_users = delta("users")
delta_obs = delta("observations")
delta_media_done = (final["media_by_status"].get("done", 0)
                    - baseline["media_by_status"].get("done", 0))
delta_media_pending = (final["media_by_status"].get("pending", 0)
                       - baseline["media_by_status"].get("pending", 0))
duplicate_count_last = final.get("duplicate_count", 0)
errors_1h_last = final.get("errors_last_hour", 0)
ckpt_base = baseline.get("checkpoint") or {}
ckpt_last = final.get("checkpoint") or {}
ckpt_moves = {k: (ckpt_last.get(k, 0) - ckpt_base.get(k, 0)) for k in ckpt_last}

postgres_ever_down = any(
    t["snapshot"].get("runtime") is None
    or t["snapshot"]["runtime"].get("heartbeat_age_sec", 0) > 300
    for t in ticks
)

# verdict
criteria = {
    "heartbeat_fresh":      max_hb is not None and max_hb <= 120,
    "postgres_always_up":   not postgres_ever_down,
    "status_syncing_or_healthy": final["runtime"]["status"] in ("syncing", "healthy"),
    "checkpoint_or_counters_move": (
        any(v != 0 for v in ckpt_moves.values())
        or delta_msgs > 0 or delta_users > 0 or delta_obs > 0
        or delta_media_done != 0 or delta_media_pending != 0
    ),
    "duplicate_count_zero": duplicate_count_last == 0,
    "errors_last_hour_zero": errors_1h_last == 0,
    "backpressure_managed": final["media_by_status"].get("pending", 0) < 50000,
    "single_collector": pid_stable,
    "live_handler_passthrough_proven": False,  # no live message observed in window
}
all_pass = all(criteria.values())

# gap findings (known, from acceptance_queries + diag)
gaps = [
    {"id": "G1", "title": "Users with access_hash but kind=NULL and no history row",
     "count": 1976,
     "detail": "A write path assigns users.access_hash without routing through "
               "upsert_user_from_entity; those users carry no classification and no "
               "access_hash_history entry. Does NOT violate the min<full rule; it "
               "degrades classification coverage. Needs code-level investigation."},
    {"id": "G2", "title": "Legacy history rows still is_active=TRUE after promotion",
     "count": 197,
     "detail": "When a user is promoted from legacy 'unknown' to full/from_message, "
               "the prior legacy access_hash_history row is not deactivated. "
               "Multiple is_active=TRUE rows coexist for the same user. Users row "
               "still cites the correct higher-priority hash."},
    {"id": "G3", "title": "Cross-session active history rows",
     "count": 183,
     "detail": "183 users have an access_hash_history row where auth_fingerprint "
               "differs from the one in the users row and is_active=TRUE. Overlaps "
               "with G2: legacy 'legacy' auth_fp vs current 'default' auth_fp."},
    {"id": "G4", "title": "No live writes during 30-min observation window",
     "count": 0,
     "detail": "msgs, users, observations, media_done and media_pending all "
               "showed zero delta over 30 consecutive 60s snapshots. backfill is in "
               "the documented 'syncing_paused_media_backpressure' state "
               "(pending=25696 > HWM=25000); media downloaders also made zero "
               "progress during the window. live_handler update_last_received_at "
               "is NULL since process start 16h ago. Cause not identified in this "
               "pass; not a data-corruption issue, but violates the "
               "'checkpoint or counters move' acceptance criterion."},
    {"id": "G5", "title": "Orphan media rows from removed target",
     "count": 54,
     "detail": "54 rows in message_media reference a chat_id (-100184507...) that "
               "is no longer in the active targets list. Historical leftover; harmless."},
    {"id": "G6", "title": "Invalid constraint on users.id",
     "count": 1,
     "detail": "users_id_positive CHECK constraint shows convalidated=false "
               "(created NOT VALID). Still enforced for new inserts. Cosmetic."},
    {"id": "G7", "title": "PostgreSQL BEX64 crash pre-observation",
     "count": 1,
     "detail": "postgres.exe crashed four times today ~09:09 EET (buffer overflow "
               "in ucrtbase.dll) BEFORE the 30-min window; restarted cleanly as "
               "admin. Defender exclusions added for postgres paths and processes. "
               "Did not recur during the 30-min window. Separate concern from the "
               "collector-idle finding above."},
]

summary = {
    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    "ticks_captured": len(ticks),
    "baseline_at": ticks[0]["at"],
    "final_at": ticks[-1]["at"],
    "config_proven": {
        "postgres_service_imagepath_target": "E:\\PostgreSQL\\17\\data",
        "postgres_data_dir_old_exists": os.path.exists("C:\\Program Files\\PostgreSQL\\17\\data\\postgresql.conf"),
        "postgres_data_dir_new_exists": os.path.exists("E:\\PostgreSQL\\17\\data\\postgresql.conf"),
        "task_execute": "E:\\TGParser\\.venv\\Scripts\\pythonw.exe",
        "task_arguments": '"E:\\TGParser\\run.py"',
        "task_working_directory": "E:\\TGParser",
        "task_logontype": "S4U",
        "task_runlevel": "Highest",
        "task_multiple_instances": "IgnoreNew",
        "task_missed_runs": 911,
        "task_missed_runs_note": "911 per-minute trigger firings skipped because an instance was already running — IgnoreNew is enforcing single-process invariant",
        "collector_pid": pids[0] if pids else None,
        "collector_started_at": final["runtime"].get("heartbeat_at") and ticks[0]["snapshot"]["runtime"].get("heartbeat_at"),
        "no_old_path_processes": True,
    },
    "run_30min": {
        "tick_count": len(ticks),
        "interval_sec": 60,
        "pid_stable": pid_stable,
        "heartbeat_age_sec": {"min": min_hb, "max": max_hb},
        "telegram_last_ok_age_sec": {"max": max_tg},
        "postgres_ever_down": postgres_ever_down,
        "status_final": final["runtime"]["status"],
        "backfill_status_final": final.get("backfill_status"),
        "checkpoint_baseline": ckpt_base,
        "checkpoint_final": ckpt_last,
        "checkpoint_delta": ckpt_moves,
        "counter_deltas": {
            "messages": delta_msgs,
            "users": delta_users,
            "observations": delta_obs,
            "media_done": delta_media_done,
            "media_pending": delta_media_pending,
        },
        "live_lag_sec_final": final.get("live_lag_sec"),
        "duplicate_count_final": duplicate_count_last,
        "errors_last_hour_final": errors_1h_last,
    },
    "schema": {
        "migrations_applied": final.get("schema_migrations"),
        "db_size_mb": final.get("db_size_mb"),
    },
    "access_hash_v71": {
        "users_by_kind_live": acc["access_hash"]["users_by_kind_live"],
        "users_by_priority_live": acc["access_hash"]["users_by_priority_live"],
        "stale_count": acc["access_hash"]["stale_count"],
        "users_without_hash": acc["access_hash"]["users_without_hash"],
        "ahh_total": acc["access_hash"]["ahh_total"],
        "ahh_active": acc["access_hash"]["ahh_active"],
        "ahh_by_kind": acc["access_hash"]["ahh_by_kind"],
        "ahh_by_source": acc["access_hash"]["ahh_by_source"],
        "ahh_new_since_v71": acc["access_hash"]["ahh_new_since_v71"],
        "lower_priority_quarantined": acc["access_hash"]["lower_priority_quarantined"],
        "promotion_cases": acc["access_hash"]["promotion_cases"],
        "cross_session_hashes_active": acc["access_hash"]["cross_session_hashes_active"],
        "cross_session_hashes_in_history": acc["access_hash"]["cross_session_hashes_in_history"],
        "auth_fp_distinct_in_users": acc["access_hash"]["auth_fp_distinct_in_users"],
        "ahh_auth_fp_distinct": acc["access_hash"]["ahh_auth_fp_distinct"],
    },
    "enrichment_queue": acc["enrichment_queue"],
    "observations": {
        "duplicates": acc["observations"]["obs_duplicates"],
        "total": acc["observations"]["obs_total"],
        "by_source_type": acc["observations"]["obs_by_source_type"],
        "fallback_estimate": acc["observations"]["obs_from_fallback_estimate"],
        "new_since_v71": acc["observations"]["obs_new_since_v71"],
        "multi_chat_users": acc["observations"]["multi_chat_users"],
    },
    "integrity": {
        "orphan_messages_without_chat": acc["orphans_and_integrity"]["orphan_messages_without_chat"],
        "orphan_observations_without_user": acc["orphans_and_integrity"]["orphan_observations_without_user"],
        "orphan_media_without_message": acc["orphans_and_integrity"]["orphan_media_without_message"],
        "invalid_indexes": acc["orphans_and_integrity"]["invalid_indexes"],
        "invalid_constraints": acc["orphans_and_integrity"]["invalid_constraints"],
        "db_size_bytes": acc["orphans_and_integrity"]["db_size_bytes"],
    },
    "pytest": {
        "runs": 3,
        "all_passed": True,
        "tests_per_run": 87,
        "durations_sec": [149.75, 216.17, 181.11],
    },
    "compileall_exit": 0,
    "criteria": criteria,
    "all_criteria_passed": all_pass,
    "gaps": gaps,
    "verdict": "POST-MIGRATION ACCEPTED" if all_pass else "POST-MIGRATION NOT ACCEPTED",
}

summary = scrub(summary)

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

# ---------- HTML ----------
def e(x): return _h.escape(str(x))
def kv(d):
    return "<table class='kv'>" + "".join(
        f"<tr><th>{e(k)}</th><td><code>{e(json.dumps(v, ensure_ascii=False) if isinstance(v,(dict,list)) else v)}</code></td></tr>"
        for k, v in d.items()
    ) + "</table>"

verdict_class = "ok" if summary["all_criteria_passed"] else "fail"
crit_rows = "".join(
    f"<tr><td>{e(k)}</td><td class='{'ok' if v else 'fail'}'>{e(v)}</td></tr>"
    for k, v in summary["criteria"].items()
)
gaps_rows = "".join(
    f"<tr><td>{e(g['id'])}</td><td>{e(g['title'])}</td><td>{e(g['count'])}</td><td>{e(g['detail'])}</td></tr>"
    for g in summary["gaps"]
)

html = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>TGParser v7.1 post-migration</title>
<style>
:root {{
  --bg:#fafafa; --fg:#111; --mut:#666; --acc:#1366d6;
  --ok:#0a7c2a; --fail:#b22222; --card:#fff; --bd:#e2e2e2; --code:#f3f4f6;
  color-scheme: light;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    --bg:#0b0d10; --fg:#e6e6e6; --mut:#9aa0a6; --acc:#6ba6ff;
    --ok:#4ade80; --fail:#f87171; --card:#15181c; --bd:#2a2d31; --code:#1e2126;
    color-scheme: dark;
  }}
}}
:root[data-theme="dark"] {{
  --bg:#0b0d10; --fg:#e6e6e6; --mut:#9aa0a6; --acc:#6ba6ff;
  --ok:#4ade80; --fail:#f87171; --card:#15181c; --bd:#2a2d31; --code:#1e2126;
  color-scheme: dark;
}}
body {{ background:var(--bg); color:var(--fg);
  font: 14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;
  padding-left:max(16px,env(safe-area-inset-left,0px));
  padding-right:max(16px,env(safe-area-inset-right,0px));
  padding-block:24px 48px; max-width:980px; margin:0 auto; }}
h1 {{ font: 600 22px/1.3 system-ui; margin:0 0 4px; }}
h2 {{ font: 600 16px/1.3 system-ui; margin:28px 0 8px; }}
.sub {{ color:var(--mut); margin-bottom:20px; }}
.card {{ background:var(--card); border:1px solid var(--bd); border-radius:8px;
  padding:14px 16px; margin:10px 0; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--bd); vertical-align:top; }}
th {{ color:var(--mut); font-weight:500; width:38%; }}
code {{ background:var(--code); padding:1px 5px; border-radius:3px; font-size:12px; }}
.verdict {{ font: 700 18px/1 system-ui; padding:14px 18px; border-radius:8px;
  margin:16px 0; letter-spacing:.3px; }}
.verdict.ok {{ background:rgba(10,124,42,.14); color:var(--ok); }}
.verdict.fail {{ background:rgba(178,34,34,.14); color:var(--fail); }}
.ok {{ color:var(--ok); }} .fail {{ color:var(--fail); }}
.gap {{ font-size:13px; }}
details summary {{ cursor:pointer; padding:6px 0; color:var(--acc); }}
</style>

<h1>TGParser v7.1 — post-migration acceptance</h1>
<div class="sub">Generated {e(summary['generated_at'])} · 30-min window {e(summary['baseline_at'])} → {e(summary['final_at'])}</div>

<div class="verdict {verdict_class}">{e(summary['verdict'])}</div>

<h2>Acceptance criteria</h2>
<div class="card"><table>
  <tr><th>criterion</th><th>result</th></tr>
  {crit_rows}
</table></div>

<h2>Config proven (post-move)</h2>
<div class="card">{kv(summary['config_proven'])}</div>

<h2>30-min watch</h2>
<div class="card">{kv(summary['run_30min'])}</div>

<h2>Schema &amp; size</h2>
<div class="card">{kv(summary['schema'])}</div>

<h2>v7.1 access_hash on live data</h2>
<div class="card">{kv(summary['access_hash_v71'])}</div>

<h2>Enrichment queue</h2>
<div class="card">{kv(summary['enrichment_queue'])}</div>

<h2>Observations</h2>
<div class="card">{kv(summary['observations'])}</div>

<h2>Integrity</h2>
<div class="card">{kv(summary['integrity'])}</div>

<h2>pytest &amp; compileall</h2>
<div class="card">{kv(summary['pytest'])}<br>compileall exit = {summary['compileall_exit']}</div>

<h2>Gaps and open findings</h2>
<div class="card"><table class="gap">
  <tr><th style="width:4%">id</th><th style="width:28%">title</th><th style="width:8%">count</th><th>detail</th></tr>
  {gaps_rows}
</table></div>
"""

with open(OUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("verdict:", summary["verdict"])
print("all_criteria_passed:", summary["all_criteria_passed"])
print("wrote:", OUT_JSON)
print("wrote:", OUT_HTML)
