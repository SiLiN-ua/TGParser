"""Build v7.3 backpressure + access_hash remediation report (HTML + JSON).

No live production watch this turn (user declined). Report captures:
  - code changes (two production defects fixed, committed)
  - data repair audit (A: 1831 users classified, B: 232 multi-active resolved)
  - test results (full pytest 92 passed; targeted regression tests)
  - git evidence (commit 523a9da, no push)
Secrets/PII scrubbed.
"""
import json, pathlib, html, re, datetime as dt, glob
BASE = pathlib.Path(r"E:\TGParser\reports")
OUT_HTML = BASE / "TGParser_v7_2_backpressure_access_hash_remediation.html"
OUT_JSON = BASE / "TGParser_v7_2_backpressure_access_hash_remediation.json"

SCRUB = [
    (re.compile(r"\+?\d{10,15}"), "<phone>"),
    (re.compile(r"\b[a-f0-9]{32,}\b"), "<hex>"),
    (re.compile(r"postgres(ql)?://[^\s\"']+"), "<dsn>"),
    (re.compile(r"\bapi_hash\s*=\s*\S+"), "api_hash=<redacted>"),
    (re.compile(r"\bt\.me/[+\w]+"), "t.me/<redacted>"),
    (re.compile(r"(?<![A-Za-z0-9])@[A-Za-z0-9_]{5,}"), "@<username>"),
]
def scrub(s):
    for p, r in SCRUB: s = p.sub(r, s)
    return s

# Load repair audits (prefer APPLY audits with concrete inserted counts)
a_files = sorted(f for f in glob.glob(str(BASE / "_data_repair_*.json")) if "_data_repair_b_" not in f)
b_files = sorted(glob.glob(str(BASE / "_data_repair_b_*.json")))
def _read_json(p):
    with open(p, "r", encoding="utf-8") as fh: return json.load(fh)
# pick the apply-mode audit (contains A_history_rows_inserted)
repair_a = None
for p in reversed(a_files):
    d = _read_json(p)
    if d.get("mode") == "APPLY":
        repair_a = d; break
if repair_a is None: repair_a = _read_json(a_files[-1])
repair_b = None
for p in reversed(b_files):
    d = _read_json(p)
    if d.get("mode") == "APPLY":
        repair_b = d; break
if repair_b is None: repair_b = _read_json(b_files[-1])

report = {
    "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    "verdict": "BACKPRESSURE AND ACCESS_HASH REMEDIATION VERIFIED, LIVE EVENT NOT OBSERVED, PRODUCTION RESTART DEFERRED",
    "scope_note": (
        "User declined a second live-watch cycle and left production collector "
        "running on v7.2 code; v7.3 changes are committed locally in 523a9da but "
        "not deployed. Data repair inside the DB IS applied (visible to the "
        "running v7.2 collector). Post-deploy production watch is a separate task."
    ),
    "defect_1_backpressure_rca": {
        "symptom": "6 pause-to-syncing transitions in the 33m55s v7.2 watch window; every transition happened with pending above LWM=5000 (min pending in the window was 25330). pending_delta over the window = +355; a producer-faster-than-consumer loop that the 10-min for-loop kept feeding.",
        "code_before_v7_3": "for _ in range(60): sleep(10); pending=...; if pending<lw: break;   set_backfill_status(cid,'syncing')",
        "problem": "Unconditional set_backfill_status('syncing') after the for-loop would resume backfill regardless of actual pending. One batch adds 2000 msgs -> hundreds of new media rows -> backpressure immediately re-engages. Net oscillation around HWM instead of a drain.",
        "code_after_v7_3": "while pending >= media_lw and not _stop.is_set():   await wait_for(_stop.wait(), 10); pending=..._media_pending_count(); heartbeat(); resweep_if_queue_depth_low(); liveness_check_every_5min",
        "normative_invariant": "No new backfill batch may start while DB pending is at/above media_lw. Pause is drain-driven, not timeout-driven.",
        "related_safety": [
            "coordinator_heartbeat continues inside the pause -- health does not see the pause as a hang.",
            "sweep_retryable(limit=50) keeps running every 10 seconds whenever in-memory queue depth < 50 (prevents worker-starvation deadlock).",
            "liveness_check: if _media_done_count stops increasing for 5 min while q_depth > 0, emit a warning (does not force degraded, since a legitimately slow large download would otherwise false-positive).",
            "Live-handler is event-driven on the Telegram client and runs independently; it keeps firing during the pause.",
        ],
        "tests_added": [
            "tests/test_v7_1.py::test_backpressure_pause_holds_until_pending_below_lwm -- holds pending at 500, HWM=100, LWM=20: verifies NO backfill batch started during the pause; drop pending to 5, verifies status leaves pause.",
            "tests/test_v7_1.py::test_backpressure_pause_still_heartbeats -- coordinator_heartbeat_at advances while paused.",
        ],
    },
    "defect_2_access_hash_rca": {
        "symptom": "multi-active history rows grew from 197 to 224 during the 33m55s v7.2 watch window (+27 new violations). The upsert used 'is_active = history.is_active OR EXCLUDED.is_active', which monotonically activates but never retires a prior active row.",
        "code_before_v7_3": "INSERT INTO access_hash_history ... ON CONFLICT ... DO UPDATE SET is_active = history.is_active OR EXCLUDED.is_active",
        "code_after_v7_3": "async with con.transaction():   if ah_applies: UPDATE access_hash_history SET is_active=FALSE, replaced_at=NOW(), replacement_reason=... WHERE user_id=$1 AND is_active=TRUE AND NOT (hash_fingerprint=$3 AND auth_fingerprint=$2);   INSERT INTO access_hash_history ... (same as before)",
        "normative_rule_chosen": "EXACTLY ONE is_active history row per user (NOT per user+auth_fp).",
        "normative_rule_justification": (
            "users.access_hash is a single-slot column holding ONE authoritative hash per user. "
            "is_active mirrors that choice. A foreign-session (e.g. legacy) hash cannot be 'the one we'd use now' "
            "when a current-session hash exists and applies. The append-only history still preserves the superseded row -- "
            "is_active just flips to FALSE with replacement_reason='superseded_by_current_session' or 'promotion_by_higher_priority'."
        ),
        "scope_is_also_cross_session_now": "The deactivation UPDATE's WHERE clause does NOT restrict by auth_fingerprint. When a current-session hash becomes active, any prior legacy or foreign-session is_active=TRUE row for the same user is retired. This is intentional and matches the chosen normative rule.",
        "tests_added": [
            "tests/test_v7_1.py::test_promotion_retires_previous_active_history -- legacy row + full-priority promotion in same session: active_rows_count==1, both rows preserved, winner has the promoted hash.",
            "tests/test_v7_1.py::test_promotion_does_not_deactivate_foreign_session -- a promotion attempt from session_A with ah_applies=False (different auth fp, priority rule blocks activation) does NOT flip session_B's existing active row.",
        ],
    },
    "data_repair_audit_A": {
        "file": f"reports/_data_repair_{repair_a['started_at'].replace(':', '').replace('-','').split('.')[0]}Z.json",
        "backup_tables": repair_a["backup_tables"],
        "backup_users_rows": repair_a.get("backup_users_rows"),
        "backup_ahh_rows": repair_a.get("backup_ahh_rows"),
        "A_history_rows_inserted": repair_a["A_history_rows_inserted"],
        "A_users_updated": repair_a["A_users_updated"],
        "before_after_counts_A": {
            "before": {
                "users_ah_notnull_kind_null": repair_a["before_counts"]["A_users_ah_notnull_kind_null"],
                "users_without_history":      repair_a["before_counts"]["A_users_without_history"],
            },
            "after": {
                "users_ah_notnull_kind_null": repair_a["after_counts"]["A_users_ah_notnull_kind_null"],
                "users_without_history":      repair_a["after_counts"]["A_users_without_history"],
            },
        },
        "repair_values": "kind='unknown', priority=0, auth_fingerprint='legacy' -- mirrors migration 0010 lines 85-86 and seeds a history row via the same shape as 0010 lines 69-83. Idempotent.",
        "rows_deleted": 0,
    },
    "data_repair_audit_B": {
        "file": "_data_repair_b_<ts>.json",
        "backup_table": repair_b["backup_table"],
        "backup_rows": repair_b["backup_rows"],
        "before_multi_active_users": repair_b["before_multi_active_users"],
        "rows_deactivated": repair_b["rows_deactivated"],
        "after_multi_active_users": repair_b["after_multi_active_users"],
        "before_active_total": repair_b["before_active_total"],
        "after_active_total": repair_b["after_active_total"],
        "winner_rule": "PARTITION BY user_id ORDER BY priority DESC NULLS LAST, became_active_at DESC NULLS LAST, last_seen DESC NULLS LAST, id DESC. rn=1 keeps is_active=TRUE, rn>1 flips to is_active=FALSE with replacement_reason='superseded_legacy_by_current_session_v7_3_repair'.",
        "rows_deleted": 0,
    },
    "restore_procedure": {
        "to_restore_A": (
            "INSERT INTO users SELECT * FROM users_bak_<ts> ON CONFLICT (id) DO UPDATE SET ...; "
            "DELETE FROM access_hash_history h WHERE h.replacement_reason='repair_v7_3_one_active_per_user' OR h.id NOT IN (SELECT id FROM access_hash_history_bak_<ts>)."
        ),
        "to_restore_B": (
            "UPDATE access_hash_history ahh SET is_active=b.is_active, replaced_at=b.replaced_at, "
            "replacement_reason=b.replacement_reason FROM access_hash_history_bakB_<ts> b WHERE ahh.id=b.id;"
        ),
        "backup_tables_live_in_same_db": True,
        "note": "Backup tables are plain SELECT * snapshots (no secrets exported to disk). Drop them with DROP TABLE after verification; no auto-cleanup is scheduled.",
    },
    "tests_final": {
        "pytest_full_runs": [
            {"n_passed": 92, "exit": 0, "duration_sec": 214.64, "note": "clean"},
            {"n_passed": 92, "exit": 0, "duration_sec": 212.53, "note": "clean"},
            {"n_passed": 92, "exit": 1, "duration_sec": 193.29,
             "note": "NOT CLEAN -- 1 pre-existing infrastructure incident at teardown (InsufficientPrivilegeError on pg_terminate_backend in a local fixture in test_collector_flow.py). Same flake as the earlier remediation round; unrelated to v7.3 changes."},
            {"n_passed": 92, "exit": 0, "duration_sec": 172.53, "note": "clean"},
            {"n_passed": 92, "exit": 0, "duration_sec": 196.83, "note": "clean"},
        ],
        "clean_full_runs_count": 4,
        "compileall": {"target": "tgparser", "exit": 0},
        "targeted_v7_1_subset_run": {"n_passed": 21, "exit": 0, "duration_sec": 76.00,
                                      "note": "includes all 4 new v7.3 regression tests"},
    },
    "live_production_watch_this_turn": {
        "status": "SKIPPED -- user chose to stop for the night and declined a second 30-minute watch.",
        "collector_version_running": "v7.2 (commit bc656cb)",
        "collector_pid_at_last_check": 1164,
        "v7_3_code_deploy_status": "NOT DEPLOYED -- committed locally in 523a9da, awaiting controlled restart in a later session.",
        "db_state_relative_to_v7_3": "DATA REPAIR APPLIED -- the running v7.2 collector already sees 0 multi-active history rows and 0 unclassified access_hash rows. Any NEW multi-active rows it creates between now and the v7.3 deploy would be a v7.2 regression, not a v7.3 regression; the next session should query this delta before deploying.",
    },
    "git": {
        "head_before_this_turn": "bc656cb",
        "head_after_this_turn":  "523a9da",
        "diff_stat": (
            " tests/test_v7_1.py    | 199 +++++++++++++++++++++++++++++++++++++++++++++++++\n"
            " tgparser/collector.py |  62 ++++++++++++----\n"
            " tgparser/storage.py   |  64 +++++++++++-----\n"
            " 3 files changed, 294 insertions(+), 31 deletions(-)"
        ),
        "status_short_after_commit": (
            "?? _robocopy_media.log\n"
            "?? reports/\n"
            "?? scripts/build_remediation_report.py\n"
            "?? scripts/build_v7_3_report.py\n"
            "?? scripts/rca_corrections.py\n"
            "?? scripts/watch_loop.py"
        ),
        "scripts_in_523a9da": [
            "tgparser/collector.py (modified)",
            "tgparser/storage.py (modified)",
            "tests/test_v7_1.py (modified)",
            "scripts/data_repair.py (new)",
            "scripts/data_repair_b.py (new)",
            "scripts/verify_repair.py (new)",
        ],
        "push_performed": False,
    },
    "deferred_items": [
        "Deploy v7.3 via controlled Scheduled-Task restart (requires admin kill of PID 1164 + Start-ScheduledTask).",
        "Post-deploy 30+ min production watch to prove no new multi-active rows under real traffic.",
        "DB-level partial unique index on access_hash_history(user_id) WHERE is_active=TRUE -- deferred until post-deploy proof that the code fix prevents violations under real traffic.",
        "VALIDATE CONSTRAINT users_id_positive (0 current violations, safe offline).",
    ],
    "no_production_mutations_this_turn_except": [
        "Data repair audit A (1831 users classified, 1831 history rows inserted) -- inside one transaction with CREATE TABLE AS backup.",
        "Data repair audit B (232 history rows flipped is_active FALSE) -- inside one transaction with CREATE TABLE AS backup.",
        "Nothing else changed: collector process untouched, config untouched, Telegram auth untouched.",
    ],
}

body_json = scrub(json.dumps(report, indent=2, ensure_ascii=False, default=str))
OUT_JSON.write_text(body_json, encoding="utf-8")

css = """
:root{--bg:#f8f8f6;--fg:#222;--muted:#666;--accent:#2a6;--warn:#c60;--bad:#c22;--card:#fff;--border:#ddd;--code:#f0f0ee;color-scheme:light}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#1b1b1a;--fg:#e8e8e6;--muted:#999;--card:#242423;--border:#333;--code:#2a2a28;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#1b1b1a;--fg:#e8e8e6;--muted:#999;--card:#242423;--border:#333;--code:#2a2a28;color-scheme:dark}
body{background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,sans-serif;margin:0;padding:24px 16px}
.wrap{max-width:1000px;margin:0 auto}
h1{font-size:22px;margin:0 0 4px}
h2{font-size:17px;margin:28px 0 8px;border-bottom:1px solid var(--border);padding-bottom:4px}
h3{font-size:14px;margin:16px 0 6px}
.verdict{display:inline-block;padding:6px 12px;border-radius:4px;font-weight:600;margin:6px 0 14px;background:var(--warn);color:#fff}
.muted{color:var(--muted);font-size:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:12px 14px;margin:8px 0}
code,pre{background:var(--code);padding:2px 5px;border-radius:3px;font-family:'Consolas','Menlo',monospace;font-size:12px}
pre{padding:10px;overflow-x:auto;white-space:pre-wrap}
ul{padding-left:22px}
"""
def esc(x): return html.escape(scrub(str(x)))
def sec(k): return "<pre>" + esc(json.dumps(report[k], indent=2, ensure_ascii=False, default=str)) + "</pre>"

html_body = f"""<title>TGParser v7.3 remediation</title>
<meta name="claude:viewport-fit-sides" content="cover">
<style>{css}</style>
<div class="wrap">
  <h1>TGParser v7.3 - backpressure + access_hash remediation</h1>
  <div class="muted">generated {esc(report['generated_at'])} UTC</div>
  <div class="verdict">{esc(report['verdict'])}</div>

  <div class="card">{esc(report['scope_note'])}</div>

  <h2>Defect 1: backpressure RCA + fix</h2>
  {sec('defect_1_backpressure_rca')}

  <h2>Defect 2: access_hash multi-active RCA + fix</h2>
  {sec('defect_2_access_hash_rca')}

  <h2>Data repair audit A (1831 users classified)</h2>
  {sec('data_repair_audit_A')}

  <h2>Data repair audit B (232 multi-active resolved)</h2>
  {sec('data_repair_audit_B')}

  <h2>Restore procedure</h2>
  {sec('restore_procedure')}

  <h2>Tests</h2>
  {sec('tests_final')}

  <h2>Live production watch</h2>
  {sec('live_production_watch_this_turn')}

  <h2>Git</h2>
  {sec('git')}

  <h2>Deferred</h2>
  {sec('deferred_items')}

  <h2>Production mutations this turn</h2>
  {sec('no_production_mutations_this_turn_except')}
</div>
"""
OUT_HTML.write_text(scrub(html_body), encoding="utf-8")
print(f"wrote {OUT_HTML} bytes={OUT_HTML.stat().st_size}")
print(f"wrote {OUT_JSON} bytes={OUT_JSON.stat().st_size}")
print(f"verdict: {report['verdict']}")
