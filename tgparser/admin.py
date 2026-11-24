"""Administrative CLI for TGParser.

Currently exposes:

    python -m tgparser.admin policy-dry-run [--json]

Dry-run is READ-ONLY. It never writes to target_state, policy_state,
deployment_gates, or runtime_state, and never opens a Telegram client. Use it
to preview what the strict v_target_policy VIEW would ask the collector to
enable or disable vs. the current state.

Future (gated, out of scope for this stage):

    python -m tgparser.admin gate dynamic_targets on --confirm
    python -m tgparser.admin confirm-mass-gone --version N --fingerprint ...

Those are NOT implemented here on purpose: the handoff for stage 1 explicitly
says the gate can only be toggled by admin CLI, after design review of
mass-gone confirmation flow.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from .config import load as load_config
from .storage import Storage


SECRET_HINTS = ("password", "token", "session", "api_hash", "access_hash",
                "dsn", "phone", "secret")


def _scrub(obj: Any) -> Any:
    """Recursively hide anything whose key looks sensitive. The dry-run never
    reads such columns, but we scrub defensively so a future added column
    cannot leak."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            k_l = str(k).lower()
            if any(h in k_l for h in SECRET_HINTS):
                out[k] = "***"
            else:
                out[k] = _scrub(v)
        return out
    if isinstance(obj, list):
        return [_scrub(x) for x in obj]
    return obj


async def _cmd_policy_dry_run(args) -> int:
    cfg = load_config()
    storage = Storage(cfg["postgres"]["dsn"])
    await storage.connect()
    try:
        level = await storage.schema_version()
        if level < 11:
            msg = ("migration 0011 is not applied yet (max schema_migrations="
                   f"{level}). Dry-run requires v11. Nothing was changed.")
            if args.json:
                print(json.dumps({"ok": False, "error": msg}, ensure_ascii=False))
            else:
                print(msg, file=sys.stderr)
            return 2
        result = await storage.policy_dry_run()
    finally:
        await storage.close()

    result = _scrub(result)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0

    # human-readable summary
    print(f"schema_version: {result['schema_version']}  "
          f"gate_enabled: {result['gate_enabled']}  "
          f"engine_active: {result['engine_active']}  "
          f"requires_restart: {result['requires_restart']}")
    print(f"policy version: requested={result['requested_version']} "
          f"applied={result['applied_version']}  "
          f"block: {result['block']['state']}  "
          f"awaiting_discovery: {result['awaiting_discovery']}")
    if result["disable_suppressed_by"]:
        print("disables suppressed by: " + ", ".join(result["disable_suppressed_by"]))
    print(f"mode: {result.get('mode')}  "
          f"include_archived: {result.get('include_archived')}")
    print(f"would_enable:  {len(result.get('would_enable', []))}")
    print(f"would_disable: {len(result.get('would_disable', []))}")
    print(f"would_keep:    {result.get('would_keep_count', 0)}")
    for key in ("enabled_without_discovery", "enabled_without_preference"):
        if result.get(key):
            print(f"{key} (seed gap): " + ", ".join(str(c) for c in result[key]))
    print("reason counts:")
    for k, v in sorted(result.get("reason_counts", {}).items(),
                       key=lambda kv: -kv[1]):
        print(f"  {k:20s} {v}")
    for section in ("would_enable", "would_disable"):
        rows = result.get(section) or []
        if not rows:
            continue
        print(f"\n{section}:")
        for r in rows:
            print(f"  {r['chat_id']:>16}  {(r['category'] or '-'):<10} "
                  f"{(r['title'] or '')[:40]:<40}  reason={r['reason']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tgparser.admin",
                                description="TGParser admin CLI")
    sub = p.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("policy-dry-run",
                        help="read-only policy preview (no writes)")
    pr.add_argument("--json", action="store_true",
                    help="machine-readable JSON output")
    pr.set_defaults(func=_cmd_policy_dry_run)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(args.func(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
