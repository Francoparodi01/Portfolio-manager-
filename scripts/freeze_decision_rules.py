"""Freeze (or check) the decision rules for the prospective evaluation.

    python scripts/freeze_decision_rules.py --check
    python scripts/freeze_decision_rules.py --freeze --note "v1 tras auditoria 2026-11"

--freeze writes config/decision_rules_freeze.json and refuses to overwrite an
existing freeze unless --replace is given (a new freeze restarts the holdout).
Commit that file: /viability reads its date and reports the decisions taken
after it as the prospective cohort.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.rules_freeze import MANIFEST_PATH, build_manifest, check_manifest, load_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--replace", action="store_true", help="replace an existing freeze (restarts the holdout)")
    parser.add_argument("--note", default="")
    args = parser.parse_args()

    existing = load_manifest()
    if args.check:
        if existing is None:
            print(json.dumps({"status": "NOT_FROZEN", "manifest": str(MANIFEST_PATH)}))
            return 1
        result = check_manifest(existing)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["status"] == "FROZEN_OK" else 1

    if existing is not None and not args.replace:
        print(json.dumps({"status": "ALREADY_FROZEN", "frozen_at": existing.get("frozen_at"),
                          "hint": "use --replace to restart the holdout"}))
        return 1
    manifest = build_manifest(note=args.note)
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                             encoding="utf-8")
    print(json.dumps({"status": "FROZEN", "frozen_at": manifest["frozen_at"],
                      "git_commit": manifest["git_commit"], "files": len(manifest["files"]),
                      "not_covered": manifest["not_covered"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
