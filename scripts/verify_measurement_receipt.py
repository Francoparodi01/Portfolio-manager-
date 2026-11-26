"""Validate Quantia's frozen, PUBLIC measurement receipt without DB access.

This checks internal invariants, not the truth of the private source data.
No network, writes, SQL, migrations, broker calls, or account identifiers.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


FROZEN_COUNTS = {
    "tables": 59,
    "backup_rows": 2_232_721,
    "shared_keys": 2_232_716,
    "missing_local": 5,
    "changed_content": 27_026,
    "local_only": 48_464,
    "changed_scope": 0,
}


def validate_receipt(data: dict, *, frozen: bool = True) -> dict:
    if data.get("schema_version") != "measurement-baseline-v1-evidence":
        raise ValueError("Unknown public receipt schema")
    comparison = data["backup_vs_local"]
    tables = comparison["tables"]
    if not tables:
        raise ValueError("Missing comparison tables")
    totals = {key: 0 for key in FROZEN_COUNTS}
    totals["tables"] = len(tables)
    for table, row in tables.items():
        if row.get("comparison_status") != "COMPARED_SHARED_COLUMNS":
            raise ValueError(f"{table}: not compared at a declared key")
        for source in ("backup", "local"):
            duplicates = row["duplicate_keys"][source]
            if duplicates["groups"] or duplicates["extra_rows"]:
                raise ValueError(f"{table}: duplicate keys in {source}")
        backup_n = row["backup"]["n"]
        local_n = row["local"]["n"]
        shared = row["shared_keys"]
        missing = row["backup_missing_local"]
        added = row["local_only"]
        changed = row["changed_content"]
        scope_changed = row["changed_scope"]
        if min(backup_n, local_n, shared, missing, added, changed, scope_changed) < 0:
            raise ValueError(f"{table}: negative counts")
        if backup_n != shared + missing or local_n != shared + added:
            raise ValueError(f"{table}: key reconciliation does not close")
        if changed > shared or scope_changed > shared:
            raise ValueError(f"{table}: changed rows exceed shared keys")
        totals["backup_rows"] += backup_n
        totals["shared_keys"] += shared
        totals["missing_local"] += missing
        totals["local_only"] += added
        totals["changed_content"] += changed
        totals["changed_scope"] += scope_changed
    if comparison["live_meta"].get("transaction_read_only") != "on":
        raise ValueError("Reported local capture was not read-only")
    if data["catalog"].get("transaction_read_only") != "on":
        raise ValueError("Reported schema census was not read-only")
    if data.get("original_qeef_exact_replay_match") is not True:
        raise ValueError("Original QEEF replay was not reported as matching")
    if frozen and totals != FROZEN_COUNTS:
        raise ValueError(f"Frozen public baseline counts changed: {totals}")
    return totals


def verify_recorded_code_hashes(data: dict, root: Path) -> None:
    """Verify the published code corresponds to this checkout, not DB provenance."""
    for relative, expected in data["code_sha256"].items():
        path = root / relative
        if not path.is_file():
            raise ValueError(f"Recorded code file missing: {relative}")
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        if observed != expected:
            raise ValueError(f"Recorded code hash changed: {relative}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, default=Path("docs/audits/evidence/measurement-baseline-v1.json"))
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--check-code", action="store_true")
    args = parser.parse_args()
    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    try:
        totals = validate_receipt(receipt)
        if args.check_code:
            verify_recorded_code_hashes(receipt, args.repo_root)
    except (KeyError, ValueError, TypeError, OSError) as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({
        "status": "PASS", "scope": "PUBLIC_RECEIPT_ONLY",
        "counts": totals, "recorded_code_hashes_checked": args.check_code,
        "not_certified": ["current PostgreSQL", "live Supabase", "broker statements", "fees", "net PnL"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
