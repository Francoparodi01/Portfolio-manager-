"""Isolated tests: synthetic receipts only, no connection to operational services."""
from copy import deepcopy
from pathlib import Path
import importlib.util
import hashlib

import pytest

PATH = Path(__file__).resolve().parents[1] / "scripts" / "verify_measurement_receipt.py"
SPEC = importlib.util.spec_from_file_location("verify_measurement_receipt", PATH)
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


def fixture():
    return {
        "schema_version": "measurement-baseline-v1-evidence",
        "original_qeef_exact_replay_match": True,
        "catalog": {"transaction_read_only": "on"},
        "backup_vs_local": {
            "live_meta": {"transaction_read_only": "on"},
            "tables": {"items": {
                "comparison_status": "COMPARED_SHARED_COLUMNS",
                "duplicate_keys": {
                    "backup": {"groups": 0, "extra_rows": 0},
                    "local": {"groups": 0, "extra_rows": 0},
                },
                "backup": {"n": 3}, "local": {"n": 4},
                "shared_keys": 2, "backup_missing_local": 1,
                "local_only": 2, "changed_content": 1, "changed_scope": 0,
            }},
        },
        "code_sha256": {},
    }


def test_valid_synthetic_reconciliation():
    assert mod.validate_receipt(fixture(), frozen=False) == {
        "tables": 1, "backup_rows": 3, "shared_keys": 2,
        "missing_local": 1, "changed_content": 1,
        "local_only": 2, "changed_scope": 0,
    }


@pytest.mark.parametrize("change", ["wrong_shared", "dup", "scope", "readonly", "qeef"])
def test_synthetic_contract_failures(change):
    data = fixture()
    row = data["backup_vs_local"]["tables"]["items"]
    if change == "wrong_shared":
        row["shared_keys"] = 3
    elif change == "dup":
        row["duplicate_keys"]["local"]["groups"] = 1
    elif change == "scope":
        row["changed_scope"] = 3
    elif change == "readonly":
        data["backup_vs_local"]["live_meta"]["transaction_read_only"] = "off"
    else:
        data["original_qeef_exact_replay_match"] = False
    with pytest.raises(ValueError):
        mod.validate_receipt(data, frozen=False)


def test_frozen_counts_are_separate_from_generic_fixture():
    with pytest.raises(ValueError, match="Frozen public baseline"):
        mod.validate_receipt(fixture(), frozen=True)


def test_code_hash_drift_detected(tmp_path):
    file = tmp_path / "src" / "test.py"
    file.parent.mkdir()
    file.write_bytes(b"original")
    data = fixture()
    data["code_sha256"] = {"src/test.py": hashlib.sha256(b"original").hexdigest()}
    mod.verify_recorded_code_hashes(data, tmp_path)
    file.write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash changed"):
        mod.verify_recorded_code_hashes(data, tmp_path)
