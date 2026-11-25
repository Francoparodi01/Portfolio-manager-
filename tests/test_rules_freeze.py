import json

from src.analysis.rules_freeze import build_manifest, check_manifest, decision_rule_files, freeze_note


def _repo(tmp_path):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "config").mkdir()
    (tmp_path / "src" / "analysis").mkdir(parents=True)
    (tmp_path / "src" / "__init__.py").write_text("")
    (tmp_path / "src" / "analysis" / "__init__.py").write_text("")
    (tmp_path / "src" / "analysis" / "engine.py").write_text("from .weights import W\n")
    (tmp_path / "src" / "analysis" / "weights.py").write_text("W = 1\n")
    (tmp_path / "src" / "analysis" / "viability_audit.py").write_text("X = 1\n")
    (tmp_path / "scripts" / "run_analysis.py").write_text(
        "def main():\n    from src.analysis.engine import W\n"
    )
    (tmp_path / "config" / "policy.json").write_text(json.dumps({"min": 0.08}))
    return tmp_path


def test_rules_are_the_import_closure_of_the_decision_entry_point(tmp_path):
    root = _repo(tmp_path)
    files = decision_rule_files(root)
    assert "src/analysis/weights.py" in files  # relative import, inside a function
    assert "config/policy.json" in files
    assert "src/analysis/viability_audit.py" not in files  # audits are not rules


def test_freeze_check_ignores_crlf_but_detects_rule_changes(tmp_path):
    root = _repo(tmp_path)
    manifest = build_manifest(root)
    weights = root / "src" / "analysis" / "weights.py"
    weights.write_bytes(b"W = 1\r\n")  # Windows checkout of the same content
    assert check_manifest(manifest, root)["status"] == "FROZEN_OK"
    weights.write_text("W = 2\n")
    result = check_manifest(manifest, root)
    assert result["status"] == "FREEZE_BROKEN"
    assert result["changed"] == ["src/analysis/weights.py"]
    assert "REGLAS MODIFICADAS" in freeze_note(result)
