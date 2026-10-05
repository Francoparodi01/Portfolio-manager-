"""Regression checks for monitor routes that must never migrate or enrich data."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_monitor_handlers_do_not_mutate_database_on_read():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")

    assert "await ensure_" not in source
    assert "mark_inferred_activity_types" not in source
    assert '"default_transaction_read_only": "on"' in source


def test_decision_ledger_read_does_not_run_schema_migration():
    source = (ROOT / "src" / "analysis" / "decision_ledger.py").read_text(encoding="utf-8")
    assert "ensure_decision_audit_scope_columns" not in source
