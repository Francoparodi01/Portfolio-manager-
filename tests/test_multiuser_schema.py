from pathlib import Path


def test_schema_scopes_private_tables_by_owner_chat_id():
    sql = Path("init.sql").read_text(encoding="utf-8")

    assert "owner_chat_id    BIGINT" in sql
    assert "idx_portfolio_snapshots_owner_scraped_at" in sql
    assert "idx_decision_log_owner_decided_at" in sql
    assert "idx_broker_fills_owner_executed_at" in sql


def test_daily_decision_uniqueness_is_user_scoped():
    sql = Path("init.sql").read_text(encoding="utf-8")

    assert "idx_decision_log_daily_action_lookup" in sql
    assert "COALESCE(owner_chat_id, 0)" in sql
    assert "ticker" in sql
    assert "decision_date" in sql
    assert "decision" in sql
    assert "COALESCE(source, 'sin_source')" in sql
    assert "COALESCE(decision_type, 'unknown')" in sql
