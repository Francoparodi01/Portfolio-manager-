from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_scheduler_passes_configured_owner_to_analysis():
    source = (ROOT / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")
    assert '"--owner-chat-id"' in source
    assert 'cfg.scraper.telegram_chat_id' in source


# Formal-plan lineage and preservation of existing outcomes are exercised against
# PostgreSQL in test_high_confidence_evidence_v2.py. The former source-text tests
# required destructive UPDATE behavior that is no longer part of this writer.


def test_candle_upsert_preserves_provenance_metadata():
    source = (ROOT / "src" / "collector" / "db.py").read_text(encoding="utf-8")
    assert "source      = EXCLUDED.source" in source
    assert "currency    = EXCLUDED.currency" in source
    assert "venue       = EXCLUDED.venue" in source


def test_snapshots_are_not_rewritten_on_duplicate_id():
    source = (ROOT / "src" / "collector" / "db.py").read_text(encoding="utf-8")
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in source
    assert "historical snapshots are immutable" in source
    assert "ON CONFLICT (snapshot_id, scraped_at) DO NOTHING" in source


def test_monitor_defaults_decision_views_to_configured_owner():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")
    for endpoint in ("decisions", "portfolio_view", "performance_view", "radar_audit"):
        start = source.index(f"async def {endpoint}")
        end = source.find("\nasync def ", start + 10)
        block = source[start:end if end != -1 else None]
        assert "request.query.get(\"owner_chat_id\") or configured_owner" in block
        assert "owner_chat_id requerido" in block
