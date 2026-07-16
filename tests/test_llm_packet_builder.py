from __future__ import annotations

from datetime import datetime, timezone

from src.analysis.llm_narratives import normalize_market_narrative
from src.analysis.llm_packet_builder import (
    build_market_report_packet,
    render_market_packet_statement_preview,
    render_market_narrative_preview,
)


def _live_portfolio() -> dict:
    return {
        "snapshot_id": "snap-test",
        "generated_at": "2026-07-15T14:00:00+00:00",
        "cash_ars": 1000,
        "invested_ars": 199000,
        "total_value_ars": 200000,
        "day_pnl_ars": 2500,
        "day_change_pct": 0.0127,
        "positions_count": 2,
        "price_coverage_count": 1,
        "positions": [
            {
                "ticker": "AXP",
                "market_value": 120000,
                "weight_in_portfolio": 0.603,
                "change_pct_1d": 0.02,
                "day_pnl_ars": 2400,
                "price_source": "market_prices",
                "market_price_ts": "2026-07-15T13:55:00+00:00",
            },
            {
                "ticker": "NVS",
                "market_value": 79000,
                "weight_in_portfolio": 0.397,
                "change_pct_1d": None,
                "day_pnl_ars": None,
                "price_source": "snapshot",
            },
        ],
    }


def test_build_market_report_packet_from_live_portfolio():
    packet = build_market_report_packet(
        _live_portfolio(),
        run_id="test-run",
        as_of=datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc),
    )
    assert packet["packet_type"] == "market_report"
    assert packet["run_id"] == "test-run"
    assert packet["coverage"]["priced_positions_count"] == 1
    assert packet["coverage"]["positions_count"] == 2
    assert packet["coverage"]["missing_tickers"] == ["NVS"]
    fact_ids = {item["fact_id"] for item in packet["evidence_items"]}
    facts = {item["fact_id"]: item for item in packet["evidence_items"]}
    assert "portfolio.total_value_ars" in fact_ids
    assert "statement.portfolio.overview" in fact_ids
    assert "statement.position.axp.move" in fact_ids
    assert "coverage.priced_weight_pct" in fact_ids
    assert "position.axp.day_change_pct" in fact_ids
    assert "position.axp.market_value_ars" in fact_ids
    assert facts["position.axp.day_pnl_ars"]["display_value"] == "+$2.400 ARS"
    assert facts["position.axp.day_change_pct"]["display_value"] == "+2,00%"


def test_statement_preview_renders_deterministic_packet_facts():
    packet = build_market_report_packet(_live_portfolio())
    preview = render_market_packet_statement_preview(packet)
    assert "FALLBACK DETERMINISTICO" in preview
    assert "Cartera: total $200.000 ARS" in preview
    assert "AXP: movimiento diario +2,00%" in preview


def test_packet_can_feed_market_narrative_validator_and_preview_renderer():
    packet = build_market_report_packet(_live_portfolio())
    payload = {
        "headline": "Cartera positiva con una fuente pendiente",
        "executive_summary": "El movimiento agregado es positivo, pero la cobertura no esta completa.",
        "sections": [
            {
                "title": "Lectura",
                "paragraph": "La cartera sube con PnL diario positivo.",
                "supporting_fact_ids": [
                    "portfolio.day_pnl_ars",
                    "portfolio.day_change_pct",
                ],
            },
            {
                "title": "Cobertura",
                "paragraph": "Una posicion queda valorizada con snapshot.",
                "supporting_fact_ids": ["coverage.priced_weight_pct"],
            },
        ],
        "caveats": ["Cobertura parcial."],
        "insufficiency_flag": True,
    }
    narrative = normalize_market_narrative(packet, payload, model="qwen-test")
    preview = render_market_narrative_preview(narrative, packet)
    assert "QWEN DAILY MARKET PREVIEW" in preview
    assert "Cartera positiva" in preview
    assert "portfolio.day_pnl_ars" in preview
