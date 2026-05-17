from datetime import datetime, timezone

from src.analysis.fill_reconciliation import (
    ExecutionCandidate,
    choose_execution_candidate,
)
from src.collector.broker_fills import broker_fill_from_mapping


def test_broker_fill_from_mapping_normalizes_fields():
    fill = broker_fill_from_mapping(
        {
            "external_fill_id": "abc-123",
            "executed_at": "2026-05-18T14:35:00+00:00",
            "ticker": " nvda ",
            "side": "buy",
            "quantity": "2",
            "avg_fill_price": "12345.50",
            "gross_amount_ars": "24691",
        }
    )

    assert fill.ticker == "NVDA"
    assert fill.side == "BUY"
    assert fill.quantity == 2.0
    assert fill.avg_fill_price == 12345.5
    assert fill.gross_amount_ars == 24691.0


def test_choose_execution_candidate_prefers_matching_approved_plan():
    fill = broker_fill_from_mapping(
        {
            "external_fill_id": "fill-1",
            "executed_at": "2026-05-18T14:35:00+00:00",
            "ticker": "NVDA",
            "side": "BUY",
            "quantity": "2",
            "avg_fill_price": "100",
            "gross_amount_ars": "200",
        }
    )
    candidates = [
        ExecutionCandidate(
            id=10,
            ticker="NVDA",
            decision="BUY",
            decided_at=datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
            status="APPROVED",
            theoretical_amount_ars=200,
        ),
        ExecutionCandidate(
            id=11,
            ticker="NVDA",
            decision="BUY",
            decided_at=datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc),
            status="APPROVED",
            theoretical_amount_ars=200,
        ),
    ]

    match = choose_execution_candidate(fill, candidates)

    assert match is not None
    assert match.id == 10


def test_choose_execution_candidate_rejects_wrong_side_or_status():
    fill = broker_fill_from_mapping(
        {
            "external_fill_id": "fill-2",
            "executed_at": "2026-05-18T14:35:00+00:00",
            "ticker": "NVDA",
            "side": "BUY",
            "quantity": "2",
            "avg_fill_price": "100",
        }
    )
    candidates = [
        ExecutionCandidate(
            id=10,
            ticker="NVDA",
            decision="SELL",
            decided_at=datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
            status="APPROVED",
        ),
        ExecutionCandidate(
            id=11,
            ticker="NVDA",
            decision="BUY",
            decided_at=datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
            status="BLOCKED",
        ),
    ]

    assert choose_execution_candidate(fill, candidates) is None
