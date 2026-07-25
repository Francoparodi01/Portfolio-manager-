import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from src.analysis.decision_timeline import (
    build_decision_timeline,
    fetch_decision_timeline,
    render_decision_timeline,
)


class _FakeConn:
    def __init__(self):
        self.queries = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        return []


def test_decision_timeline_orders_events_and_marks_lifecycle_gaps():
    data = build_decision_timeline(
        [
            {
                "id": 10,
                "decided_at": datetime(2026, 7, 23, 15, 0, tzinfo=timezone.utc),
                "ticker": "NVDA",
                "decision": "BUY",
                "source": "execution_plan",
                "status": "APPROVED",
                "decision_type": "executable",
                "final_score": 0.12,
                "confidence": 0.8,
                "theoretical_amount_ars": Decimal("100000"),
                "executed_amount_ars": Decimal("0"),
                "run_id": "run-1",
                "layers": {
                    "run_context": {
                        "run_id": "run-1",
                        "portfolio_snapshot_id": "2026-07-23T14:55:00+00:00",
                        "feature_snapshot_id": "features:abc",
                    },
                    "feature_snapshot": {"feature_snapshot_id": "features:abc"},
                },
            }
        ],
        movement_rows=[
            {
                "id": 20,
                "executed_at": datetime(2026, 7, 23, 16, 0, tzinfo=timezone.utc),
                "ticker": "NVDA",
                "movement_type": "BUY",
                "amount": Decimal("100000"),
                "quantity": Decimal("1"),
                "price": Decimal("100000"),
                "source": "cocos_movements",
                "external_movement_id": "m1",
            }
        ],
    )

    assert [event["event_type"] for event in data["events"]] == [
        "decision_logged",
        "plan_created",
        "movement_detected",
    ]
    assert data["events"][0]["gaps"] == []
    assert data["events"][1]["gaps"] == ["missing_order_id"]
    assert "missing_decision_link" in data["events"][2]["gaps"]


def test_decision_timeline_reports_missing_run_context_and_feature_snapshot():
    data = build_decision_timeline(
        [
            {
                "id": 11,
                "decided_at": "2026-07-23T15:00:00+00:00",
                "ticker": "MU",
                "decision": "SELL",
                "status": "APPROVED",
                "layers": {"source": "execution_plan"},
            }
        ]
    )

    gaps = data["summary"]["gaps"]
    assert "missing_run_id" in gaps
    assert "missing_run_context" in gaps
    assert "missing_feature_snapshot_id" in gaps
    assert "missing_portfolio_snapshot_id" in gaps


def test_render_decision_timeline_prints_stdout_friendly_summary():
    data = build_decision_timeline(
        [
            {
                "id": 12,
                "decided_at": "2026-07-23T15:00:00+00:00",
                "ticker": "NVS",
                "decision": "BUY",
                "status": "APPROVED",
                "run_id": "run-2",
                "layers": {
                    "run_context": {
                        "run_id": "run-2",
                        "portfolio_snapshot_id": "p1",
                        "feature_snapshot_id": "f1",
                    }
                },
            }
        ]
    )

    rendered = render_decision_timeline(data)

    assert "Decision Timeline" in rendered
    assert "decision_logged NVS BUY" in rendered


def test_fetch_decision_timeline_omits_unscoped_movements_for_run_filter():
    conn = _FakeConn()

    asyncio.run(fetch_decision_timeline(conn, run_id="run-1"))

    assert not any("FROM broker_movements" in query for query in conn.queries)
