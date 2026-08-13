import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from src.analysis import decision_ledger
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


def test_decision_timeline_unifies_plan_fill_and_executable_outcome():
    data = build_decision_timeline(
        [
            {
                "id": 21,
                "decided_at": "2026-08-01T14:00:00+00:00",
                "ticker": "YPFD",
                "decision": "BUY",
                "source": "execution_plan",
                "status": "EXECUTED",
                "block_reason": None,
                "theoretical_amount_ars": Decimal("250000"),
                "executed_amount_ars": Decimal("245000"),
                "is_executable": True,
                "was_blocked": False,
                "outcome_5d": Decimal("0.025"),
                "outcome_basis": "canonical_cocos",
                "is_primary_metric": True,
                "run_id": "run-21",
                "layers": {
                    "run_context": {
                        "run_id": "run-21",
                        "portfolio_snapshot_id": "p21",
                        "feature_snapshot_id": "f21",
                    }
                },
            }
        ],
        fill_rows=[
            {
                "id": 31,
                "executed_at": "2026-08-01T15:00:00+00:00",
                "ticker": "YPFD",
                "side": "BUY",
                "quantity": Decimal("10"),
                "avg_fill_price": Decimal("24500"),
                "gross_amount_ars": Decimal("245000"),
                "source": "broker_fill",
                "decision_log_id": 21,
                "run_id": "run-21",
            }
        ],
    )

    event_types = [event["event_type"] for event in data["events"]]
    assert event_types == [
        "decision_logged",
        "outcome_updated",
        "plan_created",
        "fill_detected",
    ]
    fill = next(event for event in data["events"] if event["event_type"] == "fill_detected")
    outcome = next(event for event in data["events"] if event["event_type"] == "outcome_updated")
    assert fill["decision_log_id"] == 21
    assert fill["gaps"] == []
    assert outcome["payload"]["outcome_basis"] == "canonical_cocos"
    assert outcome["payload"]["is_primary_metric"] is True
    assert outcome["payload"]["decision"] == "BUY"
    assert outcome["payload"]["status"] == "EXECUTED"
    assert outcome["payload"]["executed_amount_ars"] == 245000.0
    assert outcome["payload"]["decided_at"] == "2026-08-01T14:00:00+00:00"


def test_blocked_outcome_keeps_original_reason_and_audit_context():
    data = build_decision_timeline(
        [
            {
                "id": 22,
                "decided_at": "2026-08-01T14:00:00+00:00",
                "outcome_filled_at": "2026-08-07T00:30:00+00:00",
                "ticker": "MU",
                "decision": "BUY",
                "source": "execution_plan",
                "status": "BLOCKED",
                "block_reason": "Funding insuficiente",
                "theoretical_amount_ars": Decimal("100000"),
                "executed_amount_ars": Decimal("0"),
                "is_executable": False,
                "was_blocked": True,
                "outcome_5d": Decimal("-0.101"),
                "outcome_basis": "canonical_cocos",
                "is_primary_metric": False,
                "metric_scope": "blocked_audit",
                "layers": {},
            }
        ]
    )

    outcome = next(event for event in data["events"] if event["event_type"] == "outcome_updated")

    assert outcome["ts"] == "2026-08-07T00:30:00+00:00"
    assert outcome["payload"]["status"] == "BLOCKED"
    assert outcome["payload"]["reason"] == "Funding insuficiente"
    assert outcome["payload"]["was_blocked"] is True
    assert outcome["payload"]["is_primary_metric"] is False


def test_monitor_registers_read_only_audit_timeline_route():
    root = Path(__file__).resolve().parents[1]
    monitor_api = (root / "src" / "monitor" / "api.py").read_text(encoding="utf-8")

    assert 'app.router.add_get("/api/audit-timeline", audit_timeline)' in monitor_api


def test_pending_marks_lookup_latest_price_per_decision(monkeypatch):
    async def _schema_ready(_conn):
        return None

    monkeypatch.setattr(
        decision_ledger,
        "ensure_decision_audit_scope_columns",
        _schema_ready,
    )
    conn = _FakeConn()

    asyncio.run(decision_ledger.fetch_decision_ledger(conn))

    pending_query = conn.queries[-1]
    assert "JOIN LATERAL" in pending_query
    assert "mp.ticker = dl.ticker" in pending_query
    assert "WITH latest AS" not in pending_query

    real_query = conn.queries[0]
    assert "('broker_movement', 'broker_fill')" in real_query
    assert "plan_execution_attribution_movements" in real_query
    assert "bf.decision_log_id = dl.id" in real_query


def test_monitor_json_responses_negotiate_compression():
    root = Path(__file__).resolve().parents[1]
    monitor_api = (root / "src" / "monitor" / "api.py").read_text(encoding="utf-8")
    json_helper = monitor_api.split("def _json", 1)[1].split("async def", 1)[0]

    assert "response.enable_compression()" in json_helper
    assert 'getattr(response, "body", None)' in monitor_api
    assert "except web.HTTPException as exc:" in monitor_api
