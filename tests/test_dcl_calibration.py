import asyncio

import pandas as pd

from src.analysis.dcl.outcome_loader import decisions_from_frame
from src.analysis.dcl.report_generator import render_calibration_report
from src.analysis.dcl.run_calibration import CalibrationReport
from src.analysis.dcl.sample_safety import SampleSafety
from src.analysis.dcl.statistical_auditor import StatisticalAuditor


def test_outcome_loader_marks_only_clean_canonical_as_auditable():
    frame = pd.DataFrame(
        [
            {
                "id": 1,
                "ticker": "NVDA",
                "decision": "BUY",
                "final_score": 0.2,
                "outcome_5d": 0.04,
                "outcome_basis": "canonical_cocos",
                "candle_source_mode": "official",
                "source": "optimizer",
                "status": "THEORETICAL",
                "decision_type": "theoretical",
            },
            {
                "id": 2,
                "ticker": "AMD",
                "decision": "BUY",
                "final_score": 0.1,
                "outcome_5d": 0.01,
                "outcome_basis": "canonical_cocos",
                "candle_source_mode": "mixed",
                "source": "optimizer",
                "status": "THEORETICAL",
                "decision_type": "theoretical",
            },
        ]
    )

    decisions = decisions_from_frame(frame)

    assert decisions[0].data_quality == "clean"
    assert decisions[0].is_auditable is True
    assert decisions[1].data_quality == "mixed"
    assert decisions[1].is_auditable is True

    strict_decisions = decisions_from_frame(frame, quality_mode="strict")

    assert strict_decisions[0].is_auditable is True
    assert strict_decisions[1].is_auditable is False


def test_outcome_loader_reads_quality_and_layers_from_layers_payload():
    decisions = decisions_from_frame(
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "ticker": "PAMP",
                    "decision": "BUY",
                    "final_score": 0.12,
                    "outcome_5d": 0.03,
                    "outcome_basis": "canonical_cocos",
                    "layers": {
                        "technical": {"raw": 0.45, "weighted": 0.13},
                        "macro": {"raw": -0.02, "weighted": -0.01},
                        "technical_data_source_mode": "official",
                    },
                    "source": "execution_plan",
                    "status": "APPROVED",
                    "decision_type": "executable",
                }
            ]
        )
    )

    decision = decisions[0]

    assert decision.data_quality == "clean"
    assert decision.is_auditable is True
    assert decision.layer_scores["technical"] == 0.45
    assert decision.layer_scores["macro"] == -0.02


def test_outcome_loader_keeps_missing_layer_scores_as_none():
    decisions = decisions_from_frame(
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "ticker": "DOW",
                    "decision": "SELL",
                    "final_score": -0.03,
                    "outcome_5d": 0.01,
                    "outcome_basis": "canonical_cocos",
                    "layers": {"source": "optimizer", "delta_pct": 0.0412},
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                }
            ]
        ),
        quality_mode="all",
    )

    decision = decisions[0]

    assert decision.data_quality == "unknown"
    assert decision.is_auditable is True
    assert decision.layer_scores["technical"] is None
    assert decision.layer_scores["macro"] is None


def test_sample_safety_blocks_small_or_undiversified_samples():
    decisions = decisions_from_frame(
        pd.DataFrame(
            [
                {
                    "id": i,
                    "ticker": "NVDA",
                    "decision": "BUY",
                    "final_score": 0.1,
                    "outcome_5d": 0.01,
                    "outcome_basis": "canonical_cocos",
                    "candle_source_mode": "official",
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                }
                for i in range(5)
            ]
        )
    )

    safety = SampleSafety.evaluate(decisions)

    assert safety.n_auditable == 5
    assert safety.can_run_threshold_opt is False
    assert any("Muestra insuficiente" in warning for warning in safety.warnings)


def test_statistical_auditor_computes_ic_wr_and_ev():
    rows = []
    for i in range(30):
        score = i / 100
        rows.append(
            {
                "id": i,
                "ticker": f"T{i % 4}",
                "decision": "BUY" if i % 2 else "SELL",
                "final_score": score,
                "outcome_5d": score - 0.10,
                "outcome_basis": "canonical_cocos",
                "candle_source_mode": "official",
                "regime": "bull_low_vol",
                "source": "optimizer",
                "status": "THEORETICAL",
                "decision_type": "theoretical",
            }
        )

    audit = StatisticalAuditor().run(decisions_from_frame(pd.DataFrame(rows)))

    assert audit.n_auditable == 30
    assert audit.ic_5d is not None and audit.ic_5d > 0.9
    assert audit.win_rate is not None
    assert audit.ev_bootstrap_ci[0] is not None


def test_calibration_report_renders_no_auto_apply_policy():
    decisions = decisions_from_frame(
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "ticker": "NVDA",
                    "decision": "BUY",
                    "final_score": 0.1,
                    "outcome_5d": 0.02,
                    "outcome_basis": "canonical_cocos",
                    "candle_source_mode": "official",
                    "source": "optimizer",
                    "status": "THEORETICAL",
                    "decision_type": "theoretical",
                }
            ]
        )
    )
    safety = SampleSafety.evaluate(decisions)
    audit = StatisticalAuditor().run(decisions)

    text = render_calibration_report(
        CalibrationReport(
            decisions=decisions,
            safety=safety,
            audit=audit,
            status="INSUFFICIENT_DATA",
            lookback_days=180,
            quality_counts={"clean": 1},
        )
    )

    assert "DECISION CALIBRATION LAYER" in text
    assert "No se aplican cambios automaticos" in text
    assert "INSUFFICIENT_DATA" in text
    assert "clean 1" in text


def test_outcome_loader_scopes_owner_query(monkeypatch):
    seen = {}

    class _Conn:
        async def fetch(self, query, *args):
            if "information_schema.columns" in query:
                return [
                    {"column_name": "id"},
                    {"column_name": "owner_chat_id"},
                    {"column_name": "decided_at"},
                ]
            seen["query"] = query
            seen["args"] = args
            return []

        async def close(self):
            return None

    async def _connect(_dsn):
        return _Conn()

    monkeypatch.setattr("src.analysis.dcl.outcome_loader.asyncpg.connect", _connect)

    from src.analysis.dcl.outcome_loader import OutcomeLoader

    asyncio.run(OutcomeLoader("postgresql://x").load_frame(owner_chat_id=123))

    assert "owner_chat_id = $2" in seen["query"]
    assert seen["args"][1] == 123
