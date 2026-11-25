"""Hermetic tests for scripts/audit_data_integrity.py: pure logic only, no database."""
import asyncio
import dataclasses
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from scripts import audit_data_integrity as audit
from src.analysis.viability_audit import ViabilityAuditConfig, _scope_mask, _prepare_frame, run_viability_audit_sync
from tests.test_viability_audit import _row

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
DECIDED = datetime(2026, 11, 2, 14, 0, tzinfo=timezone.utc)     # Monday 02/11 in Buenos Aires


def days(*pairs):
    return [(date(2026, 11, d), close) for d, close in pairs]


# --- outcome recomputation -----------------------------------------------------------------------

def test_buy_outcome_uses_first_close_on_or_after_the_calendar_target_day():
    candles = days((2, 100), (6, 101), (9, 110), (10, 111))          # 7/11 is a Saturday: first close >= 7/11 is 9/11
    assert audit.recompute_outcome(100, DECIDED, "BUY", candles, NOW, 5) == pytest.approx(0.10)


def test_sell_outcome_is_positive_when_the_price_falls():
    candles = days((7, 90))
    assert audit.recompute_outcome(100, DECIDED, "SELL", candles, NOW, 5) == pytest.approx(0.10)
    assert audit.recompute_outcome(100, DECIDED, "BUY", candles, NOW, 5) == pytest.approx(-0.10)


def test_unmatured_or_candleless_horizons_return_none():
    assert audit.recompute_outcome(100, DECIDED, "BUY", days((9, 110)), NOW, 40) is None          # target 12/12 is in the future
    assert audit.recompute_outcome(100, DECIDED, "BUY", days((3, 105)), NOW, 5) is None           # no candle at/after target
    assert audit.recompute_outcome(0, DECIDED, "BUY", days((9, 110)), NOW, 5) is None


def test_decision_date_is_taken_in_buenos_aires_time():
    late_utc = datetime(2026, 11, 3, 1, 30, tzinfo=timezone.utc)      # 02/11 22:30 ART -> target 07/11, not 08/11
    assert audit.recompute_outcome(100, late_utc, "BUY", days((7, 105), (9, 120)), NOW, 5) == pytest.approx(0.05)


# --- duplicated movements ------------------------------------------------------------------------

def movement(identifier, payload, **overrides):
    row = dict(external_movement_id=identifier, executed_at=datetime(2026, 11, 5, 15, tzinfo=timezone.utc),
               movement_type="BUY", ticker="GDX", quantity=10, price=14.688, amount=146.883, payload_fingerprint=payload)
    row.update(overrides)
    return row


def test_duplicate_classification_separates_duplicates_partial_fills_and_synthetic():
    df = pd.DataFrame([
        movement("a1", "p"), movement("a2", "p"),                                  # same payload -> likely duplicate
        movement("b1", "x", ticker="SPCX"), movement("b2", "y", ticker="SPCX"),    # distinct payloads -> maybe partial fills
        movement("synthetic:c1", "s", ticker="NBIS"), movement("c2", "r", ticker="NBIS"),
        movement("d1", "z", ticker="MU"),                                          # unique: ignored
    ])
    result = audit.classify_movement_duplicates(df).set_index("ticker")["classification"].to_dict()
    assert result == {"GDX": "LIKELY_DUPLICATE", "SPCX": "POSSIBLE_PARTIAL_FILLS", "NBIS": "SYNTHETIC_AND_REAL"}
    assert audit.classify_movement_duplicates(pd.DataFrame()).empty


# --- candles -------------------------------------------------------------------------------------

def test_price_jumps_flag_large_moves_per_ticker_only():
    candles = pd.DataFrame([
        dict(ticker="YPFD", day=date(2026, 8, 3), close=100.0), dict(ticker="YPFD", day=date(2026, 8, 4), close=10.0),
        dict(ticker="AMD", day=date(2026, 8, 3), close=100.0), dict(ticker="AMD", day=date(2026, 8, 4), close=104.0),
    ])
    jumps = audit.price_jumps(candles)
    assert list(jumps["ticker"]) == ["YPFD"] and jumps.iloc[0]["change"] == pytest.approx(-0.9)
    assert audit.price_jumps(pd.DataFrame()).empty


# --- report recomputation ------------------------------------------------------------------------

def build_report(rows, **kwargs):
    config = ViabilityAuditConfig(database_url="postgresql://unused", horizons=("5d",), min_sample=3, cost_bps=50, **kwargs)
    raw = pd.DataFrame(rows)
    return config, raw, run_viability_audit_sync(raw, config)


def sample_rows():
    rows = []
    for index, outcome in enumerate([0.02, 0.03, -0.01, 0.05, 0.04]):
        rows.append(_row(source="execution_plan", status="EXECUTED", final_score=0.05 + 0.02 * index, outcome_5d=outcome))
    for index, outcome in enumerate([-0.02, 0.0, -0.03, 0.01, -0.04]):
        rows.append(_row(source="broker_movement", status="EXECUTED_MANUAL", final_score=0.04 + 0.02 * index, outcome_5d=outcome))
    return rows


def test_independent_recomputation_matches_the_report_and_detects_tampering():
    config, raw, report = build_report(sample_rows())
    frame = _prepare_frame(raw, config.horizons)
    comparison = audit.compare_with_report(frame, report, 0.005, _scope_mask)
    assert len(comparison) == 12 and comparison["ok"].all()

    bot = report.metrics["bot_only"]["5d"]
    tampered = dataclasses.replace(report, metrics={**report.metrics, "bot_only": {"5d": dataclasses.replace(bot, net_ev=bot.net_ev + 0.01)}})
    wrong = audit.compare_with_report(frame, tampered, 0.005, _scope_mask)
    assert list(wrong[~wrong["ok"]]["metric"]) == ["net_ev"]


# --- coverage ------------------------------------------------------------------------------------

def test_mature_missing_counts_only_rows_whose_horizon_has_passed():
    decisions = pd.DataFrame([
        dict(decided_at=datetime(2026, 9, 1, 15, tzinfo=timezone.utc), ticker="AMD", outcome_5d=0.1, outcome_10d=0.1, outcome_20d=0.1, outcome_40d=None),
        dict(decided_at=datetime(2026, 11, 22, 15, tzinfo=timezone.utc), ticker="MU", outcome_5d=None, outcome_10d=None, outcome_20d=None, outcome_40d=None),
    ])
    result = audit.mature_missing(decisions, NOW).set_index("horizon")
    assert result.loc["40d", "mature"] == 1 and result.loc["40d", "missing"] == 1 and result.loc["40d", "worst_tickers"] == "AMD:1"
    assert result.loc["5d", "mature"] == 1 and result.loc["5d", "missing"] == 0


# --- safety and plumbing -------------------------------------------------------------------------

def test_summary_lists_failures_first_and_states_the_calendar_day_convention():
    checks = [audit.Check("Z9", "fine"), audit.Check("A1", "broken", status="FAIL", count=3), audit.Check("B2", "meh", status="WARN", count=1)]
    text = audit.render_summary(checks, dict(generated_at="t", days=180, cutoff="2026-05-28", cost=0.0075))
    assert text.index("| A1 |") < text.index("| B2 |") < text.index("| Z9 |")
    assert "DIAS CALENDARIO" in text


def test_a_failing_check_is_reported_as_error_instead_of_hiding_the_others():
    async def boom():
        raise RuntimeError("column does not exist")
    result = asyncio.run(audit.guarded("X1", boom()))
    assert result[0].status == "ERROR" and "column does not exist" in result[0].detail


def test_the_script_cannot_write_to_the_database():
    source = Path(audit.__file__).read_text(encoding="utf-8")
    assert "transaction(readonly=True)" in source
    assert "sync_plan_execution_attributions" not in source
    sql_blocks = re.findall(r'"""(.*?)"""', source, flags=re.S) + re.findall(r'conn\.fetch\(\s*"([^"]+)"', source)
    sql_blocks = [block for block in sql_blocks if re.search(r"\bSELECT\b", block)]
    assert sql_blocks, "expected SQL in the script"
    for block in sql_blocks:
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|GRANT|COPY)\b", block, flags=re.I), block[:80]
