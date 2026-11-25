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


@pytest.mark.parametrize("url,expected", [
    ("postgresql+asyncpg://portfolio:s3cr@t%40x@db:5432/portfolio", "postgresql+asyncpg://portfolio:s3cr@t%40x@localhost:5432/portfolio"),
    ("postgresql://u:p@db/portfolio?sslmode=disable", "postgresql://u:p@localhost/portfolio?sslmode=disable"),
    ("postgresql://db:5433/x", "postgresql://localhost:5433/x"),
])
def test_override_host_changes_only_the_host(url, expected):
    assert audit.override_host(url, "localhost") == expected
    assert audit.override_host(url, None) == url


def test_database_url_is_read_from_the_env_file_and_nothing_else(tmp_path):
    env = tmp_path / ".env"
    env.write_text("\n".join([
        "# DATABASE_URL=postgresql://commented:out@x/y",
        "DATABASE_URL_LOCAL=postgresql://wrong:one@cocos_db/x",
        "TELEGRAM_BOT_TOKEN = \"must-not-be-read\"",
        "DATABASE_URL=postgresql://first:one@db/first",
        "export DATABASE_URL=\"postgresql://quantia:p-w@db:5432/quantia\"  # trailing",
    ]), encoding="utf-8")
    assert audit.database_url_from_env_file(str(env)) == "postgresql://quantia:p-w@db:5432/quantia"
    plain = tmp_path / "plain.env"
    plain.write_text("DATABASE_URL=postgresql://u:p@db/x #comment\n", encoding="utf-8")
    assert audit.database_url_from_env_file(str(plain)) == "postgresql://u:p@db/x"
    assert audit.database_url_from_env_file(str(tmp_path / "missing.env")) is None
    empty = tmp_path / "empty.env"
    empty.write_text("OTHER=1\n", encoding="utf-8")
    assert audit.database_url_from_env_file(str(empty)) is None


def test_candle_choice_follows_the_systems_source_priority_then_latest_scrape():
    candles = pd.DataFrame([
        dict(ticker="ASTS", day=date(2026, 11, 9), close=10.0, source="TRADINGVIEW_BYMA", scraped_at=datetime(2026, 11, 9, 22, tzinfo=timezone.utc)),
        dict(ticker="ASTS", day=date(2026, 11, 9), close=11.0, source="COCOS", scraped_at=datetime(2026, 11, 9, 20, tzinfo=timezone.utc)),
        dict(ticker="ASTS", day=date(2026, 11, 9), close=12.0, source="COCOS", scraped_at=datetime(2026, 11, 9, 23, tzinfo=timezone.utc)),
        dict(ticker="ASTS", day=date(2026, 11, 10), close=13.0, source="OTHER", scraped_at=datetime(2026, 11, 10, 20, tzinfo=timezone.utc)),
        dict(ticker="ASTS", day=date(2026, 11, 10), close=14.0, source="internal_snapshot", scraped_at=datetime(2026, 11, 10, 19, tzinfo=timezone.utc)),
    ])
    chosen = audit.pick_canonical_candles(candles).set_index("day")["close"].to_dict()
    assert chosen == {date(2026, 11, 9): 12.0, date(2026, 11, 10): 14.0}


def test_price_jumps_accepts_decimal_closes_as_returned_by_numeric_columns():
    from decimal import Decimal
    candles = pd.DataFrame([dict(ticker="YPFD", day=date(2026, 8, 3), close=Decimal("100.0")),
                            dict(ticker="YPFD", day=date(2026, 8, 4), close=Decimal("10.0"))])
    assert list(audit.price_jumps(candles)["ticker"]) == ["YPFD"]


def test_only_orphans_are_unexplained_when_an_executed_row_has_no_cohort():
    rows = pd.DataFrame([dict(attributed_followed=True, duplicate_movement=False),
                         dict(attributed_followed=False, duplicate_movement=True),
                         dict(attributed_followed=False, duplicate_movement=False),
                         dict(attributed_followed=True, duplicate_movement=True)])
    assert list(audit.explain_unclassified(rows, rows)) == ["ATTRIBUTED_TO_A_PLAN", "DUPLICATE_SYNTHETIC", "ORPHAN", "ATTRIBUTED_TO_A_PLAN"]


def test_coverage_reports_missing_outcomes_by_outcome_basis():
    decisions = pd.DataFrame([
        dict(decided_at=datetime(2026, 9, 1, 15, tzinfo=timezone.utc), ticker="AMD", outcome_basis="legacy_external", outcome_5d=None, outcome_10d=None, outcome_20d=None, outcome_40d=None),
        dict(decided_at=datetime(2026, 9, 2, 15, tzinfo=timezone.utc), ticker="MU", outcome_basis=None, outcome_5d=None, outcome_10d=None, outcome_20d=None, outcome_40d=None),
    ])
    row = audit.mature_missing(decisions, NOW).set_index("horizon").loc["5d"]
    assert set(row["by_basis"].split(",")) == {"legacy_external:1", "NULL:1"}


def test_one_failing_candle_query_does_not_hide_the_other_candle_checks():
    class Conn:
        async def fetch(self, sql, *params):
            if "scraped_at" in sql:
                raise TypeError("boom")
            return []
    checks = asyncio.run(audit.check_candles(Conn(), NOW))
    assert {c.check_id: c.status for c in checks} == {"C1": "PASS", "C2": "PASS", "C3": "ERROR"}


def _raw(*rows):
    return pd.DataFrame([dict(day=date(2026, 9, d), close=close, source=source) for d, close, source in rows])


def test_implied_exit_close_inverts_the_directional_return():
    assert audit.implied_exit_close(100, 0.10, "BUY") == pytest.approx(110)
    assert audit.implied_exit_close(100, 0.10, "SELL") == pytest.approx(90)


def test_stored_outcome_made_from_another_source_on_the_same_day_is_explained():
    decided = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)                  # target day 5d = 9/6
    raw = _raw((6, 110.0, "internal_snapshot"), (6, 111.0, "COCOS"), (7, 120.0, "COCOS"))
    why = audit.explain_outcome_difference(100, decided, "BUY", 0.10, 5, raw, date(2026, 9, 6))
    assert (why["cause"], why["used_source"]) == ("OTHER_SOURCE_SAME_DAY", "internal_snapshot")


def test_stored_outcome_made_before_the_target_day_candle_existed_is_explained():
    decided = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)
    raw = _raw((6, 111.0, "COCOS"), (8, 120.0, "COCOS"))
    why = audit.explain_outcome_difference(100, decided, "BUY", 0.20, 5, raw, date(2026, 9, 6))
    assert (why["cause"], why["used_day"]) == ("LATER_CANDLE_USED", date(2026, 9, 8))


def test_stored_outcome_that_matches_no_candle_is_unexplained_and_an_exit_before_target_is_flagged():
    decided = datetime(2026, 9, 1, 15, tzinfo=timezone.utc)
    raw = _raw((4, 105.0, "COCOS"), (6, 111.0, "COCOS"))
    assert audit.explain_outcome_difference(100, decided, "BUY", 0.5, 5, raw, date(2026, 9, 6))["cause"] == "NO_CANDLE_MATCHES"
    assert audit.explain_outcome_difference(100, decided, "BUY", 0.05, 5, raw, date(2026, 9, 6))["cause"] == "EXIT_BEFORE_TARGET"
    assert audit.explain_outcome_difference(100, decided, "BUY", 0.5, 5, raw.iloc[0:0], None)["cause"] == "NO_CANDLE_MATCHES"


def test_jumps_report_the_source_change_between_the_two_candles():
    candles = pd.DataFrame([dict(ticker="AOCA", day=date(2026, 6, 1), close=2400.0, source="TRADINGVIEW_BYMA"),
                            dict(ticker="AOCA", day=date(2026, 6, 2), close=35400.0, source="COCOS"),
                            dict(ticker="AOCA", day=date(2026, 6, 3), close=35500.0, source="COCOS")])
    jumps = audit.price_jumps(candles)
    assert len(jumps) == 1 and bool(jumps.loc[0, "source_change"]) and jumps.loc[0, "previous_day"] == date(2026, 6, 1)


def test_only_outcomes_exiting_on_a_flagged_candle_are_reported():
    jumps = pd.DataFrame([dict(ticker="BAYN", day=date(2026, 9, 6), previous_day=date(2026, 9, 5), previous_close=3800.0, close=0.01,
                               change=-0.99, previous_source="COCOS", source="TRADINGVIEW_BYMA", source_change=True)])
    decisions = pd.DataFrame([
        dict(id=1, decided_at=datetime(2026, 9, 1, 15, tzinfo=timezone.utc), ticker="BAYN", decision="BUY",
             outcome_5d=-0.99, outcome_10d=None, outcome_20d=None, outcome_40d=None),
        dict(id=2, decided_at=datetime(2026, 8, 1, 15, tzinfo=timezone.utc), ticker="BAYN", decision="BUY",
             outcome_5d=0.02, outcome_10d=None, outcome_20d=None, outcome_40d=None),
        dict(id=3, decided_at=datetime(2026, 9, 1, 15, tzinfo=timezone.utc), ticker="MU", decision="BUY",
             outcome_5d=0.02, outcome_10d=None, outcome_20d=None, outcome_40d=None)])
    days = {"BAYN": [date(2026, 8, 6), date(2026, 9, 5), date(2026, 9, 6), date(2026, 9, 7)], "MU": [date(2026, 9, 6)]}
    touched = audit.outcomes_touching_jumps(jumps, decisions, days, NOW)
    assert list(touched["id"]) == [1] and bool(touched.loc[0, "source_change"])


class _FakeConn:
    """Routes the audit's SQL to canned rows so the DB-facing checks run end to end without a database."""
    def __init__(self, decisions, candles):
        self.decisions, self.candles = decisions, candles

    async def fetch(self, sql, *params):
        if "FROM decision_log" in sql:
            return self.decisions
        if "DISTINCT e.ticker" in sql or "corporate_event" in sql:
            return []
        if "FROM market_candles" in sql:
            return self.candles
        raise AssertionError(sql)


def _decision(**over):
    row = dict(id=1, decided_at=datetime(2026, 9, 1, 15, tzinfo=timezone.utc), ticker="AMD", decision="BUY", decision_type="NEW",
               price_at_decision=100.0, outcome_5d=None, outcome_10d=None, outcome_20d=None, outcome_40d=None,
               outcome_filled_at=None, outcome_basis="canonical_cocos")
    row.update(over)
    return row


def _candle(ticker, day, close, source="COCOS"):
    return dict(ticker=ticker, day=day, close=close, source=source, scraped_at=datetime(2026, 9, 30, tzinfo=timezone.utc))


def test_o1_runs_end_to_end_and_names_the_candle_the_stored_outcome_came_from():
    conn = _FakeConn([_decision(outcome_5d=0.20, outcome_filled_at=datetime(2026, 9, 9, tzinfo=timezone.utc))],
                     [_candle("AMD", date(2026, 9, 6), 111.0), _candle("AMD", date(2026, 9, 8), 120.0)])
    checks = {c.check_id: c for c in asyncio.run(audit.check_decisions_and_outcomes(conn, datetime(2026, 6, 1, tzinfo=timezone.utc), NOW))}
    assert checks["O1"].status == "WARN" and "LATER_CANDLE_USED=1" in checks["O1"].detail and "cambian de signo=0" in checks["O1"].detail


def test_o1_fails_when_no_candle_explains_the_stored_outcome():
    conn = _FakeConn([_decision(outcome_5d=0.55, outcome_filled_at=datetime(2026, 9, 9, tzinfo=timezone.utc))],
                     [_candle("AMD", date(2026, 9, 6), 111.0)])
    checks = {c.check_id: c for c in asyncio.run(audit.check_decisions_and_outcomes(conn, datetime(2026, 6, 1, tzinfo=timezone.utc), NOW))}
    assert checks["O1"].status == "FAIL" and "NO_CANDLE_MATCHES=1" in checks["O1"].detail


def test_c3_reports_the_outcomes_that_exit_on_a_broken_candle():
    conn = _FakeConn([_decision(ticker="BAYN", outcome_5d=-0.9999)],
                     [_candle("BAYN", date(2026, 9, 5), 3800.0, "COCOS"), _candle("BAYN", date(2026, 9, 6), 0.01, "TRADINGVIEW_BYMA")])
    checks = {c.check_id: c for c in asyncio.run(audit._candle_jumps(conn, datetime(2026, 6, 1, tzinfo=timezone.utc)))}
    assert checks["C3"].status == "FAIL" and "1 saltos en 1 tickers" in checks["C3"].detail
    assert checks["C3b"].status == "WARN" and checks["C3b"].count == 1 and "1 con cambio de fuente" in checks["C3b"].detail
