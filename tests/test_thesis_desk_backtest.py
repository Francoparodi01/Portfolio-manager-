"""Synthetic fixtures verify mechanics, never historical profitability."""
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
import json

import pytest

from scripts.backtest_thesis_desk import capture_sql, main
from src.backtesting.thesis_desk import ART, DeskPolicy, ThesisDesk, instant, run_desk, validate_evidence


def at(day="2026-09-01", clock="11:00:00"):
    return f"{day}T{clock}-03:00"


def quote(price=100, ticker="AAA", day="2026-09-01", clock="10:59:00"):
    return dict(ts=at(day, clock), ticker=ticker, asset_type="CEDEAR", currency="ARS", last_price=str(price))


def fixture(tickers=("AAA",), state="GREEN"):
    facts, universe, bars, quotes = [], [], [], []
    for t in tickers:
        facts.append(dict(id="fact-" + t, ticker=t, thesis_id="thesis-" + t, kind="GUIDANCE_RAISED",
                          source_type="IR", source_url="https://issuer.example/release", published_at=at("2026-08-31", "09:00:00"),
                          available_at=at("2026-08-31", "09:00:00"), knowledge_basis="ARCHIVED_AT_TIME", invalidation="Official guidance cut"))
        universe.append(dict(id="member-" + t, ticker=t, asset_type="CEDEAR", currency="ARS", underlying_kind="COMMON_STOCK",
                             sleeve="CORE", membership_source="IVV", asof_at=at("2026-08-31", "09:00:00"),
                             available_at=at("2026-08-31", "09:00:00"), knowledge_basis="ARCHIVED_AT_TIME",
                             source_url="https://www.ishares.com/fixture"))
        end = datetime(2026, 8, 28, 17, tzinfo=ART)
        for i in range(35):
            ts = end - timedelta(days=i)
            if ts.weekday() < 5:
                bars.append(dict(ticker=t, ts=ts.isoformat(), bar_end=ts.isoformat(), available_at=ts.isoformat(),
                                 scraped_at=ts.isoformat(), is_closed=True, interval="1d", adjustment_policy="RAW",
                                 source="COCOS", asset_type="CEDEAR", currency="ARS", close_price="100"))
        quotes += [quote(ticker=t), quote(ticker=t, clock="11:01:00"), quote(ticker=t, clock="16:59:00")]
    evidence = dict(schema="thesis-desk-evidence-v1", facts=facts, universe=universe,
                    risk=[dict(id="risk", state=state, available_at=at("2026-08-31", "09:00:00"),
                               valid_until=at("2026-09-10", "18:00:00"), source_url="https://archive.example/risk",
                               knowledge_basis="ARCHIVED_AT_TIME")], blackouts=[],
                    coverage=[dict(id=d, domain=d, start="2026-09-01", end="2026-09-10", status="COMPLETE",
                                   scope="DECLARED_UNIVERSE", source_url="https://archive.example/coverage")
                              for d in ("facts", "universe", "risk", "blackouts")])
    return dict(intraday_points=quotes, daily_candles=bars, corporate_effects=[]), evidence


def run(rows, evidence, variant="BASE_V1", end="2026-09-01", **kwargs):
    return run_desk(rows, evidence, "2026-09-01", end, DeskPolicy(variant=variant, **kwargs))


def test_realistic_costs_and_nav_reconcile_without_forced_sale():
    rows, evidence = fixture()
    result = run(rows, evidence)
    assert result["status"] == "COMPLETE"
    fill = result["fills"][0]
    assert fill["quantity"] == 1667
    assert Decimal(fill["fill_price"]) == Decimal("100.15")
    cost = Decimal("1667") * Decimal("100.15") * Decimal("1.006")
    nav = Decimal("3000000") - cost + Decimal("166700")
    assert Decimal(result["metrics"]["final_nav_ars"]) == nav
    assert result["metrics"]["closed_cycles"] == 0
    assert result["metrics"]["win_rate_pct"] is None


def test_absent_evidence_is_not_zero_percent_performance():
    rows, _ = fixture()
    evidence = dict(schema="thesis-desk-evidence-v1")
    result = run(rows, evidence)
    assert result["metrics"] is None and result["status"] == "NOT_EVALUABLE"
    assert result["accounting"]["fills"] == 0 and result["cuts"] == 3


def test_complete_empty_fact_archive_can_legitimately_hold_cash():
    rows, evidence = fixture()
    evidence["facts"] = []
    result = run(rows, evidence)
    assert result["status"] == "COMPLETE" and Decimal(result["metrics"]["return_pct"]) == 0


@pytest.mark.parametrize("section,field", [("facts", "available_at"), ("universe", "available_at")])
def test_future_evidence_never_enters(section, field):
    rows, evidence = fixture()
    evidence[section][0][field] = at("2026-09-02")
    assert not run(rows, evidence)["fills"]


@pytest.mark.parametrize("field,value", [("available_at", at("2026-09-02")), ("scraped_at", at("2026-09-02")),
                                       ("is_closed", False), ("adjustment_policy", None), ("interval", "1h")])
def test_missing_or_future_candle_lineage_blocks_strategy(field, value):
    rows, evidence = fixture()
    for bar in rows["daily_candles"]:
        bar[field] = value
    result = run(rows, evidence)
    assert not result["fills"] and result["metrics"] is None
    assert "MISSING_PIT_TECHNICAL_HISTORY" in result["quality_flags"]


def test_daily_bar_after_publication_does_not_move_entry_zone():
    rows, evidence = fixture()
    extra = deepcopy(rows["daily_candles"][0])
    extra.update(ts=at("2026-08-31", "17:00:00"), bar_end=at("2026-08-31", "17:00:00"),
                 available_at=at("2026-08-31", "17:00:00"), scraped_at=at("2026-08-31", "17:00:00"), close_price="9999")
    rows["daily_candles"].append(extra)
    assert run(rows, evidence)["fills"][0]["quantity"] == 1667


def test_fill_latency_and_no_chasing_gap():
    rows, evidence = fixture()
    rows["intraday_points"] = [quote(), quote(clock="11:00:30"), quote(130, clock="11:01:00"), quote(130, clock="16:59:00")]
    assert not run(rows, evidence)["fills"]


def test_fill_exactly_at_next_cutoff_expires():
    rows, evidence = fixture()
    rows["intraday_points"] = [quote(), quote(clock="13:00:00")]
    assert not run(rows, evidence)["fills"]


def test_stop_gap_fills_next_quote_and_closes_after_costs():
    rows, evidence = fixture()
    rows["intraday_points"] += [quote(85, clock="12:00:00"), quote(80, clock="12:01:00")]
    result = run(rows, evidence)
    assert [f["side"] for f in result["fills"]] == ["BUY", "SELL"]
    sale = result["fills"][-1]
    assert sale["reason"] == "STOP" and Decimal(sale["fill_price"]) == Decimal("79.8800")
    assert Decimal(result["metrics"]["realized_pnl_ars"]) == Decimal(result["closed_cycles"][0]["net_pnl_ars"])
    assert Decimal(result["metrics"]["win_rate_pct"]) == 0


def test_fact_veto_after_signal_before_fill():
    rows, evidence = fixture()
    bad = deepcopy(evidence["facts"][0])
    bad.update(id="kill", kind="GUIDANCE_CUT", published_at=at(clock="11:00:30"), available_at=at(clock="11:00:30"))
    evidence["facts"].append(bad)
    assert not run(rows, evidence)["fills"]


def test_fact_invalidation_closes_at_next_available_quote():
    rows, evidence = fixture()
    bad = deepcopy(evidence["facts"][0])
    bad.update(id="kill", kind="GUIDANCE_CUT", published_at=at(clock="12:00:00"), available_at=at(clock="12:00:00"))
    evidence["facts"].append(bad)
    rows["intraday_points"] += [quote(101, clock="13:01:00")]
    result = run(rows, evidence)
    assert result["fills"][-1]["reason"] == "FACT_INVALIDATION"


def test_quota_one_early_week_two_from_third_session():
    rows, evidence = fixture(("AAA", "BBB", "CCC"))
    for t in ("AAA", "BBB", "CCC"):
        rows["intraday_points"] += [quote(ticker=t, day="2026-09-02"), quote(ticker=t, day="2026-09-02", clock="11:01:00")]
    result = run(rows, evidence, end="2026-09-02")
    assert [f["ticker"] for f in result["fills"]] == ["AAA", "BBB"]
    assert result["rejection_counts"]["WEEKLY_QUOTA"] > 0


@pytest.mark.parametrize("state,expected", [("RED", 0), ("YELLOW", 833), ("GREEN", 1667)])
def test_risk_traffic_light_size(state, expected):
    rows, evidence = fixture(state=state)
    result = run(rows, evidence, "RISK_V2")
    assert sum(f["quantity"] for f in result["fills"] if f["side"] == "BUY") == expected


def test_missing_risk_state_not_silently_green():
    rows, evidence = fixture()
    evidence["risk"] = []
    result = run(rows, evidence, "RISK_V2")
    assert result["metrics"] is None and not result["fills"]


def test_blackout_only_applies_when_known():
    rows, evidence = fixture()
    event = dict(id="employment", ticker="*", start_at=at(clock="10:00:00"), end_at=at(clock="17:00:00"),
                 available_at=at(clock="09:00:00"), source_url="https://calendar.example/event", knowledge_basis="ARCHIVED_AT_TIME")
    evidence["blackouts"] = [event]
    assert not run(rows, evidence, "RISK_V2")["fills"]
    event["available_at"] = at("2026-09-02")
    assert run(rows, evidence, "RISK_V2")["fills"]


def test_duplicate_quote_deduplicates_and_conflict_errors():
    rows, evidence = fixture()
    rows["intraday_points"].append(deepcopy(rows["intraday_points"][0]))
    assert run(rows, evidence)["quote_rows"] == 3
    rows["intraday_points"][-1]["last_price"] = "101"
    with pytest.raises(ValueError, match="CONFLICTING"):
        run(rows, evidence)


def test_missing_session_and_stale_holdings_are_not_performance():
    rows, evidence = fixture()
    result = run(rows, evidence, end="2026-09-02")
    assert result["metrics"] is None and result["missing_quote_sessions"] == ["2026-09-02"]
    assert "STALE_HELD_MARKS" in result["quality_flags"]


def test_corporate_action_adjusts_nominals_stop_and_retains_total_basis():
    rows, evidence = fixture()
    known = at("2026-08-31")
    rows["corporate_effects"] = [dict(id=1, ticker="AAA", asset_type="CEDEAR", currency="ARS", quantity_factor="2",
        price_factor="0.5", cost_basis_factor="0.5", lifecycle_status="EFFECTIVE",
        effective_at=at(clock="12:00:00"), is_active=True, event_created_at=known, event_updated_at=known,
        effect_created_at=known, effect_updated_at=known)]
    rows["intraday_points"][-1]["last_price"] = "50"
    rows["intraday_points"] += [quote(50, clock="12:00:00")]
    result = run(rows, evidence)
    assert len(result["fills"]) == 1
    assert result["journal"][-1]["action"] == "CORPORATE_ADJUSTMENT"
    base_rows, base_evidence = fixture()
    assert result["metrics"]["final_nav_ars"] == run(base_rows, base_evidence)["metrics"]["final_nav_ars"]


def test_risk_turning_yellow_between_signal_and_fill_reduces_size():
    rows, evidence = fixture()
    yellow = deepcopy(evidence["risk"][0])
    yellow.update(id="yellow", state="YELLOW", available_at=at(clock="11:00:30"))
    evidence["risk"].append(yellow)
    assert run(rows, evidence, "RISK_V2")["fills"][0]["quantity"] == 833


def test_raw_pre_split_history_uses_documented_price_factor():
    rows, evidence = fixture()
    known = at("2026-08-28", "18:00:00")
    rows["corporate_effects"] = [dict(id=1, ticker="AAA", asset_type="CEDEAR", currency="ARS", quantity_factor="2",
        price_factor="0.5", cost_basis_factor="0.5", lifecycle_status="EFFECTIVE",
        effective_at=at("2026-08-31", "08:00:00"), is_active=True, event_created_at=known, event_updated_at=known,
        effect_created_at=known, effect_updated_at=known)]
    for row in rows["intraday_points"]:
        row["last_price"] = "50"
    result = run(rows, evidence)
    assert result["fills"] and Decimal(result["fills"][0]["quote_price"]) == 50


def test_no_next_quote_means_no_invented_stop_fill():
    rows, evidence = fixture()
    rows["intraday_points"][-1]["last_price"] = "80"
    result = run(rows, evidence)
    assert len(result["fills"]) == 1 and result["accounting"]["open_positions"] == 1


def test_circuit_breaker_latches_and_liquidates():
    rows, evidence = fixture()
    # Direct portfolio fixture isolates the risk guard from deliberately small starter sizes.
    desk = ThesisDesk(rows, evidence, DeskPolicy(variant="RISK_V2"))
    desk.cash = Decimal("2000000")
    desk.positions["AAA"] = dict(qty=10000, basis=Decimal("1000000"), original_cost=Decimal("1000000"),
        cycle_pnl=Decimal(0), stop=Decimal(50), fact=evidence["facts"][0], opened_at=instant(at()))
    desk.prices["AAA"] = (Decimal(100), instant(at()))
    desk.on_quotes(instant(at(clock="12:00:00")), [quote(70, clock="12:00:00")])
    assert desk.braked and desk.pending["AAA"]["reason"] == "DRAWDOWN_BRAKE"
    desk.on_quotes(instant(at(clock="12:01:00")), [quote(69, clock="12:01:00")])
    assert not desk.positions and desk.braked


def test_simultaneous_drawdown_quote_cancels_pending_buy_before_fill():
    rows, evidence = fixture(("AAA", "BBB"))
    desk = ThesisDesk(rows, evidence, DeskPolicy(variant="RISK_V2"))
    desk.cash = Decimal("2000000")
    desk.positions["AAA"] = dict(qty=10000, basis=Decimal("1000000"), original_cost=Decimal("1000000"),
        cycle_pnl=Decimal(0), stop=Decimal(50), fact=evidence["facts"][0], opened_at=instant(at()))
    desk.prices = {t: (Decimal(100), instant(at())) for t in ("AAA", "BBB")}
    desk.decide(instant(at()), instant(at(clock="13:00:00")))
    assert desk.pending["BBB"]["side"] == "BUY"
    desk.on_quotes(instant(at(clock="11:01:00")), [quote(70, clock="11:01:00"), quote(ticker="BBB", clock="11:01:00")])
    assert desk.braked and not any(f["side"] == "BUY" for f in desk.fills)
    assert "BBB" not in desk.pending


def test_future_sale_cannot_fund_earlier_buy():
    rows, evidence = fixture(("AAA", "BBB"))
    desk = ThesisDesk(rows, evidence, DeskPolicy())
    desk.cash = Decimal(0)
    desk.positions["AAA"] = dict(qty=100, basis=Decimal("10000"), original_cost=Decimal("10000"),
        cycle_pnl=Decimal(0), stop=Decimal(50), fact=evidence["facts"][0], opened_at=instant(at()))
    desk.prices = {t: (Decimal(100), instant(at())) for t in ("AAA", "BBB")}
    desk.queue_exit("AAA", instant(at()), "TEST_EXIT")
    desk.decide(instant(at()), instant(at(clock="13:00:00")))
    assert desk.pending["BBB"]["side"] == "BUY"
    desk.on_quotes(instant(at(clock="11:01:00")), [quote(ticker="BBB", clock="11:01:00")])
    assert not desk.fills
    desk.on_quotes(instant(at(clock="11:02:00")), [quote(ticker="AAA", clock="11:02:00")])
    assert [f["side"] for f in desk.fills] == ["SELL"]
    desk.on_quotes(instant(at(clock="11:03:00")), [quote(ticker="BBB", clock="11:03:00")])
    assert [f["side"] for f in desk.fills] == ["SELL", "BUY"] and desk.cash >= 0


def test_concentration_trim_is_partial_cycle():
    rows, evidence = fixture()
    desk = ThesisDesk(rows, evidence, DeskPolicy(variant="RISK_V2"))
    desk.cash = Decimal("2400000")
    desk.positions["AAA"] = dict(qty=6000, basis=Decimal("600000"), original_cost=Decimal("600000"),
        cycle_pnl=Decimal(0), stop=Decimal(50), fact=evidence["facts"][0], opened_at=instant(at()))
    desk.prices["AAA"] = (Decimal(100), instant(at()))
    desk.decide(instant(at()), instant(at(clock="13:00:00")))
    assert desk.pending["AAA"]["qty"] == 2400
    desk.on_quotes(instant(at(clock="11:01:00")), [quote(clock="11:01:00")])
    assert desk.positions["AAA"]["qty"] == 3600 and not desk.cycles


@pytest.mark.parametrize("mutator", [
    lambda e: e["facts"][0].update(available_at="2026-09-01T11:00:00"),
    lambda e: e["universe"][0].update(underlying_kind="ETF"),
    lambda e: e["facts"].append(deepcopy(e["facts"][0])),
    lambda e: e["facts"][0].update(source_type="SEC"),
])
def test_invalid_evidence_contract_rejected(mutator):
    _, evidence = fixture()
    mutator(evidence)
    with pytest.raises(ValueError):
        validate_evidence(evidence)


def test_capture_is_read_only_and_date_input_not_sql():
    sql, queries = capture_sql("2026-09-01", "2026-10-31")
    assert "REPEATABLE READ READ ONLY" in sql and "2026-10-30T17:00:00-03:00" in sql
    assert len(queries) == 4
    with pytest.raises(ValueError):
        capture_sql("2026-09-01'; DELETE FROM market_prices; --", "2026-10-31")


def test_cli_offline_writes_two_variants_manifest_and_refuses_overwrite(tmp_path):
    rows, evidence = fixture()
    capture = tmp_path / "capture.json"
    capture.write_text(json.dumps(dict(observations=[dict(name=k, rows=v) for k, v in rows.items()])))
    ev = tmp_path / "evidence.json"
    ev.write_text(json.dumps(evidence))
    output = tmp_path / "result"
    args = ["--start", "2026-09-01", "--end", "2026-09-01", "--capture", str(capture), "--evidence", str(ev), "--output", str(output)]
    assert main(args) == 0
    manifest = json.loads((output / "manifest.json").read_text())
    assert "BASE_V1.json" in manifest["artifacts"] and "RISK_V2.json" in manifest["artifacts"]
    assert json.loads((output / "RISK_V2.json").read_text())["status"] == "COMPLETE"
    with pytest.raises(SystemExit):
        main(args)
