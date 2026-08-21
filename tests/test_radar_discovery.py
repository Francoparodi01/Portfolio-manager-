from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from types import SimpleNamespace
from uuid import UUID

import pandas as pd
import pytest

from src.analysis.opportunity_screener import (
    AsymmetryMetrics,
    CandidateStatus,
    EdgeLabel,
    EdgeMetrics,
    OpportunityCandidate,
    OpportunityReport,
    ScreenerMetrics,
    TradeType,
    opportunity_rank_score,
    screen_universe,
)
from src.analysis.radar_discovery import (
    RADAR_DISCOVERY_SCHEMA_SQL,
    RadarDiscoveryStore,
    build_discovery_observations,
    discovery_scoring_version,
    measure_discovery_outcome,
    summarize_comparisons,
)


def _frame(price: float, *, source: str = "COCOS") -> pd.DataFrame:
    index = pd.date_range("2026-01-01", periods=60, freq="D", tz="UTC")
    frame = pd.DataFrame(
        {
            "Open": [price] * 60,
            "High": [price * 1.01] * 60,
            "Low": [price * 0.99] * 60,
            "Close": [price] * 60,
            "Volume": [1_000_000] * 60,
            "Source": [source] * 60,
        },
        index=index,
    )
    frame.attrs = {
        "candle_source_mode": "cocos",
        "candle_source_counts": {source: 60},
        "has_reconstructed_candles": False,
    }
    return frame


def _signal(ticker: str, score: float, regime: str = "STRONG_UPTREND"):
    bias = "POSITIVE" if score >= 0.2 else ("NEGATIVE" if score <= -0.2 else "NEUTRAL")
    return SimpleNamespace(
        ticker=ticker,
        technical_shadow_v2={
            "version": "technical-shadow-v2",
            "score": score,
            "bias": bias,
            "structural_break_gate": False,
        },
        technical_buy_shadow_v3={"volume_quality_20": 1.0},
        technical_regime=regime,
        trend_score=score,
        reversion_score=0.1,
        structural_break_confirmed=False,
        candle_source_mode="cocos",
        has_reconstructed_candles=False,
    )


def _candidate(
    ticker: str,
    *,
    final_score: float,
    status: CandidateStatus,
) -> OpportunityCandidate:
    return OpportunityCandidate(
        ticker=ticker,
        status=status,
        trade_type=TradeType.NEW_ENTRY,
        final_score=final_score,
        conviction=0.7,
        asymmetry=AsymmetryMetrics(
            ticker=ticker,
            asymmetry_ratio=2.0,
            risk_reward=2.0,
            rr_valid=True,
        ),
        edge=EdgeMetrics(
            raw=0.04,
            label=EdgeLabel.MODERADO,
            vs_ticker="",
            vs_score=0.0,
            explanation="",
        ),
        why_not_now="bajo umbral minimo" if status == CandidateStatus.DESCARTAR else "",
    )


def test_build_discovery_observations_keeps_full_universe_and_portfolio():
    aaa = _candidate("AAA", final_score=0.3, status=CandidateStatus.COMPRABLE_AHORA)
    held = _candidate("HOLDING", final_score=0.2, status=CandidateStatus.VIGILANCIA_A)
    rejected = _candidate("BBB", final_score=0.01, status=CandidateStatus.DESCARTAR)
    report = OpportunityReport(
        generated_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
        discovery_screening_results=[
            ScreenerMetrics(ticker="AAA", asset_type="CEDEAR", price=100, passes_screen=True),
            ScreenerMetrics(ticker="HOLDING", asset_type="CEDEAR", price=80, passes_screen=True),
            ScreenerMetrics(ticker="BBB", asset_type="CEDEAR", price=50, passes_screen=True),
            ScreenerMetrics(ticker="MISS", fail_reason="sin velas Cocos suficientes"),
        ],
        discovery_technical_signals={
            "AAA": _signal("AAA", 0.6),
            "HOLDING": _signal("HOLDING", 0.4),
            "BBB": _signal("BBB", -0.4, regime="DOWNTREND"),
        },
        discovery_scored_candidates=[aaa, held, rejected],
        discovery_ranked_candidates=[aaa, held, rejected],
    )
    frames = {
        "AAA": _frame(100),
        "HOLDING": _frame(80),
        "BBB": _frame(50),
        "QQQ": _frame(200),
    }

    rows = build_discovery_observations(
        report,
        universe=["AAA", "HOLDING", "BBB", "MISS", "QQQ"],
        history_frames=frames,
        asset_types={ticker: "CEDEAR" for ticker in frames},
        portfolio_tickers=["HOLDING"],
        selected_tickers=["AAA"],
        min_score=0.1,
        min_rr=0.0,
        manual_event_risk_by_ticker={"AAA": "earnings_window"},
    )
    by_ticker = {row.ticker: row for row in rows}

    assert len(rows) == 5
    assert by_ticker["AAA"].selected_top_n is True
    assert by_ticker["AAA"].rank_position == 1
    assert by_ticker["AAA"].rank_percentile == 1.0
    assert by_ticker["AAA"].v2_percentile == 1.0
    assert by_ticker["AAA"].v3_tier == "A"
    assert by_ticker["AAA"].setup_shadow_version == "radar-setup-shadow-v1"
    assert by_ticker["AAA"].readiness_state == "PRE_BREAKOUT"
    assert by_ticker["AAA"].discovery_score is None
    assert by_ticker["AAA"].feature_quality_flag == "PARTIAL"
    assert by_ticker["AAA"].setup_features["comparison_basis"] == "LOCAL_SAME_RUN"
    assert by_ticker["AAA"].metadata["manual_event_risk"] == "earnings_window"
    assert by_ticker["HOLDING"].in_portfolio is True
    assert by_ticker["HOLDING"].radar_eligible is True
    assert by_ticker["BBB"].rejection_reason == "bajo umbral minimo"
    assert by_ticker["BBB"].rank_position == 3
    assert by_ticker["BBB"].radar_eligible is False
    assert by_ticker["MISS"].price_quality_flag == "MISSING"
    assert by_ticker["QQQ"].rejection_reason == "benchmark_only"


def test_rank_score_is_the_production_formula_exposed_once():
    candidate = _candidate(
        "AAA",
        final_score=0.3,
        status=CandidateStatus.COMPRABLE_AHORA,
    )
    expected = 60 + (0.3 * 0.7 * (1 + 2 / 3)) * (1 + 0.04 * 2)
    assert opportunity_rank_score(candidate) == pytest.approx(expected)


def test_benchmarks_remain_controls_and_never_enter_candidate_ranking():
    frames = {"QQQ": _frame(200), "SPY": _frame(180)}
    assert screen_universe(["QQQ", "SPY"], history_frames=frames) == []
    rows = build_discovery_observations(
        OpportunityReport(),
        universe=["QQQ", "SPY"],
        history_frames=frames,
        asset_types={"QQQ": "CEDEAR", "SPY": "CEDEAR"},
        portfolio_tickers=[],
        selected_tickers=[],
        min_score=0.1,
        min_rr=0.0,
    )
    assert all(row.rejection_reason == "benchmark_only" for row in rows)
    assert all(row.rank_position is None for row in rows)


def test_measure_discovery_outcome_uses_sessions_and_intraperiod_low():
    as_of = datetime(2026, 1, 1, tzinfo=timezone.utc)
    closes = [101, 98, 95, 105, 110]
    lows = [99, 96, 90, 103, 108]
    candles = [
        {
            "ts": as_of + timedelta(days=index),
            "close_price": close,
            "low_price": low,
        }
        for index, (close, low) in enumerate(zip(closes, lows), start=1)
    ]

    assert measure_discovery_outcome(
        as_of_ts=as_of,
        reference_price=100,
        horizon_sessions=5,
        future_candles=candles[:4],
    ) is None
    result = measure_discovery_outcome(
        as_of_ts=as_of,
        reference_price=100,
        horizon_sessions=5,
        future_candles=candles,
    )
    assert result is not None
    assert result["outcome_price"] == 110
    assert result["forward_return"] == pytest.approx(0.1)
    assert result["max_drawdown"] == pytest.approx(95 / 101 - 1)


def test_comparisons_include_net_excess_drawdown_and_secondary_ic():
    rows = [
        {
            "ticker": "AAA",
            "forward_return": 0.20,
            "comparison_bucket": "TOP_5",
            "radar_eligible": True,
            "v3_tier": "A",
            "selected_top_n": True,
            "in_portfolio": False,
            "rank_percentile": 1.0,
            "max_drawdown": -0.03,
            "excess_vs_universe": 0.12,
            "excess_vs_qqq": 0.08,
            "excess_vs_spy": 0.10,
            "excess_vs_own_positions": 0.07,
            "price_quality_flag": "CANONICAL_COCOS",
        },
        {
            "ticker": "BBB",
            "forward_return": 0.10,
            "comparison_bucket": "TOP_5",
            "radar_eligible": True,
            "v3_tier": "A",
            "selected_top_n": False,
            "in_portfolio": True,
            "rank_percentile": 0.5,
            "max_drawdown": -0.05,
            "excess_vs_universe": 0.02,
            "excess_vs_qqq": -0.02,
            "excess_vs_spy": 0.00,
            "excess_vs_own_positions": 0.00,
            "price_quality_flag": "CANONICAL_COCOS",
        },
        {
            "ticker": "CCC",
            "forward_return": -0.10,
            "comparison_bucket": "REST",
            "radar_eligible": False,
            "v3_tier": "REJECTED",
            "selected_top_n": False,
            "in_portfolio": False,
            "rank_percentile": 0.0,
            "max_drawdown": -0.15,
            "excess_vs_universe": -0.18,
            "excess_vs_qqq": -0.22,
            "excess_vs_spy": -0.20,
            "excess_vs_own_positions": -0.20,
            "price_quality_flag": "CANONICAL_COCOS",
        },
    ]

    result = summarize_comparisons(rows, cost_bps=75)
    assert result["cohorts"]["top_5"]["n"] == 2
    assert result["cohorts"]["top_5"]["mean_net_return"] == pytest.approx(0.1425)
    assert result["cohorts"]["v3_rejected"]["win_rate"] == 0.0
    assert result["information_coefficient_spearman"] == {
        "n": 3,
        "sessions": 1,
        "rho": 1.0,
        "median_rho": 1.0,
        "pooled_rho": 1.0,
    }
    assert result["price_quality_counts"] == {"CANONICAL_COCOS": 3}
    assert result["quality_excluded_rows"] == 0


def test_stale_prices_remain_counted_but_do_not_enter_comparisons():
    result = summarize_comparisons([
        {
            "ticker": "STALE",
            "forward_return": 0.50,
            "price_quality_flag": "CANONICAL_COCOS_STALE",
            "rejection_reason": None,
        }
    ])

    assert result["sample_rows"] == 0
    assert result["quality_excluded_rows"] == 1
    assert result["price_quality_counts"] == {"CANONICAL_COCOS_STALE": 1}


def test_schema_is_additive_and_version_filter_is_mandatory():
    lowered = RADAR_DISCOVERY_SCHEMA_SQL.lower()
    assert "create table if not exists radar_discovery_runs" in lowered
    assert "create table if not exists radar_discovery_snapshots" in lowered
    assert "create table if not exists radar_discovery_outcomes" in lowered
    assert "create table if not exists radar_setup_events" in lowered
    assert "create table if not exists radar_setup_outcomes" in lowered
    assert "add column if not exists setup_shadow_version" in lowered
    assert "decision_log" not in lowered
    assert "control_count integer not null default 0" in lowered
    assert "unique (owner_chat_id, captured_session, scoring_version)" in lowered
    assert discovery_scoring_version().startswith(
        "radar-v2+technical-shadow-v2+technical-buy-shadow-v3:"
    )
    assert discovery_scoring_version(period="1y") != discovery_scoring_version(period="2y")

    store = RadarDiscoveryStore(pool=None)
    with pytest.raises(ValueError, match="scoring_version is required"):
        asyncio.run(store.comparison_rows(scoring_version="", horizon_sessions=20))


def test_comparison_query_is_read_only_and_scoped_to_owner_and_version():
    captured = {}

    class _Connection:
        async def fetch(self, statement, *args):
            captured["statement"] = statement
            captured["args"] = args
            return []

    class _Acquire:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    store = RadarDiscoveryStore(pool=_Pool())
    rows = asyncio.run(
        store.comparison_rows(
            scoring_version="frozen-v1",
            horizon_sessions=20,
            owner_chat_id=77,
        )
    )

    assert rows == []
    assert captured["args"] == ("frozen-v1", 20, 77, None)
    assert "r.scoring_version = $1" in captured["statement"]
    assert "r.owner_chat_id = $3" in captured["statement"]
    assert "CREATE TABLE" not in captured["statement"]


def test_setup_comparison_query_is_read_only_and_uses_trigger_outcomes():
    captured = {}

    class _Connection:
        async def fetch(self, statement, *args):
            captured["statement"] = statement
            captured["args"] = args
            return []

    class _Acquire:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    rows = asyncio.run(
        RadarDiscoveryStore(_Pool()).setup_comparison_rows(
            scoring_version="frozen-v2",
            horizon_sessions=20,
            owner_chat_id=77,
        )
    )
    assert rows == []
    assert captured["args"] == ("frozen-v2", 20, 77, None)
    assert "radar_setup_outcomes" in captured["statement"]
    assert "TRIGGERED_AFTER_DISCOVERY" in captured["statement"]
    assert "CREATE TABLE" not in captured["statement"]


def test_snapshot_insert_persists_all_shadow_fields_with_matching_placeholders():
    observation = build_discovery_observations(
        OpportunityReport(generated_at=datetime(2026, 8, 20, tzinfo=timezone.utc)),
        universe=["AAA"],
        history_frames={"AAA": _frame(100)},
        asset_types={"AAA": "CEDEAR"},
        portfolio_tickers=[],
        selected_tickers=[],
        min_score=0.1,
        min_rr=0.0,
    )[0]
    captured = {}

    class _Transaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Connection:
        def transaction(self):
            return _Transaction()

        async def fetchval(self, statement, *args):
            if "INSERT INTO radar_discovery_runs" in statement:
                return UUID("00000000-0000-0000-0000-000000000001")
            return None

        async def execute(self, statement, *args):
            if "INSERT INTO radar_discovery_snapshots" in statement:
                captured["statement"] = statement
                captured["args"] = args

    connection = _Connection()

    class _Acquire:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    result = asyncio.run(
        RadarDiscoveryStore(_Pool()).save_snapshot(
            run_id="00000000-0000-0000-0000-000000000001",
            owner_chat_id=77,
            captured_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
            scoring_version="frozen-v2",
            observations=[observation],
            parameters={},
        )
    )
    placeholders = [int(value) for value in re.findall(r"\$(\d+)", captured["statement"])]
    assert result["inserted"] == 1
    assert max(placeholders) == 55
    assert len(captured["args"]) == 55
    assert observation.setup_shadow_version in captured["args"]


def test_feature_flag_defaults_off_and_operational_top_six_is_unchanged():
    root = Path(__file__).resolve().parents[1]
    opportunity = (root / "scripts" / "run_opportunity.py").read_text(encoding="utf-8")
    scheduler = (root / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")
    assert '"RADAR_DISCOVERY_LEDGER_ENABLED", "false"' in opportunity
    assert '"RADAR_DISCOVERY_LEDGER_ENABLED", "false"' in scheduler
    assert '"--top",\n        "6"' in scheduler
    main_source = opportunity[opportunity.index("async def main("):]
    assert "and capture_discovery" in main_source
    assert main_source.index("await _save_radar_candidates(") < main_source.index(
        "await _capture_radar_discovery("
    )


def test_capture_job_persists_full_observations_with_frozen_parameters(monkeypatch):
    from scripts import run_opportunity

    captured = {}
    report = OpportunityReport(
        generated_at=datetime(2026, 8, 13, 19, 50, tzinfo=timezone.utc)
    )
    observations = [SimpleNamespace(ticker="AAA")]

    def _build(*args, **kwargs):
        captured["build"] = kwargs
        return observations

    class _Database:
        def __init__(self, url):
            captured["database_url"] = url

        async def connect(self):
            captured["connected"] = True

        async def get_pool(self):
            return "pool"

        async def close(self):
            captured["closed"] = True

    class _Store:
        def __init__(self, pool):
            assert pool == "pool"

        async def save_snapshot(self, **kwargs):
            captured["save"] = kwargs
            return {"inserted": len(kwargs["observations"]), "duplicate": False}

    monkeypatch.setattr(run_opportunity, "build_discovery_observations", _build)
    monkeypatch.setattr(run_opportunity, "PortfolioDatabase", _Database)
    monkeypatch.setattr(run_opportunity, "RadarDiscoveryStore", _Store)
    monkeypatch.setattr(
        run_opportunity,
        "discovery_scoring_version",
        lambda **_kwargs: "frozen-v1",
    )

    result = asyncio.run(
        run_opportunity._save_radar_discovery_snapshot(
            SimpleNamespace(database=SimpleNamespace(url="postgresql://test")),
            run_id="00000000-0000-0000-0000-000000000001",
            owner_chat_id=77,
            report=report,
            universe=["AAA", "BBB"],
            history_frames={},
            asset_types={},
            portfolio_tickers=["BBB"],
            selected_tickers=["AAA"],
            min_score=0.10,
            min_rr=0.0,
            top_n=6,
            period="1y",
        )
    )

    assert result == {"inserted": 1, "duplicate": False}
    assert captured["build"]["universe"] == ["AAA", "BBB"]
    assert captured["build"]["portfolio_tickers"] == ["BBB"]
    assert captured["save"]["scoring_version"] == "frozen-v1"
    assert captured["save"]["parameters"]["operational_top_n"] == 6
    assert captured["save"]["parameters"]["setup_shadow_version"] == "radar-setup-shadow-v1"
    assert captured["save"]["parameters"]["affects_execution"] is False
    assert captured["closed"] is True


def test_scheduler_requests_discovery_without_changing_top_six(monkeypatch):
    from src.scheduler import runner

    commands = []

    class _Proc:
        returncode = 0

        async def communicate(self):
            return b"ok", b""

    async def _create_subprocess_exec(*cmd, stdout=None, stderr=None):
        commands.append(cmd)
        return _Proc()

    cfg = SimpleNamespace(scraper=SimpleNamespace(telegram_chat_id="77"))
    monkeypatch.setattr(runner, "_is_business_day", lambda: True)
    monkeypatch.setattr(runner, "get_config", lambda: cfg)
    monkeypatch.setattr(runner, "RADAR_DISCOVERY_LEDGER_ENABLED", True)
    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", _create_subprocess_exec)

    asyncio.run(runner.run_radar_audit_capture())

    assert len(commands) == 1
    command = commands[0]
    assert command[command.index("--top") + 1] == "6"
    assert "--capture-discovery" in command


def test_load_portfolio_uses_legacy_snapshot_only_for_configured_owner(monkeypatch):
    from scripts import run_opportunity

    calls = []
    legacy_snapshot = {
        "positions": [{"ticker": "MSFT", "market_value": 100.0}],
        "cash_ars": 10.0,
        "total_value_ars": 110.0,
        "scraped_at": datetime(2026, 8, 21, tzinfo=timezone.utc),
    }

    class _Database:
        def __init__(self, _url):
            pass

        async def connect(self):
            pass

        async def get_latest_snapshot(self, owner_chat_id=None):
            calls.append(owner_chat_id)
            return legacy_snapshot if owner_chat_id is None else None

        async def get_latest_market_prices(self):
            return []

        async def close(self):
            pass

    cfg = SimpleNamespace(
        database=SimpleNamespace(url="postgresql://test"),
        scraper=SimpleNamespace(telegram_chat_id="77"),
    )
    monkeypatch.setattr(run_opportunity, "PortfolioDatabase", _Database)

    positions, total, cash = asyncio.run(
        run_opportunity._load_portfolio(cfg, owner_chat_id=77)
    )

    assert calls == [77, None]
    assert [row["ticker"] for row in positions] == ["MSFT"]
    assert total == 110.0
    assert cash == 10.0


def test_load_portfolio_does_not_expose_legacy_snapshot_to_other_owner(monkeypatch):
    from scripts import run_opportunity

    calls = []

    class _Database:
        def __init__(self, _url):
            pass

        async def connect(self):
            pass

        async def get_latest_snapshot(self, owner_chat_id=None):
            calls.append(owner_chat_id)
            return None

        async def close(self):
            pass

    cfg = SimpleNamespace(
        database=SimpleNamespace(url="postgresql://test"),
        scraper=SimpleNamespace(telegram_chat_id="77"),
    )
    monkeypatch.setattr(run_opportunity, "PortfolioDatabase", _Database)

    positions, total, cash = asyncio.run(
        run_opportunity._load_portfolio(cfg, owner_chat_id=88)
    )

    assert calls == [88]
    assert positions == []
    assert total == 0.0
    assert cash == 0.0


def test_assign_configured_snapshot_owner_fills_legacy_snapshot():
    from src.scheduler.runner import _assign_configured_snapshot_owner

    snapshot = SimpleNamespace(owner_chat_id=None)

    result = _assign_configured_snapshot_owner(snapshot, "77")

    assert result is snapshot
    assert snapshot.owner_chat_id == 77


def test_assign_configured_snapshot_owner_preserves_explicit_owner():
    from src.scheduler.runner import _assign_configured_snapshot_owner

    snapshot = SimpleNamespace(owner_chat_id=88)

    _assign_configured_snapshot_owner(snapshot, "77")

    assert snapshot.owner_chat_id == 88


def test_opportunity_universe_excludes_non_ticker_market_labels():
    from scripts.run_opportunity import _filter_operable_cocos_assets

    valid, invalid = _filter_operable_cocos_assets([
        {"ticker": "NVDA", "asset_type": "CEDEAR"},
        {"ticker": "BA.C", "asset_type": "ACCION"},
        {"ticker": "C.", "asset_type": "ACCION"},
        {"ticker": "ETF", "asset_type": "CEDEAR"},
    ])

    assert [row["ticker"] for row in valid] == ["NVDA", "BA.C"]
    assert invalid == ["C.", "ETF"]
