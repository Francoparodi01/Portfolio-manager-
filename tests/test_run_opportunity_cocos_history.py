from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_opportunity


def _rows(count: int):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "ts": start + timedelta(days=i),
            "open_price": 100 + i,
            "high_price": 101 + i,
            "low_price": 99 + i,
            "close_price": 100.5 + i,
            "volume": 1000 + i,
        }
        for i in range(count)
    ]


class _FakeDatabase:
    def __init__(self, _url):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_cocos_universe_assets(self):
        return [
            {"ticker": "T", "asset_type": "CEDEAR"},
            {"ticker": "GGAL", "asset_type": "ACCION"},
        ]

    async def get_market_candles(self, ticker, **_kwargs):
        return _rows(60 if ticker == "T" else 20)


class _RadarConn:
    def __init__(self):
        self.insert_args = None

    async def execute(self, *_args):
        return None

    async def fetchval(self, *_args):
        return None

    async def fetchrow(self, _query, *args):
        self.insert_args = args
        return {"id": 123}


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_args):
        return None


class _RadarPool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _RadarDatabase:
    conn = _RadarConn()

    def __init__(self, _url):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_pool(self):
        return _RadarPool(self.conn)


def test_load_cocos_universe_assets_from_db():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))

    with patch("scripts.run_opportunity.PortfolioDatabase", _FakeDatabase):
        assets = asyncio.run(run_opportunity._load_cocos_universe_assets(cfg))

    assert assets == [
        {"ticker": "T", "asset_type": "CEDEAR"},
        {"ticker": "GGAL", "asset_type": "ACCION"},
    ]


def test_load_cocos_history_frames_for_opportunities_requires_sufficient_history():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))
    assets = [
        {"ticker": "T", "asset_type": "CEDEAR"},
        {"ticker": "GGAL", "asset_type": "ACCION"},
    ]

    with patch("scripts.run_opportunity.PortfolioDatabase", _FakeDatabase):
        frames = asyncio.run(run_opportunity._load_cocos_history_frames(cfg, assets))

    assert list(frames) == ["T"]


def test_intraday_samples_append_provisional_candle_only_on_trading_day():
    from src.collector.cocos_history import candles_to_frame

    frame = candles_to_frame(_rows(60))
    monday = datetime(2026, 4, 6, 13, 40, tzinfo=timezone.utc)
    result = run_opportunity._append_intraday_price_samples(
        {"T": frame},
        [
            {"ticker": "T", "ts": monday, "last_price": 175.0, "volume": 100},
            {"ticker": "T", "ts": monday + timedelta(hours=3), "last_price": 168.0, "volume": 500},
        ],
    )

    provisional = result["T"].iloc[-1]
    assert len(result["T"]) == 61
    assert provisional["Open"] == 175.0
    assert provisional["High"] == 175.0
    assert provisional["Low"] == 168.0
    assert provisional["Close"] == 168.0
    assert provisional["Volume"] == 500.0
    assert provisional["Source"] == "market_prices_intraday"

    sunday = datetime(2026, 4, 5, 15, 0, tzinfo=timezone.utc)
    weekend = run_opportunity._append_intraday_price_samples(
        {"T": frame},
        [{"ticker": "T", "ts": sunday, "last_price": 999.0, "volume": 1}],
    )
    assert len(weekend["T"]) == 60


def test_portfolio_equity_total_includes_cash_for_radar_sizing():
    total = run_opportunity._portfolio_equity_total(
        positions=[
            {"ticker": "VST", "market_value": 152_250},
            {"ticker": "IBM", "market_value": 135_800},
        ],
        cash_ars=1_330_905,
        snapshot_total_ars=1_618_955,
    )

    assert total == 1_618_955


def test_save_radar_candidates_persists_auditable_signal():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))
    candidate = SimpleNamespace(
        ticker="MU",
        status=run_opportunity.CandidateStatus.SWAP_CANDIDATO,
        trade_type="SWAP_CANDIDATE",
        final_score=0.22,
        conviction=0.71,
        tech_score=0.18,
        macro_score=-0.01,
        sentiment_score=0.0,
        momentum_score=0.12,
        technical_candle_source_mode="mixed",
        technical_has_reconstructed_candles=True,
        technical_candle_sources=("COCOS", "internal_snapshot"),
        technical_candle_source_counts={"COCOS": 258, "internal_snapshot": 2},
        asymmetry=SimpleNamespace(
            stop_loss_pct=0.08,
            risk_reward=1.7,
            asymmetry_ratio=1.3,
        ),
        edge=SimpleNamespace(
            raw=0.06,
            label="fuerte",
            vs_ticker="AMD",
        ),
        sizing_suggested=0.05,
        price_usd=11920.0,
        why_not_now="",
        action_concreta="Swap vs AMD",
        alerts=[],
    )
    report = SimpleNamespace(candidates=[candidate])
    macro_snap = SimpleNamespace(vix=18.0)

    with patch("scripts.run_opportunity.PortfolioDatabase", _RadarDatabase):
        saved = asyncio.run(
            run_opportunity._save_radar_candidates(
                cfg,
                report,
                macro_snap,
                {"state": "NORMAL"},
                portfolio_total_ars=2_000_000,
                owner_chat_id=77,
            )
        )

    args = _RadarDatabase.conn.insert_args
    layers = json.loads(args[5])

    assert saved == [123]
    assert args[0] == 77
    assert args[2] == "MU"
    assert args[6] == 11920.0
    assert args[14] == "radar_swap_candidato_swap_candidate"
    assert args[15] == "THEORETICAL"
    assert args[18] is True
    assert args[19] is False
    assert layers["source"] == "radar"
    assert layers["technical_data_source_mode"] == "mixed"
