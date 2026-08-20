"""Prospective, audit-only ledger for the complete Radar universe."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import fmean, median
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID
from zoneinfo import ZoneInfo

from src.analysis.corporate_actions import normalize_candle_rows, rebase_reference_price
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityReport,
    opportunity_rank_score,
)
from src.analysis.technical_buy_shadow_v3 import (
    TECHNICAL_BUY_SHADOW_V3_VERSION,
    build_technical_buy_shadow_v3,
)
from src.analysis.radar_setup_shadow import (
    RADAR_SETUP_SHADOW_VERSION,
    RADAR_SETUP_TRIGGER_WINDOW_SESSIONS,
    build_radar_setup_shadow_universe,
    resolve_setup_event,
)
from src.analysis.technical_shadow_v2 import TECHNICAL_SHADOW_V2_VERSION
from src.analysis.thesis_shadow import mature_forecast


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
RADAR_DISCOVERY_PROTOCOL_VERSION = "radar-discovery-ledger-v2"
RADAR_DISCOVERY_HORIZONS = (5, 10, 20, 40)
DEFAULT_THEORETICAL_COST_BPS = 75
BENCHMARK_TICKERS = {"QQQ", "SPY"}
SCORING_FINGERPRINT_FILES = (
    "scripts/run_opportunity.py",
    "src/analysis/radar_discovery.py",
    "src/analysis/radar_setup_shadow.py",
    "src/analysis/opportunity_screener.py",
    "src/analysis/technical.py",
    "src/analysis/synthesis.py",
    "src/analysis/macro.py",
    "src/analysis/signal_aggregator.py",
    "src/analysis/trend_regime.py",
    "src/analysis/technical_shadow_v2.py",
    "src/analysis/technical_buy_shadow_v3.py",
    "src/analysis/execution_planner.py",
    "src/collector/cocos_history.py",
)


RADAR_DISCOVERY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS radar_discovery_runs (
    run_id UUID PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    captured_at TIMESTAMPTZ NOT NULL,
    captured_session DATE NOT NULL,
    scoring_version TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    universe_count INTEGER NOT NULL,
    control_count INTEGER NOT NULL DEFAULT 0,
    evaluated_count INTEGER NOT NULL,
    eligible_count INTEGER NOT NULL,
    selected_count INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMPLETE',
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_chat_id, captured_session, scoring_version)
);

ALTER TABLE radar_discovery_runs
    ADD COLUMN IF NOT EXISTS control_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS radar_discovery_snapshots (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES radar_discovery_runs(run_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    asset_type TEXT NOT NULL DEFAULT 'UNKNOWN',
    reference_ts TIMESTAMPTZ,
    reference_price FLOAT,
    price_quality_flag TEXT NOT NULL,
    data_source_mode TEXT NOT NULL DEFAULT 'unknown',
    data_source_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    screener_passed BOOLEAN NOT NULL DEFAULT FALSE,
    radar_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    acceptance_reason TEXT,
    rejection_reason TEXT,
    v2_version TEXT,
    v2_score FLOAT,
    v2_bias TEXT,
    v2_percentile FLOAT,
    v3_version TEXT,
    v3_score FLOAT,
    v3_classification TEXT,
    v3_tier TEXT,
    v3_percentile FLOAT,
    v3_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    technical_regime TEXT,
    trend_score FLOAT,
    reversion_score FLOAT,
    setup_shadow_version TEXT,
    trend_component_score FLOAT,
    relative_strength_component_score FLOAT,
    compression_component_score FLOAT,
    setup_component_score FLOAT,
    discovery_score FLOAT,
    setup_score FLOAT,
    composite_shadow_score FLOAT,
    discovery_percentile FLOAT,
    setup_percentile FLOAT,
    composite_shadow_percentile FLOAT,
    readiness_state TEXT,
    trigger_price FLOAT,
    invalidation_price FLOAT,
    target_price FLOAT,
    setup_risk_reward FLOAT,
    trigger_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    feature_quality_flag TEXT,
    setup_features JSONB NOT NULL DEFAULT '{}'::jsonb,
    setup_warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    radar_final_score FLOAT,
    ranking_score FLOAT,
    rank_position INTEGER,
    rank_percentile FLOAT,
    comparison_bucket TEXT NOT NULL,
    radar_status TEXT,
    trade_type TEXT,
    in_portfolio BOOLEAN NOT NULL DEFAULT FALSE,
    selected_top_n BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, ticker)
);

ALTER TABLE radar_discovery_snapshots
    ADD COLUMN IF NOT EXISTS setup_shadow_version TEXT,
    ADD COLUMN IF NOT EXISTS trend_component_score FLOAT,
    ADD COLUMN IF NOT EXISTS relative_strength_component_score FLOAT,
    ADD COLUMN IF NOT EXISTS compression_component_score FLOAT,
    ADD COLUMN IF NOT EXISTS setup_component_score FLOAT,
    ADD COLUMN IF NOT EXISTS discovery_score FLOAT,
    ADD COLUMN IF NOT EXISTS setup_score FLOAT,
    ADD COLUMN IF NOT EXISTS composite_shadow_score FLOAT,
    ADD COLUMN IF NOT EXISTS discovery_percentile FLOAT,
    ADD COLUMN IF NOT EXISTS setup_percentile FLOAT,
    ADD COLUMN IF NOT EXISTS composite_shadow_percentile FLOAT,
    ADD COLUMN IF NOT EXISTS readiness_state TEXT,
    ADD COLUMN IF NOT EXISTS trigger_price FLOAT,
    ADD COLUMN IF NOT EXISTS invalidation_price FLOAT,
    ADD COLUMN IF NOT EXISTS target_price FLOAT,
    ADD COLUMN IF NOT EXISTS setup_risk_reward FLOAT,
    ADD COLUMN IF NOT EXISTS trigger_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS feature_quality_flag TEXT,
    ADD COLUMN IF NOT EXISTS setup_features JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS setup_warnings JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS radar_discovery_outcomes (
    snapshot_id BIGINT NOT NULL REFERENCES radar_discovery_snapshots(id) ON DELETE CASCADE,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 10, 20, 40)),
    target_session_ts TIMESTAMPTZ NOT NULL,
    outcome_price FLOAT NOT NULL,
    forward_return FLOAT NOT NULL,
    max_drawdown FLOAT,
    universe_benchmark_return FLOAT,
    qqq_benchmark_return FLOAT,
    spy_benchmark_return FLOAT,
    own_positions_benchmark_return FLOAT,
    excess_vs_universe FLOAT,
    excess_vs_qqq FLOAT,
    excess_vs_spy FLOAT,
    excess_vs_own_positions FLOAT,
    universe_sample_count INTEGER,
    outcome_basis TEXT NOT NULL DEFAULT 'canonical_cocos_sessions_v1',
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (snapshot_id, horizon_sessions)
);

CREATE TABLE IF NOT EXISTS radar_setup_events (
    snapshot_id BIGINT PRIMARY KEY
        REFERENCES radar_discovery_snapshots(id) ON DELETE CASCADE,
    setup_shadow_version TEXT NOT NULL,
    event_status TEXT NOT NULL,
    event_ts TIMESTAMPTZ,
    event_session DATE,
    event_price FLOAT,
    sessions_from_discovery INTEGER,
    trigger_price FLOAT,
    invalidation_price FLOAT,
    trigger_volume_ratio FLOAT,
    event_basis TEXT NOT NULL DEFAULT 'canonical_cocos_sessions_v1',
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS radar_setup_outcomes (
    snapshot_id BIGINT NOT NULL
        REFERENCES radar_setup_events(snapshot_id) ON DELETE CASCADE,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 10, 20, 40)),
    target_session_ts TIMESTAMPTZ NOT NULL,
    outcome_price FLOAT NOT NULL,
    forward_return FLOAT NOT NULL,
    max_drawdown FLOAT,
    qqq_benchmark_return FLOAT,
    spy_benchmark_return FLOAT,
    excess_vs_qqq FLOAT,
    excess_vs_spy FLOAT,
    outcome_basis TEXT NOT NULL DEFAULT 'canonical_cocos_trigger_sessions_v1',
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (snapshot_id, horizon_sessions)
);

CREATE INDEX IF NOT EXISTS idx_radar_discovery_runs_version_session
    ON radar_discovery_runs(scoring_version, captured_session DESC);
CREATE INDEX IF NOT EXISTS idx_radar_discovery_runs_owner_captured
    ON radar_discovery_runs(owner_chat_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_discovery_snapshots_rank
    ON radar_discovery_snapshots(run_id, radar_eligible, rank_position);
CREATE INDEX IF NOT EXISTS idx_radar_discovery_snapshots_v3
    ON radar_discovery_snapshots(run_id, v3_tier, v3_eligible);
CREATE INDEX IF NOT EXISTS idx_radar_discovery_snapshots_ticker
    ON radar_discovery_snapshots(ticker, reference_ts DESC);
CREATE INDEX IF NOT EXISTS idx_radar_discovery_snapshots_pending
    ON radar_discovery_snapshots(reference_ts, id)
    WHERE reference_ts IS NOT NULL AND reference_price > 0;
CREATE INDEX IF NOT EXISTS idx_radar_discovery_outcomes_horizon
    ON radar_discovery_outcomes(horizon_sessions, resolved_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_setup_events_status_session
    ON radar_setup_events(event_status, event_session DESC);
CREATE INDEX IF NOT EXISTS idx_radar_setup_outcomes_horizon
    ON radar_setup_outcomes(horizon_sessions, resolved_at DESC);
"""


@dataclass(frozen=True)
class RadarDiscoveryObservation:
    ticker: str
    asset_type: str
    reference_ts: datetime | None
    reference_price: float | None
    price_quality_flag: str
    data_source_mode: str
    data_source_counts: dict[str, int]
    screener_passed: bool
    radar_eligible: bool
    acceptance_reason: str | None
    rejection_reason: str | None
    v2_version: str | None
    v2_score: float | None
    v2_bias: str | None
    v2_percentile: float | None
    v3_version: str | None
    v3_score: float | None
    v3_classification: str | None
    v3_tier: str | None
    v3_percentile: float | None
    v3_eligible: bool
    technical_regime: str | None
    trend_score: float | None
    reversion_score: float | None
    setup_shadow_version: str | None
    trend_component_score: float | None
    relative_strength_component_score: float | None
    compression_component_score: float | None
    setup_component_score: float | None
    discovery_score: float | None
    setup_score: float | None
    composite_shadow_score: float | None
    discovery_percentile: float | None
    setup_percentile: float | None
    composite_shadow_percentile: float | None
    readiness_state: str | None
    trigger_price: float | None
    invalidation_price: float | None
    target_price: float | None
    setup_risk_reward: float | None
    trigger_confirmed: bool
    feature_quality_flag: str | None
    setup_features: dict[str, Any]
    setup_warnings: tuple[str, ...]
    radar_final_score: float | None
    ranking_score: float | None
    rank_position: int | None
    rank_percentile: float | None
    comparison_bucket: str
    radar_status: str | None
    trade_type: str | None
    in_portfolio: bool
    selected_top_n: bool
    metadata: dict[str, Any] = field(default_factory=dict)


def discovery_scoring_version(
    *,
    period: str = "1y",
    min_score: float = 0.10,
    min_rr: float = 0.0,
    operational_top_n: int = 6,
    no_sentiment: bool = False,
) -> str:
    """Fingerprint every source file that can alter the captured ranking."""
    root = Path(__file__).resolve().parents[2]
    digest = hashlib.sha256()
    for relative in SCORING_FINGERPRINT_FILES:
        digest.update(relative.encode("utf-8"))
        source = (root / relative).read_text(encoding="utf-8").replace("\r\n", "\n")
        digest.update(source.encode("utf-8"))
    digest.update(json.dumps({
        "period": str(period),
        "min_score": float(min_score),
        "min_rr": float(min_rr),
        "operational_top_n": int(operational_top_n),
        "no_sentiment": bool(no_sentiment),
    }, sort_keys=True).encode("utf-8"))
    return (
        f"radar-v2+{TECHNICAL_SHADOW_V2_VERSION}+"
        f"{TECHNICAL_BUY_SHADOW_V3_VERSION}:{digest.hexdigest()[:16]}"
    )


def build_discovery_observations(
    report: OpportunityReport,
    *,
    universe: Sequence[str],
    history_frames: Mapping[str, Any],
    asset_types: Mapping[str, str],
    portfolio_tickers: Iterable[str],
    selected_tickers: Iterable[str],
    min_score: float,
    min_rr: float,
    manual_event_risk_by_ticker: Mapping[str, str] | None = None,
) -> list[RadarDiscoveryObservation]:
    """Build one immutable observation per requested universe ticker."""
    tickers = list(dict.fromkeys(str(value).upper().strip() for value in universe if value))
    held = {str(value).upper().strip() for value in portfolio_tickers if value}
    selected = {str(value).upper().strip() for value in selected_tickers if value}
    manual_risks = {
        str(ticker).upper(): str(reason)
        for ticker, reason in dict(manual_event_risk_by_ticker or {}).items()
    }
    screen_map = {
        str(item.ticker).upper(): item
        for item in report.discovery_screening_results
    }
    tech_map = {
        str(ticker).upper(): signal
        for ticker, signal in report.discovery_technical_signals.items()
    }
    scored_map = {
        str(item.ticker).upper(): item
        for item in report.discovery_scored_candidates
    }
    ranked = list(report.discovery_ranked_candidates)
    ranked_map = {str(item.ticker).upper(): item for item in ranked}
    rank_by_ticker = {
        str(item.ticker).upper(): position
        for position, item in enumerate(ranked, start=1)
    }
    rank_count = len(ranked)
    setup_shadow_map = build_radar_setup_shadow_universe(
        tickers=tickers,
        history_frames=history_frames,
        asset_types=asset_types,
        screening_metrics=screen_map,
        candidates=scored_map,
    )

    drafts: list[dict[str, Any]] = []
    for ticker in tickers:
        frame = history_frames.get(ticker)
        reference_ts, reference_price = _frame_reference(frame)
        screen = screen_map.get(ticker)
        tech = tech_map.get(ticker)
        candidate = scored_map.get(ticker)
        ranked_candidate = ranked_map.get(ticker)
        setup_shadow = setup_shadow_map.get(ticker)

        v2 = dict(getattr(tech, "technical_shadow_v2", {}) or {})
        v3: dict[str, Any] = {}
        if tech is not None:
            base_v3 = dict(getattr(tech, "technical_buy_shadow_v3", {}) or {})
            v3 = build_technical_buy_shadow_v3(
                technical_shadow_v2=v2,
                regime=str(getattr(tech, "technical_regime", "TRANSITIONAL") or "TRANSITIONAL"),
                trend_score=float(getattr(tech, "trend_score", 0.0) or 0.0),
                reversion_score=float(getattr(tech, "reversion_score", 0.0) or 0.0),
                structural_break_confirmed=bool(
                    getattr(tech, "structural_break_confirmed", False)
                ),
                asset_type=str(asset_types.get(ticker, "UNKNOWN") or "UNKNOWN"),
                source_mode=str(getattr(tech, "candle_source_mode", "unknown") or "unknown"),
                volume_quality_20=_optional_float(base_v3.get("volume_quality_20")),
            ).to_dict()

        rank_position = rank_by_ticker.get(ticker)
        radar_eligible = _is_radar_eligible(
            candidate,
            min_score=min_score,
            min_rr=min_rr,
        )
        acceptance_reason, rejection_reason = _selection_reasons(
            ticker=ticker,
            screen=screen,
            candidate=candidate,
            radar_eligible=radar_eligible,
            min_score=min_score,
            min_rr=min_rr,
        )
        source_mode, source_counts, quality = _price_quality(
            frame,
            tech,
            captured_at=report.generated_at,
        )
        drafts.append({
            "ticker": ticker,
            "asset_type": str(asset_types.get(ticker, "UNKNOWN") or "UNKNOWN").upper(),
            "reference_ts": reference_ts,
            "reference_price": reference_price,
            "price_quality_flag": quality,
            "data_source_mode": source_mode,
            "data_source_counts": source_counts,
            "screener_passed": bool(getattr(screen, "passes_screen", False)),
            "radar_eligible": radar_eligible,
            "acceptance_reason": acceptance_reason,
            "rejection_reason": rejection_reason,
            "v2_version": str(v2.get("version") or "") or None,
            "v2_score": _optional_float(v2.get("score")),
            "v2_bias": str(v2.get("bias") or "") or None,
            "v3_version": str(v3.get("version") or "") or None,
            "v3_score": _optional_float(v3.get("source_score")),
            "v3_classification": str(v3.get("classification") or "") or None,
            "v3_tier": str(v3.get("priority_tier") or "") or None,
            "v3_eligible": bool(v3.get("eligible_for_buy_research", False)),
            "technical_regime": (
                str(getattr(tech, "technical_regime", "") or "") or None
            ),
            "trend_score": _optional_float(getattr(tech, "trend_score", None)),
            "reversion_score": _optional_float(getattr(tech, "reversion_score", None)),
            "setup_shadow_version": (
                str(getattr(setup_shadow, "version", "") or "") or None
            ),
            "trend_component_score": _optional_float(
                getattr(setup_shadow, "trend_component_score", None)
            ),
            "relative_strength_component_score": _optional_float(
                getattr(setup_shadow, "relative_strength_component_score", None)
            ),
            "compression_component_score": _optional_float(
                getattr(setup_shadow, "compression_component_score", None)
            ),
            "setup_component_score": _optional_float(
                getattr(setup_shadow, "setup_component_score", None)
            ),
            "discovery_score": _optional_float(
                getattr(setup_shadow, "discovery_score", None)
            ),
            "setup_score": _optional_float(
                getattr(setup_shadow, "setup_score", None)
            ),
            "composite_shadow_score": _optional_float(
                getattr(setup_shadow, "composite_score", None)
            ),
            "discovery_percentile": _optional_float(
                getattr(setup_shadow, "discovery_percentile", None)
            ),
            "setup_percentile": _optional_float(
                getattr(setup_shadow, "setup_percentile", None)
            ),
            "composite_shadow_percentile": _optional_float(
                getattr(setup_shadow, "composite_percentile", None)
            ),
            "readiness_state": (
                str(getattr(setup_shadow, "readiness_state", "") or "") or None
            ),
            "trigger_price": _optional_float(
                getattr(setup_shadow, "trigger_price", None)
            ),
            "invalidation_price": _optional_float(
                getattr(setup_shadow, "invalidation_price", None)
            ),
            "target_price": _optional_float(
                getattr(setup_shadow, "target_price", None)
            ),
            "setup_risk_reward": _optional_float(
                getattr(setup_shadow, "risk_reward", None)
            ),
            "trigger_confirmed": bool(
                getattr(setup_shadow, "trigger_confirmed", False)
            ),
            "feature_quality_flag": (
                str(getattr(setup_shadow, "feature_quality_flag", "") or "") or None
            ),
            "setup_features": dict(
                getattr(setup_shadow, "features", {}) or {}
            ),
            "setup_warnings": tuple(
                getattr(setup_shadow, "warnings", ()) or ()
            ),
            "radar_final_score": _optional_float(getattr(candidate, "final_score", None)),
            "ranking_score": (
                float(opportunity_rank_score(ranked_candidate))
                if ranked_candidate is not None else None
            ),
            "rank_position": rank_position,
            "rank_percentile": _rank_percentile(rank_position, rank_count),
            "comparison_bucket": _comparison_bucket(rank_position),
            "radar_status": _enum_value(getattr(candidate, "status", None)) or None,
            "trade_type": _enum_value(getattr(candidate, "trade_type", None)) or None,
            "in_portfolio": ticker in held,
            "selected_top_n": ticker in selected,
            "metadata": {
                "screener_fail_reason": str(getattr(screen, "fail_reason", "") or ""),
                "rank_scope": "all_scored_screen_passers",
                "v3_score_basis": "technical_shadow_v2_source_score",
                "is_benchmark": ticker in BENCHMARK_TICKERS,
                "manual_event_risk": manual_risks.get(ticker),
                "reference_age_days": _reference_age_days(
                    reference_ts,
                    report.generated_at,
                ),
                "v3_gates": list(v3.get("gates") or []),
                "v3_warnings": list(v3.get("warnings") or []),
                "why_not_now": str(getattr(candidate, "why_not_now", "") or ""),
                "action": str(getattr(candidate, "action_concreta", "") or ""),
                "radar_setup_protocol": {
                    "version": RADAR_SETUP_SHADOW_VERSION,
                    "trigger_window_sessions": RADAR_SETUP_TRIGGER_WINDOW_SESSIONS,
                    "promotion_eligible": False,
                    "affects_radar_ranking": False,
                    "affects_analysis": False,
                    "affects_execution": False,
                },
            },
        })

    v2_percentiles = _percentiles(drafts, "v2_score")
    v3_percentiles = _percentiles(drafts, "v3_score")
    return [
        RadarDiscoveryObservation(
            **draft,
            v2_percentile=v2_percentiles.get(draft["ticker"]),
            v3_percentile=v3_percentiles.get(draft["ticker"]),
        )
        for draft in drafts
    ]


class RadarDiscoveryStore:
    def __init__(self, pool: Any):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(RADAR_DISCOVERY_SCHEMA_SQL)

    async def save_snapshot(
        self,
        *,
        run_id: str | UUID,
        owner_chat_id: int | None,
        captured_at: datetime,
        scoring_version: str,
        observations: Sequence[RadarDiscoveryObservation],
        parameters: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not observations:
            return {"run_id": str(run_id), "inserted": 0, "duplicate": False}
        await self.ensure_schema()
        normalized_run_id = UUID(str(run_id))
        captured = _aware_datetime(captured_at)
        session = captured.astimezone(ART_TZ).date()
        owner = int(owner_chat_id or 0)
        controls = sum(row.ticker in BENCHMARK_TICKERS for row in observations)
        universe_count = len(observations) - controls
        evaluated = sum(
            row.reference_price is not None and row.ticker not in BENCHMARK_TICKERS
            for row in observations
        )
        eligible = sum(row.radar_eligible for row in observations)
        selected = sum(row.selected_top_n for row in observations)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                inserted_run_id = await conn.fetchval(
                    """
                    INSERT INTO radar_discovery_runs (
                        run_id, owner_chat_id, captured_at, captured_session,
                        scoring_version, protocol_version, universe_count, control_count,
                        evaluated_count, eligible_count, selected_count, parameters
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
                    ON CONFLICT (owner_chat_id, captured_session, scoring_version)
                    DO NOTHING
                    RETURNING run_id
                    """,
                    normalized_run_id,
                    owner,
                    captured,
                    session,
                    scoring_version,
                    RADAR_DISCOVERY_PROTOCOL_VERSION,
                    universe_count,
                    controls,
                    evaluated,
                    eligible,
                    selected,
                    json.dumps(dict(parameters), default=str),
                )
                if inserted_run_id is None:
                    existing = await conn.fetchval(
                        """
                        SELECT run_id
                        FROM radar_discovery_runs
                        WHERE owner_chat_id = $1
                          AND captured_session = $2
                          AND scoring_version = $3
                        """,
                        owner,
                        session,
                        scoring_version,
                    )
                    return {
                        "run_id": str(existing),
                        "inserted": 0,
                        "duplicate": True,
                    }

                for row in observations:
                    await conn.execute(
                        """
                        INSERT INTO radar_discovery_snapshots (
                            run_id, ticker, asset_type, reference_ts, reference_price,
                            price_quality_flag, data_source_mode, data_source_counts,
                            screener_passed, radar_eligible, acceptance_reason,
                            rejection_reason, v2_version, v2_score, v2_bias,
                            v2_percentile, v3_version, v3_score, v3_classification,
                            v3_tier, v3_percentile, v3_eligible, technical_regime,
                            trend_score, reversion_score, setup_shadow_version,
                            trend_component_score, relative_strength_component_score,
                            compression_component_score, setup_component_score,
                            discovery_score, setup_score, composite_shadow_score,
                            discovery_percentile, setup_percentile,
                            composite_shadow_percentile, readiness_state,
                            trigger_price, invalidation_price, target_price,
                            setup_risk_reward, trigger_confirmed,
                            feature_quality_flag, setup_features, setup_warnings,
                            radar_final_score, ranking_score, rank_position,
                            rank_percentile, comparison_bucket, radar_status,
                            trade_type, in_portfolio, selected_top_n, metadata
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,
                            $13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,
                            $25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,
                            $37,$38,$39,$40,$41,$42,$43,$44::jsonb,$45::jsonb,
                            $46,$47,$48,$49,$50,$51,$52,$53,$54,$55::jsonb
                        )
                        """,
                        normalized_run_id,
                        row.ticker,
                        row.asset_type,
                        row.reference_ts,
                        row.reference_price,
                        row.price_quality_flag,
                        row.data_source_mode,
                        json.dumps(row.data_source_counts),
                        row.screener_passed,
                        row.radar_eligible,
                        row.acceptance_reason,
                        row.rejection_reason,
                        row.v2_version,
                        row.v2_score,
                        row.v2_bias,
                        row.v2_percentile,
                        row.v3_version,
                        row.v3_score,
                        row.v3_classification,
                        row.v3_tier,
                        row.v3_percentile,
                        row.v3_eligible,
                        row.technical_regime,
                        row.trend_score,
                        row.reversion_score,
                        row.setup_shadow_version,
                        row.trend_component_score,
                        row.relative_strength_component_score,
                        row.compression_component_score,
                        row.setup_component_score,
                        row.discovery_score,
                        row.setup_score,
                        row.composite_shadow_score,
                        row.discovery_percentile,
                        row.setup_percentile,
                        row.composite_shadow_percentile,
                        row.readiness_state,
                        row.trigger_price,
                        row.invalidation_price,
                        row.target_price,
                        row.setup_risk_reward,
                        row.trigger_confirmed,
                        row.feature_quality_flag,
                        json.dumps(row.setup_features, default=str),
                        json.dumps(list(row.setup_warnings), default=str),
                        row.radar_final_score,
                        row.ranking_score,
                        row.rank_position,
                        row.rank_percentile,
                        row.comparison_bucket,
                        row.radar_status,
                        row.trade_type,
                        row.in_portfolio,
                        row.selected_top_n,
                        json.dumps(row.metadata, default=str),
                    )
        return {
            "run_id": str(normalized_run_id),
            "inserted": len(observations),
            "duplicate": False,
        }

    async def resolve_pending_outcomes(
        self,
        db: Any,
        *,
        owner_chat_id: int | None = None,
        limit: int = 50000,
    ) -> int:
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.run_id, s.ticker, s.asset_type,
                       s.reference_ts, s.reference_price,
                       h.horizon_sessions
                FROM radar_discovery_snapshots s
                JOIN radar_discovery_runs r ON r.run_id = s.run_id
                CROSS JOIN (VALUES (5), (10), (20), (40)) AS h(horizon_sessions)
                LEFT JOIN radar_discovery_outcomes o
                  ON o.snapshot_id = s.id
                 AND o.horizon_sessions = h.horizon_sessions
                WHERE o.snapshot_id IS NULL
                  AND s.reference_ts IS NOT NULL
                  AND s.reference_price > 0
                  AND s.reference_ts <= NOW() - (h.horizon_sessions * INTERVAL '1 day')
                  AND ($1::bigint IS NULL OR r.owner_chat_id = $1)
                ORDER BY s.reference_ts, s.id, h.horizon_sessions
                LIMIT $2
                """,
                owner_chat_id,
                int(limit),
            )
        if not rows:
            return 0

        tickers = sorted({str(row["ticker"]).upper() for row in rows})
        all_effects = await db.get_corporate_action_effects(tickers=tickers)
        effects_by_ticker: dict[str, list[Any]] = {ticker: [] for ticker in tickers}
        for effect in all_effects:
            ticker = str(getattr(effect, "ticker", "") or "").upper()
            if ticker:
                effects_by_ticker.setdefault(ticker, []).append(effect)
        candles_by_instrument: dict[tuple[str, str], list[dict[str, Any]]] = {}
        affected: set[tuple[UUID, int]] = set()
        measurements: list[tuple[dict[str, Any], dict[str, Any], float]] = []
        resolved = 0
        for raw in rows:
            row = dict(raw)
            ticker = str(row["ticker"]).upper()
            asset_type = str(row.get("asset_type") or "UNKNOWN").upper()
            instrument_key = (ticker, asset_type)
            effects = [
                effect
                for effect in effects_by_ticker.get(ticker, [])
                if asset_type == "UNKNOWN"
                or str(getattr(effect, "asset_type", "") or "").upper() == asset_type
            ]
            if instrument_key not in candles_by_instrument:
                candles = await db.get_market_candles(
                    ticker,
                    asset_type=asset_type if asset_type != "UNKNOWN" else None,
                    limit=520,
                )
                candles_by_instrument[instrument_key] = normalize_candle_rows(
                    candles,
                    effects,
                )
            candles = candles_by_instrument[instrument_key]
            if not candles:
                continue
            latest_ts = candles[-1]["ts"]
            adjusted_reference, factor = rebase_reference_price(
                float(row["reference_price"]),
                reference_at=row["reference_ts"],
                as_of=latest_ts,
                effects=effects,
            )
            measurement = measure_discovery_outcome(
                as_of_ts=row["reference_ts"],
                reference_price=float(adjusted_reference or row["reference_price"]),
                horizon_sessions=int(row["horizon_sessions"]),
                future_candles=candles,
            )
            if measurement is None:
                continue
            measurements.append((row, measurement, factor))

        if not measurements:
            return 0

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for row, measurement, factor in measurements:
                    inserted = await conn.fetchval(
                        """
                        INSERT INTO radar_discovery_outcomes (
                            snapshot_id, horizon_sessions, target_session_ts,
                            outcome_price, forward_return, max_drawdown, metadata
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                        ON CONFLICT (snapshot_id, horizon_sessions) DO NOTHING
                        RETURNING snapshot_id
                        """,
                        int(row["id"]),
                        int(row["horizon_sessions"]),
                        measurement["target_session_ts"],
                        measurement["outcome_price"],
                        measurement["forward_return"],
                        measurement["max_drawdown"],
                        json.dumps({
                            "corporate_action_factor": factor,
                            "drawdown_basis": "session_close_from_running_peak",
                        }),
                    )
                    if inserted is not None:
                        resolved += 1
                        affected.add((
                            UUID(str(row["run_id"])),
                            int(row["horizon_sessions"]),
                        ))

        for run_id, horizon in affected:
            await self._refresh_benchmarks(run_id=run_id, horizon_sessions=horizon)
        return resolved

    async def resolve_pending_setup_events(
        self,
        db: Any,
        *,
        owner_chat_id: int | None = None,
        limit: int = 50000,
    ) -> int:
        """Resolve trigger, invalidation or expiry after each discovery snapshot."""
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.ticker, s.asset_type, s.reference_ts,
                       s.reference_price, s.setup_shadow_version,
                       s.readiness_state, s.trigger_price,
                       s.invalidation_price
                FROM radar_discovery_snapshots s
                JOIN radar_discovery_runs r ON r.run_id = s.run_id
                LEFT JOIN radar_setup_events e ON e.snapshot_id = s.id
                WHERE e.snapshot_id IS NULL
                  AND s.setup_shadow_version IS NOT NULL
                  AND s.feature_quality_flag IN ('GOOD', 'PARTIAL')
                  AND s.reference_ts IS NOT NULL
                  AND s.reference_price > 0
                  AND s.trigger_price > 0
                  AND s.invalidation_price > 0
                  AND ($1::bigint IS NULL OR r.owner_chat_id = $1)
                ORDER BY s.reference_ts, s.id
                LIMIT $2
                """,
                owner_chat_id,
                int(limit),
            )
        if not rows:
            return 0

        tickers = sorted({str(row["ticker"]).upper() for row in rows})
        all_effects = await db.get_corporate_action_effects(tickers=tickers)
        effects_by_ticker: dict[str, list[Any]] = {ticker: [] for ticker in tickers}
        for effect in all_effects:
            ticker = str(getattr(effect, "ticker", "") or "").upper()
            if ticker:
                effects_by_ticker.setdefault(ticker, []).append(effect)
        candle_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        resolutions: list[tuple[dict[str, Any], dict[str, Any], float, datetime]] = []

        for raw in rows:
            row = dict(raw)
            ticker = str(row["ticker"]).upper()
            asset_type = str(row.get("asset_type") or "UNKNOWN").upper()
            key = (ticker, asset_type)
            effects = [
                effect for effect in effects_by_ticker.get(ticker, [])
                if asset_type == "UNKNOWN"
                or str(getattr(effect, "asset_type", "") or "").upper() == asset_type
            ]
            if key not in candle_cache:
                candles = await db.get_market_candles(
                    ticker,
                    asset_type=asset_type if asset_type != "UNKNOWN" else None,
                    limit=520,
                )
                candle_cache[key] = normalize_candle_rows(candles, effects)
            candles = candle_cache[key]
            basis_as_of = candles[-1]["ts"] if candles else row["reference_ts"]
            adjusted_reference, factor = rebase_reference_price(
                float(row["reference_price"]),
                reference_at=row["reference_ts"],
                as_of=basis_as_of,
                effects=effects,
            )
            adjusted_trigger, _ = rebase_reference_price(
                float(row["trigger_price"]),
                reference_at=row["reference_ts"],
                as_of=basis_as_of,
                effects=effects,
            )
            adjusted_invalidation, _ = rebase_reference_price(
                float(row["invalidation_price"]),
                reference_at=row["reference_ts"],
                as_of=basis_as_of,
                effects=effects,
            )
            resolution = resolve_setup_event(
                reference_ts=row["reference_ts"],
                reference_price=float(adjusted_reference or row["reference_price"]),
                readiness_state=str(row.get("readiness_state") or ""),
                trigger_price=float(adjusted_trigger or row["trigger_price"]),
                invalidation_price=float(
                    adjusted_invalidation or row["invalidation_price"]
                ),
                candles=candles,
            )
            if resolution is not None:
                resolutions.append((row, resolution, factor, _aware_datetime(basis_as_of)))

        if not resolutions:
            return 0
        inserted_count = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for row, resolution, factor, basis_as_of in resolutions:
                    event_ts = resolution.get("event_ts")
                    event_session = (
                        _aware_datetime(event_ts).astimezone(ART_TZ).date()
                        if event_ts is not None else None
                    )
                    inserted = await conn.fetchval(
                        """
                        INSERT INTO radar_setup_events (
                            snapshot_id, setup_shadow_version, event_status,
                            event_ts, event_session, event_price,
                            sessions_from_discovery, trigger_price,
                            invalidation_price, trigger_volume_ratio, metadata
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
                        ON CONFLICT (snapshot_id) DO NOTHING
                        RETURNING snapshot_id
                        """,
                        int(row["id"]),
                        str(row["setup_shadow_version"]),
                        str(resolution["event_status"]),
                        event_ts,
                        event_session,
                        resolution.get("event_price"),
                        resolution.get("sessions_from_discovery"),
                        float(row["trigger_price"]) * factor,
                        float(row["invalidation_price"]) * factor,
                        resolution.get("trigger_volume_ratio"),
                        json.dumps({
                            "trigger_window_sessions": RADAR_SETUP_TRIGGER_WINDOW_SESSIONS,
                            "corporate_action_factor_at_resolution": factor,
                            "price_basis_as_of": basis_as_of.isoformat(),
                            "same_session_high_low_order": "unknown",
                        }),
                    )
                    if inserted is not None:
                        inserted_count += 1
        return inserted_count

    async def resolve_pending_setup_outcomes(
        self,
        db: Any,
        *,
        owner_chat_id: int | None = None,
        limit: int = 50000,
    ) -> int:
        """Measure returns from a clean setup trigger, separate from discovery time."""
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.snapshot_id, e.event_ts, e.event_price, e.metadata,
                       s.ticker, s.asset_type, h.horizon_sessions
                FROM radar_setup_events e
                JOIN radar_discovery_snapshots s ON s.id = e.snapshot_id
                JOIN radar_discovery_runs r ON r.run_id = s.run_id
                CROSS JOIN (VALUES (5), (10), (20), (40)) AS h(horizon_sessions)
                LEFT JOIN radar_setup_outcomes o
                  ON o.snapshot_id = e.snapshot_id
                 AND o.horizon_sessions = h.horizon_sessions
                WHERE e.event_status IN (
                    'TRIGGERED_AT_CAPTURE', 'TRIGGERED_AFTER_DISCOVERY'
                )
                  AND e.event_ts IS NOT NULL
                  AND e.event_price > 0
                  AND o.snapshot_id IS NULL
                  AND e.event_ts <= NOW() - (h.horizon_sessions * INTERVAL '1 day')
                  AND ($1::bigint IS NULL OR r.owner_chat_id = $1)
                ORDER BY e.event_ts, e.snapshot_id, h.horizon_sessions
                LIMIT $2
                """,
                owner_chat_id,
                int(limit),
            )
        if not rows:
            return 0

        candidate_tickers = sorted({str(row["ticker"]).upper() for row in rows})
        effect_tickers = sorted(set(candidate_tickers) | BENCHMARK_TICKERS)
        all_effects = await db.get_corporate_action_effects(tickers=effect_tickers)
        effects_by_ticker: dict[str, list[Any]] = {ticker: [] for ticker in effect_tickers}
        for effect in all_effects:
            ticker = str(getattr(effect, "ticker", "") or "").upper()
            if ticker:
                effects_by_ticker.setdefault(ticker, []).append(effect)
        candle_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}

        async def _candles(ticker: str, asset_type: str = "UNKNOWN"):
            key = (ticker, asset_type)
            if key not in candle_cache:
                effects = effects_by_ticker.get(ticker, [])
                raw_candles = await db.get_market_candles(
                    ticker,
                    asset_type=asset_type if asset_type != "UNKNOWN" else None,
                    limit=520,
                )
                candle_cache[key] = normalize_candle_rows(raw_candles, effects)
            return candle_cache[key]

        measurements: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for raw in rows:
            row = dict(raw)
            ticker = str(row["ticker"]).upper()
            asset_type = str(row.get("asset_type") or "UNKNOWN").upper()
            candles = await _candles(ticker, asset_type)
            if not candles:
                continue
            metadata = dict(row.get("metadata") or {})
            basis_as_of = _coerce_datetime(
                metadata.get("price_basis_as_of"),
                fallback=row["event_ts"],
            )
            latest_ts = candles[-1]["ts"]
            adjusted_event_price, factor = rebase_reference_price(
                float(row["event_price"]),
                reference_at=basis_as_of,
                as_of=latest_ts,
                effects=effects_by_ticker.get(ticker, []),
            )
            measurement = measure_discovery_outcome(
                as_of_ts=row["event_ts"],
                reference_price=float(adjusted_event_price or row["event_price"]),
                horizon_sessions=int(row["horizon_sessions"]),
                future_candles=candles,
            )
            if measurement is None:
                continue
            benchmark_measurements: dict[str, Any] = {}
            for benchmark in sorted(BENCHMARK_TICKERS):
                benchmark_candles = await _candles(benchmark)
                benchmark_measurements[benchmark] = _measure_benchmark_from_event(
                    candles=benchmark_candles,
                    event_ts=row["event_ts"],
                    horizon_sessions=int(row["horizon_sessions"]),
                )
            measurements.append((
                row,
                measurement,
                {
                    "factor": factor,
                    "benchmarks": benchmark_measurements,
                },
            ))

        if not measurements:
            return 0
        inserted_count = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for row, measurement, context in measurements:
                    qqq = context["benchmarks"].get("QQQ")
                    spy = context["benchmarks"].get("SPY")
                    qqq_return = qqq.get("forward_return") if qqq else None
                    spy_return = spy.get("forward_return") if spy else None
                    inserted = await conn.fetchval(
                        """
                        INSERT INTO radar_setup_outcomes (
                            snapshot_id, horizon_sessions, target_session_ts,
                            outcome_price, forward_return, max_drawdown,
                            qqq_benchmark_return, spy_benchmark_return,
                            excess_vs_qqq, excess_vs_spy, metadata
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
                        ON CONFLICT (snapshot_id, horizon_sessions) DO NOTHING
                        RETURNING snapshot_id
                        """,
                        int(row["snapshot_id"]),
                        int(row["horizon_sessions"]),
                        measurement["target_session_ts"],
                        measurement["outcome_price"],
                        measurement["forward_return"],
                        measurement["max_drawdown"],
                        qqq_return,
                        spy_return,
                        (
                            measurement["forward_return"] - qqq_return
                            if qqq_return is not None else None
                        ),
                        (
                            measurement["forward_return"] - spy_return
                            if spy_return is not None else None
                        ),
                        json.dumps({
                            "corporate_action_factor": context["factor"],
                            "anchor": "setup_trigger",
                            "benchmark_basis": "local_same_session",
                        }),
                    )
                    if inserted is not None:
                        inserted_count += 1
        return inserted_count

    async def _refresh_benchmarks(self, *, run_id: UUID, horizon_sessions: int) -> None:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.id, s.ticker, s.in_portfolio, s.rejection_reason,
                       s.price_quality_flag,
                       o.forward_return
                FROM radar_discovery_snapshots s
                JOIN radar_discovery_outcomes o ON o.snapshot_id = s.id
                WHERE s.run_id = $1 AND o.horizon_sessions = $2
                """,
                run_id,
                int(horizon_sessions),
            )
        values = [dict(row) for row in rows]
        universe = [
            float(row["forward_return"])
            for row in values
            if row.get("rejection_reason") != "benchmark_only"
            and _quality_is_comparable(row.get("price_quality_flag"))
        ]
        universe_return = median(universe) if universe else None
        qqq_return = _ticker_return(values, "QQQ")
        spy_return = _ticker_return(values, "SPY")
        own = [
            float(row["forward_return"])
            for row in values
            if row.get("in_portfolio")
            and _quality_is_comparable(row.get("price_quality_flag"))
        ]
        own_return = fmean(own) if own else None

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE radar_discovery_outcomes o
                SET universe_benchmark_return = $3,
                    qqq_benchmark_return = $4,
                    spy_benchmark_return = $5,
                    own_positions_benchmark_return = $6,
                    excess_vs_universe = o.forward_return - $3,
                    excess_vs_qqq = CASE WHEN $4::float IS NULL THEN NULL ELSE o.forward_return - $4 END,
                    excess_vs_spy = CASE WHEN $5::float IS NULL THEN NULL ELSE o.forward_return - $5 END,
                    excess_vs_own_positions = CASE WHEN $6::float IS NULL THEN NULL ELSE o.forward_return - $6 END,
                    universe_sample_count = $7
                FROM radar_discovery_snapshots s
                WHERE o.snapshot_id = s.id
                  AND s.run_id = $1
                  AND o.horizon_sessions = $2
                """,
                run_id,
                int(horizon_sessions),
                universe_return,
                qqq_return,
                spy_return,
                own_return,
                len(universe),
            )

    async def latest_scoring_version(self, *, owner_chat_id: int | None = None) -> str | None:
        owner = int(owner_chat_id or 0)
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT scoring_version
                FROM radar_discovery_runs
                WHERE owner_chat_id = $1
                ORDER BY captured_at DESC
                LIMIT 1
                """,
                owner,
            )
        return str(value) if value else None

    async def comparison_rows(
        self,
        *,
        scoring_version: str,
        horizon_sessions: int,
        owner_chat_id: int | None = None,
        since: date | None = None,
    ) -> list[dict[str, Any]]:
        if not scoring_version:
            raise ValueError("scoring_version is required; mixed versions are forbidden")
        if int(horizon_sessions) not in RADAR_DISCOVERY_HORIZONS:
            raise ValueError("horizon_sessions must be one of 5, 10, 20, 40")
        owner = int(owner_chat_id or 0)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.captured_session, r.scoring_version, s.ticker,
                       s.radar_eligible, s.v3_tier, s.v3_eligible,
                       s.rank_position, s.rank_percentile, s.comparison_bucket,
                       s.setup_shadow_version, s.trend_component_score,
                       s.relative_strength_component_score,
                       s.compression_component_score, s.setup_component_score,
                       s.discovery_score, s.setup_score,
                       s.composite_shadow_score, s.discovery_percentile,
                       s.setup_percentile, s.composite_shadow_percentile,
                       s.readiness_state, s.trigger_confirmed,
                       s.feature_quality_flag, s.setup_risk_reward,
                       s.in_portfolio, s.selected_top_n, s.price_quality_flag,
                       s.data_source_mode, s.rejection_reason, o.forward_return,
                       o.max_drawdown, o.excess_vs_universe, o.excess_vs_qqq,
                       o.excess_vs_spy, o.excess_vs_own_positions
                FROM radar_discovery_runs r
                JOIN radar_discovery_snapshots s ON s.run_id = r.run_id
                JOIN radar_discovery_outcomes o ON o.snapshot_id = s.id
                WHERE r.scoring_version = $1
                  AND o.horizon_sessions = $2
                  AND r.owner_chat_id = $3
                  AND ($4::date IS NULL OR r.captured_session >= $4)
                ORDER BY r.captured_session, s.rank_position NULLS LAST, s.ticker
                """,
                scoring_version,
                int(horizon_sessions),
                owner,
                since,
            )
        return [dict(row) for row in rows]

    async def setup_comparison_rows(
        self,
        *,
        scoring_version: str,
        horizon_sessions: int,
        owner_chat_id: int | None = None,
        since: date | None = None,
    ) -> list[dict[str, Any]]:
        """Read-only outcomes anchored at a setup trigger rather than discovery."""
        if not scoring_version:
            raise ValueError("scoring_version is required; mixed versions are forbidden")
        if int(horizon_sessions) not in RADAR_DISCOVERY_HORIZONS:
            raise ValueError("horizon_sessions must be one of 5, 10, 20, 40")
        owner = int(owner_chat_id or 0)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.captured_session, r.scoring_version, s.ticker,
                       s.radar_eligible, s.v3_tier, s.v3_eligible,
                       s.rank_position, s.rank_percentile, s.comparison_bucket,
                       s.setup_shadow_version, s.trend_component_score,
                       s.relative_strength_component_score,
                       s.compression_component_score, s.setup_component_score,
                       s.discovery_score, s.setup_score,
                       s.composite_shadow_score, s.discovery_percentile,
                       s.setup_percentile, s.composite_shadow_percentile,
                       s.readiness_state, s.trigger_confirmed,
                       s.feature_quality_flag, s.setup_risk_reward,
                       s.in_portfolio, s.selected_top_n, s.price_quality_flag,
                       s.data_source_mode, s.rejection_reason,
                       e.event_status, e.event_session,
                       e.sessions_from_discovery,
                       o.forward_return, o.max_drawdown,
                       NULL::float AS excess_vs_universe,
                       o.excess_vs_qqq, o.excess_vs_spy,
                       NULL::float AS excess_vs_own_positions
                FROM radar_discovery_runs r
                JOIN radar_discovery_snapshots s ON s.run_id = r.run_id
                JOIN radar_setup_events e ON e.snapshot_id = s.id
                JOIN radar_setup_outcomes o ON o.snapshot_id = s.id
                WHERE r.scoring_version = $1
                  AND o.horizon_sessions = $2
                  AND r.owner_chat_id = $3
                  AND ($4::date IS NULL OR r.captured_session >= $4)
                  AND e.event_status IN (
                      'TRIGGERED_AT_CAPTURE', 'TRIGGERED_AFTER_DISCOVERY'
                  )
                ORDER BY e.event_session, s.ticker
                """,
                scoring_version,
                int(horizon_sessions),
                owner,
                since,
            )
        return [dict(row) for row in rows]


def summarize_comparisons(
    rows: Sequence[Mapping[str, Any]],
    *,
    cost_bps: int = DEFAULT_THEORETICAL_COST_BPS,
) -> dict[str, Any]:
    """Aggregate fixed-version evidence; Spearman IC is secondary diagnostics."""
    resolved = [dict(row) for row in rows if row.get("forward_return") is not None]
    controls = [
        row for row in resolved
        if row.get("rejection_reason") == "benchmark_only"
    ]
    quality_excluded = [
        row for row in resolved
        if row.get("rejection_reason") != "benchmark_only"
        and not _quality_is_comparable(row.get("price_quality_flag"))
    ]
    normalized = [
        row for row in resolved
        if row.get("rejection_reason") != "benchmark_only"
        and _quality_is_comparable(row.get("price_quality_flag"))
    ]
    cohorts = {
        "all_universe": normalized,
        "top_5": [row for row in normalized if row.get("comparison_bucket") == "TOP_5"],
        "rank_6_20": [
            row for row in normalized if row.get("comparison_bucket") == "RANK_6_20"
        ],
        "rank_21_50": [
            row for row in normalized if row.get("comparison_bucket") == "RANK_21_50"
        ],
        "rest": [row for row in normalized if row.get("comparison_bucket") == "REST"],
        "all_eligible": [row for row in normalized if row.get("radar_eligible")],
        "eligible_outside_top_5": [
            row for row in normalized
            if row.get("radar_eligible") and row.get("comparison_bucket") != "TOP_5"
        ],
        "v3_tier_a": [row for row in normalized if str(row.get("v3_tier") or "") == "A"],
        "v3_rejected": [
            row for row in normalized
            if str(row.get("v3_tier") or "") == "REJECTED"
        ],
        "selected_top_n": [row for row in normalized if row.get("selected_top_n")],
        "own_positions": [row for row in normalized if row.get("in_portfolio")],
        "feature_quality_good": [
            row for row in normalized if row.get("feature_quality_flag") == "GOOD"
        ],
        "feature_quality_partial": [
            row for row in normalized if row.get("feature_quality_flag") == "PARTIAL"
        ],
        "discovery_top_quintile": [
            row for row in normalized
            if _at_least(row.get("discovery_percentile"), 0.80)
        ],
        "setup_top_quintile": [
            row for row in normalized
            if _at_least(row.get("setup_percentile"), 0.80)
        ],
        "trend_and_rs_high": [
            row for row in normalized
            if _at_least(row.get("trend_component_score"), 20.0)
            and _at_least(row.get("relative_strength_component_score"), 20.0)
        ],
        "compression_and_rs_high": [
            row for row in normalized
            if _at_least(row.get("compression_component_score"), 20.0)
            and _at_least(row.get("relative_strength_component_score"), 20.0)
        ],
        "pre_breakout": [
            row for row in normalized if row.get("readiness_state") == "PRE_BREAKOUT"
        ],
        "triggered": [
            row for row in normalized if row.get("readiness_state") == "TRIGGERED"
        ],
        "extended": [
            row for row in normalized if row.get("readiness_state") == "EXTENDED"
        ],
        "triggered_at_capture": [
            row for row in normalized
            if row.get("event_status") == "TRIGGERED_AT_CAPTURE"
        ],
        "triggered_after_discovery": [
            row for row in normalized
            if row.get("event_status") == "TRIGGERED_AFTER_DISCOVERY"
        ],
    }
    metrics = {
        name: _cohort_metrics(values, cost_bps=cost_bps)
        for name, values in cohorts.items()
    }
    return {
        "sample_rows": len(normalized),
        "control_rows": len(controls),
        "quality_excluded_rows": len(quality_excluded),
        "cost_bps": int(cost_bps),
        "price_quality_counts": _value_counts(
            [
                row for row in resolved
                if row.get("rejection_reason") != "benchmark_only"
            ],
            "price_quality_flag",
        ),
        "feature_quality_counts": _value_counts(
            [row for row in normalized if row.get("setup_shadow_version")],
            "feature_quality_flag",
        ),
        "cohorts": metrics,
        "comparisons": {
            "top_5_minus_all_eligible_net": _metric_delta(
                metrics["top_5"], metrics["all_eligible"], "mean_net_return"
            ),
            "top_5_minus_eligible_rest_net": _metric_delta(
                metrics["top_5"],
                metrics["eligible_outside_top_5"],
                "mean_net_return",
            ),
            "v3_a_minus_rejected_net": _metric_delta(
                metrics["v3_tier_a"], metrics["v3_rejected"], "mean_net_return"
            ),
            "discovery_top_quintile_minus_universe_net": _metric_delta(
                metrics["discovery_top_quintile"],
                metrics["all_universe"],
                "mean_net_return",
            ),
            "setup_top_quintile_minus_universe_net": _metric_delta(
                metrics["setup_top_quintile"],
                metrics["all_universe"],
                "mean_net_return",
            ),
            "pre_breakout_minus_extended_net": _metric_delta(
                metrics["pre_breakout"], metrics["extended"], "mean_net_return"
            ),
            "triggered_after_minus_at_capture_net": _metric_delta(
                metrics["triggered_after_discovery"],
                metrics["triggered_at_capture"],
                "mean_net_return",
            ),
        },
        "information_coefficient_spearman": _cross_sectional_spearman(normalized),
        "shadow_information_coefficients": {
            "discovery": _cross_sectional_spearman(
                normalized, score_key="discovery_percentile"
            ),
            "setup": _cross_sectional_spearman(
                normalized, score_key="setup_percentile"
            ),
            "composite": _cross_sectional_spearman(
                normalized, score_key="composite_shadow_percentile"
            ),
        },
        "methodology": {
            "universe_benchmark": "same-run median forward return",
            "own_positions_benchmark": "same-run equal-weight mean forward return",
            "net_return": "forward return minus configurable theoretical cost",
            "information_coefficient": (
                "mean daily cross-sectional Spearman; pooled rho is diagnostic only"
            ),
            "shadow_scores": (
                "point-in-time audit hypotheses; 0-25 components do not affect ranking"
            ),
            "price_basis": (
                "local same-run comparison; CEDEAR CCL is flagged and not separated"
            ),
        },
    }


def measure_discovery_outcome(
    *,
    as_of_ts: datetime,
    reference_price: float,
    horizon_sessions: int,
    future_candles: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Measure an N-session long outcome using the shared shadow convention."""
    outcome = mature_forecast(
        as_of_ts=as_of_ts,
        reference_price=reference_price,
        horizon_sessions=horizon_sessions,
        expected_return=0.0,
        future_candles=future_candles,
    )
    if outcome is None:
        return None
    return {
        "target_session_ts": outcome.target_session_ts,
        "outcome_price": outcome.outcome_price,
        "forward_return": outcome.realized_return,
        "max_drawdown": _max_drawdown(
            future_candles,
            reference_ts=as_of_ts,
            target_ts=outcome.target_session_ts,
            reference_price=reference_price,
        ),
    }


def _measure_benchmark_from_event(
    *,
    candles: Sequence[Mapping[str, Any]],
    event_ts: datetime,
    horizon_sessions: int,
) -> dict[str, Any] | None:
    event_at = _aware_datetime(event_ts)
    reference = None
    for row in candles:
        ts = row.get("ts")
        if ts is None or _aware_datetime(ts) > event_at:
            continue
        price = _optional_float(row.get("close_price"))
        if price is not None and price > 0:
            reference = price
    if reference is None:
        return None
    return measure_discovery_outcome(
        as_of_ts=event_at,
        reference_price=reference,
        horizon_sessions=horizon_sessions,
        future_candles=candles,
    )


def _selection_reasons(
    *,
    ticker: str,
    screen: Any,
    candidate: Any,
    radar_eligible: bool,
    min_score: float,
    min_rr: float,
) -> tuple[str | None, str | None]:
    if ticker in BENCHMARK_TICKERS:
        return None, "benchmark_only"
    if screen is None:
        return None, "not_screened"
    if not bool(getattr(screen, "passes_screen", False)):
        return None, str(getattr(screen, "fail_reason", "screener_rejected") or "screener_rejected")
    if candidate is None:
        return None, "technical_or_scoring_unavailable"
    status = _enum_value(getattr(candidate, "status", None))
    if status == CandidateStatus.DESCARTAR.value:
        return None, str(getattr(candidate, "why_not_now", "below_radar_threshold") or "below_radar_threshold")
    if float(getattr(candidate, "final_score", 0.0) or 0.0) < float(min_score or 0.0):
        return None, "below_job_min_score"
    asymmetry = getattr(candidate, "asymmetry", None)
    if min_rr > 0 and (
        asymmetry is None
        or not bool(getattr(asymmetry, "rr_valid", False))
        or float(getattr(asymmetry, "risk_reward", 0.0) or 0.0) < min_rr
    ):
        return None, "below_job_min_rr"
    if radar_eligible:
        return status or "ranked", None
    return None, "post_scoring_filter"


def _is_radar_eligible(candidate: Any, *, min_score: float, min_rr: float) -> bool:
    if candidate is None:
        return False
    if _enum_value(getattr(candidate, "status", None)) == CandidateStatus.DESCARTAR.value:
        return False
    if float(getattr(candidate, "final_score", 0.0) or 0.0) < float(min_score or 0.0):
        return False
    asymmetry = getattr(candidate, "asymmetry", None)
    if min_rr > 0 and (
        asymmetry is None
        or not bool(getattr(asymmetry, "rr_valid", False))
        or float(getattr(asymmetry, "risk_reward", 0.0) or 0.0) < float(min_rr)
    ):
        return False
    return True


def _frame_reference(frame: Any) -> tuple[datetime | None, float | None]:
    if frame is None or getattr(frame, "empty", True):
        return None, None
    try:
        raw_ts = frame.index[-1]
        ts = raw_ts.to_pydatetime() if hasattr(raw_ts, "to_pydatetime") else raw_ts
        ts = _aware_datetime(ts)
        price = float(frame["Close"].iloc[-1])
        if not math.isfinite(price) or price <= 0:
            return ts, None
        return ts, price
    except (KeyError, TypeError, ValueError, IndexError):
        return None, None


def _price_quality(
    frame: Any,
    tech: Any,
    *,
    captured_at: datetime,
) -> tuple[str, dict[str, int], str]:
    if frame is None or getattr(frame, "empty", True):
        return "unknown", {}, "MISSING"
    attrs = dict(getattr(frame, "attrs", {}) or {})
    counts = {
        str(key): int(value)
        for key, value in dict(attrs.get("candle_source_counts") or {}).items()
    }
    mode = str(
        getattr(tech, "candle_source_mode", None)
        or attrs.get("candle_source_mode")
        or "unknown"
    ).lower()
    normalized_sources = {source.lower() for source in counts}
    if mode == "unknown" and normalized_sources:
        if normalized_sources == {"cocos"}:
            mode = "cocos"
        elif normalized_sources == {"internal_snapshot"}:
            mode = "reconstructed"
        else:
            mode = "mixed"
    latest_source = ""
    try:
        latest_source = str(frame["Source"].iloc[-1] or "").lower()
    except (KeyError, TypeError, IndexError):
        pass
    reconstructed = bool(
        getattr(tech, "has_reconstructed_candles", False)
        or attrs.get("has_reconstructed_candles", False)
    )
    if latest_source == "market_prices_intraday":
        quality = "INTRADAY_PROVISIONAL"
    elif reconstructed and mode == "reconstructed":
        quality = "RECONSTRUCTED"
    elif reconstructed or mode == "mixed":
        quality = "MIXED"
    elif mode in {"cocos", "official"}:
        quality = "CANONICAL_COCOS"
    else:
        quality = "AVAILABLE_UNCLASSIFIED"
    reference_ts, _ = _frame_reference(frame)
    age_days = _reference_age_days(reference_ts, captured_at)
    if age_days is not None and age_days > 4:
        quality = f"{quality}_STALE"
    return mode, counts, quality


def _percentiles(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, float]:
    valid = [
        (str(row["ticker"]), float(row[key]))
        for row in rows
        if row.get(key) is not None and math.isfinite(float(row[key]))
    ]
    if not valid:
        return {}
    values = [value for _, value in valid]
    ranks = _average_ranks(values)
    denominator = max(len(values) - 1, 1)
    return {
        ticker: (1.0 if len(values) == 1 else (rank - 1.0) / denominator)
        for (ticker, _), rank in zip(valid, ranks)
    }


def _average_ranks(values: Sequence[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        average = ((index + 1) + end) / 2.0
        for original_index, _ in ordered[index:end]:
            ranks[original_index] = average
        index = end
    return ranks


def _rank_percentile(position: int | None, count: int) -> float | None:
    if position is None or count <= 0:
        return None
    if count == 1:
        return 1.0
    return (count - position) / (count - 1)


def _comparison_bucket(position: int | None) -> str:
    if position is None or position > 50:
        return "REST"
    if position <= 5:
        return "TOP_5"
    if position <= 20:
        return "RANK_6_20"
    return "RANK_21_50"


def _max_drawdown(
    candles: Sequence[Mapping[str, Any]],
    *,
    reference_ts: datetime,
    target_ts: datetime,
    reference_price: float,
) -> float | None:
    if reference_price <= 0:
        return None
    peak = float(reference_price)
    drawdowns: list[float] = []
    reference_at = _aware_datetime(reference_ts)
    target_at = _aware_datetime(target_ts)
    for row in candles:
        ts = row.get("ts")
        if ts is None:
            continue
        ts = _aware_datetime(ts)
        if ts <= reference_at or ts > target_at:
            continue
        close_value = row.get("close_price")
        try:
            close = float(close_value)
        except (TypeError, ValueError):
            continue
        if close <= 0:
            continue
        peak = max(peak, close)
        drawdowns.append(close / peak - 1.0)
    return min(drawdowns) if drawdowns else None


def _ticker_return(rows: Sequence[Mapping[str, Any]], ticker: str) -> float | None:
    for row in rows:
        if (
            str(row.get("ticker") or "").upper() == ticker
            and _quality_is_comparable(row.get("price_quality_flag"))
        ):
            return float(row["forward_return"])
    return None


def _quality_is_comparable(value: Any) -> bool:
    quality = str(value or "UNKNOWN").upper()
    return quality != "MISSING" and not quality.endswith("_STALE")


def _reference_age_days(
    reference_ts: datetime | None,
    captured_at: datetime,
) -> int | None:
    if reference_ts is None:
        return None
    reference_day = _aware_datetime(reference_ts).astimezone(ART_TZ).date()
    captured_day = _aware_datetime(captured_at).astimezone(ART_TZ).date()
    return max((captured_day - reference_day).days, 0)


def _cohort_metrics(rows: Sequence[Mapping[str, Any]], *, cost_bps: int) -> dict[str, Any]:
    returns = [float(row["forward_return"]) for row in rows]
    if not returns:
        return {
            "n": 0,
            "win_rate": None,
            "mean_return": None,
            "mean_net_return": None,
            "median_return": None,
            "mean_max_drawdown": None,
            "mean_excess_vs_universe": None,
            "mean_excess_vs_qqq": None,
            "mean_excess_vs_spy": None,
            "mean_excess_vs_own_positions": None,
        }
    return {
        "n": len(returns),
        "win_rate": sum(value > 0 for value in returns) / len(returns),
        "mean_return": fmean(returns),
        "mean_net_return": fmean(returns) - int(cost_bps) / 10000.0,
        "median_return": median(returns),
        "mean_max_drawdown": _mean_optional(rows, "max_drawdown"),
        "mean_excess_vs_universe": _mean_optional(rows, "excess_vs_universe"),
        "mean_excess_vs_qqq": _mean_optional(rows, "excess_vs_qqq"),
        "mean_excess_vs_spy": _mean_optional(rows, "excess_vs_spy"),
        "mean_excess_vs_own_positions": _mean_optional(rows, "excess_vs_own_positions"),
    }


def _mean_optional(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return fmean(values) if values else None


def _value_counts(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _metric_delta(left: Mapping[str, Any], right: Mapping[str, Any], key: str) -> float | None:
    if left.get(key) is None or right.get(key) is None:
        return None
    return float(left[key]) - float(right[key])


def _spearman(pairs: Sequence[tuple[float, float]]) -> dict[str, Any]:
    if len(pairs) < 3:
        return {"n": len(pairs), "rho": None}
    x = [pair[0] for pair in pairs]
    y = [pair[1] for pair in pairs]
    x_rank = _average_ranks(x)
    y_rank = _average_ranks(y)
    x_mean = fmean(x_rank)
    y_mean = fmean(y_rank)
    numerator = sum((a - x_mean) * (b - y_mean) for a, b in zip(x_rank, y_rank))
    x_ss = sum((a - x_mean) ** 2 for a in x_rank)
    y_ss = sum((b - y_mean) ** 2 for b in y_rank)
    rho = numerator / math.sqrt(x_ss * y_ss) if x_ss > 0 and y_ss > 0 else None
    return {"n": len(pairs), "rho": rho}


def _cross_sectional_spearman(
    rows: Sequence[Mapping[str, Any]],
    *,
    score_key: str = "rank_percentile",
) -> dict[str, Any]:
    by_session: dict[str, list[tuple[float, float]]] = {}
    pooled: list[tuple[float, float]] = []
    for row in rows:
        if row.get(score_key) is None or row.get("forward_return") is None:
            continue
        pair = (float(row[score_key]), float(row["forward_return"]))
        pooled.append(pair)
        session = str(
            row.get("event_session")
            or row.get("captured_session")
            or "single_session"
        )
        by_session.setdefault(session, []).append(pair)
    session_rhos = [
        float(result["rho"])
        for pairs in by_session.values()
        if (result := _spearman(pairs))["rho"] is not None
    ]
    pooled_result = _spearman(pooled)
    return {
        "n": len(pooled),
        "sessions": len(session_rhos),
        "rho": fmean(session_rhos) if session_rhos else None,
        "median_rho": median(session_rhos) if session_rhos else None,
        "pooled_rho": pooled_result["rho"],
    }


def _at_least(value: Any, threshold: float) -> bool:
    normalized = _optional_float(value)
    return normalized is not None and normalized >= float(threshold)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "")


def _aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _coerce_datetime(value: Any, *, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return _aware_datetime(value)
    if value:
        try:
            return _aware_datetime(datetime.fromisoformat(str(value)))
        except ValueError:
            pass
    return _aware_datetime(fallback)


__all__ = [
    "BENCHMARK_TICKERS",
    "DEFAULT_THEORETICAL_COST_BPS",
    "RADAR_DISCOVERY_HORIZONS",
    "RADAR_DISCOVERY_PROTOCOL_VERSION",
    "RADAR_DISCOVERY_SCHEMA_SQL",
    "RadarDiscoveryObservation",
    "RadarDiscoveryStore",
    "build_discovery_observations",
    "discovery_scoring_version",
    "measure_discovery_outcome",
    "summarize_comparisons",
]
