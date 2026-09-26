"""
Read-only audit of Quantia decisions against later market outcomes.

The module compares persisted decision cohorts with their matured outcomes and
benchmark context. It never writes to decision_log, broker tables, planner
state, thresholds, orders, or shadow outputs.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg


DEFAULT_BENCHMARKS = ("SPY", "QQQ")
DEFAULT_HORIZONS = ("5d", "10d", "20d", "40d")


@dataclass(frozen=True)
class DecisionMarketAuditConfig:
    database_url: str
    days: int = 180
    cost_bps: float = 75.0
    owner_chat_id: int | None = None
    benchmarks: tuple[str, ...] = DEFAULT_BENCHMARKS


@dataclass(frozen=True)
class QualityRow:
    table_name: str
    rows: int
    min_ts: str | None
    max_ts: str | None


@dataclass(frozen=True)
class CohortHorizonMetric:
    cohort: str
    horizon: str
    total_decisions: int
    matured: int
    with_live_fill: int
    hit_rate: float | None
    avg_return: float | None
    net_avg_return: float | None
    median_return: float | None
    avg_win: float | None
    avg_loss: float | None
    worst: float | None
    best: float | None
    avg_score: float | None
    score_corr: float | None
    tickers: int


@dataclass(frozen=True)
class BenchmarkMetric:
    cohort: str
    decision: str
    horizon: str
    benchmark: str
    n: int
    avg_decision_directional: float | None
    avg_benchmark_raw: float | None
    buy_alpha_vs_benchmark: float | None
    hit_rate: float | None
    benchmark_positive_rate: float | None


@dataclass(frozen=True)
class FollowedMetric:
    follow_status: str
    temporal_quality: str
    n: int
    matured_5d: int
    hit_5d: float | None
    avg_5d: float | None
    avg_10d: float | None
    avg_20d: float | None
    avg_40d: float | None
    avg_follow_ratio: float | None
    tickers: int


@dataclass(frozen=True)
class ExtremeDecision:
    id: int
    decided_at: str
    ticker: str
    decision: str
    final_score: float | None
    status: str
    decision_type: str
    outcome_5d: float | None
    outcome_10d: float | None
    outcome_20d: float | None


@dataclass(frozen=True)
class DecisionMarketAuditReport:
    generated_at: str
    days: int
    cost_bps: float
    benchmarks: tuple[str, ...]
    quality: list[QualityRow]
    summary: list[CohortHorizonMetric]
    benchmark_summary: list[BenchmarkMetric]
    followed_summary: list[FollowedMetric]
    worst_execution_plan_5d: list[ExtremeDecision]
    best_execution_plan_5d: list[ExtremeDecision]
    warnings: list[str]
    timesfm3_shadow_next_step: str


READONLY_SQL_FRAGMENTS = (
    "WITH base AS",
    "WITH decisions AS",
    "SELECT",
)


SUMMARY_SQL = r"""
WITH base AS (
    SELECT
        dl.id,
        dl.decided_at,
        dl.decision_date,
        dl.ticker,
        UPPER(COALESCE(dl.decision,'')) AS decision,
        COALESCE(dl.source, dl.layers->>'source', '') AS source,
        COALESCE(dl.status, '') AS status,
        COALESCE(dl.decision_type, '') AS decision_type,
        COALESCE(dl.metric_scope, '') AS metric_scope,
        COALESCE(dl.run_intent, '') AS run_intent,
        dl.final_score,
        dl.confidence,
        dl.price_at_decision,
        dl.theoretical_amount_ars,
        dl.executed_amount_ars,
        dl.was_blocked,
        dl.block_reason,
        COALESCE(dl.executable_outcome_5d, dl.outcome_5d) AS eff_5d,
        COALESCE(dl.executable_outcome_10d, dl.outcome_10d) AS eff_10d,
        COALESCE(dl.executable_outcome_20d, dl.outcome_20d) AS eff_20d,
        COALESCE(dl.executable_outcome_40d, dl.outcome_40d) AS eff_40d,
        dl.outcome_basis,
        dl.outcome_basis_ratio,
        EXISTS (
            SELECT 1 FROM broker_fills bf
            WHERE bf.decision_log_id = dl.id
              AND NOT (COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
        ) AS has_live_fill,
        EXISTS (
            SELECT 1 FROM broker_fills bf
            WHERE bf.decision_log_id = dl.id
              AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
              AND NOT EXISTS (
                  SELECT 1 FROM broker_fills live_bf
                  WHERE live_bf.decision_log_id = dl.id
                    AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
              )
        ) AS superseded_only
    FROM decision_log dl
    WHERE dl.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND ($2::bigint IS NULL OR dl.owner_chat_id = $2)
), scoped AS (
    SELECT *,
        CASE
            WHEN source = 'execution_plan' AND status IN ('EXECUTED','EXECUTED_MANUAL') THEN 'execution_plan_executed'
            WHEN source = 'execution_plan' AND status = 'APPROVED' AND decision_type = 'executable' THEN 'execution_plan_approved'
            WHEN source = 'execution_plan' AND status = 'BLOCKED' THEN 'execution_plan_blocked'
            WHEN source IN ('broker_fill','broker_movement') AND status IN ('EXECUTED','EXECUTED_MANUAL') THEN 'manual_or_broker_real'
            WHEN source = 'optimizer' OR status = 'THEORETICAL' OR decision_type = 'theoretical' THEN 'optimizer_theoretical'
            WHEN source = 'radar' THEN 'radar_idea'
            ELSE 'other_audit'
        END AS cohort
    FROM base
    WHERE NOT superseded_only
), long AS (
    SELECT cohort, source, status, decision_type, metric_scope, run_intent, decision,
           id, decided_at, ticker, final_score, confidence, price_at_decision,
           has_live_fill, outcome_basis, outcome_basis_ratio,
           horizon, outcome
    FROM scoped
    CROSS JOIN LATERAL (VALUES
        ('5d', eff_5d), ('10d', eff_10d), ('20d', eff_20d), ('40d', eff_40d)
    ) h(horizon, outcome)
    WHERE decision IN ('BUY','SELL','SELL_PARTIAL','SELL_FULL')
)
SELECT
    cohort,
    horizon,
    COUNT(*) AS total_decisions,
    COUNT(outcome) AS matured,
    COUNT(*) FILTER (WHERE has_live_fill) AS with_live_fill,
    AVG(CASE WHEN outcome IS NOT NULL AND outcome > 0 THEN 1.0 WHEN outcome IS NOT NULL THEN 0.0 END) AS hit_rate,
    AVG(outcome) AS avg_return,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY outcome) AS median_return,
    AVG(outcome) FILTER (WHERE outcome > 0) AS avg_win,
    AVG(outcome) FILTER (WHERE outcome < 0) AS avg_loss,
    MIN(outcome) AS worst,
    MAX(outcome) AS best,
    AVG(final_score) FILTER (WHERE outcome IS NOT NULL) AS avg_score,
    corr(final_score, outcome) FILTER (WHERE outcome IS NOT NULL AND final_score IS NOT NULL) AS score_corr,
    COUNT(DISTINCT ticker) FILTER (WHERE outcome IS NOT NULL) AS tickers
FROM long
GROUP BY cohort, horizon
ORDER BY cohort, CASE horizon WHEN '5d' THEN 1 WHEN '10d' THEN 2 WHEN '20d' THEN 3 WHEN '40d' THEN 4 ELSE 5 END
"""


QUALITY_SQL = r"""
SELECT 'decision_log' AS table_name, COUNT(*) AS rows, MIN(decided_at) AS min_ts, MAX(decided_at) AS max_ts FROM decision_log
UNION ALL
SELECT 'broker_fills', COUNT(*), MIN(executed_at), MAX(executed_at) FROM broker_fills
UNION ALL
SELECT 'plan_execution_attributions', COUNT(*), MIN(executed_at), MAX(executed_at) FROM plan_execution_attributions
"""


FOLLOWED_SQL = r"""
SELECT
    follow_status,
    temporal_quality,
    COUNT(*) AS n,
    COUNT(outcome_5d) AS matured_5d,
    AVG(CASE WHEN outcome_5d > 0 THEN 1.0 WHEN outcome_5d IS NOT NULL THEN 0.0 END) AS hit_5d,
    AVG(outcome_5d) AS avg_5d,
    AVG(outcome_10d) AS avg_10d,
    AVG(outcome_20d) AS avg_20d,
    AVG(outcome_40d) AS avg_40d,
    AVG(follow_ratio) AS avg_follow_ratio,
    COUNT(DISTINCT ticker) AS tickers
FROM plan_execution_attributions
WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
  AND ($2::bigint IS NULL OR owner_chat_id = $2)
  AND eligible_for_viability = TRUE
GROUP BY follow_status, temporal_quality
ORDER BY n DESC
"""


EXTREMES_SQL = r"""
WITH rows AS (
    SELECT id, decided_at, ticker, decision, final_score,
           COALESCE(source, layers->>'source','') AS source,
           COALESCE(status,'') AS status,
           COALESCE(decision_type,'') AS decision_type,
           COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
           COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
           COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d
    FROM decision_log dl
    WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND ($2::bigint IS NULL OR owner_chat_id = $2)
      AND COALESCE(source, layers->>'source','') = 'execution_plan'
      AND COALESCE(status,'') IN ('APPROVED','EXECUTED','BLOCKED','EXECUTED_MANUAL')
      AND decision IN ('BUY','SELL','SELL_PARTIAL','SELL_FULL')
      AND NOT EXISTS (
          SELECT 1 FROM broker_fills bf
          WHERE bf.decision_log_id = dl.id
            AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
            AND NOT EXISTS (
                SELECT 1 FROM broker_fills live_bf
                WHERE live_bf.decision_log_id = dl.id
                  AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
            )
      )
)
SELECT id, decided_at, ticker, decision, final_score, status, decision_type,
       outcome_5d, outcome_10d, outcome_20d
FROM rows
WHERE outcome_5d IS NOT NULL
ORDER BY outcome_5d __DIRECTION__
LIMIT 8
"""


BENCHMARK_SQL = r"""
WITH decisions AS (
    SELECT
        dl.id,
        (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date AS decision_day,
        UPPER(dl.decision) AS decision,
        CASE
            WHEN COALESCE(dl.source, dl.layers->>'source','') = 'execution_plan' AND COALESCE(dl.status,'') IN ('EXECUTED','EXECUTED_MANUAL') THEN 'execution_plan_executed'
            WHEN COALESCE(dl.source, dl.layers->>'source','') = 'execution_plan' AND COALESCE(dl.status,'') = 'APPROVED' AND COALESCE(dl.decision_type,'') = 'executable' THEN 'execution_plan_approved'
            WHEN COALESCE(dl.source, dl.layers->>'source','') = 'execution_plan' AND COALESCE(dl.status,'') = 'BLOCKED' THEN 'execution_plan_blocked'
            WHEN COALESCE(dl.source, dl.layers->>'source','') IN ('broker_fill','broker_movement') AND COALESCE(dl.status,'') IN ('EXECUTED','EXECUTED_MANUAL') THEN 'manual_or_broker_real'
            WHEN COALESCE(dl.source, dl.layers->>'source','') = 'optimizer' OR COALESCE(dl.status,'') = 'THEORETICAL' OR COALESCE(dl.decision_type,'') = 'theoretical' THEN 'optimizer_theoretical'
            WHEN COALESCE(dl.source, dl.layers->>'source','') = 'radar' THEN 'radar_idea'
            ELSE 'other_audit'
        END AS cohort,
        h.horizon,
        h.horizon_n,
        h.outcome
    FROM decision_log dl
    CROSS JOIN LATERAL (VALUES
        ('5d', 5, COALESCE(dl.executable_outcome_5d, dl.outcome_5d)),
        ('10d', 10, COALESCE(dl.executable_outcome_10d, dl.outcome_10d)),
        ('20d', 20, COALESCE(dl.executable_outcome_20d, dl.outcome_20d)),
        ('40d', 40, COALESCE(dl.executable_outcome_40d, dl.outcome_40d))
    ) h(horizon, horizon_n, outcome)
    WHERE dl.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND ($2::bigint IS NULL OR dl.owner_chat_id = $2)
      AND UPPER(dl.decision) IN ('BUY','SELL','SELL_PARTIAL','SELL_FULL')
      AND h.outcome IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM broker_fills bf
          WHERE bf.decision_log_id = dl.id
            AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
            AND NOT EXISTS (
                SELECT 1 FROM broker_fills live_bf
                WHERE live_bf.decision_log_id = dl.id
                  AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
            )
      )
), bench AS (
    SELECT
        ticker,
        (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date AS day,
        close_price::float AS close_price,
        row_number() OVER (PARTITION BY ticker ORDER BY (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AS rn
    FROM market_candles
    WHERE interval='1d'
      AND ticker = ANY($3::text[])
      AND long_ticker = ANY($4::text[])
), matched AS (
    SELECT
        d.*,
        bt.benchmark,
        ((b1.close_price - b0.close_price) / NULLIF(b0.close_price,0)) AS benchmark_return
    FROM decisions d
    CROSS JOIN unnest($3::text[]) bt(benchmark)
    JOIN LATERAL (
        SELECT * FROM bench b
        WHERE b.ticker = bt.benchmark AND b.day >= d.decision_day
        ORDER BY b.day
        LIMIT 1
    ) b0 ON TRUE
    JOIN bench b1
      ON b1.ticker = b0.ticker
     AND b1.rn = b0.rn + d.horizon_n
)
SELECT
    cohort,
    decision,
    horizon,
    benchmark,
    COUNT(*) AS n,
    AVG(outcome) AS avg_decision_directional,
    AVG(benchmark_return) AS avg_benchmark_raw,
    AVG(outcome - benchmark_return) FILTER (WHERE decision='BUY') AS buy_alpha_vs_benchmark,
    AVG(CASE WHEN outcome > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
    AVG(CASE WHEN benchmark_return > 0 THEN 1.0 ELSE 0.0 END) AS benchmark_positive_rate
FROM matched
WHERE cohort IN ('execution_plan_executed','execution_plan_approved','execution_plan_blocked','manual_or_broker_real','radar_idea')
GROUP BY cohort, decision, horizon, benchmark
ORDER BY cohort, decision, CASE horizon WHEN '5d' THEN 1 WHEN '10d' THEN 2 WHEN '20d' THEN 3 WHEN '40d' THEN 4 END, benchmark
"""


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _to_int(value: Any) -> int:
    return int(value or 0)


def _pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:+.1%}"


def _num(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.3f}"


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _net(avg_return: float | None, cost_bps: float) -> float | None:
    if avg_return is None:
        return None
    return avg_return - (cost_bps / 10_000.0)


def _quality(row: asyncpg.Record | dict[str, Any]) -> QualityRow:
    return QualityRow(
        table_name=str(row["table_name"]),
        rows=_to_int(row["rows"]),
        min_ts=_iso(row["min_ts"]),
        max_ts=_iso(row["max_ts"]),
    )


def _cohort_metric(row: asyncpg.Record | dict[str, Any], cost_bps: float) -> CohortHorizonMetric:
    avg = _to_float(row["avg_return"])
    return CohortHorizonMetric(
        cohort=str(row["cohort"]),
        horizon=str(row["horizon"]),
        total_decisions=_to_int(row["total_decisions"]),
        matured=_to_int(row["matured"]),
        with_live_fill=_to_int(row["with_live_fill"]),
        hit_rate=_to_float(row["hit_rate"]),
        avg_return=avg,
        net_avg_return=_net(avg, cost_bps),
        median_return=_to_float(row["median_return"]),
        avg_win=_to_float(row["avg_win"]),
        avg_loss=_to_float(row["avg_loss"]),
        worst=_to_float(row["worst"]),
        best=_to_float(row["best"]),
        avg_score=_to_float(row["avg_score"]),
        score_corr=_to_float(row["score_corr"]),
        tickers=_to_int(row["tickers"]),
    )


def _benchmark_metric(row: asyncpg.Record | dict[str, Any]) -> BenchmarkMetric:
    return BenchmarkMetric(
        cohort=str(row["cohort"]),
        decision=str(row["decision"]),
        horizon=str(row["horizon"]),
        benchmark=str(row["benchmark"]),
        n=_to_int(row["n"]),
        avg_decision_directional=_to_float(row["avg_decision_directional"]),
        avg_benchmark_raw=_to_float(row["avg_benchmark_raw"]),
        buy_alpha_vs_benchmark=_to_float(row["buy_alpha_vs_benchmark"]),
        hit_rate=_to_float(row["hit_rate"]),
        benchmark_positive_rate=_to_float(row["benchmark_positive_rate"]),
    )


def _followed_metric(row: asyncpg.Record | dict[str, Any]) -> FollowedMetric:
    return FollowedMetric(
        follow_status=str(row["follow_status"]),
        temporal_quality=str(row["temporal_quality"]),
        n=_to_int(row["n"]),
        matured_5d=_to_int(row["matured_5d"]),
        hit_5d=_to_float(row["hit_5d"]),
        avg_5d=_to_float(row["avg_5d"]),
        avg_10d=_to_float(row["avg_10d"]),
        avg_20d=_to_float(row["avg_20d"]),
        avg_40d=_to_float(row["avg_40d"]),
        avg_follow_ratio=_to_float(row["avg_follow_ratio"]),
        tickers=_to_int(row["tickers"]),
    )


def _extreme(row: asyncpg.Record | dict[str, Any]) -> ExtremeDecision:
    return ExtremeDecision(
        id=_to_int(row["id"]),
        decided_at=_iso(row["decided_at"]) or "",
        ticker=str(row["ticker"]),
        decision=str(row["decision"]),
        final_score=_to_float(row["final_score"]),
        status=str(row["status"]),
        decision_type=str(row["decision_type"]),
        outcome_5d=_to_float(row["outcome_5d"]),
        outcome_10d=_to_float(row["outcome_10d"]),
        outcome_20d=_to_float(row["outcome_20d"]),
    )


def _warnings(summary: list[CohortHorizonMetric], followed: list[FollowedMetric]) -> list[str]:
    warnings: list[str] = []
    executed_5d = next((m for m in summary if m.cohort == "execution_plan_executed" and m.horizon == "5d"), None)
    if not executed_5d or executed_5d.matured < 30:
        warnings.append("La cohorte execution_plan/EXECUTED a 5d esta cerca o debajo del minimo n=30; leer como evidencia temprana.")
    if any(m.score_corr is not None and m.score_corr < 0 for m in summary if m.cohort == "execution_plan_executed"):
        warnings.append("En planes ejecutados, final_score no ordena bien el outcome; requiere calibracion antes de tocar thresholds.")
    if not followed:
        warnings.append("No hay atribuciones elegibles plan->ejecucion en la ventana seleccionada.")
    warnings.append("TimesFM-3 queda fuera de ejecucion: solo shadow T-1 por licencia/evidencia pendiente.")
    return warnings


async def load_decision_market_audit(config: DecisionMarketAuditConfig) -> DecisionMarketAuditReport:
    dsn = config.database_url.replace("postgresql+asyncpg://", "postgresql://")
    benchmarks = tuple(b.upper() for b in config.benchmarks)
    benchmark_long_tickers = tuple(f"TV:BYMA:{b}" for b in benchmarks)
    conn = await asyncpg.connect(dsn)
    try:
        generated_at = await conn.fetchval("SELECT NOW()")
        quality_rows = [_quality(row) for row in await conn.fetch(QUALITY_SQL)]
        summary = [
            _cohort_metric(row, config.cost_bps)
            for row in await conn.fetch(SUMMARY_SQL, config.days, config.owner_chat_id)
        ]
        benchmark_summary = [
            _benchmark_metric(row)
            for row in await conn.fetch(
                BENCHMARK_SQL,
                config.days,
                config.owner_chat_id,
                list(benchmarks),
                list(benchmark_long_tickers),
            )
        ]
        followed_summary = [
            _followed_metric(row)
            for row in await conn.fetch(FOLLOWED_SQL, config.days, config.owner_chat_id)
        ]
        worst = [
            _extreme(row)
            for row in await conn.fetch(EXTREMES_SQL.replace("__DIRECTION__", "ASC"), config.days, config.owner_chat_id)
        ]
        best = [
            _extreme(row)
            for row in await conn.fetch(EXTREMES_SQL.replace("__DIRECTION__", "DESC"), config.days, config.owner_chat_id)
        ]
    finally:
        await conn.close()

    return DecisionMarketAuditReport(
        generated_at=_iso(generated_at) or "",
        days=config.days,
        cost_bps=config.cost_bps,
        benchmarks=benchmarks,
        quality=quality_rows,
        summary=summary,
        benchmark_summary=benchmark_summary,
        followed_summary=followed_summary,
        worst_execution_plan_5d=worst,
        best_execution_plan_5d=best,
        warnings=[],
        timesfm3_shadow_next_step=(
            "Agregar timesfm3_tminus1_shadow como consumidor de snapshots point-in-time; "
            "sus deltas solo comparan contra la heuristica y no escriben decisiones reales."
        ),
    )


def with_warnings(report: DecisionMarketAuditReport) -> DecisionMarketAuditReport:
    return DecisionMarketAuditReport(
        **{
            **asdict(report),
            "quality": report.quality,
            "summary": report.summary,
            "benchmark_summary": report.benchmark_summary,
            "followed_summary": report.followed_summary,
            "worst_execution_plan_5d": report.worst_execution_plan_5d,
            "best_execution_plan_5d": report.best_execution_plan_5d,
            "warnings": _warnings(report.summary, report.followed_summary),
        }
    )


def _metric(summary: list[CohortHorizonMetric], cohort: str, horizon: str) -> CohortHorizonMetric | None:
    return next((m for m in summary if m.cohort == cohort and m.horizon == horizon), None)


def _summary_table(summary: list[CohortHorizonMetric]) -> str:
    cohorts = [
        ("execution_plan_executed", "Plan ejecutado"),
        ("execution_plan_approved", "Plan aprobado sin fill"),
        ("execution_plan_blocked", "Plan bloqueado"),
        ("manual_or_broker_real", "Manual/broker real"),
        ("radar_idea", "Radar idea"),
    ]
    lines = [
        "| Cohorte | 5d n | Hit 5d | Ret. neto 5d | 20d n | Hit 20d | Ret. neto 20d | Score corr 5d |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cohort, label in cohorts:
        m5 = _metric(summary, cohort, "5d")
        m20 = _metric(summary, cohort, "20d")
        lines.append(
            "| {label} | {n5} | {hit5} | {ret5} | {n20} | {hit20} | {ret20} | {corr5} |".format(
                label=label,
                n5=m5.matured if m5 else 0,
                hit5=_pct(m5.hit_rate if m5 else None),
                ret5=_pct(m5.net_avg_return if m5 else None),
                n20=m20.matured if m20 else 0,
                hit20=_pct(m20.hit_rate if m20 else None),
                ret20=_pct(m20.net_avg_return if m20 else None),
                corr5=_num(m5.score_corr if m5 else None),
            )
        )
    return "\n".join(lines)


def _benchmark_table(rows: list[BenchmarkMetric]) -> str:
    selected = [
        r
        for r in rows
        if r.horizon in {"5d", "20d"}
        and r.benchmark in {"SPY", "QQQ"}
        and r.cohort in {"execution_plan_executed", "execution_plan_approved", "execution_plan_blocked", "radar_idea"}
        and r.decision == "BUY"
    ]
    lines = [
        "| Cohorte BUY | Horizonte | Benchmark | n | Ret. decision | Benchmark | Alpha BUY |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for r in selected:
        lines.append(
            "| {cohort} | {horizon} | {bench} | {n} | {ret} | {bret} | {alpha} |".format(
                cohort=r.cohort,
                horizon=r.horizon,
                bench=r.benchmark,
                n=r.n,
                ret=_pct(r.avg_decision_directional),
                bret=_pct(r.avg_benchmark_raw),
                alpha=_pct(r.buy_alpha_vs_benchmark),
            )
        )
    return "\n".join(lines)


def _extreme_table(rows: list[ExtremeDecision]) -> str:
    lines = [
        "| ID | Fecha | Ticker | Decision | Estado | Score | Outcome 5d |",
        "|---:|---|---|---|---|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {id} | {date} | {ticker} | {decision} | {status} | {score} | {outcome} |".format(
                id=row.id,
                date=row.decided_at[:10],
                ticker=row.ticker,
                decision=row.decision,
                status=row.status,
                score=_num(row.final_score),
                outcome=_pct(row.outcome_5d),
            )
        )
    return "\n".join(lines)


def render_decision_market_audit(report: DecisionMarketAuditReport) -> str:
    report = with_warnings(report)
    quality = {row.table_name: row for row in report.quality}
    decision_log = quality.get("decision_log")
    fills = quality.get("broker_fills")
    attributions = quality.get("plan_execution_attributions")
    executed_5d = _metric(report.summary, "execution_plan_executed", "5d")
    executed_20d = _metric(report.summary, "execution_plan_executed", "20d")
    blocked_5d = _metric(report.summary, "execution_plan_blocked", "5d")

    verdict = "Evidencia prometedora, no promovible automaticamente."
    if executed_5d and executed_5d.matured >= 30 and executed_5d.net_avg_return and executed_5d.net_avg_return > 0:
        verdict = "Plan ejecutado positivo neto a 5d; requiere estabilidad y calibracion de score."
    if blocked_5d and blocked_5d.net_avg_return and blocked_5d.net_avg_return < 0:
        guard_note = "Los bloqueos capturaron una cohorte con retorno neto negativo; no aflojar guards."
    else:
        guard_note = "El efecto de los bloqueos queda mixto; revisar por lado y regimen."

    return "\n".join(
        [
            "# Decision vs Market Audit",
            "",
            f"Generado: {report.generated_at}",
            f"Ventana: {report.days} dias; costo usado para neto: {report.cost_bps:.0f} bps.",
            "",
            "## Lectura ejecutiva",
            "",
            f"- Veredicto: {verdict}",
            f"- Plan ejecutado 5d: n={executed_5d.matured if executed_5d else 0}, hit={_pct(executed_5d.hit_rate if executed_5d else None)}, neto={_pct(executed_5d.net_avg_return if executed_5d else None)}.",
            f"- Plan ejecutado 20d: n={executed_20d.matured if executed_20d else 0}, hit={_pct(executed_20d.hit_rate if executed_20d else None)}, neto={_pct(executed_20d.net_avg_return if executed_20d else None)}.",
            f"- {guard_note}",
            "- final_score se reporta como diagnostico, no como autorizacion para cambiar thresholds.",
            "",
            "## Cobertura de datos",
            "",
            f"- decision_log: {decision_log.rows if decision_log else 0} filas ({decision_log.min_ts if decision_log else 'N/A'} a {decision_log.max_ts if decision_log else 'N/A'}).",
            f"- broker_fills: {fills.rows if fills else 0} filas ({fills.min_ts if fills else 'N/A'} a {fills.max_ts if fills else 'N/A'}).",
            f"- plan_execution_attributions: {attributions.rows if attributions else 0} filas ({attributions.min_ts if attributions else 'N/A'} a {attributions.max_ts if attributions else 'N/A'}).",
            "",
            "## Cohortes principales",
            "",
            _summary_table(report.summary),
            "",
            "## BUY vs benchmarks",
            "",
            _benchmark_table(report.benchmark_summary),
            "",
            "## Mejores planes 5d",
            "",
            _extreme_table(report.best_execution_plan_5d),
            "",
            "## Peores planes 5d",
            "",
            _extreme_table(report.worst_execution_plan_5d),
            "",
            "## TimesFM-3 T-1 shadow",
            "",
            report.timesfm3_shadow_next_step,
            "",
            "Regla de seguridad: el shadow puede proponer deltas de heuristica, pero no modifica score, optimizer, planner, orders ni fills.",
            "",
            "## Caveats",
            "",
            "\n".join(f"- {warning}" for warning in report.warnings),
            "",
        ]
    )


def report_to_json(report: DecisionMarketAuditReport) -> str:
    return json.dumps(asdict(with_warnings(report)), ensure_ascii=False, indent=2)


def write_report_files(report: DecisionMarketAuditReport, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "decision_market_audit.json"
    md_path = output_dir / "decision_market_audit.md"
    json_path.write_text(report_to_json(report), encoding="utf-8")
    md_path.write_text(render_decision_market_audit(report), encoding="utf-8")
    return json_path, md_path
