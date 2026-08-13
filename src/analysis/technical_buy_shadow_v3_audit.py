"""Read-only audit for the BUY-only technical shadow V3."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import random
from statistics import median
from typing import Any, Iterable, Mapping

from src.analysis.technical_buy_shadow_v3 import build_technical_buy_shadow_v3


DEFAULT_ROUND_TRIP_COST = 0.0075
BOOTSTRAP_SEED = 20260813


@dataclass(frozen=True, slots=True)
class BuyAuditMetric:
    sample: str
    n: int
    win_rate: float | None
    gross_ev: float | None
    net_ev: float | None
    median_return: float | None
    excess_ev: float | None
    ci95_low: float | None
    ci95_high: float | None


@dataclass(frozen=True, slots=True)
class TechnicalBuyShadowV3Audit:
    rows_loaded: int
    v2_buy_episodes: int
    first_signal_date: date | None
    last_signal_date: date | None
    cost_rate: float
    split_date: date
    metrics: tuple[BuyAuditMetric, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_loaded": self.rows_loaded,
            "v2_buy_episodes": self.v2_buy_episodes,
            "first_signal_date": self.first_signal_date,
            "last_signal_date": self.last_signal_date,
            "cost_rate": self.cost_rate,
            "split_date": self.split_date,
            "metrics": [asdict(metric) for metric in self.metrics],
            "boundary": {
                "objective": "NEW_POSITION_BUY_DISCOVERY",
                "outcome": "20 calendar days; first available candle on/after target",
                "affects_analysis": False,
                "affects_execution": False,
            },
        }


def build_technical_buy_shadow_v3_audit(
    rows: Iterable[Mapping[str, Any]],
    *,
    cost_rate: float = DEFAULT_ROUND_TRIP_COST,
    split_date: date = date(2026, 7, 1),
) -> TechnicalBuyShadowV3Audit:
    normalized = [_normalize_row(row) for row in rows]
    universe_median = _same_date_universe_median(normalized)
    episodes = _buy_episodes(normalized)
    evaluated: list[dict[str, Any]] = []
    for row in episodes:
        if row["return_20d"] is None or not row["quality_20d"]:
            continue
        v3 = build_technical_buy_shadow_v3(
            technical_shadow_v2={
                "version": "technical-shadow-v2",
                "score": row["v2_score"],
                "bias": "POSITIVE",
                "structural_break_gate": row["structural_break"],
            },
            regime=row["regime"],
            trend_score=row["trend_score"],
            reversion_score=row["reversion_score"],
            structural_break_confirmed=row["structural_break"],
            source_mode=row["source_mode"],
            volume_quality_20=row["volume_quality_20"],
        )
        enriched = dict(row)
        enriched["v3"] = v3.to_dict()
        benchmark = universe_median.get(row["date"])
        enriched["excess_return"] = (
            row["return_20d"] - benchmark if benchmark is not None else None
        )
        evaluated.append(enriched)

    primary = [
        row for row in evaluated
        if row["v3"]["classification"] == "PRIMARY_BUY_CANDIDATE"
    ]
    eligible = [row for row in evaluated if row["v3"]["eligible_for_buy_research"]]
    metrics = (
        _metric("V2_BUY_ALL", evaluated, cost_rate),
        _metric("V3_PRIMARY_ALL", primary, cost_rate),
        _metric("V3_PRIMARY_BEFORE_SPLIT", [r for r in primary if r["date"] < split_date], cost_rate),
        _metric("V3_PRIMARY_AFTER_SPLIT", [r for r in primary if r["date"] >= split_date], cost_rate),
        _metric("V3_ELIGIBLE_A_B", eligible, cost_rate),
    )
    dates = [row["date"] for row in evaluated]
    return TechnicalBuyShadowV3Audit(
        rows_loaded=len(normalized),
        v2_buy_episodes=len(evaluated),
        first_signal_date=min(dates) if dates else None,
        last_signal_date=max(dates) if dates else None,
        cost_rate=float(cost_rate),
        split_date=split_date,
        metrics=metrics,
    )


def render_technical_buy_shadow_v3_audit(report: TechnicalBuyShadowV3Audit) -> str:
    lines = [
        "TECHNICAL BUY SHADOW V3 - AUDITORIA READ-ONLY",
        f"V2 BUY maduros: {report.v2_buy_episodes} | costo supuesto: {report.cost_rate:.2%}",
        "Muestra | n | win | EV bruto | EV neto | exceso vs universo | IC95 bruto",
    ]
    for metric in report.metrics:
        lines.append(
            f"{metric.sample} | {metric.n} | {_pct(metric.win_rate)} | "
            f"{_pct(metric.gross_ev)} | {_pct(metric.net_ev)} | "
            f"{_pct(metric.excess_ev)} | "
            f"[{_pct(metric.ci95_low)}, {_pct(metric.ci95_high)}]"
        )
    lines.extend([
        "Outcome histórico: 20 días calendario, no 20 ruedas exactas.",
        "No modifica Radar, /analisis, scoring, planes ni órdenes.",
    ])
    return "\n".join(lines)


def _buy_episodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append(row)
    for ticker_rows in by_ticker.values():
        active = False
        for row in sorted(ticker_rows, key=lambda item: item["date"]):
            if row["v2_direction"] != "BUY":
                active = False
                continue
            if not active:
                selected.append(row)
                active = True
    return selected


def _same_date_universe_median(rows: list[dict[str, Any]]) -> dict[date, float]:
    grouped: dict[date, list[float]] = {}
    for row in rows:
        value = row["return_20d"]
        if value is None or not row["quality_20d"]:
            continue
        grouped.setdefault(row["date"], []).append(value)
    return {day: float(median(values)) for day, values in grouped.items() if values}


def _metric(sample: str, rows: list[dict[str, Any]], cost_rate: float) -> BuyAuditMetric:
    values = [float(row["return_20d"]) for row in rows]
    excess = [float(row["excess_return"]) for row in rows if row["excess_return"] is not None]
    low, high = _bootstrap_mean_ci(values)
    return BuyAuditMetric(
        sample=sample,
        n=len(values),
        win_rate=(sum(value > 0.0 for value in values) / len(values) if values else None),
        gross_ev=(sum(values) / len(values) if values else None),
        net_ev=(sum(value - cost_rate for value in values) / len(values) if values else None),
        median_return=(float(median(values)) if values else None),
        excess_ev=(sum(excess) / len(excess) if excess else None),
        ci95_low=low,
        ci95_high=high,
    )


def _bootstrap_mean_ci(values: list[float], repetitions: int = 2000) -> tuple[float | None, float | None]:
    if len(values) < 2:
        return None, None
    rng = random.Random(BOOTSTRAP_SEED + len(values))
    means = sorted(
        sum(rng.choice(values) for _ in values) / len(values)
        for _ in range(repetitions)
    )
    return means[int(0.025 * repetitions)], means[int(0.975 * repetitions) - 1]


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "date": _date(row.get("date")),
        "ticker": str(row.get("ticker") or "").upper(),
        "v2_direction": str(row.get("v2_direction") or "HOLD").upper(),
        "v2_score": _float(row.get("v2_score")) or 0.0,
        "regime": str(row.get("regime") or "TRANSITIONAL").upper(),
        "trend_score": _float(row.get("trend_score")) or 0.0,
        "reversion_score": _float(row.get("reversion_score")) or 0.0,
        "structural_break": _bool(row.get("structural_break")),
        "volume_quality_20": _float(row.get("volume_quality_20")),
        "source_mode": str(row.get("source_mode") or "unknown"),
        "return_20d": _float(row.get("return_20d")),
        "quality_20d": _bool(row.get("quality_20d")),
    }


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _float(value: Any) -> float | None:
    try:
        if value in {None, "", "nan", "NaN"}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _pct(value: float | None) -> str:
    return "pend." if value is None else f"{value:+.2%}"


__all__ = [
    "DEFAULT_ROUND_TRIP_COST",
    "TechnicalBuyShadowV3Audit",
    "build_technical_buy_shadow_v3_audit",
    "render_technical_buy_shadow_v3_audit",
]
