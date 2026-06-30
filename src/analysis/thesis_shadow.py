"""Independent price forecasts and position theses for shadow evaluation.

The forecast path deliberately consumes only canonical historical candles. A
separate context overlay can adjust thesis action/confidence using macro,
sentiment, cash and concentration without changing the price forecast itself.
It does not import or call the optimizer, execution planner, or order lifecycle.
Its output is evidence to evaluate before any live decision coupling is
considered.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from html import escape
from math import erf, exp, isfinite, log, sqrt
from statistics import fmean, pstdev
from typing import Any, Iterable, Mapping, Sequence


MODEL_VERSION = "price_trend_context_overlay_v2"
SCHEMA_VERSION = 1
FORECAST_HORIZONS = (5, 20, 40)
MIN_INPUT_SESSIONS = 80
PRIMARY_HORIZON = 20
PRICE_BASIS = "canonical_cocos"
MAX_AS_OF_LAG_DAYS = 7


@dataclass(frozen=True)
class HorizonForecast:
    horizon_sessions: int
    expected_return: float
    probability_up: float
    lower_return: float
    upper_return: float
    uncertainty: float
    confidence: float
    signal_strength: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizon_sessions": self.horizon_sessions,
            "expected_return": self.expected_return,
            "probability_up": self.probability_up,
            "lower_return": self.lower_return,
            "upper_return": self.upper_return,
            "uncertainty": self.uncertainty,
            "confidence": self.confidence,
            "signal_strength": self.signal_strength,
        }


@dataclass(frozen=True)
class ShadowThesis:
    ticker: str
    universe_role: str
    as_of_ts: datetime
    reference_price: float
    thesis_action: str
    thesis_confidence: float
    forecasts: tuple[HorizonForecast, ...]
    input_sessions: int
    feature_snapshot: dict[str, Any]
    rationale: tuple[str, ...]
    model_version: str = MODEL_VERSION
    schema_version: int = SCHEMA_VERSION
    price_basis: str = PRICE_BASIS

    def forecast_for(self, horizon_sessions: int) -> HorizonForecast:
        for forecast in self.forecasts:
            if forecast.horizon_sessions == horizon_sessions:
                return forecast
        raise KeyError(horizon_sessions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "universe_role": self.universe_role,
            "as_of_ts": self.as_of_ts.isoformat(),
            "reference_price": self.reference_price,
            "thesis_action": self.thesis_action,
            "thesis_confidence": self.thesis_confidence,
            "forecasts": [forecast.to_dict() for forecast in self.forecasts],
            "input_sessions": self.input_sessions,
            "feature_snapshot": self.feature_snapshot,
            "rationale": list(self.rationale),
            "model_version": self.model_version,
            "schema_version": self.schema_version,
            "price_basis": self.price_basis,
        }


@dataclass(frozen=True)
class MaturedOutcome:
    target_session_ts: datetime
    outcome_price: float
    realized_return: float
    direction_correct: bool
    absolute_error: float
    squared_error: float


@dataclass(frozen=True)
class ShadowContext:
    macro_score: float | None = None
    macro_regime: str | None = None
    macro_reasons: tuple[str, ...] = ()
    sentiment_score: float | None = None
    sentiment_confidence: float | None = None
    sentiment_event_count: int = 0
    sentiment_high_impact_count: int = 0
    sentiment_summary: str = ""
    cash_pct: float | None = None
    current_weight: float | None = None
    max_position_weight: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "macro_score": self.macro_score,
            "macro_regime": self.macro_regime,
            "macro_reasons": list(self.macro_reasons),
            "sentiment_score": self.sentiment_score,
            "sentiment_confidence": self.sentiment_confidence,
            "sentiment_event_count": self.sentiment_event_count,
            "sentiment_high_impact_count": self.sentiment_high_impact_count,
            "sentiment_summary": self.sentiment_summary,
            "cash_pct": self.cash_pct,
            "current_weight": self.current_weight,
            "max_position_weight": self.max_position_weight,
        }


def partition_fresh_theses(
    theses: Sequence[ShadowThesis],
    *,
    max_calendar_lag_days: int = MAX_AS_OF_LAG_DAYS,
) -> tuple[list[ShadowThesis], list[ShadowThesis]]:
    """Exclude assets whose last canonical candle is stale versus the run."""
    if not theses:
        return [], []
    latest_session = max(item.as_of_ts.date() for item in theses)
    fresh: list[ShadowThesis] = []
    stale: list[ShadowThesis] = []
    for thesis in theses:
        lag_days = (latest_session - thesis.as_of_ts.date()).days
        (fresh if lag_days <= int(max_calendar_lag_days) else stale).append(thesis)
    return fresh, stale


def build_shadow_thesis(
    ticker: str,
    candles: Sequence[Mapping[str, Any]],
    *,
    universe_role: str,
    context: ShadowContext | Mapping[str, Any] | None = None,
) -> ShadowThesis:
    """Build a causal 5/20/40-session forecast from candles available at T."""
    clean = _normalise_candles(candles)
    if len(clean) < MIN_INPUT_SESSIONS:
        raise ValueError(
            f"{ticker.upper()}: requires {MIN_INPUT_SESSIONS} sessions, got {len(clean)}"
        )

    closes = [row[1] for row in clean]
    log_prices = [log(value) for value in closes]
    daily_returns = [b - a for a, b in zip(log_prices, log_prices[1:])]
    daily_vol = max(_std(daily_returns[-60:]), 0.001)

    windows = (20, 60, 120)
    regressions: dict[int, dict[str, float]] = {}
    for requested_window in windows:
        window = min(requested_window, len(log_prices))
        slope, r_squared, residual_std = _linear_trend(log_prices[-window:])
        regressions[requested_window] = {
            "effective_window": float(window),
            "daily_log_slope": slope,
            "r_squared": r_squared,
            "residual_std": residual_std,
        }

    forecasts = tuple(
        _forecast_horizon(
            horizon,
            regressions=regressions,
            daily_vol=daily_vol,
            input_sessions=len(clean),
        )
        for horizon in FORECAST_HORIZONS
    )
    action, thesis_confidence, rationale = _classify_thesis(
        universe_role=universe_role,
        forecasts=forecasts,
    )
    context_obj = _normalise_context(context)
    action, thesis_confidence, context_rationale, context_payload = _apply_context_overlay(
        universe_role=universe_role,
        action=action,
        thesis_confidence=thesis_confidence,
        context=context_obj,
    )
    feature_snapshot = {
        "daily_log_volatility_60": round(daily_vol, 8),
        "trend_windows": {
            str(window): {
                key: round(value, 8)
                for key, value in values.items()
            }
            for window, values in regressions.items()
        },
        "input_start_ts": clean[0][0].isoformat(),
        "input_end_ts": clean[-1][0].isoformat(),
        "forecast_return_basis": "price_only",
        "context_overlay": context_payload,
    }
    return ShadowThesis(
        ticker=ticker.upper(),
        universe_role=_normalise_role(universe_role),
        as_of_ts=clean[-1][0],
        reference_price=round(closes[-1], 8),
        thesis_action=action,
        thesis_confidence=round(thesis_confidence, 4),
        forecasts=forecasts,
        input_sessions=len(clean),
        feature_snapshot=feature_snapshot,
        rationale=tuple(rationale + context_rationale),
    )


def mature_forecast(
    *,
    as_of_ts: datetime,
    reference_price: float,
    horizon_sessions: int,
    expected_return: float,
    future_candles: Sequence[Mapping[str, Any]],
) -> MaturedOutcome | None:
    """Evaluate only after N later market sessions, never after N calendar days."""
    as_of = _coerce_datetime(as_of_ts)
    future = [row for row in _normalise_candles(future_candles) if row[0] > as_of]
    if len(future) < int(horizon_sessions):
        return None
    target_ts, outcome_price = future[int(horizon_sessions) - 1]
    realised = outcome_price / float(reference_price) - 1.0
    error = realised - float(expected_return)
    direction_correct = (
        (expected_return > 0.0 and realised > 0.0)
        or (expected_return < 0.0 and realised < 0.0)
        or (expected_return == 0.0 and realised == 0.0)
    )
    return MaturedOutcome(
        target_session_ts=target_ts,
        outcome_price=round(outcome_price, 8),
        realized_return=round(realised, 8),
        direction_correct=direction_correct,
        absolute_error=round(abs(error), 8),
        squared_error=round(error * error, 10),
    )


def render_shadow_report(
    theses: Sequence[ShadowThesis],
    *,
    metrics: Sequence[Mapping[str, Any]] = (),
    candidate_limit: int = 10,
) -> str:
    positions = sorted(
        (item for item in theses if item.universe_role == "POSITION"),
        key=lambda item: item.ticker,
    )
    candidates = sorted(
        (item for item in theses if item.universe_role == "CANDIDATE"),
        key=lambda item: (
            item.forecast_for(PRIMARY_HORIZON).expected_return,
            item.forecast_for(PRIMARY_HORIZON).probability_up,
        ),
        reverse=True,
    )[: max(0, int(candidate_limit))]

    lines = [
        "SHADOW THESIS — NO EJECUTABLE",
        f"Modelo {MODEL_VERSION} | horizontes 5/20/40 ruedas | {len(theses)} activos",
        "",
        "Posiciones actuales:",
    ]
    lines.extend(_render_thesis_line(item) for item in positions)
    if not positions:
        lines.append("  Sin posiciones evaluables.")

    lines.extend(["", "Candidatos mejor pronosticados (20 ruedas):"])
    lines.extend(_render_thesis_line(item) for item in candidates)
    if not candidates:
        lines.append("  Sin candidatos evaluables.")

    if metrics:
        lines.extend(["", "Validacion historica madura:"])
        for row in sorted(metrics, key=lambda value: int(value["horizon_sessions"])):
            count = int(row.get("samples") or 0)
            accuracy = float(row.get("directional_accuracy") or 0.0)
            mae = float(row.get("mean_absolute_error") or 0.0)
            lines.append(
                f"  {int(row['horizon_sessions'])}r: n={count} "
                f"direccion={accuracy:.1%} MAE={mae:.1%}"
            )
    else:
        lines.extend(["", "Validacion historica: aun sin pronosticos maduros."])
    return "\n".join(lines)


def render_shadow_telegram_report(
    theses: Sequence[ShadowThesis],
    *,
    metrics: Sequence[Mapping[str, Any]] = (),
    candidate_limit: int = 8,
) -> str:
    """Render the latest shadow run as concise, valid Telegram HTML."""
    positions = sorted(
        (item for item in theses if item.universe_role == "POSITION"),
        key=lambda item: item.ticker,
    )
    candidates = sorted(
        (item for item in theses if item.universe_role == "CANDIDATE"),
        key=lambda item: (
            item.forecast_for(PRIMARY_HORIZON).expected_return,
            item.forecast_for(PRIMARY_HORIZON).probability_up,
        ),
        reverse=True,
    )[: max(0, int(candidate_limit))]
    latest_ts = max((item.as_of_ts for item in theses), default=None)
    data_date = latest_ts.strftime("%d/%m/%Y") if latest_ts else "sin datos"

    lines = [
        "🔬 <b>Tesis shadow 5/20/40</b>",
        "<i>Experimental · no ejecuta compras, ventas ni rebalanceos</i>",
        "",
        f"📅 Datos hasta: <b>{data_date}</b>",
        f"🔎 Activos evaluados: <b>{len(theses)}</b>",
        "",
        "<b>Tu cartera</b>",
    ]
    if positions:
        for thesis in positions:
            lines.extend(_render_telegram_thesis(thesis))
    else:
        lines.append("⚪ No hay posiciones evaluables en esta corrida.")

    lines.extend(["", "<b>Candidatos en vigilancia</b>"])
    if candidates:
        for index, thesis in enumerate(candidates, start=1):
            lines.extend(_render_telegram_thesis(thesis, rank=index))
    else:
        lines.append("⚪ No hay candidatos evaluables en esta corrida.")

    mature_samples = sum(int(row.get("samples") or 0) for row in metrics)
    lines.append("")
    if mature_samples:
        lines.append("<b>Validación disponible</b>")
        for row in sorted(metrics, key=lambda value: int(value["horizon_sessions"])):
            lines.append(
                f"• {int(row['horizon_sessions'])} ruedas: "
                f"dirección <b>{float(row.get('directional_accuracy') or 0.0):.0%}</b> "
                f"· error medio {float(row.get('mean_absolute_error') or 0.0):.1%} "
                f"· n={int(row.get('samples') or 0)}"
            )
    else:
        lines.append("⏳ <b>Validación:</b> todavía no hay pronósticos maduros.")
        lines.append("La primera medición llega después de 5 ruedas.")

    lines.extend(
        [
            "",
            "⚠️ <i>Las proyecciones extrapolan tendencia reciente. No son objetivos "
            "de precio ni recomendaciones hasta acumular resultados reales.</i>",
        ]
    )
    return "\n".join(lines)


def render_shadow_ticker_telegram_report(
    theses: Sequence[ShadowThesis],
    ticker: str,
    *,
    metrics: Sequence[Mapping[str, Any]] = (),
) -> str:
    """Render a single ticker from the latest persisted shadow run."""
    requested = str(ticker or "").strip().upper()
    thesis = next((item for item in theses if item.ticker.upper() == requested), None)
    if thesis is None:
        available = ", ".join(sorted(item.ticker for item in theses)[:20])
        suffix = f" Disponibles: {escape(available)}" if available else ""
        return (
            f"⚪ <b>Shadow {escape(requested or 'ticker')}</b>\n"
            "No encontré ese ticker en la última corrida shadow."
            f"{suffix}"
        )

    labels = {
        "HOLD": ("🟢", "MANTENER EN SHADOW"),
        "EXIT_WATCH": ("🔴", "VIGILAR SALIDA"),
        "REVIEW": ("🟡", "REVISAR"),
        "CONTEXT_REVIEW": ("🟡", "REVISAR CONTEXTO"),
        "ENTRY_WATCH": ("🔵", "VIGILAR ENTRADA"),
        "AVOID": ("⚫", "EVITAR"),
        "ABSTAIN": ("⚪", "SIN SEÑAL"),
    }
    icon, action_label = labels.get(thesis.thesis_action, ("⚪", thesis.thesis_action))
    context = thesis.feature_snapshot.get("context_overlay") or {}
    trend_windows = thesis.feature_snapshot.get("trend_windows") or {}
    flags = list(context.get("context_flags") or [])
    data_date = thesis.as_of_ts.strftime("%d/%m/%Y")
    input_start = str(thesis.feature_snapshot.get("input_start_ts") or "")[:10]
    input_end = str(thesis.feature_snapshot.get("input_end_ts") or "")[:10]

    def _pct(value: Any) -> str:
        try:
            return f"{float(value):+.1%}"
        except Exception:
            return "n/d"

    def _prob(value: Any) -> str:
        try:
            return f"{float(value):.0%}"
        except Exception:
            return "n/d"

    def _num(value: Any) -> str:
        try:
            rendered = f"{float(value):,.2f}"
            return rendered.replace(",", "_").replace(".", ",").replace("_", ".")
        except Exception:
            return "n/d"

    lines = [
        f"🔬 <b>Shadow · {escape(thesis.ticker)}</b>",
        "<i>Experimental · lectura por acción; no ejecuta órdenes</i>",
        "",
        f"{icon} <b>{escape(action_label)}</b> · confianza <b>{thesis.thesis_confidence:.0%}</b>",
        f"📅 Datos hasta: <b>{data_date}</b>",
        f"💵 Precio referencia: <b>${_num(thesis.reference_price)}</b>",
        f"📚 Ventana usada: <b>{thesis.input_sessions}</b> ruedas"
        + (f" ({escape(input_start)} → {escape(input_end)})" if input_start and input_end else ""),
        "",
        "<b>Proyección por horizonte</b>",
    ]

    for horizon in FORECAST_HORIZONS:
        forecast = thesis.forecast_for(horizon)
        lines.append(
            f"• {horizon}r: retorno <code>{forecast.expected_return:+.1%}</code> · "
            f"P+ <b>{forecast.probability_up:.0%}</b> · "
            f"rango <code>{forecast.lower_return:+.1%}/{forecast.upper_return:+.1%}</code> · "
            f"conf {forecast.confidence:.0%}"
        )

    lines.extend(["", "<b>Lectura del modelo</b>"])
    for item in thesis.rationale[:6]:
        lines.append(f"• {escape(str(item))}")

    lines.extend(["", "<b>Contexto overlay</b>"])
    if flags:
        for flag in flags[:6]:
            lines.append(f"• {escape(str(flag))}")
    else:
        lines.append("• Sin flags contextuales relevantes.")

    macro_score = context.get("macro_score")
    sentiment_score = context.get("sentiment_score")
    sentiment_conf = context.get("sentiment_confidence")
    if macro_score is not None:
        lines.append(f"• Macro score: <code>{float(macro_score):+.2f}</code>")
    if sentiment_score is not None:
        lines.append(
            f"• Sentiment: <code>{float(sentiment_score):+.2f}</code>"
            + (f" · conf {_prob(sentiment_conf)}" if sentiment_conf is not None else "")
        )
    if context.get("current_weight") is not None:
        lines.append(f"• Peso actual: <b>{_prob(context.get('current_weight'))}</b>")
    if context.get("cash_pct") is not None:
        lines.append(f"• Cash cartera: <b>{_prob(context.get('cash_pct'))}</b>")

    slopes = []
    for window in ("20", "60", "120"):
        values = trend_windows.get(window) or {}
        slope = values.get("daily_log_slope")
        r2 = values.get("r_squared")
        if slope is not None:
            slopes.append(
                f"{window}r {_pct(float(slope) * int(window))}"
                + (f" R² {float(r2):.2f}" if r2 is not None else "")
            )
    if slopes:
        lines.extend(["", "<b>Tendencia base precio</b>", "• " + escape(" · ".join(slopes))])

    mature_samples = sum(int(row.get("samples") or 0) for row in metrics)
    lines.append("")
    if mature_samples:
        lines.append(f"Validación shadow global disponible: <b>{mature_samples}</b> outcomes maduros.")
    else:
        lines.append("⏳ Todavía sin validación madura suficiente.")

    lines.extend(
        [
            "",
            "⚠️ <i>Price forecast y contexto son capas separadas. Esto no es objetivo "
            "de precio ni recomendación operativa.</i>",
        ]
    )
    return "\n".join(lines)


def _render_telegram_thesis(
    thesis: ShadowThesis,
    *,
    rank: int | None = None,
) -> list[str]:
    f5 = thesis.forecast_for(5)
    f20 = thesis.forecast_for(20)
    f40 = thesis.forecast_for(40)
    labels = {
        "HOLD": ("🟢", "MANTENER EN SHADOW"),
        "EXIT_WATCH": ("🔴", "VIGILAR SALIDA"),
        "REVIEW": ("🟡", "REVISAR"),
        "CONTEXT_REVIEW": ("🟡", "REVISAR CONTEXTO"),
        "ENTRY_WATCH": ("🔵", "VIGILAR ENTRADA"),
        "AVOID": ("⚫", "EVITAR"),
        "ABSTAIN": ("⚪", "SIN SEÑAL"),
    }
    icon, label = labels.get(thesis.thesis_action, ("⚪", thesis.thesis_action))
    prefix = f"{rank}. " if rank is not None else ""
    ticker = escape(thesis.ticker)
    lines = [
        f"{prefix}{icon} <b>{ticker}</b> · {escape(label)}",
        f"   Proyección: 5r <code>{f5.expected_return:+.1%}</code> · "
        f"20r <code>{f20.expected_return:+.1%}</code> · "
        f"40r <code>{f40.expected_return:+.1%}</code>",
        f"   Probabilidad positiva a 20r: <b>{f20.probability_up:.0%}</b>",
    ]
    context = thesis.feature_snapshot.get("context_overlay") or {}
    flags = list(context.get("context_flags") or [])
    if flags:
        lines.append(f"   Contexto: {escape('; '.join(flags[:2]))}")
    return lines


def _render_thesis_line(thesis: ShadowThesis) -> str:
    f5 = thesis.forecast_for(5)
    f20 = thesis.forecast_for(20)
    f40 = thesis.forecast_for(40)
    return (
        f"  {thesis.ticker}: {thesis.thesis_action} "
        f"| E[r] 5r {f5.expected_return:+.1%}, 20r {f20.expected_return:+.1%}, "
        f"40r {f40.expected_return:+.1%} | P(+20r) {f20.probability_up:.0%}"
    )


def _forecast_horizon(
    horizon: int,
    *,
    regressions: Mapping[int, Mapping[str, float]],
    daily_vol: float,
    input_sessions: int,
) -> HorizonForecast:
    weights_by_horizon = {
        5: {20: 0.50, 60: 0.35, 120: 0.15},
        20: {20: 0.25, 60: 0.45, 120: 0.30},
        40: {20: 0.15, 60: 0.35, 120: 0.50},
    }
    weights = weights_by_horizon[horizon]
    component_returns: list[float] = []
    weighted_log_return = 0.0
    weighted_r_squared = 0.0
    for window, weight in weights.items():
        values = regressions[window]
        reliability = 0.35 + 0.65 * _clip(values["r_squared"], 0.0, 1.0)
        component = values["daily_log_slope"] * horizon * reliability
        component_returns.append(component)
        weighted_log_return += weight * component
        weighted_r_squared += weight * values["r_squared"]

    weighted_log_return = _clip(weighted_log_return, -0.60, 0.60)
    disagreement = _std(component_returns)
    base_uncertainty = daily_vol * sqrt(horizon)
    uncertainty = max(sqrt(base_uncertainty**2 + disagreement**2), 0.005)
    probability_up = _normal_cdf(weighted_log_return / uncertainty)
    expected_return = exp(weighted_log_return) - 1.0
    lower_return = exp(weighted_log_return - 1.281552 * uncertainty) - 1.0
    upper_return = exp(weighted_log_return + 1.281552 * uncertainty) - 1.0

    direction_confidence = abs(probability_up - 0.5) * 2.0
    agreement = 1.0 - min(1.0, disagreement / max(uncertainty, 1e-9))
    data_quality = min(1.0, input_sessions / 120.0)
    confidence = (
        direction_confidence
        * (0.40 + 0.60 * weighted_r_squared)
        * (0.60 + 0.40 * agreement)
        * data_quality
    )
    if (probability_up >= 0.65 or probability_up <= 0.35) and confidence >= 0.45:
        strength = "HIGH"
    elif probability_up >= 0.58 or probability_up <= 0.42:
        strength = "MODERATE"
    else:
        strength = "NEUTRAL"

    return HorizonForecast(
        horizon_sessions=horizon,
        expected_return=round(expected_return, 8),
        probability_up=round(_clip(probability_up, 0.0, 1.0), 6),
        lower_return=round(lower_return, 8),
        upper_return=round(upper_return, 8),
        uncertainty=round(uncertainty, 8),
        confidence=round(_clip(confidence, 0.0, 1.0), 4),
        signal_strength=strength,
    )


def _classify_thesis(
    *,
    universe_role: str,
    forecasts: Iterable[HorizonForecast],
) -> tuple[str, float, list[str]]:
    role = _normalise_role(universe_role)
    by_horizon = {item.horizon_sessions: item for item in forecasts}
    medium = by_horizon[20]
    long = by_horizon[40]
    positive = (
        medium.expected_return >= 0.015
        and medium.probability_up >= 0.58
        and long.probability_up >= 0.52
    )
    negative = (
        medium.expected_return <= -0.015
        and medium.probability_up <= 0.42
        and long.probability_up <= 0.48
    )
    confidence = fmean((medium.confidence, long.confidence))

    if role == "POSITION":
        action = "HOLD" if positive else ("EXIT_WATCH" if negative else "REVIEW")
    else:
        action = "ENTRY_WATCH" if positive else ("AVOID" if negative else "ABSTAIN")

    rationale = [
        f"20-session expected return {medium.expected_return:+.2%}",
        f"20-session probability up {medium.probability_up:.1%}",
        f"40-session probability up {long.probability_up:.1%}",
        "shadow only; no sizing or order instruction",
    ]
    return action, confidence, rationale


def _normalise_context(context: ShadowContext | Mapping[str, Any] | None) -> ShadowContext:
    if context is None:
        return ShadowContext()
    if isinstance(context, ShadowContext):
        return context
    data = dict(context)
    return ShadowContext(
        macro_score=_optional_float(data.get("macro_score")),
        macro_regime=str(data.get("macro_regime") or "") or None,
        macro_reasons=tuple(str(item) for item in data.get("macro_reasons", ()) or ()),
        sentiment_score=_optional_float(data.get("sentiment_score")),
        sentiment_confidence=_optional_float(data.get("sentiment_confidence")),
        sentiment_event_count=int(data.get("sentiment_event_count") or 0),
        sentiment_high_impact_count=int(data.get("sentiment_high_impact_count") or 0),
        sentiment_summary=str(data.get("sentiment_summary") or ""),
        cash_pct=_optional_float(data.get("cash_pct")),
        current_weight=_optional_float(data.get("current_weight")),
        max_position_weight=_optional_float(data.get("max_position_weight")),
    )


def _apply_context_overlay(
    *,
    universe_role: str,
    action: str,
    thesis_confidence: float,
    context: ShadowContext,
) -> tuple[str, float, list[str], dict[str, Any]]:
    role = _normalise_role(universe_role)
    adjusted_action = action
    adjusted_confidence = float(thesis_confidence)
    rationale: list[str] = []
    flags: list[str] = []

    macro_score = context.macro_score
    if macro_score is not None:
        if macro_score <= -0.20:
            flags.append(f"macro adverso {macro_score:+.2f}")
            rationale.append(f"macro overlay adverse {macro_score:+.2f}")
            adjusted_confidence *= 0.85
            if adjusted_action in {"HOLD", "ENTRY_WATCH"}:
                adjusted_action = "CONTEXT_REVIEW"
        elif macro_score >= 0.20:
            flags.append(f"macro favorable {macro_score:+.2f}")
            rationale.append(f"macro overlay favorable {macro_score:+.2f}")
            adjusted_confidence = min(1.0, adjusted_confidence + 0.05)

    sentiment_score = context.sentiment_score
    sentiment_confidence = context.sentiment_confidence or 0.0
    if sentiment_score is not None and sentiment_confidence >= 0.20:
        if sentiment_score <= -0.12:
            flags.append(f"sentiment negativo {sentiment_score:+.2f}")
            rationale.append(f"sentiment overlay negative {sentiment_score:+.2f}")
            adjusted_confidence *= 0.80
            if adjusted_action in {"HOLD", "ENTRY_WATCH"}:
                adjusted_action = "CONTEXT_REVIEW"
        elif sentiment_score >= 0.12:
            flags.append(f"sentiment positivo {sentiment_score:+.2f}")
            rationale.append(f"sentiment overlay positive {sentiment_score:+.2f}")
            adjusted_confidence = min(1.0, adjusted_confidence + 0.08)

    if role == "CANDIDATE" and context.cash_pct is not None:
        if context.cash_pct < 0.03 and adjusted_action == "ENTRY_WATCH":
            flags.append(f"cash bajo {context.cash_pct:.1%}")
            rationale.append("cash overlay blocks entry watch from becoming actionable")
            adjusted_action = "CONTEXT_REVIEW"
            adjusted_confidence *= 0.90
        elif context.cash_pct >= 0.10:
            flags.append(f"cash disponible {context.cash_pct:.1%}")
            adjusted_confidence = min(1.0, adjusted_confidence + 0.03)

    if role == "POSITION" and context.current_weight is not None:
        if context.current_weight >= 0.35 and adjusted_action == "HOLD":
            flags.append(f"concentración alta {context.current_weight:.1%}")
            rationale.append("concentration overlay requires review despite positive price trend")
            adjusted_action = "CONTEXT_REVIEW"
            adjusted_confidence *= 0.90
        elif context.current_weight >= 0.25:
            flags.append(f"peso relevante {context.current_weight:.1%}")
            rationale.append("position concentration included in shadow context")

    if context.macro_regime:
        flags.append(f"régimen {context.macro_regime}")

    context_payload = context.to_dict()
    context_payload["context_flags"] = flags
    context_payload["price_forecast_adjusted"] = False
    context_payload["action_before_context"] = action
    context_payload["action_after_context"] = adjusted_action
    context_payload["confidence_before_context"] = round(float(thesis_confidence), 4)
    context_payload["confidence_after_context"] = round(_clip(adjusted_confidence, 0.0, 1.0), 4)

    if not rationale:
        rationale.append("context overlay neutral")

    return adjusted_action, _clip(adjusted_confidence, 0.0, 1.0), rationale, context_payload


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _normalise_candles(
    candles: Sequence[Mapping[str, Any]],
) -> list[tuple[datetime, float]]:
    by_session: dict[date, tuple[datetime, float]] = {}
    for row in candles:
        raw_price = row.get("close_price", row.get("close"))
        raw_ts = row.get("ts", row.get("date"))
        if raw_price is None or raw_ts is None:
            continue
        try:
            price = float(raw_price)
            ts = _coerce_datetime(raw_ts)
        except (TypeError, ValueError):
            continue
        if not isfinite(price) or price <= 0.0:
            continue
        previous = by_session.get(ts.date())
        if previous is None or ts > previous[0]:
            by_session[ts.date()] = (ts, price)
    return sorted(by_session.values(), key=lambda item: item[0])


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _linear_trend(values: Sequence[float]) -> tuple[float, float, float]:
    count = len(values)
    if count < 3:
        return 0.0, 0.0, 0.0
    x_mean = (count - 1) / 2.0
    y_mean = fmean(values)
    denominator = sum((index - x_mean) ** 2 for index in range(count))
    slope = sum(
        (index - x_mean) * (value - y_mean)
        for index, value in enumerate(values)
    ) / denominator
    intercept = y_mean - slope * x_mean
    residuals = [
        value - (intercept + slope * index)
        for index, value in enumerate(values)
    ]
    ss_res = sum(value * value for value in residuals)
    ss_tot = sum((value - y_mean) ** 2 for value in values)
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
    return slope, _clip(r_squared, 0.0, 1.0), sqrt(ss_res / max(1, count - 2))


def _std(values: Sequence[float]) -> float:
    return pstdev(values) if len(values) >= 2 else 0.0


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + erf(value / sqrt(2.0)))


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _normalise_role(value: str) -> str:
    role = str(value or "").upper()
    if role not in {"POSITION", "CANDIDATE"}:
        raise ValueError(f"unsupported universe role: {value}")
    return role
