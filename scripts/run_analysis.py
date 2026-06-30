"""
scripts/run_analysis.py — Pipeline cuantitativo completo.

Flujo:
  1.  Carga posiciones desde DB (o override por CLI)
  2.  Descarga macro (yfinance + APIs Argentina)
  3.  Análisis técnico multicapa (todos los tickers)
  4.  Risk engine por posición
  5.  Sentiment (RSS, opcional)
  6.  Síntesis: blend de capas → score + decisión + conviction
  7.  LLM: razonamiento explicativo (no modifica decisión)
  8.  Universo Cocos: escaneo de candidatos fuera de cartera
  9.  Portfolio Optimizer (Black-Litterman / Min-Variance)
  9.5 Execution Plan: reconcilia fondos, genera órdenes ejecutables
  9.6 Guardar trades en decision_log
  10. IC histórico
  11. Render HTML → stdout (Telegram lo captura)

Regla de oro del render:
  Las secciones operativas (acción principal, plan de rotación, veredicto)
  leen EXCLUSIVAMENTE de ExecutionPlan — nunca de PortfolioTarget.
  El bloque OPTIMIZER es solo informativo.

Output:
  - Todo el logging va a stderr
  - Solo print(report) va a stdout — el bot captura esto

Uso:
  python scripts/run_analysis.py
  python scripts/run_analysis.py --tickers CVX NVDA
  python scripts/run_analysis.py --no-llm --skip-radar
  python scripts/run_analysis.py --no-llm --skip-radar --no-persist
  python scripts/run_analysis.py --no-telegram
  python scripts/run_analysis.py --no-optimizer
"""
from __future__ import annotations

import argparse
import asyncio
import json as _json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from datetime import datetime, time, timedelta, timezone
from html import escape
from types import SimpleNamespace
from uuid import uuid4

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.technical import (
    analyze_portfolio_from_frames,
)
from src.analysis.macro import fetch_macro, score_macro_for_ticker, get_macro_regime
from src.analysis.sentiment import fetch_sentiment
from src.analysis.risk import build_portfolio_risk_report
from src.analysis.synthesis import SynthesisResult, LayerScore, blend_scores, synthesize_with_llm_local
from src.analysis.optimizer import run_optimizer
from src.analysis.execution_planner import (
    derive_decision_intents,
    reconcile_funding,
    build_signals_from_synthesis,
    build_positions_from_snapshot,
    ExecutionPlan,
    MIN_TRADE_ARS,
)
from src.analysis.manual_market_events import (
    ManualMarketEvent,
    active_event_risk_by_ticker,
    manual_event_layers_for_ticker,
    render_manual_market_events_html,
)
from src.analysis.opportunity_screener import (
    CandidateStatus,
    OpportunityReport,
    TradeType,
    run_opportunity_analysis,
)
from src.analysis.signal_aggregator import load_sentiment_contexts
from src.analysis.enums import DecisionType
from src.analysis.validators import (
    validate_execution_plan,
    validate_report_consistency,
    soft_validate,
    PlanValidationError,
)
from src.core.telegram_format import (
    header as tg_header,
    note as tg_note,
    section as tg_section,
    validate_telegram_html,
)
from src.analysis.risk_levels import compute_risk_levels
from src.analysis.audit_scope import (
    ART_TZ,
    classify_decision_audit_scope,
    ensure_decision_audit_scope_columns,
    is_art_business_day,
    is_regular_market_session,
    run_id_to_db,
)
from src.collector.cocos_history import candles_to_frame
from src.collector.portfolio_quality import (
    PRICE_STATUS_FRESH,
    is_position_operable,
    normalize_positions_with_fresh_market_prices,
    price_discrepancy_warnings,
)

logger = get_logger(__name__)

LAYER_WEIGHTS = {"technical": 0.30, "macro": 0.30, "risk": 0.25, "sentiment": 0.15}

# ══════════════════════════════════════════════════════════════════════════════
# UTILIDADES DE RENDER
# ══════════════════════════════════════════════════════════════════════════════

from dataclasses import dataclass
from typing import Optional


def _get(obj, name, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _money_ars(x: float) -> str:
    try:
        return f"${float(x):,.0f} ARS".replace(",", ".")
    except Exception:
        return "$0 ARS"


def _pct(x: float) -> str:
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "0.0%"


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=ART_TZ)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _fmt_dt_art(value, fmt: str = "%d/%m %H:%M") -> str:
    dt = _parse_dt(value)
    if not dt:
        return "N/A"
    return dt.astimezone(ART_TZ).strftime(fmt)


def _render_analysis_data_context(
    portfolio_snapshot: dict | None,
    latest_broker_movement: dict | None,
) -> list[str]:
    lines: list[str] = []
    if portfolio_snapshot:
        scraped_at = portfolio_snapshot.get("scraped_at")
        confidence = portfolio_snapshot.get("confidence_score")
        conf_txt = ""
        try:
            conf_txt = f" | conf {float(confidence):.2f}"
        except Exception:
            pass
        lines.append(f"Datos usados: snapshot portfolio <b>{_fmt_dt_art(scraped_at)}</b>{conf_txt}.")
        stale_reason = str(portfolio_snapshot.get("_stale_reason") or "").strip()
        if stale_reason:
            lines.append(
                "⚠️ Snapshot portfolio <b>stale</b>: "
                f"{escape(stale_reason)}. No se persisten decisiones formales."
            )

    if latest_broker_movement:
        side = escape(str(latest_broker_movement.get("movement_type") or "").upper())
        ticker = escape(str(latest_broker_movement.get("ticker") or "").upper())
        created_at = latest_broker_movement.get("created_at")
        precision = str(latest_broker_movement.get("executed_at_precision") or "").upper()
        lines.append(
            f"Ultimo movimiento real detectado: <b>{ticker} {side}</b> "
            f"({_fmt_dt_art(created_at)})."
        )
        if precision == "DATE_ONLY":
            lines.append("Hora broker: <b>DATE_ONLY</b>; Cocos informa fecha, no hora intradia exacta.")

    return lines


def _portfolio_snapshot_stale_reason(
    portfolio_snapshot: dict | None,
    now: datetime | None = None,
    *,
    max_age_minutes: int = 60,
) -> str | None:
    if not portfolio_snapshot:
        return "no hay snapshot de portfolio disponible"

    scraped_at = _parse_dt(portfolio_snapshot.get("scraped_at"))
    if not scraped_at:
        return "scraped_at ausente o inválido"

    current = now or datetime.now(ART_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=ART_TZ)
    current = current.astimezone(ART_TZ)
    scraped_art = scraped_at.astimezone(ART_TZ)

    if scraped_art.date() != current.date():
        return (
            f"último snapshot {scraped_art.strftime('%d/%m %H:%M')} ART; "
            f"rueda actual {current.strftime('%d/%m')}"
        )

    age_minutes = max(0.0, (current - scraped_art).total_seconds() / 60.0)
    if age_minutes > max_age_minutes:
        return (
            f"último snapshot {scraped_art.strftime('%H:%M')} ART; "
            f"antigüedad {age_minutes:.0f} min"
        )

    return None


def _render_manual_event_position_exposure(
    events: list[ManualMarketEvent] | None,
    positions: list | None,
    total_ars: float,
    *,
    compact: bool = False,
) -> list[str]:
    active_events = list(events or [])
    if not active_events or not positions:
        return []

    impacted: list[tuple[str, float, float, ManualMarketEvent]] = []
    for position in positions or []:
        ticker = str(_get(position, "ticker", "") or "").upper().strip()
        if not ticker:
            continue
        market_value = float(_get(position, "market_value", 0.0) or 0.0)
        weight = market_value / total_ars if total_ars > 0 and market_value > 0 else 0.0
        for event in active_events:
            if ticker in event.impacted_tickers:
                impacted.append((ticker, weight, market_value, event))
                break

    if not impacted:
        return []

    impacted.sort(key=lambda item: item[1], reverse=True)

    if compact:
        ticker, weight, _, event = impacted[0]
        suffix = f" (+{len(impacted) - 1})" if len(impacted) > 1 else ""
        return [
            "🔴 <b>Exposición bajo evento</b>: "
            f"{escape(ticker)} {_pct(weight)} afectado por {escape(event.title)}; "
            f"policy <b>{escape(event.action_policy)}</b>{suffix}."
        ]

    lines = ["🔴 <b>Exposición actual bajo catalyst</b>"]
    for ticker, weight, market_value, event in impacted[:5]:
        policy = (
            "bloquea compras nuevas"
            if event.action_policy == "block_new_buys"
            else event.action_policy.replace("_", " ")
        )
        concentration = " | concentración alta" if weight >= 0.25 else ""
        lines.append(
            f"   • <b>{escape(ticker)}</b>: {_pct(weight)} "
            f"({_money_ars(market_value)}) afectado por <b>{escape(event.title)}</b> "
            f"| policy: <b>{escape(policy)}</b>{concentration}"
        )
    lines.append(
        "     Esto no fuerza venta automática, pero sí exige tesis explícita antes de agregar riesgo."
    )
    return lines


def _rankdata(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    n = arr.size
    if n == 0:
        return np.array([], dtype=float)

    order = np.argsort(arr, kind="mergesort")
    sorted_vals = arr[order]
    ranks_sorted = np.zeros(n, dtype=float)

    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_vals[j + 1] == sorted_vals[i]:
            j += 1

        avg_rank = (i + j + 2) / 2.0
        ranks_sorted[i:j + 1] = avg_rank
        i = j + 1

    ranks = np.empty(n, dtype=float)
    ranks[order] = ranks_sorted
    return ranks


def _safe_corr(x_vals: list[float], y_vals: list[float]) -> float | None:
    if len(x_vals) < 5 or len(y_vals) < 5:
        return None

    x = np.asarray(x_vals, dtype=float)
    y = np.asarray(y_vals, dtype=float)

    if x.size != y.size or x.size < 5:
        return None

    if np.std(x) < 1e-12 or np.std(y) < 1e-12:
        return None

    corr = float(np.corrcoef(x, y)[0, 1])

    if not np.isfinite(corr):
        return None

    return corr


def _ic_label(ic: float | None) -> str:
    if ic is None:
        return "SIN DATOS"

    a = abs(ic)

    if a >= 0.10:
        return "FUERTE"
    if a >= 0.05:
        return "MODERADO"
    if a >= 0.02:
        return "DÉBIL"

    return "NULO"


@dataclass
class ICTextRegime:
    label: str
    icon: str
    note: str


def _build_ic_text_regime(
    ic_5d: Optional[float] = None,
    ic_10d: Optional[float] = None,
) -> ICTextRegime:
    """
    Régimen textual de IC.

    Importante:
    Esto NO cambia todavía los thresholds del planner.
    Solo explica el comportamiento conservador cuando el IC viene negativo.
    """
    values = [x for x in (ic_5d, ic_10d) if x is not None]

    if not values:
        return ICTextRegime(
            label="SIN DATOS",
            icon="⚪",
            note="IC no disponible: el sistema mantiene postura conservadora.",
        )

    worst_ic = min(values)

    if worst_ic < -0.10:
        return ICTextRegime(
            label="CAUTELA ALTA",
            icon="🔴",
            note=(
                "IC negativo fuerte: el sistema evita rotaciones con señales débiles "
                "y prioriza mantener posiciones salvo señal clara."
            ),
        )

    if worst_ic < -0.05:
        return ICTextRegime(
            label="CAUTELA",
            icon="🟡",
            note=(
                "IC negativo: el sistema reduce agresividad y evita operar señales débiles."
            ),
        )

    if worst_ic < 0:
        return ICTextRegime(
            label="NEUTRAL CON ADVERTENCIA",
            icon="🟡",
            note="IC levemente negativo: las señales se interpretan con cautela.",
        )

    return ICTextRegime(
        label="NORMAL",
        icon="✅",
        note="IC positivo: el sistema mantiene umbrales normales de operación.",
    )


def _classify_signal_label(score: Optional[float]) -> str:
    """
    Convierte score numérico en label operativo para mostrar en el reporte.

    Score = dirección/fuerza cuantitativa.
    Señal = interpretación operativa.
    """
    if score is None:
        return "SIN DATOS"

    if score >= 0.12:
        return "POSITIVA FUERTE"
    if score >= 0.08:
        return "POSITIVA OPERABLE"
    if score >= 0.05:
        return "POSITIVA DÉBIL"
    if score > -0.05:
        return "NEUTRAL / RUIDO"
    if score > -0.08:
        return "NEGATIVA DÉBIL"

    return "NEGATIVA OPERABLE"


def _agreement_bar(value: float, width: int = 5) -> str:
    value = max(0.0, min(1.0, float(value or 0.0)))
    filled = round(value * width)
    return "█" * filled + "░" * (width - filled)


def _agreement_label(agreement: float) -> str:
    if agreement >= 0.75:
        return "ALTO"
    if agreement >= 0.45:
        return "PARCIAL"
    return "BAJO"


def _count_assets_by_type(assets: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for asset in assets:
        asset_type = str(asset.get("asset_type", "") or "UNKNOWN").upper()
        counts[asset_type] = counts.get(asset_type, 0) + 1
    return counts


def _calculate_agreement_from_layers(
    technical: float = 0.0,
    macro: float = 0.0,
    sentiment: float = 0.0,
    momentum: Optional[float] = None,
    datos_frescos: bool = True,
) -> float:
    """
    Calcula 'Acuerdo capas' sin inflarlo artificialmente.

    No mide fuerza operativa.
    Mide cuántas capas acompañan positivamente.

    Sistema:
    - Técnico positivo: +30
    - Macro positivo: +20
    - Momentum positivo: +20, si existe
    - Sentiment positivo: +10, si existe y no es 0
    - Datos no frescos: -20

    Base:
    - Técnico + macro: 50
    - Si hay momentum: +20
    - Si hay sentiment real: +10
    """
    points = 0
    base = 50

    if technical > 0:
        points += 30

    if macro > 0:
        points += 20

    if momentum is not None:
        base += 20
        if momentum > 0:
            points += 20

    # sentiment = 0.0 se interpreta como "sin señal", no como positivo.
    if sentiment is not None and abs(sentiment) > 0.0001:
        base += 10
        if sentiment > 0:
            points += 10

    if not datos_frescos:
        points -= 20

    if base <= 0:
        return 0.0

    return max(0.0, min(1.0, points / base))


def _render_signal_line(
    score: Optional[float],
    technical: float = 0.0,
    macro: float = 0.0,
    sentiment: float = 0.0,
    momentum: Optional[float] = None,
) -> str:
    """
    Nueva línea de señal.

    Antes:
      Score: +0.015 | Conv: ALTA (100%) [█████]

    Ahora:
      Score: +0.015 | Señal: NEUTRAL / RUIDO | Acuerdo capas: PARCIAL (40%) [██░░░]
    """
    signal_label = _classify_signal_label(score)

    agreement = _calculate_agreement_from_layers(
        technical=technical,
        macro=macro,
        sentiment=sentiment,
        momentum=momentum,
    )

    agreement_pct = agreement * 100
    agreement_lbl = _agreement_label(agreement)
    score_txt = "N/A" if score is None else f"{score:+.3f}"

    return (
        f"Score: <code>{score_txt}</code> | "
        f"Señal: <b>{signal_label}</b> | "
        f"Acuerdo capas: <b>{agreement_lbl}</b> "
        f"({agreement_pct:.0f}%) [{_agreement_bar(agreement)}]"
    )


def _layer_map(result) -> dict:
    out = {}
    for l in getattr(result, "layers", []) or []:
        name = getattr(l, "name", None)
        if name:
            out[name] = l
    return out


def _layer_weighted(result, name: str) -> float:
    l = _layer_map(result).get(name)
    return float(getattr(l, "weighted", 0.0)) if l else 0.0


def _technical_source_label(result) -> str:
    mode = str(getattr(result, "technical_candle_source_mode", "unknown") or "unknown")
    counts = getattr(result, "technical_candle_source_counts", {}) or {}
    if not counts:
        return mode
    detail = ", ".join(
        f"{source} {int(count)}"
        for source, count in sorted(counts.items())
    )
    return f"{mode} ({detail})"


def _technical_data_quality(result) -> tuple[str, str]:
    counts = getattr(result, "technical_candle_source_counts", {}) or {}
    counts = {str(k): int(v or 0) for k, v in dict(counts).items()}
    total = sum(counts.values())
    if total <= 0:
        return "SIN_DATOS", "sin velas atribuibles"

    canonical = counts.get("COCOS", 0) + counts.get("TRADINGVIEW_BYMA", 0)
    internal = counts.get("internal_snapshot", 0)
    internal_share = internal / total if total else 0.0
    mode = str(getattr(result, "technical_candle_source_mode", "unknown") or "unknown")

    if canonical >= 200 and internal_share <= 0.10:
        level = "ALTA"
        reason = f"{canonical}/{total} velas canonicas; fallback {internal_share:.0%}"
    elif canonical >= 60 and internal_share <= 0.25:
        level = "MEDIA"
        reason = f"{canonical}/{total} velas canonicas; fallback {internal_share:.0%}"
    elif canonical >= 60:
        level = "MEDIA"
        reason = f"historia suficiente, pero fallback relevante ({internal_share:.0%})"
    else:
        level = "BAJA"
        reason = f"solo {canonical} velas canonicas; fuente {mode}"

    return level, reason


def _technical_source_summary(results) -> str:
    counts: dict[str, int] = {}
    for result in results or []:
        mode = str(
            getattr(result, "technical_candle_source_mode", "unknown") or "unknown"
        )
        counts[mode] = counts.get(mode, 0) + 1

    if not counts:
        return "sin datos"

    return " | ".join(
        f"{mode} {count}"
        for mode, count in sorted(counts.items())
    )


def _component_reason(result) -> tuple[str, str]:
    tech = _layer_weighted(result, "technical")
    macro = _layer_weighted(result, "macro")
    sent = _layer_weighted(result, "sentiment")

    positives, negatives = [], []

    if tech > 0.02:
        positives.append("técnico")
    elif tech < -0.02:
        negatives.append("técnico")

    if macro > 0.02:
        positives.append("macro")
    elif macro < -0.02:
        negatives.append("macro")

    if sent > 0.02:
        positives.append("sentiment")
    elif sent < -0.02:
        negatives.append("sentiment")

    if positives and negatives:
        lectura = f"{positives[0]} ayuda, pero {negatives[0]} frena"
    elif positives:
        lectura = f"{positives[0]} sostiene la señal"
    elif negatives:
        lectura = f"{negatives[0]} domina en contra"
    else:
        lectura = "señal plana sin ventaja clara"

    mags = {
        "technical": abs(tech),
        "macro": abs(macro),
        "sentiment": abs(sent),
    }

    top = max(mags, key=mags.get)
    top_val = {
        "technical": tech,
        "macro": macro,
        "sentiment": sent,
    }[top]

    if top_val > 0.02:
        motivo = f"{top} favorable"
    elif top_val < -0.02:
        motivo = f"{top} en contra"
    else:
        motivo = "sin ventaja clara"

    return lectura, motivo


def _append_ic_section(h: list[str], ic_metrics: dict | None) -> None:
    ic_data = ic_metrics or {}
    if not ic_data.get("has_data"):
        return

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>DIAGNÓSTICO DEL SISTEMA</b>")

    by_h = ic_data.get("by_horizon", {}) or {}
    ic_5d = None
    ic_10d = None

    for hz in ("5d", "10d", "20d"):
        v = by_h.get(hz, {})
        n_obs = int(v.get("n_obs", 0) or 0)
        n_tk = int(v.get("n_tickers", 0) or 0)
        ic = v.get("ic", None)
        ric = v.get("rank_ic", None)

        if ic is None or n_obs < 5:
            continue

        if hz == "5d":
            ic_5d = ic
        elif hz == "10d":
            ic_10d = ic

        q = v.get("quality", "NULO")
        h.append(
            f"{hz}: IC <code>{ic:+.3f}</code> | "
            f"Rank IC <code>{(ric or 0.0):+.3f}</code> | "
            f"n={n_obs} ({n_tk} tickers) | <b>{q}</b>"
        )

    ic_regime = _build_ic_text_regime(ic_5d=ic_5d, ic_10d=ic_10d)
    h.append(
        f"{ic_regime.icon} Régimen IC: <b>{ic_regime.label}</b> — "
        f"{escape(ic_regime.note)}"
    )
    h.append("")


def _result_map(results) -> dict[str, object]:
    return {
        str(getattr(result, "ticker", "")).upper(): result
        for result in (results or [])
        if str(getattr(result, "ticker", "") or "").strip()
    }


def _best_non_executable_candidate(plan, results):
    if not plan:
        return None, None

    by_ticker = _result_map(results)
    candidates = []

    for order in getattr(plan, "blocked_orders", []) or []:
        ticker = str(getattr(order, "ticker", "") or "").upper()
        result = by_ticker.get(ticker)
        score = (
            float(
                getattr(result, "final_score", getattr(result, "score", 0.0))
                or 0.0
            )
            if result is not None
            else 0.0
        )
        candidates.append((score, order, result))

    if not candidates:
        return None, None

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, order, result = candidates[0]
    return order, result


def _current_weights(positions: list[dict], total_ars: float) -> dict[str, float]:
    denom = float(total_ars) if total_ars else 1.0

    return {
        str(p.get("ticker", "")).upper(): float(p.get("market_value", 0.0) or 0.0) / denom
        for p in positions or []
        if str(p.get("ticker", ""))
    }


def _normalize_conviction(x) -> float:
    try:
        if x is None:
            return 0.0

        x = float(x)
        return max(0.0, min(1.0, x / 100.0 if x > 1.0 else x))
    except Exception:
        return 0.0


def _extract_conviction(result) -> float:
    """
    Se mantiene por compatibilidad con radar / ordenamientos viejos.
    Ya no se muestra como 'Conv' en cartera actual.
    """
    for key in ("conviction", "confidence", "confidence_pct", "conviction_pct"):
        val = _get(result, key, None)
        if val is not None:
            return _normalize_conviction(val)

    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# INFORMATION COEFFICIENT
# ══════════════════════════════════════════════════════════════════════════════

async def _compute_information_coefficient(
    cfg,
    tickers: list[str],
    lookback_days: int = 180,
    owner_chat_id: int | None = None,
) -> dict:
    db = PortfolioDatabase(cfg.database.url)
    cutoff = datetime.now() - timedelta(days=lookback_days)
    horizons = ("5d", "10d", "20d")
    metrics = {"lookback_days": lookback_days, "by_horizon": {}, "has_data": False}

    try:
        await db.connect()
        pool = await db.get_pool()
        if not pool:
            return metrics

        ticker_filter = [str(t).upper() for t in (tickers or []) if str(t).strip()]
        async with pool.acquire() as conn:
            if ticker_filter:
                rows = await conn.fetch(
                    """
                    SELECT ticker, final_score, outcome_5d, outcome_10d, outcome_20d
                    FROM decision_log
                    WHERE decided_at >= $1
                      AND ticker = ANY($2::text[])
                      AND ($3::bigint IS NULL OR owner_chat_id = $3)
                      AND decision != 'HOLD'
                    """,
                    cutoff, ticker_filter, owner_chat_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT ticker, final_score, outcome_5d, outcome_10d, outcome_20d
                    FROM decision_log
                    WHERE decided_at >= $1
                      AND ($2::bigint IS NULL OR owner_chat_id = $2)
                      AND decision != 'HOLD'
                    """,
                    cutoff, owner_chat_id,
                )

        for hz in horizons:
            k = f"outcome_{hz}"
            xs, ys = [], []
            covered = set()
            for r in rows:
                score = r["final_score"]
                out   = r[k]
                if score is None or out is None:
                    continue
                score = float(score); out = float(out)
                if not np.isfinite(score) or not np.isfinite(out):
                    continue
                xs.append(score); ys.append(out)
                covered.add(str(r["ticker"]).upper())

            pearson  = _safe_corr(xs, ys)
            rank_ic  = _safe_corr(
                _rankdata(np.asarray(xs)), _rankdata(np.asarray(ys))
            ) if len(xs) >= 5 else None
            metrics["by_horizon"][hz] = {
                "ic": pearson, "rank_ic": rank_ic,
                "n_obs": len(xs), "n_tickers": len(covered),
                "quality": _ic_label(pearson),
            }

        primary = metrics["by_horizon"].get("5d", {})
        metrics["primary_horizon"] = "5d"
        metrics["primary_ic"]      = primary.get("ic")
        metrics["primary_rank_ic"] = primary.get("rank_ic")
        metrics["primary_n_obs"]   = primary.get("n_obs", 0)
        metrics["has_data"]        = any(
            (v.get("n_obs", 0) >= 5) for v in metrics["by_horizon"].values()
        )
        return metrics
    except Exception as e:
        logger.warning(f"IC: no se pudo calcular ({e})")
        return metrics
    finally:
        try:
            await db.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# GUARDAR DECISIONES EN DECISION_LOG
# ══════════════════════════════════════════════════════════════════════════════


def _layers_payload_for_decision(result, extra: dict | None = None) -> dict:
    """
    Payload completo para decision_log.layers.

    Antes se guardaba solo:
        {"source": "optimizer", "delta_pct": ...}

    Ahora se conserva esa metadata, pero también se guardan las capas reales
    del análisis para que regression_audit.py pueda medir qué aporta cada una.
    """
    payload = dict(extra or {})

    def _safe_float(x, default: float = 0.0) -> float:
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    def _find_layer(name: str):
        lname_target = name.lower()

        for layer in getattr(result, "layers", []) or []:
            lname = str(getattr(layer, "name", "") or "").lower()
            if lname == lname_target:
                return layer

        return None

    def _layer_payload(name: str) -> dict:
        layer = _find_layer(name)

        if not layer:
            if name.lower() == "sentiment" and not bool(getattr(result, "sentiment_active", True)):
                return {
                    "weighted": 0.0,
                    "raw": 0.0,
                    "weight": 0.0,
                    "reason": "sentiment_off",
                    "active": False,
                    "redistributed_to": ["technical", "macro"],
                }
            return {
                "weighted": 0.0,
                "raw": 0.0,
                "weight": 0.0,
                "reason": "layer_missing",
            }

        return {
            "weighted": _safe_float(getattr(layer, "weighted", 0.0)),
            "raw": _safe_float(getattr(layer, "raw_score", getattr(layer, "score", 0.0))),
            "weight": _safe_float(getattr(layer, "weight", 0.0)),
            "reason": str(getattr(layer, "reason", "") or ""),
        }

    payload.setdefault("source", "optimizer")

    if result is None:
        payload["technical"] = {"weighted": 0.0, "raw": 0.0, "weight": 0.0, "reason": "result_missing"}
        payload["macro"] = {"weighted": 0.0, "raw": 0.0, "weight": 0.0, "reason": "result_missing"}
        payload["sentiment"] = {"weighted": 0.0, "raw": 0.0, "weight": 0.0, "reason": "result_missing"}
        payload["risk"] = {"weighted": 0.0, "raw": 0.0, "weight": 0.0, "reason": "result_missing"}
        payload["technical_data_source_mode"] = "unknown"
        payload["technical_has_reconstructed_candles"] = False
        payload["technical_candle_sources"] = []
        payload["technical_candle_source_counts"] = {}
        return payload

    payload["technical"] = _layer_payload("technical")
    payload["macro"] = _layer_payload("macro")
    payload["sentiment"] = _layer_payload("sentiment")
    payload["risk"] = _layer_payload("risk")
    payload["sentiment_active"] = bool(getattr(result, "sentiment_active", True))
    sentiment_context = getattr(result, "sentiment_context", None)
    if sentiment_context is not None and hasattr(sentiment_context, "to_layers_payload"):
        context_payload = sentiment_context.to_layers_payload()
        context_payload["used_in_score"] = payload["sentiment_active"]
        context_payload["input_mode"] = (
            "weighted_input" if payload["sentiment_active"] else "disabled"
        )
        context_payload["reason"] = (
            "used_as_sentiment_layer"
            if payload["sentiment_active"]
            else "sentiment_disabled_for_run"
        )
        payload["sentiment_context"] = context_payload

    payload["final_score"] = _safe_float(getattr(result, "final_score", 0.0))
    payload["decision_from_synthesis"] = str(getattr(result, "decision", "") or "")
    payload["confidence"] = _safe_float(
        getattr(result, "conviction", getattr(result, "confidence", 0.0))
    )
    payload["technical_data_source_mode"] = str(
        getattr(result, "technical_candle_source_mode", "unknown") or "unknown"
    )
    payload["technical_has_reconstructed_candles"] = bool(
        getattr(result, "technical_has_reconstructed_candles", False)
    )
    payload["technical_candle_sources"] = list(
        getattr(result, "technical_candle_sources", ()) or ()
    )
    payload["technical_candle_source_counts"] = dict(
        getattr(result, "technical_candle_source_counts", {}) or {}
    )
    payload["trend_shadow"] = {
        "regime": str(getattr(result, "technical_regime", "TRANSITIONAL") or "TRANSITIONAL"),
        "score": _safe_float(getattr(result, "trend_score", 0.0)),
        "components": dict(getattr(result, "trend_components", {}) or {}),
        "structural_break_confirmed": bool(
            getattr(result, "structural_break_confirmed", False)
        ),
        "overbought_momentum": bool(getattr(result, "overbought_momentum", False)),
        "connected_to_primary_score": False,
    }

    return payload

# ══════════════════════════════════════════════════════════════════════════════
# GUARDAR DECISIONES DEL EXECUTION PLAN EN DECISION_LOG
# ══════════════════════════════════════════════════════════════════════════════

def _build_position_price_map(positions: list | None) -> dict[str, float]:
    prices: dict[str, float] = {}
    for position in positions or []:
        ticker = str(
            position.get("ticker", "")
            if isinstance(position, dict)
            else getattr(position, "ticker", "")
        ).upper().strip()
        if not ticker:
            continue
        for key in ("current_price", "price", "last_price"):
            value = (
                position.get(key)
                if isinstance(position, dict)
                else getattr(position, key, None)
            )
            try:
                price = float(value)
            except Exception:
                continue
            if price > 0:
                prices[ticker] = price
                break
    return prices


async def _save_execution_plan_events(
    *,
    cfg,
    execution_plan,
    results,
    macro_snap,
    macro_regime,
    total_ars: float,
    positions: list | None = None,
    owner_chat_id: int | None = None,
    run_id: str | None = None,
    run_intent: str = "formal_plan",
    manual_market_events: list[ManualMarketEvent] | None = None,
) -> list[int]:
    """
    Guarda eventos del ExecutionPlan en decision_log.

    Guarda:
      - buy_orders / sell_orders como APPROVED + executable
      - blocked_orders como BLOCKED + was_blocked
      - pending_buys como BLOCKED por funding si existen

    Importante:
      Esto mide el sistema operativo real, no el optimizer teórico.
    """
    import asyncpg
    from datetime import datetime, timezone

    if not execution_plan:
        return []

    db_url = cfg.database.url
    saved_ids: list[int] = []

    result_by_ticker = {
        str(getattr(r, "ticker", "")).upper(): r
        for r in (results or [])
        if str(getattr(r, "ticker", "") or "").strip()
    }

    decision_by_ticker = {
        str(getattr(d, "ticker", "")).upper(): d
        for d in (getattr(execution_plan, "decisions", []) or [])
        if str(getattr(d, "ticker", "") or "").strip()
    }
    position_price_by_ticker = _build_position_price_map(positions)
    manual_market_events = list(manual_market_events or [])

    def _safe_float(x, default: float = 0.0):
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    def _side_to_decision(side) -> str:
        raw = getattr(side, "value", side)
        raw = str(raw or "").upper().strip()
        return "SELL" if raw == "SELL" else "BUY"

    def _action_to_text(action) -> str:
        return str(getattr(action, "value", action) or "")

    def _get_order_side(order, ticker: str) -> str:
        side = getattr(order, "side", None)

        if side is not None:
            return _side_to_decision(side)

        d = decision_by_ticker.get(ticker)
        delta = _safe_float(getattr(d, "delta_weight", 0.0), 0.0)

        return "SELL" if delta < 0 else "BUY"

    def _get_amount(order) -> float:
        for key in ("amount_ars", "executable_ars", "theoretical_ars"):
            val = getattr(order, key, None)
            if val is not None:
                return _safe_float(val, 0.0)
        return 0.0

    def _get_theoretical_amount(order) -> float:
        val = getattr(order, "theoretical_ars", None)
        if val is not None:
            return _safe_float(val, 0.0)
        return _get_amount(order)

    def _confidence_from_result(r) -> float:
        if r is None:
            return 0.0

        val = getattr(r, "conviction", getattr(r, "confidence", 0.0))
        val = _safe_float(val, 0.0)

        if abs(val) > 1:
            val = val / 100.0

        return max(0.0, min(1.0, val))

    def _price_from_result(r):
        if r is None:
            return None

        for key in ("price", "price_at_decision", "current_price", "last_price"):
            val = getattr(r, key, None)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    pass

        return None

    def _layers_for(ticker: str, r, d, order, *, status: str, decision_type: str) -> dict:
        extra = {
            "source": "execution_plan",
            "status": status,
            "decision_type": decision_type,
            "action": _action_to_text(getattr(d, "action", None)),
            "order_side": str(
                getattr(
                    getattr(order, "side", None),
                    "value",
                    getattr(order, "side", ""),
                )
            ),
            "reason": str(getattr(order, "reason", "") or ""),
            "gate": str(getattr(execution_plan, "gate", "") or ""),
            "current_weight": _safe_float(getattr(d, "current_weight", None), 0.0),
            "target_weight": _safe_float(getattr(d, "target_weight", None), 0.0),
            "delta_weight": _safe_float(getattr(d, "delta_weight", None), 0.0),
            "amount_ars": _get_amount(order),
            "theoretical_amount_ars": _get_theoretical_amount(order),
        }

        try:
            event_layers = manual_event_layers_for_ticker(ticker, manual_market_events)
            if event_layers:
                extra["manual_event_risk"] = event_layers
            return _layers_payload_for_decision(r, extra=extra)
        except Exception:
            event_layers = manual_event_layers_for_ticker(ticker, manual_market_events)
            if event_layers:
                extra["manual_event_risk"] = event_layers
            return extra

    async def _insert_event(
        conn,
        *,
        order,
        status: str,
        decision_type: str,
        is_executable: bool,
        was_blocked: bool,
        forced_reason: str | None = None,
    ):
        ticker = str(getattr(order, "ticker", "") or "").upper().strip()

        if not ticker:
            return None

        r = result_by_ticker.get(ticker)
        d = decision_by_ticker.get(ticker)

        decision = _get_order_side(order, ticker)

        final_score = _safe_float(
            getattr(r, "final_score", getattr(r, "score", 0.0)),
            0.0,
        )

        confidence = _confidence_from_result(r)

        amount_ars = _get_amount(order)
        theoretical_ars = _get_theoretical_amount(order)

        current_weight = _safe_float(getattr(d, "current_weight", None), 0.0)
        target_weight = _safe_float(getattr(d, "target_weight", None), 0.0)
        delta_weight = _safe_float(getattr(d, "delta_weight", None), 0.0)

        price = (
            _price_from_result(r)
            or _safe_float(getattr(order, "price", None), None)
            or _safe_float(getattr(order, "current_price", None), None)
            or _safe_float(getattr(order, "reference_price", None), None)
            or position_price_by_ticker.get(ticker)
        )
        vix = _safe_float(getattr(macro_snap, "vix", None), None)

        block_reason = None
        if was_blocked:
            block_reason = forced_reason or str(
                getattr(order, "reason", "") or "Bloqueado por guards"
            )

        layers_payload = _layers_for(
            ticker,
            r,
            d,
            order,
            status=status,
            decision_type=decision_type,
        )

        if forced_reason:
            layers_payload["forced_reason"] = forced_reason

        decided_at = datetime.now(timezone.utc)
        audit_scope = classify_decision_audit_scope(
            source="execution_plan",
            status=status,
            decision_type=decision_type,
            decided_at=decided_at,
            run_intent=run_intent,
        )
        layers_payload["run_intent"] = audit_scope["run_intent"]
        layers_payload["decision_stage"] = audit_scope["decision_stage"]
        layers_payload["metric_scope"] = audit_scope["metric_scope"]

        existing_id = await conn.fetchval(
            """
            SELECT id
            FROM decision_log
            WHERE ticker = $1
              AND decision = $2
              AND decision_date = (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND COALESCE(owner_chat_id, 0) = COALESCE($3::bigint, 0)
            ORDER BY decided_at DESC
            LIMIT 1
            """,
            ticker,
            decision,
            owner_chat_id,
        )

        size_pct = abs(delta_weight) if delta_weight else (
            _safe_float(amount_ars, 0.0) / total_ars if total_ars else 0.0
        )

        if existing_id:
            matched_movements = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log dl
                JOIN broker_movements bm
                  ON bm.ticker = dl.ticker
                 AND bm.movement_type IN (
                    dl.decision,
                    CASE WHEN dl.decision = 'BUY' THEN 'SELL' ELSE 'BUY' END
                 )
                 AND bm.executed_at >= (
                    CASE
                        WHEN dl.next_executable_at IS NOT NULL THEN dl.next_executable_at
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                            THEN ((((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + 1) + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                            THEN (((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        ELSE dl.decided_at
                    END
                 )
                 AND bm.executed_at < NOW()
                 AND bm.quantity IS NOT NULL
                 AND bm.price IS NOT NULL
                WHERE dl.id = $1
                """,
                int(existing_id),
            )
            if matched_movements:
                logger.info(
                    "ExecutionPlan %s %s already has real movement; creating a new signal",
                    decision,
                    ticker,
                )
                existing_id = None

        if existing_id:
            await conn.execute(
                """
                UPDATE decision_log SET
                    decided_at = $2,
                    final_score = $3,
                    confidence = $4,
                    layers = $5::jsonb,
                    price_at_decision = $6,
                    vix_at_decision = $7,
                    regime = $8,
                    size_pct = $9,
                    decision_type = $10,
                    source = $11,
                    status = $12,
                    block_reason = $13,
                    theoretical_amount_ars = $14,
                    executed_amount_ars = $15,
                    current_weight = $16,
                    target_weight = $17,
                    delta_weight = $18,
                    is_executable = $19,
                    was_blocked = $20,
                    run_id = $21::uuid,
                    run_intent = $22,
                    decision_stage = $23,
                    metric_scope = $24,
                    is_primary_metric = $25,
                    outcome_5d = NULL,
                    outcome_10d = NULL,
                    outcome_20d = NULL,
                    was_correct = NULL,
                    outcome_filled_at = NULL,
                    next_executable_at = NULL,
                    next_executable_price = NULL,
                    executable_outcome_5d = NULL,
                    executable_outcome_10d = NULL,
                    executable_outcome_20d = NULL,
                    executable_was_correct = NULL
                WHERE id = $1
                """,
                int(existing_id),
                decided_at,
                final_score,
                confidence,
                _json.dumps(layers_payload),
                price,
                vix,
                str(macro_regime),
                size_pct,
                decision_type,
                "execution_plan",
                status,
                block_reason,
                theoretical_ars,
                amount_ars if is_executable else 0.0,
                current_weight,
                target_weight,
                delta_weight,
                bool(is_executable),
                bool(was_blocked),
                run_id_to_db(run_id),
                audit_scope["run_intent"],
                audit_scope["decision_stage"],
                audit_scope["metric_scope"],
                audit_scope["is_primary_metric"],
            )
            logger.info(
                "ExecutionPlan updated: id=%s %s %s status=%s",
                existing_id,
                decision,
                ticker,
                status,
            )
            return int(existing_id)

        row = await conn.fetchrow(
            """
            INSERT INTO decision_log (
                owner_chat_id,
                decided_at,
                ticker,
                decision,
                final_score,
                confidence,
                layers,
                price_at_decision,
                vix_at_decision,
                regime,
                size_pct,
                stop_loss_pct,
                target_pct,
                horizon_days,
                rr_ratio,
                decision_type,
                source,
                status,
                block_reason,
                theoretical_amount_ars,
                executed_amount_ars,
                current_weight,
                target_weight,
                delta_weight,
                is_executable,
                was_blocked,
                run_id,
                run_intent,
                decision_stage,
                metric_scope,
                is_primary_metric
            )
            VALUES (
                $1,
                $2, $3, $4, $5, $6,
                $7::jsonb,
                $8, $9, $10,
                $11, $12, $13, $14, $15,
                $16, $17, $18, $19,
                $20, $21, $22, $23, $24,
                $25, $26, $27::uuid, $28, $29, $30, $31
            )
            RETURNING id
            """,
            owner_chat_id,
            decided_at,
            ticker,
            decision,
            final_score,
            confidence,
            _json.dumps(layers_payload),
            price,
            vix,
            str(macro_regime),
            size_pct,
            None,
            None,
            None,
            None,
            decision_type,
            "execution_plan",
            status,
            block_reason,
            theoretical_ars,
            amount_ars if is_executable else 0.0,
            current_weight,
            target_weight,
            delta_weight,
            bool(is_executable),
            bool(was_blocked),
            run_id_to_db(run_id),
            audit_scope["run_intent"],
            audit_scope["decision_stage"],
            audit_scope["metric_scope"],
            audit_scope["is_primary_metric"],
        )

        return int(row["id"]) if row else None

    conn = await asyncpg.connect(db_url)

    try:
        await ensure_decision_audit_scope_columns(conn)
        for order in (getattr(execution_plan, "sell_orders", []) or []):
            row_id = await _insert_event(
                conn,
                order=order,
                status="APPROVED",
                decision_type="executable",
                is_executable=True,
                was_blocked=False,
            )
            if row_id:
                saved_ids.append(row_id)

        for order in (getattr(execution_plan, "buy_orders", []) or []):
            row_id = await _insert_event(
                conn,
                order=order,
                status="APPROVED",
                decision_type="executable",
                is_executable=True,
                was_blocked=False,
            )
            if row_id:
                saved_ids.append(row_id)

        for order in (getattr(execution_plan, "blocked_orders", []) or []):
            row_id = await _insert_event(
                conn,
                order=order,
                status="BLOCKED",
                decision_type="blocked",
                is_executable=False,
                was_blocked=True,
            )
            if row_id:
                saved_ids.append(row_id)

        # pending_buys suele ser lista de tickers. Lo guardamos como BLOCKED por funding
        # para que el blocked audit pueda aprender si esos bloqueos fueron correctos.
        pending_buys = getattr(execution_plan, "pending_buys", []) or []

        for ticker in pending_buys:
            ticker = str(ticker or "").upper().strip()
            if not ticker:
                continue

            d = decision_by_ticker.get(ticker)

            if d is None:
                continue

            class _SyntheticPendingOrder:
                pass

            order = _SyntheticPendingOrder()
            order.ticker = ticker
            order.side = "BUY"
            order.amount_ars = 0.0
            order.theoretical_ars = abs(
                _safe_float(getattr(d, "delta_weight", 0.0), 0.0)
            ) * total_ars
            order.reason = "Compra pendiente por funding/señal"

            row_id = await _insert_event(
                conn,
                order=order,
                status="BLOCKED",
                decision_type="blocked",
                is_executable=False,
                was_blocked=True,
                forced_reason="Compra pendiente por funding/señal",
            )

            if row_id:
                saved_ids.append(row_id)

    finally:
        await conn.close()

    if not saved_ids:
        logger.info("_save_execution_plan_events: no se guardaron eventos nuevos")

    return saved_ids

# ══════════════════════════════════════════════════════════════════════════════
# GUARDAR TRADES EN DECISION_LOG
# ══════════════════════════════════════════════════════════════════════════════

async def _save_optimizer_trades(
    cfg,
    rebalance_report,
    current_w: dict,
    positions: list,
    results: list,
    macro_snap,
    macro_regime,
    total_ars: float,
    owner_chat_id: int | None = None,
) -> list[int]:
    """
    Traduce los trades del optimizer (delta de pesos) en decisiones BUY/SELL
    y las guarda en decision_log.

    Filtros:
      |delta| < 3%       → skip (MANTENER, sin ruido)
      |score| < 0.05     → skip (señal demasiado débil)

    Stops dinámicos (prioridad):
      VIX > 30           → -5%
      VIX > 25 / def.   → -6%
      vol ticker > 60%  → -5%
      vol ticker > 40%  → -7%
      normal            → -8%
    """
    from src.analysis.decision_engine import _normalize_regime, HORIZON_MED

    MIN_DELTA     = 0.03
    MIN_SCORE_ABS = 0.05
    regime        = _normalize_regime(macro_regime)
    vix           = getattr(macro_snap, "vix", None)
    vix_f         = float(vix) if vix else 0.0
    is_defensive  = regime in ("RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS")

    score_map = {
        str(getattr(r, "ticker", "")).upper(): {
            "score":      float(getattr(r, "final_score", 0.0) or 0.0),
            "conviction": min(1.0, float(getattr(r, "conviction",
                        getattr(r, "confidence", 0.5)) or 0.5)),
            "vol_annual": float(
                getattr(r, "volatility_annual", None) or
                next((
                    p.get("volatility_annual", 0.0)
                    for p in (positions or [])
                    if str(p.get("ticker", "")).upper() == str(getattr(r, "ticker", "")).upper()
                ), 0.0) or 0.0
            ),
        }
        for r in (results or [])
    }

    result_map = {
        str(getattr(r, "ticker", "")).upper(): r
        for r in (results or [])
        if str(getattr(r, "ticker", "") or "").strip()
    }

    trades_to_save = []
    trades = getattr(rebalance_report, "trades", []) or []
    tickers_to_price = sorted({
        str(getattr(tr, "ticker", "") or "").upper()
        for tr in trades
        if str(getattr(tr, "ticker", "") or "").strip()
    })
    price_map = await _load_internal_price_map(cfg, tickers_to_price, positions)

    logger.info(
        "price_map Cocos ARS: %s",
        {k: f"${v:.2f}" if v else "None" for k, v in price_map.items()},
    )

    for tr in trades:
        ticker  = str(getattr(tr, "ticker", "") or "").upper()
        w_cur   = float(getattr(tr, "weight_current", 0.0) or 0.0)
        w_opt   = float(getattr(tr, "weight_optimal", 0.0) or 0.0)
        delta   = w_opt - w_cur

        if abs(delta) < MIN_DELTA:
            continue

        direction = "BUY" if delta > 0 else "SELL"
        size_pct  = abs(delta)
        sm        = score_map.get(ticker, {})
        score     = sm.get("score", 0.0)
        conv      = sm.get("conviction", 0.5)
        vol       = sm.get("vol_annual", 0.0)
        price     = price_map.get(ticker)

        if abs(score) < MIN_SCORE_ABS:
            logger.info(
                f"SKIP {direction} {ticker}: score {score:+.3f} es ruido "
                f"(|score| < {MIN_SCORE_ABS})"
            )
            continue

        # CONVENTION: SELL returns are positive-up.
        risk_levels = compute_risk_levels(
            entry_price=float(price) if price else 1.0,
            signal_class=score,
            action=direction,
            regime=regime,
            vix=vix_f,
            vol_annual=vol,
        )
        stop_pct   = risk_levels.stop_pct
        target_pct = risk_levels.target_pct
        rr         = risk_levels.rr

        result_for_ticker = result_map.get(ticker)

        layers_payload = _layers_payload_for_decision(
            result_for_ticker,
            {
                "source": "optimizer",
                "delta_pct": size_pct,
                "optimizer_direction": direction,
                "weight_current": w_cur,
                "weight_optimal": w_opt,
                "weight_delta": delta,
            },
        )

        trades_to_save.append({
            "ticker":        ticker,
            "direction":     direction,
            "score":         score,
            "conviction":    conv,
            "size_pct":      size_pct,
            "price":         float(price) if price else None,
            "stop_loss_pct": stop_pct,
            "target_pct":    target_pct,
            "rr_ratio":      rr,
            "regime":        regime,
            "vix":           float(vix) if vix else None,
            "decided_at":    datetime.now(timezone.utc),
            "layers":        layers_payload,
        })

    if not trades_to_save:
        logger.info("_save_optimizer_trades: sin deltas >= 3%")
        return []

    saved_ids = []
    try:
        db = PortfolioDatabase(cfg.database.url)
        await db.connect()
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            for t in trades_to_save:
                exists = await conn.fetchrow(
                    """
                    SELECT 1 FROM decision_log
                    WHERE ticker   = $1
                      AND decision = $2
                      AND ($3::bigint IS NULL OR owner_chat_id = $3)
                      AND decided_at > NOW() - INTERVAL '20 hours'
                    LIMIT 1
                    """,
                    t["ticker"], t["direction"], owner_chat_id,
                )
                if exists:
                    logger.info(f"Dedup: {t['direction']} {t['ticker']} ya existe hoy — skip")
                    continue

                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        owner_chat_id, decided_at, ticker, decision, final_score, confidence,
                        layers, price_at_decision, vix_at_decision, regime,
                        size_pct, stop_loss_pct, target_pct, horizon_days, rr_ratio
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15
                    ) RETURNING id
                    """,
                    owner_chat_id, t["decided_at"], t["ticker"], t["direction"],
                    t["score"], t["conviction"],
                    _json.dumps(t["layers"]),
                    t["price"], t["vix"], t["regime"],
                    t["size_pct"], t["stop_loss_pct"], t["target_pct"],
                    HORIZON_MED, t["rr_ratio"],
                )
                saved_ids.append(row["id"])
                logger.info(
                    f"Trade guardado: id={row['id']} {t['direction']} {t['ticker']} "
                    f"size={t['size_pct']:.1%} stop={t['stop_loss_pct']:+.1%} "
                    f"target={t['target_pct']:+.1%}"
                )
        await db.close()
    except Exception as e:
        logger.error(f"_save_optimizer_trades error: {e}", exc_info=True)

    return saved_ids


async def _load_internal_price_map(
    cfg,
    tickers: list[str],
    positions: list[dict],
) -> dict[str, float | None]:
    """Resuelve precios operativos desde fuentes propias Cocos, en ARS."""
    wanted = {str(ticker or "").upper() for ticker in tickers if str(ticker or "").strip()}
    if not wanted:
        return {}

    prices: dict[str, float | None] = {ticker: None for ticker in wanted}
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        latest_rows = await db.get_latest_market_prices()
        for row in latest_rows:
            ticker = str(row.get("ticker", "") or "").upper()
            if ticker in wanted and row.get("last_price") is not None:
                prices[ticker] = float(row["last_price"])

        for position in positions or []:
            ticker = str(position.get("ticker", "") or "").upper()
            if not is_position_operable(position):
                continue
            if ticker in wanted and prices.get(ticker) is None:
                current_price = position.get("current_price")
                if current_price is not None:
                    prices[ticker] = float(current_price)

        for ticker in sorted(wanted):
            if prices.get(ticker) is not None:
                continue
            rows = await db.get_market_candles(ticker, limit=1)
            if rows:
                prices[ticker] = float(rows[-1]["close_price"])
    finally:
        await db.close()

    missing = sorted(ticker for ticker, price in prices.items() if price is None)
    if missing:
        logger.warning("Sin precio Cocos disponible para %s", missing)
    return prices


# ══════════════════════════════════════════════════════════════════════════════
# RENDER — fuente única de verdad: ExecutionPlan
# ══════════════════════════════════════════════════════════════════════════════

def _action_icon(action: DecisionType | str) -> str:
    m = {
        DecisionType.SELL_FULL:    "🔴",
        DecisionType.SELL_PARTIAL: "🔴",
        DecisionType.BUY:          "🟢",
        DecisionType.HOLD:         "🟡",
        DecisionType.WATCH:        "🔵",
        DecisionType.BLOCKED:      "⛔",
    }
    if isinstance(action, str):
        try:
            action = DecisionType(action)
        except ValueError:
            return "🟡"
    return m.get(action, "🟡")


def _execution_timing_context(now: datetime | None = None) -> dict[str, str | bool]:
    """
    Classify whether a rendered plan is actionable in-session or only next wheel.

    The analysis often runs at EOD, after BYMA is effectively closed. In that
    case the decision is valid as a signal, but execution must be measured from
    the next tradable reference, not from the close that generated the signal.
    """
    current = now or datetime.now(ART_TZ)
    if current.tzinfo is not None:
        current = current.astimezone(ART_TZ)

    if not is_art_business_day(current):
        return {
            "next_session": True,
            "headline": "Plan para próxima rueda",
            "note": (
                "El análisis se generó en día sin rueda; no hay ejecución operativa "
                "hasta la próxima apertura."
            ),
            "order_note": "Revalidar con precio fresco de apertura antes de operar.",
        }

    if not is_regular_market_session(current):
        return {
            "next_session": True,
            "headline": "Plan para próxima rueda",
            "note": (
                "El análisis se generó fuera de rueda; validar apertura y no "
                "perseguir gaps fuertes."
            ),
            "order_note": "Ejecutar solo si el precio sigue razonable al abrir.",
        }
    return {
        "next_session": False,
        "headline": "Plan intradía",
        "note": "El análisis se generó durante rueda; validar liquidez y spread.",
        "order_note": "Validar precio límite, liquidez y spread antes de operar.",
    }


def _analysis_run_policy(
    no_persist: bool,
    run_intent: str,
    now: datetime | None = None,
) -> tuple[bool, str, bool]:
    """Keep off-market recalculations exploratory while sentiment remains live."""
    current = now or datetime.now(ART_TZ)
    off_market_context = not is_art_business_day(current)
    if off_market_context:
        return True, "exploratory", True
    return bool(no_persist), str(run_intent or "formal_plan"), False


def _opportunity_rr(candidate) -> float:
    asymmetry = getattr(candidate, "asymmetry", None)
    try:
        return float(getattr(asymmetry, "risk_reward", 0.0) or 0.0)
    except Exception:
        return 0.0


def _opportunity_edge(candidate) -> float:
    edge = getattr(candidate, "edge", None)
    try:
        return float(getattr(edge, "raw", 0.0) or 0.0)
    except Exception:
        return 0.0


def _opportunity_source_label(candidate) -> str:
    counts = getattr(candidate, "technical_candle_source_counts", None) or {}
    if isinstance(counts, dict) and counts:
        return ", ".join(
            f"{str(source).upper()} {int(count)}"
            for source, count in sorted(counts.items())
            if count
        )
    mode = str(getattr(candidate, "technical_candle_source_mode", "") or "").upper()
    return mode or "UNKNOWN"


def _radar_buys_for_execution(
    opportunity_report: OpportunityReport | None,
    total_ars: float,
    *,
    max_candidates: int = 3,
) -> list[dict]:
    """Return radar entries that passed every guard except current-cash funding."""
    if opportunity_report is None or total_ars <= 0:
        return []

    actionable_statuses = {
        CandidateStatus.COMPRABLE_AHORA,
        CandidateStatus.COMPRA_HABILITADA,
    }
    buys: list[dict] = []
    seen: set[str] = set()
    for candidate in getattr(opportunity_report, "candidates", []) or []:
        ticker = str(getattr(candidate, "ticker", "") or "").upper()
        if not ticker or ticker in seen:
            continue
        if getattr(candidate, "trade_type", None) != TradeType.NEW_ENTRY:
            continue
        eligible = (
            getattr(candidate, "status", None) in actionable_statuses
            or bool(getattr(candidate, "cash_funding_required", False))
        )
        if not eligible:
            continue

        amount_ars = total_ars * float(
            getattr(candidate, "sizing_suggested", 0.0) or 0.0
        )
        reference_price = float(getattr(candidate, "price_usd", 0.0) or 0.0)
        if amount_ars < MIN_TRADE_ARS or reference_price <= 0:
            continue

        buys.append({
            "ticker": ticker,
            "amount_ars": amount_ars,
            "score": float(getattr(candidate, "final_score", 0.0) or 0.0),
            "reference_price": reference_price,
            "reason": (
                f"Radar elegible: {str(getattr(candidate, 'action_concreta', '') or 'entrada nueva')}"
            ),
        })
        seen.add(ticker)
        if len(buys) >= max(1, max_candidates):
            break
    return buys


def _append_analysis_radar(
    h: list[str],
    *,
    opportunity_report: OpportunityReport,
    total_ars: float,
    available_cash_ars: float,
    execution_plan: ExecutionPlan | None = None,
) -> None:
    """Render compact same-engine radar inside /analysis."""
    market_session_open = is_regular_market_session()
    h.append("<b>RADAR OPERATIVO</b>" if market_session_open else "<b>RADAR PARA PRÓXIMA RUEDA</b>")
    h.append("<i>Mismo motor que /radar; no entra al EV principal ni calibra thresholds.</i>")
    if not market_session_open:
        h.append("Mercado cerrado/sin rueda: ideas no ejecutables hasta revalidar apertura.")

    cash = max(float(available_cash_ars or 0.0), 0.0)
    planned_external = [
        order.ticker
        for order in (getattr(execution_plan, "buy_orders", []) or [])
        if order.priority >= 3
    ]
    planned_external_set = set(planned_external)
    if planned_external:
        h.append(
            "Funding resuelto por el plan de ventas: "
            f"<b>{escape(', '.join(planned_external))}</b>."
        )
    elif cash >= MIN_TRADE_ARS:
        h.append(f"Cash ejecutable: <b>{_money_ars(cash)}</b>")
    else:
        h.append("Sin cash ejecutable: compras nuevas solo via funding o swap.")

    raw_actionable = (
        list(getattr(opportunity_report, "comprable_ahora", []) or [])
        + list(getattr(opportunity_report, "compra_habilitada", []) or [])
    )
    actionable = [
        c for c in raw_actionable
        if str(getattr(c, "trade_type", "") or "").upper() != "SWAP_CANDIDATE"
        and not str(getattr(c, "swap_vs", "") or "").strip()
        and str(getattr(c, "ticker", "") or "").upper() not in planned_external_set
    ]
    swaps = (
        [
            c for c in raw_actionable
            if str(getattr(c, "trade_type", "") or "").upper() == "SWAP_CANDIDATE"
            or str(getattr(c, "swap_vs", "") or "").strip()
        ]
        + list(getattr(opportunity_report, "swap_candidatos", []) or [])
    )
    watch = [
        c for c in (getattr(opportunity_report, "en_vigilancia", []) or [])
        if str(getattr(c, "ticker", "") or "").upper() not in planned_external_set
    ]

    shown_any = bool(planned_external)

    if actionable:
        shown_any = True
        if not market_session_open:
            title = "Candidatos para próxima rueda"
        else:
            title = "Compras con cash" if cash >= MIN_TRADE_ARS else "Candidatos sin funding"
        h.append(f"<b>{escape(title)}</b>")
        for c in actionable[:3]:
            suggested = float(total_ars or 0.0) * float(getattr(c, "sizing_suggested", 0.0) or 0.0)
            executable = min(suggested, cash) if cash >= MIN_TRADE_ARS else 0.0
            amount_txt = (
                f" | ejecutable {_money_ars(executable)}"
                if executable >= MIN_TRADE_ARS
                else ""
            )
            h.append(
                f"  {escape(str(c.ticker).upper())}: "
                f"score <code>{float(c.final_score):+.3f}</code> | "
                f"R/R {_opportunity_rr(c):.1f}x | edge {_opportunity_edge(c):+.3f}"
                f"{amount_txt}"
            )
            if getattr(c, "why_not_now", ""):
                h.append(f"   Motivo: {escape(str(c.why_not_now))}")
            elif getattr(c, "action_concreta", ""):
                action_text = str(c.action_concreta)
                if market_session_open:
                    h.append(f"   Accion: {escape(action_text)}")
                else:
                    h.append(f"   Revalidar al abrir: {escape(action_text)}. No ejecutar ahora.")

    if swaps:
        shown_any = True
        h.append("<b>Swaps posibles</b>")
        for c in swaps[:3]:
            swap_vs = str(getattr(c, "swap_vs", "") or "?").upper()
            strength = str(getattr(c, "swap_strength", "") or "MODERADO").upper()
            h.append(
                f"  {escape(str(c.ticker).upper())} vs {escape(swap_vs)}: "
                f"{escape(strength)} | score <code>{float(c.final_score):+.3f}</code> | "
                f"edge {_opportunity_edge(c):+.3f} | R/R {_opportunity_rr(c):.1f}x"
            )
            if getattr(c, "why_not_now", ""):
                h.append(f"   Motivo: {escape(str(c.why_not_now))}")

    if watch:
        shown_any = True
        h.append("<b>Vigilancia</b>")
        for c in watch[:3]:
            why = str(getattr(c, "why_not_now", "") or getattr(c, "action_concreta", ""))
            h.append(
                f"  {escape(str(c.ticker).upper())}: "
                f"score <code>{float(c.final_score):+.3f}</code> | "
                f"R/R {_opportunity_rr(c):.1f}x | {escape(why)}"
            )

    if not shown_any:
        h.append("Sin compras o swaps operativos en el universo Cocos.")

    externos = list(getattr(opportunity_report, "externos", []) or [])
    if externos:
        h.append(
            f"{len(externos)} tickers de Cocos sin historico operable: "
            "detectados, pero sin 60 velas canonicas."
        )

    top_sources = []
    for c in (actionable + swaps + watch)[:4]:
        label = _opportunity_source_label(c)
        if label and label not in top_sources:
            top_sources.append(label)
    if top_sources:
        h.append(f"Fuente tecnica radar: <b>{escape(' | '.join(top_sources))}</b>")


def _compact_chg(value) -> str:
    try:
        return f"{float(value):+.1f}%"
    except Exception:
        return "N/A"


def _compact_num(value, decimals: int = 1) -> str:
    try:
        return f"{float(value):,.{decimals}f}".replace(",", ".")
    except Exception:
        return "N/A"


def _compact_reason(text: str, max_len: int = 72) -> str:
    clean = " ".join(str(text or "").split())
    if not clean:
        return "sin motivo operativo claro"
    if "Optimizer suger" in clean and "score" in clean:
        return "optimizer diverge; score no confirma"
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 1].rstrip() + "..."


def _compact_ic_line(ic_metrics: dict | None) -> str:
    ic_data = ic_metrics or {}
    by_h = ic_data.get("by_horizon", {}) or {}
    parts = []
    ic_5d = None
    ic_10d = None
    for hz in ("5d", "10d"):
        data = by_h.get(hz, {}) or {}
        ic = data.get("ic")
        n_obs = int(data.get("n_obs", 0) or 0)
        if ic is None or n_obs < 5:
            continue
        if hz == "5d":
            ic_5d = float(ic)
        if hz == "10d":
            ic_10d = float(ic)
        quality = str(data.get("quality") or _ic_quality(float(ic))).upper()
        parts.append(f"IC {hz.upper()} <code>{float(ic):+.3f}</code> {escape(quality)}")
    if not parts:
        return "IC: sin muestra suficiente"
    regime = _build_ic_text_regime(ic_5d=ic_5d, ic_10d=ic_10d)
    return " | ".join(parts) + f" | {regime.icon} {escape(regime.label)}"


def _optimizer_compact_line(rebalance_report) -> str:
    opt = _get(rebalance_report, "optimization", rebalance_report)
    if not opt:
        return "Optimizer: sin datos"
    engine = str(_get(opt, "actual_engine", "") or _get(opt, "method", "N/A"))
    engine_note = str(_get(opt, "engine_note", "") or "")
    note = ""
    if "PyPortfolioOpt" in engine_note:
        note = " | PyPortfolioOpt"
    elif engine == "FALLBACK_MAX_SHARPE" or "fallback" in engine_note.lower():
        note = " | fallback"
    sharpe = float(_get(opt, "sharpe_ratio", 0.0) or 0.0)
    return f"Optimizer: <b>{escape(engine)}</b>{note} | Sharpe <b>{sharpe:.2f}</b>"


def _render_compact_report(
    results,
    macro_snap,
    total_ars: float,
    cash_ars: float,
    rebalance_report,
    positions: list,
    *,
    ic_metrics: dict | None = None,
    execution_plan: ExecutionPlan | None = None,
    portfolio_snapshot: dict | None = None,
    latest_broker_movement: dict | None = None,
    no_persist: bool = False,
    off_market_context: bool = False,
    manual_market_events: list[ManualMarketEvent] | None = None,
) -> str:
    plan = execution_plan
    timing_ctx = _execution_timing_context()
    decision_map = {d.ticker: d for d in (plan.decisions if plan else [])}
    gate = plan.gate if plan else "NORMAL"
    current_w = _current_weights(positions, total_ars)
    now_txt = datetime.now(ART_TZ).strftime("%d/%m %H:%M")
    plan_title = (
        "SIMULACIÓN CONTEXTUAL"
        if off_market_context
        else ("PLAN MAÑANA" if timing_ctx["next_session"] else "PLAN AHORA")
    )
    lines: list[str] = [
        f"🧠 <b>ANÁLISIS — {now_txt} ART</b>",
        f"💼 <b>{_money_ars(total_ars)}</b> | Cash <b>{_money_ars(cash_ars)}</b> | Régimen: <b>{escape(str(gate))}</b>",
    ]
    context_lines = _render_analysis_data_context(portfolio_snapshot, latest_broker_movement)
    if context_lines:
        lines.extend(context_lines[:2])
    event_lines = render_manual_market_events_html(
        manual_market_events or [],
        compact=True,
    )
    if event_lines:
        lines.extend(event_lines)
    exposure_lines = _render_manual_event_position_exposure(
        manual_market_events or [],
        positions,
        total_ars,
        compact=True,
    )
    if exposure_lines:
        lines.extend(exposure_lines)
    if timing_ctx["next_session"]:
        lines.append("⚠️ Fuera de rueda — validar apertura, no perseguir gaps.")
    if off_market_context:
        lines.append(
            "🛰 Sentiment sigue activo; esta simulación no reemplaza el último plan formal."
        )
    elif no_persist:
        lines.append("🧪 Modo prueba — no guarda eventos en decision_log.")
    lines.append("")

    lines.append(f"━━━ <b>{plan_title}</b> ━━━")
    if plan and (plan.sell_orders or plan.buy_orders or plan.blocked_orders):
        compact_sell_label = "REVALIDAR SELL" if timing_ctx["next_session"] else "SELL"
        compact_buy_label = "REVALIDAR BUY" if timing_ctx["next_session"] else "BUY"
        for order in sorted(plan.sell_orders, key=lambda x: x.priority):
            d = decision_map.get(order.ticker)
            score = f"score {float(d.score):+.3f}" if d and d.score is not None else ""
            lines.append(f"🔴 {compact_sell_label} {escape(order.ticker)} -{_money_ars(order.amount_ars)} {escape(score)}".rstrip())
        for order in sorted(plan.buy_orders, key=lambda x: x.priority):
            d = decision_map.get(order.ticker)
            score = f"score {float(d.score):+.3f}" if d and d.score is not None else ""
            lines.append(f"🟢 {compact_buy_label} {escape(order.ticker)} +{_money_ars(order.amount_ars)} {escape(score)}".rstrip())
        for order in sorted(plan.blocked_orders, key=lambda x: x.priority)[:4]:
            d = decision_map.get(order.ticker)
            score = f"score {float(d.score):+.3f}" if d and d.score is not None else ""
            reason = _compact_reason(order.reason, 46)
            lines.append(f"🔵 WATCH {escape(order.ticker)} bloqueado {escape(score)} ({escape(reason)})".rstrip())
        fees = float(plan.fee_sell_ars or 0.0) + float(plan.fee_buy_ars or 0.0)
        if plan.gross_sell_ars:
            lines.append(
                f"Ventas: <b>{_money_ars(plan.gross_sell_ars)}</b> | "
                f"Compras: <b>{_money_ars(plan.gross_buy_ars)}</b> | "
                f"Fees: <b>{_money_ars(fees)}</b> | Cash post: <b>{_money_ars(plan.cash_after)}</b>"
            )
        else:
            lines.append(
                f"Compras: <b>{_money_ars(plan.gross_buy_ars)}</b> | "
                f"Fees: <b>{_money_ars(fees)}</b> | Cash post: <b>{_money_ars(plan.cash_after)}</b>"
            )
    else:
        lines.append("🟡 Sin órdenes ejecutables. Mantener y esperar mejor setup.")
    lines.append("Nota: plan sin fill no entra al EV operativo.")
    lines.append("")

    lines.append("━━━ <b>CARTERA</b> ━━━")
    action_priority = {
        DecisionType.SELL_FULL.value: 0,
        DecisionType.SELL_PARTIAL.value: 1,
        DecisionType.BUY.value: 2,
        DecisionType.BLOCKED.value: 3,
        DecisionType.WATCH.value: 4,
        DecisionType.HOLD.value: 5,
    }
    sorted_results = sorted(
        results or [],
        key=lambda r: (
            action_priority.get(
                getattr(decision_map.get(str(getattr(r, "ticker", "")).upper()), "action", DecisionType.HOLD).value
                if decision_map.get(str(getattr(r, "ticker", "")).upper()) else DecisionType.HOLD.value,
                5,
            ),
            -abs(float(getattr(r, "final_score", 0.0) or 0.0)),
        ),
    )
    for r in sorted_results:
        ticker = str(getattr(r, "ticker", "") or "").upper()
        if not ticker:
            continue
        d = decision_map.get(ticker)
        action = d.action.value if d else "HOLD"
        icon = _action_icon(d.action if d else DecisionType.HOLD)
        score = float(getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0)
        tech = _layer_weighted(r, "technical")
        macro = _layer_weighted(r, "macro")
        sent = _layer_weighted(r, "sentiment")
        technical_regime = str(getattr(r, "technical_regime", "TRANSITIONAL") or "TRANSITIONAL")
        trend_score = float(getattr(r, "trend_score", 0.0) or 0.0)
        cw = float(current_w.get(ticker, 0.0))
        tw = float(d.target_weight if d else cw)
        if action == DecisionType.HOLD.value and d and d.reason_secondary:
            tail = f"hold — {_compact_reason(d.reason_secondary, 58)}"
        elif action == DecisionType.WATCH.value and d and d.reason_secondary:
            tail = f"watch — {_compact_reason(d.reason_secondary, 58)}"
        elif action == DecisionType.BLOCKED.value and d and d.reason_secondary:
            tail = f"bloqueado — {_compact_reason(d.reason_secondary, 54)}"
        else:
            tail = action
        lines.append(
            f"{icon} <b>{escape(ticker)}</b> <code>{score:+.3f}</code> "
            f"T<code>{tech:+.3f}</code> M<code>{macro:+.3f}</code> S<code>{sent:+.3f}</code> "
            f"R=<b>{escape(technical_regime)}</b> trend=<code>{trend_score:+.3f}</code> "
            f"{_pct(cw)}→{_pct(tw)} {escape(tail)}"
        )
        if (
            str(getattr(r, "technical_signal", "HOLD") or "HOLD") == "SELL"
            and float(getattr(macro_snap, "sp500_chg", 0.0) or 0.0) > 1.0
            and float(getattr(macro_snap, "vix", 99.0) or 99.0) < 18.0
        ):
            lines.append("⚠️ Divergencia técnico/macro: SELL técnico con contexto risk-on.")
    lines.append("T=técnico | M=macro | S=sentiment")
    lines.append("")

    lines.append("━━━ <b>MACRO</b> ━━━")
    lines.append(
        f"SP500 {_compact_num(getattr(macro_snap, 'sp500', None), 0)} {_compact_chg(getattr(macro_snap, 'sp500_chg', None))} | "
        f"VIX {_compact_num(getattr(macro_snap, 'vix', None), 1)} | "
        f"WTI ${_compact_num(getattr(macro_snap, 'wti', None), 1)} {_compact_chg(getattr(macro_snap, 'wti_chg', None))}"
    )
    lines.append(
        f"DXY {_compact_num(getattr(macro_snap, 'dxy', None), 1)} | "
        f"10Y {_compact_num(getattr(macro_snap, 'tnx', None), 2)}% | "
        f"Merval {_compact_num(getattr(macro_snap, 'merval', None), 0)}"
    )
    riesgo = getattr(macro_snap, "riesgo_pais", None)
    rp_icon = "🔴" if (riesgo or 0) > 1000 else "🟡" if (riesgo or 0) > 600 else "🟢"
    lines.append(
        f"AR: CCL ${_compact_num(getattr(macro_snap, 'ccl', None), 0)} | "
        f"MEP ${_compact_num(getattr(macro_snap, 'mep', None), 0)} | "
        f"Riesgo país {riesgo or 'N/A'}pb {rp_icon}"
    )
    lines.append("")

    lines.append("━━━ <b>SISTEMA</b> ━━━")
    lines.append(_compact_ic_line(ic_metrics))
    lines.append(_optimizer_compact_line(rebalance_report))
    lines.append("No es asesoramiento financiero.")

    report = "\n".join(lines)
    valid_html, errors = validate_telegram_html(report)
    if not valid_html:
        logger.warning("run_analysis compact HTML potencialmente invalido: %s", errors[:3])
    return report


def render_report(
    results,
    macro_snap,
    total_ars:        float,
    cash_ars:         float,
    portfolio_risk,
    rebalance_report,
    positions:        list,
    universe_results: list,
    external_universe_tickers: list[str] | None = None,
    ic_metrics:       dict | None = None,
    execution_plan:   ExecutionPlan | None = None,
    opportunity_report: OpportunityReport | None = None,
    portfolio_snapshot: dict | None = None,
    latest_broker_movement: dict | None = None,
    radar_skipped: bool = False,
    no_persist: bool = False,
    off_market_context: bool = False,
    manual_market_events: list[ManualMarketEvent] | None = None,
) -> str:
    plan = execution_plan
    gate = plan.gate if plan else "NORMAL"
    h = []

    result_by_ticker = _result_map(results)
    timing_ctx = _execution_timing_context()
    if radar_skipped:
        return _render_compact_report(
            results,
            macro_snap,
            total_ars,
            cash_ars,
            rebalance_report,
            positions,
            ic_metrics=ic_metrics,
            execution_plan=execution_plan,
            portfolio_snapshot=portfolio_snapshot,
            latest_broker_movement=latest_broker_movement,
            no_persist=no_persist,
            off_market_context=off_market_context,
            manual_market_events=manual_market_events,
        )

    # ── Header ────────────────────────────────────────────────────────────────
    h.extend(tg_header("🧠 Análisis de cartera", subtitle=f"{datetime.now().strftime('%d/%m/%Y %H:%M')} ART"))
    h.append(
        f"💼 Portfolio: <b>{_money_ars(total_ars)}</b> | "
        f"Cash libre: <b>{_money_ars(cash_ars)}</b>"
    )
    h.extend(_render_analysis_data_context(portfolio_snapshot, latest_broker_movement))
    event_lines = render_manual_market_events_html(manual_market_events or [])
    if event_lines:
        h.extend(event_lines)
    exposure_lines = _render_manual_event_position_exposure(
        manual_market_events or [],
        positions,
        total_ars,
    )
    if exposure_lines:
        h.extend(exposure_lines)
    if timing_ctx["next_session"]:
        h.append(f"🕒 <b>{timing_ctx['headline']}</b> — {timing_ctx['note']}")
    if off_market_context:
        h.append(
            "🛰 <b>Contexto fuera de rueda</b> — sentiment sigue activo, pero este "
            "cálculo no reemplaza el último plan formal ni guarda decisiones."
        )
    elif no_persist:
        h.append("🧪 <b>Modo prueba</b> — no guarda eventos en decision_log.")
    h.append("")

    # ── DECISIÓN DE HOY — desde ExecutionPlan, nunca desde optimizer ──────────
    h.append(tg_section(
        "Simulación contextual" if off_market_context else "Decisión de cartera"
    ))

    if gate == "BLOCKED":
        h.append("🔴 <b>NO OPERAR / DEFENSIVO</b>")
        h.append("   Sistema bloqueado: solo se permitirían stops urgentes.")
    elif gate in ("CAUTIOUS", "DEFENSIVE"):
        h.append("⚠️ Mercado en modo defensivo — priorizar reducción de riesgo.")
    else:
        h.append("✅ Régimen operativo normal.")

    if plan and plan.main_action:
        main_order = plan.main_action
        next_session_plan = bool(timing_ctx["next_session"] or off_market_context)
        if next_session_plan:
            verb = (
                "REVALIDAR VENTA"
                if main_order.side.value == "SELL"
                else "REVALIDAR COMPRA"
            )
        else:
            verb = "VENDER" if main_order.side.value == "SELL" else "COMPRAR"
        icon = "🔴" if main_order.side.value == "SELL" else "🟢"
        partial_tag = " <i>(parcial)</i>" if main_order.partial else ""

        h.append(
            f"{icon} <b>{verb} {main_order.ticker} "
            f"({_money_ars(main_order.amount_ars)}){partial_tag}</b>"
        )

        d = next((x for x in plan.decisions if x.ticker == main_order.ticker), None)
        result = result_by_ticker.get(main_order.ticker)

        if d:
            h.append(
                f"   Recomendación: "
                f"{'reducir' if main_order.side.value == 'SELL' else 'aumentar'} "
                f"exposición de <b>{_pct(d.current_weight)}</b> a "
                f"<b>{_pct(d.target_weight)}</b>."
            )

        if result is not None:
            tech = _layer_weighted(result, "technical")
            macro = _layer_weighted(result, "macro")
            sent = _layer_weighted(result, "sentiment")
            score = float(
                getattr(result, "final_score", getattr(result, "score", 0.0))
                or 0.0
            )
            h.append(f"   {_render_signal_line(score, tech, macro, sent)}")

        h.append(f"   Motivo: {escape(main_order.reason)}")
        if next_session_plan:
            h.append(
                "   Estado: idea para próxima rueda; no ejecutar sin precio fresco, "
                "liquidez y spread de apertura."
            )
        else:
            h.append("   Estado: plan aprobado/tentativo; no es ejecución real ni entra al EV hasta fill confirmado.")

        if main_order.partial and main_order.theoretical_ars > main_order.amount_ars:
            h.append(
                f"   💡 Target teórico: {_money_ars(main_order.theoretical_ars)} — "
                f"ejecutable hoy: {_money_ars(main_order.amount_ars)} "
                f"(completar cuando haya más funding)"
            )
    else:
        h.append("🟡 <b>MANTENER / NO COMPRAR HOY</b>")
        h.append("   No hay órdenes ejecutables en este momento.")

        blocked_order, blocked_result = _best_non_executable_candidate(plan, results)
        if blocked_order is not None and blocked_result is not None:
            score = float(
                getattr(
                    blocked_result,
                    "final_score",
                    getattr(blocked_result, "score", 0.0),
                )
                or 0.0
            )
            d = next(
                (
                    item
                    for item in (getattr(plan, "decisions", []) or [])
                    if item.ticker == blocked_order.ticker
                ),
                None,
            )
            weight_text = (
                f" | peso {_pct(d.current_weight)} → {_pct(d.target_weight)}"
                if d is not None
                else ""
            )
            h.append(
                f"   Mejor señal interna: <b>{blocked_order.ticker}</b> — "
                f"score <code>{score:+.3f}</code> | "
                f"<b>{_classify_signal_label(score)}</b>{weight_text}"
            )
            h.append(f"   Motivo de no ejecución: {escape(blocked_order.reason)}")

        if plan and plan.cash_after < MIN_TRADE_ARS:
            h.append(
                f"   Cash libre: <b>{_money_ars(plan.cash_after)}</b>; "
                f"mínimo por orden: <b>{_money_ars(MIN_TRADE_ARS)}</b>."
            )
            h.append("   Próximo paso: esperar funding o evaluar swap financiado.")
        elif plan and plan.pending_buys:
            h.append("   Próximo paso: esperar funding o evaluar swap financiado.")

    h.append("")

    # ── EJECUCIÓN — cash accounting + órdenes ─────────────────────────────────
    h.append(tg_section("Plan operativo"))

    if plan:
        h.append(f"   {timing_ctx['headline']}: {timing_ctx['note']}")
        h.append("   Confirmación requerida: fill real en Cocos movements.")

        purchases = (
            "$0 ARS"
            if gate in ("CAUTIOUS", "BLOCKED") and plan.gross_sell_ars > 0
            else _money_ars(plan.gross_buy_ars)
        )
        h.append(
            f"   Plan ventas: <b>{_money_ars(plan.gross_sell_ars)}</b> | "
            f"Plan compras: <b>{purchases}</b> | "
            f"Cash post-plan: <b>{_money_ars(plan.cash_after)}</b>"
        )

        total_fees = plan.fee_sell_ars + plan.fee_buy_ars

        if total_fees > 500:
            h.append(f"   Fees estimados: {_money_ars(total_fees)}")

        if plan.pending_buys:
            h.append(
                f"   ⏳ Compras pendientes por funding: {', '.join(plan.pending_buys)}"
            )
    else:
        h.append(f"   Sin plan disponible — cash actual: {_money_ars(cash_ars)}")

    if plan and (plan.sell_orders or plan.buy_orders):
        h.append("   Plan operativo:")

        step = 1

        for o in sorted(plan.sell_orders, key=lambda x: x.priority):
            sell_verb = "Revalidar venta" if timing_ctx["next_session"] else "Vender"
            h.append(
                f"   {step}. 🔴 {sell_verb} <b>{o.ticker}</b>: "
                f"<b>{int(o.quantity_est)} nominal(es)</b> × "
                f"{_money_ars(o.reference_price)} = -{_money_ars(o.amount_ars)}"
            )

            if o.action == DecisionType.SELL_FULL:
                h.append("      → Liquidación total (target 0%)")
            h.append(f"      Condición: {timing_ctx['order_note']}")

            step += 1

        for o in sorted(plan.buy_orders, key=lambda x: x.priority):
            ext_icon = "🌍" if o.priority >= 3 else "📈"
            buy_verb = "Revalidar compra" if timing_ctx["next_session"] else "Comprar"
            partial_tag = " <i>(parcial)</i>" if o.partial else ""
            d = next((x for x in plan.decisions if x.ticker == o.ticker), None)
            score_tag = f" | score {d.score:+.3f}" if d and d.score is not None else ""

            h.append(
                f"   {step}. {ext_icon} {buy_verb} <b>{o.ticker}</b>: "
                f"<b>{int(o.quantity_est)} nominal(es)</b> × "
                f"{_money_ars(o.reference_price)} = +{_money_ars(o.amount_ars)}"
                f"{partial_tag}{score_tag}"
            )
            h.append(f"      Condición: {timing_ctx['order_note']}")
            step += 1

        if plan.cash_after > 5_000:
            h.append(f"   → Cash remanente: {_money_ars(plan.cash_after)}")

    elif plan and not plan.blocked_orders:
        if timing_ctx["next_session"]:
            h.append("   Sin órdenes para próxima rueda.")
        else:
            h.append("   Sin órdenes ejecutables hoy.")

    elif plan and gate in ("CAUTIOUS", "BLOCKED") and plan.gross_sell_ars > 0:
        h.append(
            f"   Gate {gate} activo: los {_money_ars(plan.gross_sell_ars)} "
            f"de ventas quedan en cash."
        )

    if plan and plan.blocked_orders:
        h.append("   🚫 Señales no ejecutables:")
        for o in plan.blocked_orders[:4]:
            action_name = o.action.value if hasattr(o.action, "value") else str(o.action)
            icon = "🔵" if action_name == "WATCH" else "⛔"
            h.append(
                f"      {icon} {o.ticker}: {action_name} — "
                f"{_money_ars(o.theoretical_ars)} teórico — "
                f"{escape(o.reason)}"
            )

    h.append("")

    # ── CARTERA ACTUAL — señal del activo + decisión de cartera separadas ─────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(tg_section("Lectura de cartera"))
    h.append(f"Historia técnica: <b>{escape(_technical_source_summary(results))}</b>")
    stale_positions = [
        p for p in (positions or [])
        if str(p.get("market_data_status", PRICE_STATUS_FRESH)).upper() != PRICE_STATUS_FRESH
    ]
    if stale_positions:
        stale_txt = ", ".join(
            f"{escape(str(p.get('ticker', '?')).upper())} "
            f"({escape(str(p.get('market_data_reason', 'precio no fresco')))})"
            for p in stale_positions[:6]
        )
        h.append(f"Datos no operables: <b>{stale_txt}</b>")
    h.append("")

    current_w = _current_weights(positions, total_ars)
    decision_map = {d.ticker: d for d in (plan.decisions if plan else [])}

    action_priority = {
        DecisionType.SELL_FULL.value: 0,
        DecisionType.SELL_PARTIAL.value: 1,
        DecisionType.BUY.value: 2,
        DecisionType.BLOCKED.value: 3,
        DecisionType.WATCH.value: 4,
        DecisionType.HOLD.value: 5,
    }

    def _result_priority(r):
        ticker = str(getattr(r, "ticker", "")).upper()
        d = decision_map.get(ticker)
        action_value = d.action.value if d else "HOLD"
        score_value = abs(float(getattr(r, "final_score", 0) or 0))
        return (
            action_priority.get(action_value, 5),
            -score_value,
        )

    sorted_results = sorted(results or [], key=_result_priority)

    for r in sorted_results:
        ticker = str(getattr(r, "ticker", "")).upper()
        score = float(getattr(r, "final_score", getattr(r, "score", 0)) or 0)
        lectura, _ = _component_reason(r)
        d = decision_map.get(ticker)

        cw = float(current_w.get(ticker, 0.0))
        tw = d.target_weight if d else cw
        action_str = d.action.value if d else "HOLD"
        icon = _action_icon(d.action if d else DecisionType.HOLD)

        tech = _layer_weighted(r, "technical")
        macro = _layer_weighted(r, "macro")
        sent = _layer_weighted(r, "sentiment")
        sentiment_label = (
            "sentiment OFF"
            if not bool(getattr(r, "sentiment_active", True))
            else f"sentiment {sent:+.3f}"
        )

        ars_str = ""

        if plan:
            for o in plan.sell_orders + plan.buy_orders:
                if o.ticker == ticker:
                    verb = "-" if o.side.value == "SELL" else "+"
                    ars_str = f" → {verb}{_money_ars(o.amount_ars)}"

                    if o.partial:
                        ars_str += " <i>(parcial)</i>"

                    break

        signal_label = _classify_signal_label(score)
        h.append(
            f"{icon} <b>{ticker}</b> → <b>{action_str}</b>{ars_str} | "
            f"score <code>{score:+.3f}</code> | <b>{signal_label}</b> | "
            f"peso {_pct(cw)} → {_pct(tw)}"
        )
        h.append(
            f"   Régimen técnico: <b>{escape(str(getattr(r, 'technical_regime', 'TRANSITIONAL')))}</b> | "
            f"trend shadow <code>{float(getattr(r, 'trend_score', 0.0) or 0.0):+.3f}</code>"
        )
        if (
            str(getattr(r, "technical_signal", "HOLD") or "HOLD") == "SELL"
            and float(getattr(macro_snap, "sp500_chg", 0.0) or 0.0) > 1.0
            and float(getattr(macro_snap, "vix", 99.0) or 99.0) < 18.0
        ):
            h.append("   ⚠️ <b>Divergencia técnico/macro:</b> SELL técnico con contexto risk-on.")

        if d and d.reason_secondary:
            h.append(f"   Motivo: {escape(d.reason_secondary)}.")
            if (
                "Optimizer sugería" in str(d.reason_secondary)
                and action_str in {
                    DecisionType.HOLD.value,
                    DecisionType.WATCH.value,
                    DecisionType.BLOCKED.value,
                }
            ):
                h.append(
                    "   Lectura: optimizer teórico y señal operativa divergen; "
                    "no se ejecuta sin confirmación del score."
                )
        else:
            h.append(f"   Lectura: {escape(lectura)}.")

        if action_str != DecisionType.HOLD.value:
            h.append(
                f"   Capas: <code>técnico {tech:+.3f} | "
                f"macro {macro:+.3f} | "
                f"{sentiment_label}</code>"
            )

        source_mode = str(
            getattr(r, "technical_candle_source_mode", "unknown") or "unknown"
        )
        data_quality, data_quality_reason = _technical_data_quality(r)
        h.append(
            f"   Datos: <b>{escape(data_quality)}</b> — "
            f"{escape(data_quality_reason)}"
        )
        if source_mode != "official":
            h.append(f"   Fuente técnica: <b>{escape(_technical_source_label(r))}</b>")

        h.append("")

    result_tickers = {str(getattr(r, "ticker", "") or "").upper() for r in sorted_results}
    missing_positions = [
        p for p in (positions or [])
        if str(p.get("ticker", "") or "").upper()
        and str(p.get("ticker", "") or "").upper() not in result_tickers
    ]
    if missing_positions:
        for p in sorted(
            missing_positions,
            key=lambda item: -float(item.get("market_value", 0) or 0),
        ):
            ticker = str(p.get("ticker", "") or "").upper()
            cw = float(current_w.get(ticker, 0.0))
            price = float(p.get("current_price", p.get("last_price", 0)) or 0)
            market_value = float(p.get("market_value", 0) or 0)
            reason = str(p.get("market_data_reason") or "sin velas Cocos suficientes")
            if is_position_operable(p):
                reason = "sin mínimo de 60 velas Cocos para análisis técnico"

            h.append(
                f"⚪ <b>{escape(ticker)}</b> → <b>NO_EVALUABLE</b> | "
                f"peso {_pct(cw)} | valor {_money_ars(market_value)}"
            )
            if price > 0:
                h.append(f"   Precio actual: <b>${price:,.2f}</b>")
            h.append(
                "   Lectura: está en cartera, pero no entra al optimizer ni a "
                f"señal operativa: {escape(reason)}."
            )
            h.append("")

    # ── CONTEXTO MACRO ────────────────────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(tg_section("Contexto de mercado"))

    macro_parts = []

    for attr, fmt in [
        ("wti", "WTI ${:.1f}"),
        ("brent", "Brent ${:.1f}"),
        ("dxy", "DXY {:.1f}"),
        ("vix", "VIX {:.1f}"),
        ("sp500", "SP500 {:,.0f}"),
        ("merval", "Merval {:,.0f}"),
    ]:
        v = getattr(macro_snap, attr, None)

        if v is not None:
            macro_parts.append(fmt.format(float(v)).replace(",", "."))

    tnx = getattr(macro_snap, "tnx", None)

    if tnx:
        macro_parts.append(f"10Y {float(tnx):.2f}%")

    h.append(" | ".join(macro_parts))

    arg_parts = []
    ccl_v = getattr(macro_snap, "ccl", None)
    mep_v = getattr(macro_snap, "mep", None)
    reservas_v = getattr(macro_snap, "reservas", None)
    riesgo_v = getattr(macro_snap, "riesgo_pais", None)

    if ccl_v:
        arg_parts.append(f"CCL ${ccl_v:,.0f}")
    if mep_v:
        arg_parts.append(f"MEP ${mep_v:,.0f}")
    if reservas_v:
        arg_parts.append(f"Reservas ${reservas_v:,.0f}M")
    if riesgo_v:
        rp_icon = "🔴" if riesgo_v > 1000 else "🟡" if riesgo_v > 600 else "🟢"
        arg_parts.append(f"Riesgo País {rp_icon} {riesgo_v} pb")

    if arg_parts:
        h.append("🇦🇷 " + " | ".join(arg_parts))

    h.append(f"Gate actual: <b>{escape(gate)}</b>")
    h.append("")

    _append_ic_section(h, ic_metrics)

    # ── OPTIMIZER — bloque INFORMATIVO ────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(tg_section("Optimizer"))

    opt = _get(rebalance_report, "optimization", rebalance_report)

    if opt:
        method = escape(str(_get(opt, "method", "N/A")))
        obj_str = escape(str(_get(opt, "method_reason", _get(opt, "reason", "N/A"))))
        actual_engine_raw = str(_get(opt, "actual_engine", "") or _get(opt, "method", "N/A"))
        engine_note_raw = str(_get(opt, "engine_note", "") or "")
        actual_engine = escape(actual_engine_raw)
        engine_note = escape(engine_note_raw)
        ret = float(_get(opt, "expected_return_annual", 0.0) or 0.0)
        vol = float(_get(opt, "expected_vol_annual", 0.0) or 0.0)
        sharpe = float(_get(opt, "sharpe_ratio", 0.0) or 0.0)

        h.append(f"Método: <b>{method}</b> | {obj_str}")
        h.append(
            f"Motor real: <b>{actual_engine}</b>"
            + (f" — {engine_note}" if engine_note else "")
        )

        if 0 < ret < 2.0:
            h.append(
                f"Ret esperado: <b>{ret:.1%}</b> | "
                f"Vol: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>"
            )
        else:
            h.append(f"Vol estimada: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>")

    if plan and plan.decisions:
        h.append("<b>Pesos objetivo:</b>")

        for d in sorted(plan.decisions, key=lambda x: x.ticker):
            if d.action == DecisionType.HOLD and abs(d.delta_weight) < 0.015:
                continue

            arrow = "📈" if d.delta_weight > 0.03 else "📉" if d.delta_weight < -0.03 else "➡️"

            h.append(
                f"  {arrow} <b>{d.ticker}</b>: {d.current_weight:.1%} → "
                f"<b>{d.target_weight:.1%}</b>  ({d.delta_weight:+.1%})"
            )

        h.append("")
        h.append(
            tg_note(
                "Los pesos objetivo son teóricos; el execution planner puede bloquearlos por guards de calidad."
            )
        )

    h.append("")

    # ── RADAR COCOS — compacto dentro de /analisis ────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if radar_skipped:
        h.append(tg_section("Radar Cocos"))
        h.append("Omitido en modo rapido para acelerar el plan de cartera.")
        h.append("Usa <code>/radar</code> o <code>/analisis_full</code> para revisar oportunidades del universo.")
        h.append("")
        h.append(tg_note("Plan informativo hasta fill confirmado. No es asesoramiento financiero."))
        report = "\n".join(h)
        valid_html, errors = validate_telegram_html(report)
        if not valid_html:
            logger.warning("run_analysis HTML potencialmente invalido: %s", errors[:3])
        return report

    if opportunity_report is not None:
        _append_analysis_radar(
            h,
            opportunity_report=opportunity_report,
            total_ars=total_ars,
            available_cash_ars=(
                execution_plan.cash_after
                if execution_plan is not None
                else getattr(opportunity_report, "available_cash_ars", cash_ars)
            ),
            execution_plan=execution_plan,
        )
        h.append("")
        h.append(tg_note("Plan y radar son informativos hasta fill confirmado. No es asesoramiento financiero."))
        report = "\n".join(h)
        valid_html, errors = validate_telegram_html(report)
        if not valid_html:
            logger.warning("run_analysis HTML potencialmente inválido: %s", errors[:3])
        return report

    h.append(tg_section("Radar Cocos"))

    owned = {str(p.get("ticker", "")).upper() for p in positions or []}
    radar = []

    for r in universe_results or []:
        ticker = str(getattr(r, "ticker", "")).upper()

        if not ticker or ticker in owned:
            continue

        score = float(getattr(r, "final_score", getattr(r, "score", 0)) or 0)
        conviction = _extract_conviction(r)
        decision = str(getattr(r, "decision", "HOLD")).upper()
        lectura, _ = _component_reason(r)

        if score >= 0.18 and conviction >= 0.50 and decision in ("BUY", "ACCUMULATE"):
            tier, label = 0, "COMPRA FUERTE"
        elif score >= 0.10 and conviction >= 0.35 and decision in ("BUY", "ACCUMULATE"):
            tier, label = 1, "EN VIGILANCIA"
        elif score >= 0.03 and conviction >= 0.20:
            tier, label = 2, "OBSERVAR"
        else:
            continue

        radar.append({
            "ticker": ticker,
            "score": score,
            "conviction": conviction,
            "label": label,
            "tier": tier,
            "lectura": lectura,
        })

    radar.sort(key=lambda x: (x["tier"], -x["conviction"], -x["score"]))

    # Máximo 3 dentro de /analisis para evitar que Telegram lo parta.
    shown_radar = radar[:3]

    if not shown_radar:
        h.append("Sin compras claras en el universo de Cocos.")

        if gate in ("CAUTIOUS", "BLOCKED"):
            h.append(f"Gate {gate}: mantener en observación hasta mejora del régimen.")
        else:
            h.append("Esperar señales técnicas más definidas para actuar.")
    else:
        strong = [x for x in shown_radar if x["tier"] == 0]
        watch = [x for x in shown_radar if x["tier"] == 1]
        obs = [x for x in shown_radar if x["tier"] == 2]
        available_for_new_entries = plan.cash_after if plan else cash_ars

        if strong:
            if available_for_new_entries >= MIN_TRADE_ARS:
                h.append("🟢🟢 <b>Compras fuertes</b>")
            else:
                h.append("🟢🟢 <b>Candidatos fuertes sin funding</b>")
                h.append("   Sin cash libre: solo vía swap o venta financiadora.")
            for x in strong:
                h.append(
                    f"   <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code>"
                )
                h.append(f"   └ {escape(x['lectura'])}")

        if watch:
            h.append("👁 <b>En vigilancia</b>")
            for x in watch:
                h.append(
                    f"  <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code> "
                    f"| {escape(x['lectura'])}"
                )

        if obs:
            h.append("👁 En observación")
            for x in obs:
                h.append(
                    f"  <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code>"
                )

        if len(radar) > len(shown_radar):
            h.append(f"  <i>+{len(radar) - len(shown_radar)} más en /radar</i>")

    external_universe_tickers = external_universe_tickers or []
    if external_universe_tickers:
        h.append(
            f"🕯️ <b>{len(external_universe_tickers)} tickers de Cocos sin histórico operable</b>: "
            "detectados en mercado, pero sin 60 velas canónicas."
        )

    h.append("")

    h.append(tg_note("Plan y radar son informativos hasta fill confirmado. No es asesoramiento financiero."))

    report = "\n".join(h)
    valid_html, errors = validate_telegram_html(report)
    if not valid_html:
        logger.warning("run_analysis HTML potencialmente inválido: %s", errors[:3])
    return report

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def _portfolio_equity_total(
    positions: list[dict],
    cash_ars: float | int | None,
    snapshot_total_ars: float | int | None,
) -> float:
    """Valor total de cartera para pesos/sizing: posiciones + cash.

    `snapshot_total_ars` queda solo como fallback defensivo. La fuente operativa
    para pesos debe incluir cash explícito; si se usa solo invertido, el optimizer
    infla posiciones cuando hay mucho efectivo.
    """
    invested = sum(float(p.get("market_value", 0) or 0) for p in positions or [])
    cash = max(float(cash_ars or 0.0), 0.0)
    if invested > 0 or cash > 0:
        return invested + cash
    return max(float(snapshot_total_ars or 0.0), 0.0)


async def _load_portfolio(cfg, owner_chat_id: int | None = None):
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
        if not snap:
            logger.error("Sin snapshots en DB — correr scraper primero")
            sys.exit(1)
        positions = snap.get("positions", [])
        cash_ars  = float(snap.get("cash_ars", 0))
        try:
            positions = normalize_positions_with_fresh_market_prices(
                positions,
                await db.get_latest_market_prices(),
            )
            discrepancies = price_discrepancy_warnings(positions)
            if discrepancies:
                for item in discrepancies[:8]:
                    logger.warning(
                        "Precio portfolio vs market_prices discrepante: %s snapshot=%s market=%s diff=%+.1f%%",
                        item.get("ticker"),
                        item.get("snapshot_price"),
                        item.get("market_price"),
                        float(item.get("discrepancy_pct") or 0.0) * 100.0,
                    )
        except Exception as exc:
            logger.warning("No se pudo auditar frescura de portfolio: %s", exc)

        total_ars = _portfolio_equity_total(
            positions,
            cash_ars,
            float(snap.get("total_value_ars", 0) or 0),
        )
        history   = await db.get_portfolio_history(limit=60, owner_chat_id=owner_chat_id)
        latest_broker_movement = await db.get_latest_broker_movement_summary()
        return positions, total_ars, cash_ars, history, snap, latest_broker_movement
    finally:
        await db.close()


async def _load_active_manual_market_events(cfg) -> list[ManualMarketEvent]:
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        return await db.get_active_manual_market_events()
    finally:
        await db.close()


async def _load_cocos_history_frames(cfg, positions: list[dict], limit: int = 260) -> dict:
    """Carga historia local de Cocos desde DB para los tickers disponibles."""
    frames = {}
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        for position in positions:
            ticker = str(position.get("ticker", "") or "").upper()
            if not ticker:
                continue
            if not is_position_operable(position):
                logger.warning(
                    "Ticker %s no operable para técnico: %s",
                    ticker,
                    position.get("market_data_reason", "precio no fresco"),
                )
                continue
            rows = await db.get_market_candles(
                ticker,
                asset_type=position.get("asset_type"),
                limit=limit,
            )
            frame = candles_to_frame(rows)
            if len(frame) >= 60:
                frames[ticker] = frame
    finally:
        await db.close()
    return frames


async def main(
    tickers_override: list[str],
    period:           str,
    no_telegram:      bool,
    no_llm:           bool,
    no_sentiment:     bool,
    no_optimizer:     bool = False,
    skip_radar:       bool = False,
    no_persist:       bool = False,
    owner_chat_id:    int | None = None,
    run_intent:       str = "formal_plan",
):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    analysis_run_id = str(uuid4())
    no_persist, run_intent, off_market_context = _analysis_run_policy(
        no_persist,
        run_intent,
    )
    if off_market_context:
        logger.info(
            "Analisis fuera de rueda: modo exploratory/no-persist; sentiment permanece activo"
        )

    # ── 1. Posiciones ──────────────────────────────────────────────────────────
    if tickers_override:
        positions = [{"ticker": t, "market_value": 0} for t in tickers_override]
        total_ars = cash_ars = 0.0
        history   = []
        portfolio_snapshot = None
        latest_broker_movement = None
    else:
        (
            positions,
            total_ars,
            cash_ars,
            history,
            portfolio_snapshot,
            latest_broker_movement,
        ) = await _load_portfolio(
            cfg,
            owner_chat_id=owner_chat_id,
        )
        snapshot_stale_reason = _portfolio_snapshot_stale_reason(portfolio_snapshot)
        if snapshot_stale_reason:
            if isinstance(portfolio_snapshot, dict):
                portfolio_snapshot["_stale_reason"] = snapshot_stale_reason
            logger.error(
                "Snapshot portfolio stale para plan formal: %s; fuerzo no-persist/exploratory",
                snapshot_stale_reason,
            )
            no_persist = True
            run_intent = "exploratory"

    tickers = [p["ticker"] for p in positions]
    logger.info(f"Pipeline: {tickers} | periodo={period}")

    manual_market_events: list[ManualMarketEvent] = []
    manual_event_blocklist: dict[str, str] = {}
    try:
        manual_market_events = await _load_active_manual_market_events(cfg)
        manual_event_blocklist = active_event_risk_by_ticker(manual_market_events)
        if manual_market_events:
            logger.warning(
                "Eventos/catalysts manuales activos: %s; blocklist compras=%s",
                [event.title for event in manual_market_events],
                sorted(manual_event_blocklist),
            )
    except Exception as exc:
        logger.warning("No se pudieron cargar eventos manuales; sigo sin guard: %s", exc)

    # ── 2. Macro ───────────────────────────────────────────────────────────────
    logger.info("Descargando macro...")
    macro_snap   = fetch_macro()
    macro_regime = get_macro_regime(macro_snap)
    logger.info(f"Régimen: {macro_regime}")

    # ── 3. Técnico ─────────────────────────────────────────────────────────────
    logger.info("Calculando técnico...")
    cocos_frames = await _load_cocos_history_frames(cfg, positions)
    if cocos_frames:
        logger.info(f"Historial Cocos disponible para {len(cocos_frames)}/{len(tickers)} tickers")

    tech_signals = analyze_portfolio_from_frames(cocos_frames)
    missing_tickers = [ticker for ticker in tickers if ticker not in cocos_frames]
    if missing_tickers:
        logger.warning(
            "Historial canonico faltante para %s; se omite tecnico operativo",
            missing_tickers,
        )
    tech_map     = {s.ticker: s for s in tech_signals}
    prices_map   = {}
    for ticker in tickers:
        df = cocos_frames.get(ticker)
        if df is not None and "Close" in df.columns:
            prices_map[ticker] = df["Close"].squeeze()

    # ── 4. Risk ────────────────────────────────────────────────────────────────
    logger.info("Calculando riesgo...")
    portfolio_risk = build_portfolio_risk_report(
        positions  = positions,
        prices_map = prices_map,
        total_ars  = total_ars,
        cash_ars   = cash_ars,
        history    = history,
        vix        = macro_snap.vix,
    )
    risk_map = {p["ticker"]: p for p in portfolio_risk.positions}

    # ── 5. Sentiment ───────────────────────────────────────────────────────────
    sentiment_contexts = {}
    try:
        db_sent = PortfolioDatabase(cfg.database.url)
        await db_sent.connect()
        pool_sent = await db_sent.get_pool()
        if pool_sent:
            async with pool_sent.acquire() as conn:
                sentiment_contexts = await load_sentiment_contexts(conn, tickers)
        await db_sent.close()
        active_contexts = [
            ticker for ticker, ctx in sentiment_contexts.items()
            if ticker != "MACRO" and getattr(ctx, "active", False)
        ]
        if active_contexts:
            logger.info("Sentiment contextual disponible para %s", active_contexts)
    except Exception as exc:
        logger.debug("Sentiment contextual no disponible: %s", exc)

    sentiment_map = {}
    if not no_sentiment:
        logger.info("Cargando sentiment contextual...")
        macro_context = sentiment_contexts.get("MACRO")
        for ticker in tickers:
            context = sentiment_contexts.get(ticker)
            scoring_context = context
            if scoring_context is None or not getattr(scoring_context, "active", False):
                if macro_context is not None and getattr(macro_context, "active", False):
                    scoring_context = macro_context
                else:
                    scoring_context = None
            if scoring_context is None:
                continue
            raw_score = float(getattr(scoring_context, "score", 0.0) or 0.0)
            confidence = float(getattr(scoring_context, "confidence", 0.0) or 0.0)
            guarded_score = max(-1.0, min(1.0, raw_score * max(confidence, 0.2)))
            sentiment_contexts[ticker] = scoring_context
            sentiment_map[ticker] = SimpleNamespace(
                score=guarded_score,
                active=True,
                top_headlines=[
                    {
                        "title": getattr(scoring_context, "top_summary", ""),
                        "source": "sentiment_aggregated",
                    }
                ],
            )
    else:
        logger.info("Sentiment omitido (--no-sentiment)")

    # ── 6. Síntesis ────────────────────────────────────────────────────────────
    logger.info("Sintetizando...")
    results = []
    for ticker in tickers:
        tech   = tech_map.get(ticker)
        risk_p = risk_map.get(ticker, {
            "risk_level": "NORMAL", "warnings": [],
            "suggested_pct_adj": 0.10, "current_pct": 0.25,
            "volatility_annual": 0.0, "sharpe": 0.0, "action": "MANTENER",
        })
        sent        = sentiment_map.get(ticker)
        sentiment_active = bool(sent and getattr(sent, "active", False))
        macro_score, macro_reasons = score_macro_for_ticker(ticker, macro_snap)

        if not tech:
            logger.warning(f"Sin datos técnicos para {ticker}")
            continue

        result = blend_scores(
            ticker            = ticker,
            technical_signal  = tech.signal,
            technical_strength= tech.strength,
            macro_score       = macro_score,
            risk_position     = risk_p,
            sentiment_score   = sent.score if sent else 0.0,
            technical_score_raw = getattr(tech, "score_raw", 0.0),
            skip_sentiment    = no_sentiment or not sentiment_active,
            technical_candle_source_mode=getattr(tech, "candle_source_mode", "unknown"),
            technical_has_reconstructed_candles=getattr(tech, "has_reconstructed_candles", False),
            technical_candle_sources=getattr(tech, "candle_sources", ()),
            technical_candle_source_counts=getattr(tech, "candle_source_counts", {}),
        )
        result.technical_signal = str(getattr(tech, "signal", "HOLD") or "HOLD")
        result.technical_regime = str(
            getattr(tech, "technical_regime", "TRANSITIONAL") or "TRANSITIONAL"
        )
        result.trend_score = float(getattr(tech, "trend_score", 0.0) or 0.0)
        result.trend_components = dict(getattr(tech, "trend_components", {}) or {})
        result.structural_break_confirmed = bool(
            getattr(tech, "structural_break_confirmed", False)
        )
        result.overbought_momentum = bool(getattr(tech, "overbought_momentum", False))
        context = sentiment_contexts.get(ticker)
        if context is not None:
            result.sentiment_context = context

        if not no_llm:
            result = synthesize_with_llm_local(
                result             = result,
                macro_snap         = macro_snap,
                macro_reasons      = macro_reasons,
                technical_reasons  = tech.reasons,
                sentiment_headlines= sent.top_headlines if sent else [],
                risk_position      = risk_p,
                portfolio_context  = {
                    "total_ars": total_ars,
                    "cash_ars": cash_ars,
                    "regime": macro_regime,
                },
            )

        results.append(result)

    # ── 7. Universo Cocos ──────────────────────────────────────────────────────
    universe_results = []
    opportunity_report: OpportunityReport | None = None
    external_universe_tickers: list[str] = []
    cocos_universe: list[str] = []
    cocos_universe_assets: list[dict] = []
    universe_frames: dict = {}
    try:
        db_u = PortfolioDatabase(cfg.database.url)
        await db_u.connect()
        cocos_universe_assets = await db_u.get_cocos_universe_assets()
        cocos_universe = [asset["ticker"] for asset in cocos_universe_assets]
        await db_u.close()

        owned_set        = {t.upper() for t in tickers}
        universe_assets  = [
            asset for asset in cocos_universe_assets
            if asset["ticker"].upper() not in owned_set
        ]
        universe_tickers = [asset["ticker"] for asset in universe_assets]

        if not cocos_universe:
            logger.warning("Universo Cocos vacío — el scraper aún no pobló market_prices.")
        elif not universe_tickers:
            logger.info("Universo Cocos: todos los tickers ya están en cartera.")

        if skip_radar:
            logger.info("Radar interno omitido (--skip-radar); usando solo cartera para plan operativo")
        elif universe_tickers:
            logger.info(
                "Analizando universo Cocos: %s activos por segmento %s",
                len(universe_tickers),
                _count_assets_by_type(universe_assets),
            )
            universe_frames = await _load_cocos_history_frames(cfg, universe_assets)
            logger.info(
                "Historial Cocos disponible para universo: %s/%s tickers",
                len(universe_frames),
                len(universe_tickers),
            )
            u_tech_signals = analyze_portfolio_from_frames(universe_frames)
            missing_universe = [ticker for ticker in universe_tickers if ticker not in universe_frames]
            if missing_universe:
                logger.warning(
                    "Historial Cocos faltante para universo %s; quedan sin histórico operable",
                    missing_universe,
                )
                external_universe_tickers = missing_universe
            u_tech_map     = {s.ticker: s for s in u_tech_signals}

            u_sent_map = {}
            universe_sentiment_contexts = {}
            if not no_sentiment:
                try:
                    db_sent = PortfolioDatabase(cfg.database.url)
                    await db_sent.connect()
                    pool_sent = await db_sent.get_pool()
                    if pool_sent:
                        async with pool_sent.acquire() as conn:
                            universe_sentiment_contexts = await load_sentiment_contexts(conn, universe_tickers)
                    await db_sent.close()
                except Exception as exc:
                    logger.debug("Sentiment contextual universo no disponible: %s", exc)
                for ticker in universe_tickers:
                    context = universe_sentiment_contexts.get(ticker)
                    if context is None or not getattr(context, "active", False):
                        continue
                    raw_score = float(getattr(context, "score", 0.0) or 0.0)
                    confidence = float(getattr(context, "confidence", 0.0) or 0.0)
                    u_sent_map[ticker] = SimpleNamespace(
                        score=max(-1.0, min(1.0, raw_score * max(confidence, 0.2))),
                        active=True,
                        top_headlines=[
                            {
                                "title": getattr(context, "top_summary", ""),
                                "source": "sentiment_aggregated",
                            }
                        ],
                    )

            for ticker in universe_tickers:
                u_tech = u_tech_map.get(ticker)
                if not u_tech:
                    continue
                u_macro_score, _ = score_macro_for_ticker(ticker, macro_snap)
                u_sent           = u_sent_map.get(ticker)
                u_sentiment_active = bool(u_sent and getattr(u_sent, "active", False))
                u_result         = blend_scores(
                    ticker              = ticker,
                    technical_signal    = u_tech.signal,
                    technical_strength  = u_tech.strength,
                    macro_score         = u_macro_score,
                    risk_position       = {
                        "risk_level": "NORMAL", "warnings": [],
                        "suggested_pct_adj": 0.05, "current_pct": 0.0,
                        "volatility_annual": 0.0, "sharpe": 0.0, "action": "MANTENER",
                    },
                    sentiment_score     = u_sent.score if u_sent else 0.0,
                    technical_score_raw = getattr(u_tech, "score_raw", 0.0),
                    skip_sentiment      = no_sentiment or not u_sentiment_active,
                    technical_candle_source_mode=getattr(u_tech, "candle_source_mode", "unknown"),
                    technical_has_reconstructed_candles=getattr(u_tech, "has_reconstructed_candles", False),
                    technical_candle_sources=getattr(u_tech, "candle_sources", ()),
                    technical_candle_source_counts=getattr(u_tech, "candle_source_counts", {}),
                )
                context = universe_sentiment_contexts.get(ticker)
                if context is not None:
                    u_result.sentiment_context = context
                universe_results.append(u_result)

            n_strong = sum(
                1 for r in universe_results
                if r.decision in ("BUY", "ACCUMULATE") and r.final_score > 0.25
            )
            logger.info(f"Universo: {len(universe_results)} resultados, {n_strong} compras claras")

            portfolio_scores = {
                str(getattr(r, "ticker", "") or "").upper(): float(
                    getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0
                )
                for r in results or []
                if str(getattr(r, "ticker", "") or "").strip()
            }
            asset_types = {
                str(asset.get("ticker", "") or "").upper(): asset.get("asset_type", "UNKNOWN")
                for asset in universe_assets
                if str(asset.get("ticker", "") or "").strip()
            }
            opportunity_report = run_opportunity_analysis(
                universe=universe_tickers,
                portfolio_positions=positions,
                macro_snap=macro_snap,
                macro_regime=macro_regime,
                period=period,
                no_sentiment=no_sentiment,
                portfolio_scores=portfolio_scores,
                max_candidates=8,
                min_score=0.10,
                min_rr=0.0,
                exclude_portfolio=True,
                history_frames=universe_frames,
                asset_types=asset_types,
                available_cash_ars=cash_ars,
                sentiment_contexts=sentiment_contexts,
            )
            if opportunity_report.externos:
                external_universe_tickers = [c.ticker for c in opportunity_report.externos]

    except Exception as e:
        logger.warning(f"Análisis de universo falló (no crítico): {e}")

    # ── 8. Portfolio Optimizer ─────────────────────────────────────────────────
    rebalance_report = None
    if not no_optimizer and results:
        logger.info("Ejecutando Portfolio Optimizer...")
        optimizer_positions = [
            p for p in (positions or [])
            if is_position_operable(p)
            and str(p.get("ticker", "") or "").upper() in tech_map
        ]
        rebalance_report = run_optimizer(
            current_positions   = optimizer_positions,
            portfolio_value_ars = total_ars,
            cash_ars            = cash_ars,
            macro_regime        = macro_regime,
            vix                 = macro_snap.vix,
            synthesis_results   = results,
            market_assets       = cocos_universe_assets,
            history_frames       = cocos_frames,
        )
        if rebalance_report:
            opt = rebalance_report.optimization
            logger.info(
                f"Optimizer [{opt.method}] gate={rebalance_report.risk_gate_state}: "
                f"{rebalance_report.n_trades} trades — "
                f"ventas ${rebalance_report.total_sells_ars:,.0f}  "
                f"compras ${rebalance_report.total_buys_ars:,.0f}"
            )
    else:
        logger.info("Optimizer omitido")

    # ── 9. Execution Plan ──────────────────────────────────────────────────────
    # Convierte el target teórico del optimizer en órdenes ejecutables reales.
    # cash_after siempre cuadra. El render usa SOLO este objeto para las secciones
    # operativas (acción principal, plan de rotación, veredicto).
    execution_plan: ExecutionPlan | None = None

    if rebalance_report and total_ars > 0:
        logger.info("Construyendo execution plan...")
        try:
            signals_by_ticker = build_signals_from_synthesis(results)
            current_positions = build_positions_from_snapshot(positions, total_ars)
            gate_state        = rebalance_report.risk_gate_state

            decisions = derive_decision_intents(
                rebalance_report    = rebalance_report,
                signals_by_ticker   = signals_by_ticker,
                current_positions   = current_positions,
                portfolio_value_ars = total_ars,
                gate                = gate_state,
            )
            logger.info(
                f"Decisions: "
                + ", ".join(
                    f"{d.ticker}={d.action.value}" for d in decisions
                    if d.action not in (DecisionType.HOLD,)
                )
            )

            execution_plan = reconcile_funding(
                decisions           = decisions,
                current_positions   = current_positions,
                cash_before         = cash_ars,
                portfolio_value_ars = total_ars,
                gate                = gate_state,
                external_buys       = _radar_buys_for_execution(
                    opportunity_report,
                    total_ars,
                ),
                blocked_buy_tickers = manual_event_blocklist,
            )
            logger.info(
                f"ExecutionPlan: sells={_money_ars(execution_plan.gross_sell_ars)} "
                f"buys={_money_ars(execution_plan.gross_buy_ars)} "
                f"cash_after={_money_ars(execution_plan.cash_after)} "
                f"feasible={execution_plan.feasible}"
            )

            # Validación dura — si falla, el plan se marca infeasible pero no aborta
            try:
                validate_execution_plan(execution_plan)
            except PlanValidationError as ve:
                logger.error(f"Validación plan falló: {ve}")
                execution_plan.warnings.insert(0, f"⚠️ Error de validación: {str(ve)[:200]}")
                execution_plan.feasible = False

        except Exception as e:
            logger.error(f"Execution plan falló (no crítico): {e}", exc_info=True)
            execution_plan = None
    else:
        logger.info("Paso 9: sin optimizer o portfolio vacío — execution plan vacío")
        if rebalance_report:
            # Construir plan vacío con gate info para el render
            from src.analysis.execution_planner import (
                DecisionIntent, ExecutionPlan as EP
            )
            gate_state = rebalance_report.risk_gate_state
            execution_plan = EP(
                decisions=[], sell_orders=[], buy_orders=[],
                blocked_orders=[], cash_before=cash_ars,
                gross_sell_ars=0.0, fee_sell_ars=0.0, net_sell_ars=0.0,
                gross_buy_ars=0.0, fee_buy_ars=0.0, cash_after=cash_ars,
                feasible=True, gate=gate_state,
                summary="Sin rebalanceo necesario.",
            )

    # ── 9.5 Guardar eventos del ExecutionPlan en decision_log ─────────────────
    if no_persist:
        logger.info("Paso 9.5: omitido por --no-persist; no se guarda decision_log")
    elif execution_plan and total_ars > 0:
        logger.info(
            "Paso 9.5: guardando eventos ExecutionPlan — "
            "APPROVED/BLOCKED con source=execution_plan"
        )
        try:
            saved = await _save_execution_plan_events(
                cfg=cfg,
                execution_plan=execution_plan,
                results=results,
                macro_snap=macro_snap,
                macro_regime=macro_regime,
                total_ars=total_ars,
                positions=positions,
                owner_chat_id=owner_chat_id,
                run_id=analysis_run_id,
                run_intent=run_intent,
                manual_market_events=manual_market_events,
            )
            logger.info(f"Eventos ExecutionPlan guardados en DB: ids={saved}")
        except Exception as e:
            logger.warning(f"No se pudieron guardar eventos ExecutionPlan (no crítico): {e}")
    else:
        logger.info("Paso 9.5: sin execution_plan o portfolio vacío — skip")

    # ── 10. Information Coefficient ────────────────────────────────────────────
    ic_metrics = await _compute_information_coefficient(
        cfg,
        tickers=tickers,
        lookback_days=180,
        owner_chat_id=owner_chat_id,
    )
    p_h  = ic_metrics.get("primary_horizon", "5d")
    p_ic = ic_metrics.get("primary_ic", None)
    p_n  = int(ic_metrics.get("primary_n_obs", 0) or 0)
    if p_ic is None:
        logger.info(f"IC {p_h}: sin datos suficientes (n={p_n})")
    else:
        logger.info(f"IC {p_h}: {p_ic:+.3f} (n={p_n})")

    # ── 11. Render → stdout ────────────────────────────────────────────────────
    report = render_report(
        results          = results,
        macro_snap       = macro_snap,
        total_ars        = total_ars,
        cash_ars         = cash_ars,
        portfolio_risk   = portfolio_risk,
        rebalance_report = rebalance_report,
        positions        = positions,
        universe_results = universe_results,
        external_universe_tickers = external_universe_tickers,
        ic_metrics       = ic_metrics,
        execution_plan   = execution_plan,
        opportunity_report = opportunity_report,
        portfolio_snapshot = portfolio_snapshot,
        latest_broker_movement = latest_broker_movement,
        radar_skipped = skip_radar,
        no_persist = no_persist,
        off_market_context = off_market_context,
        manual_market_events = manual_market_events,
    )

    # Validación de consistencia del header vs plan (no bloquea el envío)
    if execution_plan and execution_plan.main_action:
        try:
            validate_report_consistency(
                main_ticker = execution_plan.main_action.ticker,
                main_amount = execution_plan.main_action.amount_ars,
                plan        = execution_plan,
            )
        except PlanValidationError as ve:
            logger.error(f"Inconsistencia reporte/plan: {ve}")

    print(report)

    if not no_telegram and cfg.scraper.telegram_enabled:
        logger.info("Enviando a Telegram...")
        notifier.send_raw(report)
        logger.info("Reporte enviado")
    else:
        logger.info("Telegram omitido")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Pipeline cuantitativo Cocos")
    p.add_argument("--tickers",      nargs="+", default=[])
    p.add_argument("--period",       default="1y",
                   choices=["1mo", "3mo", "6mo", "1y", "2y"])
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-llm",       action="store_true")
    p.add_argument("--no-sentiment", action="store_true")
    p.add_argument("--no-optimizer", action="store_true")
    p.add_argument(
        "--no-persist",
        action="store_true",
        help="Ejecuta el analisis sin guardar eventos en decision_log",
    )
    p.add_argument(
        "--skip-radar",
        action="store_true",
        help="Omite el radar interno de universo dentro de /analisis para acelerar el plan de cartera",
    )
    p.add_argument("--owner-chat-id", type=int, default=None)
    p.add_argument(
        "--run-intent",
        choices=["formal_plan", "exploratory"],
        default="formal_plan",
        help="Alcance auditable para decision_log (default: formal_plan)",
    )
    args = p.parse_args()
    asyncio.run(main(
        tickers_override = args.tickers,
        period           = args.period,
        no_telegram      = args.no_telegram,
        no_llm           = args.no_llm,
        no_sentiment     = args.no_sentiment,
        no_optimizer     = args.no_optimizer,
        skip_radar       = args.skip_radar,
        no_persist       = args.no_persist,
        owner_chat_id    = args.owner_chat_id,
        run_intent       = args.run_intent,
    ))
