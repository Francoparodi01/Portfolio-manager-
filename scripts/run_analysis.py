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
  python scripts/run_analysis.py --no-llm --no-sentiment
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
from datetime import datetime, timedelta
from html import escape

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.technical import (
    analyze_portfolio,
    analyze_portfolio_from_frames,
    fetch_history,
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
    Action,
)
from src.analysis.validators import (
    validate_execution_plan,
    validate_report_consistency,
    soft_validate,
    PlanValidationError,
)
from src.analysis.risk_levels import compute_risk_levels
from src.collector.cocos_history import candles_to_frame

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
    cfg, tickers: list[str], lookback_days: int = 180
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
                      AND decision != 'HOLD'
                    """,
                    cutoff, ticker_filter,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT ticker, final_score, outcome_5d, outcome_10d, outcome_20d
                    FROM decision_log
                    WHERE decided_at >= $1 AND decision != 'HOLD'
                    """,
                    cutoff,
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
        return payload

    payload["technical"] = _layer_payload("technical")
    payload["macro"] = _layer_payload("macro")
    payload["sentiment"] = _layer_payload("sentiment")
    payload["risk"] = _layer_payload("risk")

    payload["final_score"] = _safe_float(getattr(result, "final_score", 0.0))
    payload["decision_from_synthesis"] = str(getattr(result, "decision", "") or "")
    payload["confidence"] = _safe_float(
        getattr(result, "conviction", getattr(result, "confidence", 0.0))
    )

    return payload

# ══════════════════════════════════════════════════════════════════════════════
# GUARDAR DECISIONES DEL EXECUTION PLAN EN DECISION_LOG
# ══════════════════════════════════════════════════════════════════════════════

async def _save_execution_plan_events(
    *,
    cfg,
    execution_plan,
    results,
    macro_snap,
    macro_regime,
    total_ars: float,
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
            return _layers_payload_for_decision(r, extra=extra)
        except Exception:
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

        price = _price_from_result(r)
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

        exists = await conn.fetchval(
            """
            SELECT 1
            FROM decision_log
            WHERE ticker = $1
              AND decision = $2
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND COALESCE(status, '') = $3
              AND decided_at > NOW() - INTERVAL '20 hours'
            LIMIT 1
            """,
            ticker,
            decision,
            status,
        )

        if exists:
            logger.info(
                "Dedup execution_plan: %s %s status=%s ya existe en últimas 20h — skip",
                decision,
                ticker,
                status,
            )
            return None

        size_pct = abs(delta_weight) if delta_weight else (
            _safe_float(amount_ars, 0.0) / total_ars if total_ars else 0.0
        )

        row = await conn.fetchrow(
            """
            INSERT INTO decision_log (
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
                was_blocked
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6::jsonb,
                $7, $8, $9,
                $10, $11, $12, $13, $14,
                $15, $16, $17, $18,
                $19, $20, $21, $22, $23,
                $24, $25
            )
            RETURNING id
            """,
            datetime.now(timezone.utc),
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
        )

        return int(row["id"]) if row else None

    conn = await asyncpg.connect(db_url)

    try:
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
    import yfinance as yf
    import pandas as _pd
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

    _tickers_to_price = list(current_w.keys())
    price_map: dict[str, float | None] = {}
    if _tickers_to_price:
        try:
            _raw = yf.download(
                _tickers_to_price, period="5d",
                progress=False, auto_adjust=True,
            )["Close"]
            if isinstance(_raw, _pd.Series):
                _raw = _raw.to_frame(name=_tickers_to_price[0])
            for _t in _tickers_to_price:
                _col = _raw.get(_t) if hasattr(_raw, "get") else (
                    _raw[_t] if _t in _raw.columns else None
                )
                if _col is not None and not _col.dropna().empty:
                    price_map[_t] = float(_col.dropna().iloc[-1])
                else:
                    price_map[_t] = None
                    logger.warning(f"price_usd: sin datos para {_t}")
        except Exception as _e:
            logger.warning(f"yfinance bulk price fetch error: {_e}")
            price_map = {t: None for t in _tickers_to_price}

    logger.info(
        f"price_map USD: { {k: f'${v:.2f}' if v else 'None' for k,v in price_map.items()} }"
    )

    trades_to_save = []
    trades = getattr(rebalance_report, "trades", []) or []

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
            "decided_at":    datetime.utcnow(),
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
                      AND decided_at > NOW() - INTERVAL '20 hours'
                    LIMIT 1
                    """,
                    t["ticker"], t["direction"],
                )
                if exists:
                    logger.info(f"Dedup: {t['direction']} {t['ticker']} ya existe hoy — skip")
                    continue

                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        decided_at, ticker, decision, final_score, confidence,
                        layers, price_at_decision, vix_at_decision, regime,
                        size_pct, stop_loss_pct, target_pct, horizon_days, rr_ratio
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14
                    ) RETURNING id
                    """,
                    t["decided_at"], t["ticker"], t["direction"],
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


# ══════════════════════════════════════════════════════════════════════════════
# RENDER — fuente única de verdad: ExecutionPlan
# ══════════════════════════════════════════════════════════════════════════════

def _action_icon(action: Action | str) -> str:
    m = {
        Action.SELL_FULL:    "🔴",
        Action.SELL_PARTIAL: "🔴",
        Action.BUY:          "🟢",
        Action.HOLD:         "🟡",
        Action.WATCH:        "🔵",
        Action.BLOCKED:      "⛔",
    }
    if isinstance(action, str):
        try:
            action = Action(action)
        except ValueError:
            return "🟡"
    return m.get(action, "🟡")


def render_report(
    results,
    macro_snap,
    total_ars:        float,
    cash_ars:         float,
    portfolio_risk,
    rebalance_report,
    positions:        list,
    universe_results: list,
    ic_metrics:       dict | None = None,
    execution_plan:   ExecutionPlan | None = None,
) -> str:
    plan = execution_plan
    gate = plan.gate if plan else "NORMAL"
    h = []

    # ── Header ────────────────────────────────────────────────────────────────
    h.append("🧠 <b>ANÁLISIS SEMANAL — SISTEMA CUANTITATIVO</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART")
    h.append(f"💼 Portfolio: <b>{_money_ars(total_ars)}</b>")
    h.append("")

    # ── RESUMEN EJECUTIVO ─────────────────────────────────────────────────────
    h.append("<b>RESUMEN EJECUTIVO</b>")

    if gate == "BLOCKED":
        h.append("🔴 Sistema en modo bloqueado — solo se ejecutan stops urgentes.")
    elif gate in ("CAUTIOUS", "DEFENSIVE"):
        h.append("⚠️ Mercado en modo defensivo — VIX elevado o régimen risk-off.")
    else:
        h.append("✅ Régimen operativo normal — sistema operando sin restricciones.")

    if plan:
        h.append(plan.summary)

        for w in plan.warnings[:3]:
            h.append(f"⚠️ {escape(w)}")
    else:
        h.append("Sin plan de ejecución disponible.")

    h.append("")

    # ── INFORMATION COEFFICIENT ────────────────────────────────────────────────
    ic_data = ic_metrics or {}

    if ic_data.get("has_data"):
        h.append("<b>INFORMATION COEFFICIENT (IC)</b>")

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

        h.append("IC > 0 indica poder predictivo direccional.")
        h.append(
            f"{ic_regime.icon} Régimen IC: <b>{ic_regime.label}</b> — "
            f"{escape(ic_regime.note)}"
        )
        h.append("")

    # ── ACCIÓN PRINCIPAL — desde plan.main_action, NUNCA del optimizer ────────
    h.append("<b>ACCIÓN PRINCIPAL</b>")

    if plan and plan.main_action:
        main_order = plan.main_action
        verb = "VENDER" if main_order.side.value == "SELL" else "COMPRAR"
        icon = "🔴" if main_order.side.value == "SELL" else "🟢"
        partial_tag = " <i>(parcial)</i>" if main_order.partial else ""

        h.append(
            f"{icon} <b>{verb} {main_order.ticker} "
            f"({_money_ars(main_order.amount_ars)}){partial_tag}</b>"
        )
        h.append(f"   {escape(main_order.reason)}")

        d = next((x for x in plan.decisions if x.ticker == main_order.ticker), None)

        if d:
            h.append(
                f"   Peso actual: <b>{_pct(d.current_weight)}</b> → "
                f"objetivo: <b>{_pct(d.target_weight)}</b>"
            )

        if main_order.partial and main_order.theoretical_ars > main_order.amount_ars:
            h.append(
                f"   💡 Target teórico: {_money_ars(main_order.theoretical_ars)} — "
                f"ejecutable hoy: {_money_ars(main_order.amount_ars)} "
                f"(completar cuando haya más funding)"
            )
    else:
        h.append("🟡 <b>SIN ACCIÓN ACTIVA</b>")
        h.append("   No hay órdenes ejecutables en este momento.")

    h.append("")

    # ── RESULTADO ESPERADO — del plan cash accounting ─────────────────────────
    h.append("💵 <b>Resultado esperado</b>")

    if plan:
        h.append(f"   Ventas: <b>{_money_ars(plan.gross_sell_ars)}</b>")

        if gate in ("CAUTIOUS", "BLOCKED") and plan.gross_sell_ars > 0:
            h.append(
                f"   Compras: <b>$0 ARS</b> "
                f"(gate {gate} — solo reducciones activas)"
            )
        else:
            h.append(f"   Compras: <b>{_money_ars(plan.gross_buy_ars)}</b>")

        total_fees = plan.fee_sell_ars + plan.fee_buy_ars

        if total_fees > 500:
            h.append(f"   Fees estimados: {_money_ars(total_fees)}")

        h.append(f"   Cash luego del ajuste: <b>{_money_ars(plan.cash_after)}</b>")

        if plan.pending_buys:
            h.append(
                f"   ⏳ Compras pendientes por funding: {', '.join(plan.pending_buys)}"
            )
    else:
        h.append(f"   Sin plan disponible — cash actual: {_money_ars(cash_ars)}")

    h.append("")

    # ── PLAN DE ROTACIÓN — SOLO sell_orders + buy_orders del plan ─────────────
    if plan and (plan.sell_orders or plan.buy_orders):
        h.append("📋 <b>PLAN DE ROTACIÓN</b>")

        step = 1

        for o in sorted(plan.sell_orders, key=lambda x: x.priority):
            h.append(
                f"   {step}. 🔴 Vender <b>{o.ticker}</b>: "
                f"-{_money_ars(o.amount_ars)}"
            )

            if o.action == Action.SELL_FULL:
                h.append("      → Liquidación total (target 0%)")

            step += 1

        for o in sorted(plan.buy_orders, key=lambda x: x.priority):
            ext_icon = "🌍" if o.priority >= 3 else "📈"
            partial_tag = " <i>(parcial)</i>" if o.partial else ""
            d = next((x for x in plan.decisions if x.ticker == o.ticker), None)
            score_tag = f" | score {d.score:+.3f}" if d and d.score is not None else ""

            h.append(
                f"   {step}. {ext_icon} Comprar <b>{o.ticker}</b>: "
                f"+{_money_ars(o.amount_ars)}{partial_tag}{score_tag}"
            )
            step += 1

        if plan.cash_after > 5_000:
            h.append(f"   → Cash remanente: {_money_ars(plan.cash_after)}")

        if plan.blocked_orders:
            h.append("")
            h.append("   🚫 <b>Bloqueadas / WATCH por guardias:</b>")

            for o in plan.blocked_orders[:3]:
                action_name = o.action.value if hasattr(o.action, "value") else str(o.action)
                icon = "🔵" if action_name == "WATCH" else "⛔"

                h.append(
                    f"      {icon} {o.ticker}: {action_name} — "
                    f"{_money_ars(o.theoretical_ars)} teórico — "
                    f"{escape(o.reason)}"
                )

        h.append("")

    elif plan and plan.blocked_orders:
        h.append("📋 <b>PLAN DE ROTACIÓN</b>")
        h.append("   Sin órdenes ejecutables.")

        h.append("")
        h.append("   🚫 <b>Bloqueadas / WATCH por guardias:</b>")

        for o in plan.blocked_orders[:5]:
            action_name = o.action.value if hasattr(o.action, "value") else str(o.action)
            icon = "🔵" if action_name == "WATCH" else "⛔"

            h.append(
                f"      {icon} {o.ticker}: {action_name} — "
                f"{_money_ars(o.theoretical_ars)} teórico — "
                f"{escape(o.reason)}"
            )

        h.append("")

    elif plan and gate in ("CAUTIOUS", "BLOCKED") and plan.gross_sell_ars > 0:
        h.append("📋 <b>PLAN DE ROTACIÓN</b>")
        h.append(
            f"   Gate {gate} activo: los {_money_ars(plan.gross_sell_ars)} "
            f"de ventas quedan en cash."
        )
        h.append("")

    # ── CARTERA ACTUAL — señal del activo + decisión de cartera separadas ─────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>CARTERA ACTUAL</b>")
    h.append("")

    current_w = _current_weights(positions, total_ars)
    decision_map = {d.ticker: d for d in (plan.decisions if plan else [])}

    action_priority = {
        Action.SELL_FULL.value: 0,
        Action.SELL_PARTIAL.value: 1,
        Action.BUY.value: 2,
        Action.BLOCKED.value: 3,
        Action.WATCH.value: 4,
        Action.HOLD.value: 5,
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
        icon = _action_icon(d.action if d else Action.HOLD)

        tech = _layer_weighted(r, "technical")
        macro = _layer_weighted(r, "macro")
        sent = _layer_weighted(r, "sentiment")

        ars_str = ""

        if plan:
            for o in plan.sell_orders + plan.buy_orders:
                if o.ticker == ticker:
                    verb = "-" if o.side.value == "SELL" else "+"
                    ars_str = f" → {verb}{_money_ars(o.amount_ars)}"

                    if o.partial:
                        ars_str += " <i>(parcial)</i>"

                    break

        h.append(f"{icon} <b>{ticker}</b> → <b>{action_str}</b>{ars_str}")

        h.append(
            f"   {_render_signal_line(score, tech, macro, sent)} | "
            f"Peso: {_pct(cw)} → {_pct(tw)}"
        )

        if d and d.reason_secondary:
            h.append(f"   {escape(d.reason_secondary)}.")
        else:
            h.append(f"   {escape(lectura)}.")

        h.append(
            f"   <code>técnico {tech:+.3f} | "
            f"macro {macro:+.3f} | "
            f"sentiment {sent:+.3f}</code>"
        )

        h.append("")

    # ── CONTEXTO MACRO ────────────────────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>CONTEXTO DE MERCADO</b>")

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

    # ── OPTIMIZER — bloque INFORMATIVO ────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>OPTIMIZER</b>")

    opt = _get(rebalance_report, "optimization", rebalance_report)

    if opt:
        method = escape(str(_get(opt, "method", "N/A")))
        obj_str = escape(str(_get(opt, "method_reason", _get(opt, "reason", "N/A"))))
        ret = float(_get(opt, "expected_return_annual", 0.0) or 0.0)
        vol = float(_get(opt, "expected_vol_annual", 0.0) or 0.0)
        sharpe = float(_get(opt, "sharpe_ratio", 0.0) or 0.0)

        h.append(f"Método: <b>{method}</b> | {obj_str}")

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
            if d.action == Action.HOLD and abs(d.delta_weight) < 0.015:
                continue

            arrow = "📈" if d.delta_weight > 0.03 else "📉" if d.delta_weight < -0.03 else "➡️"

            h.append(
                f"  {arrow} <b>{d.ticker}</b>: {d.current_weight:.1%} → "
                f"<b>{d.target_weight:.1%}</b>  ({d.delta_weight:+.1%})"
            )

        h.append("")
        h.append(
            "<i>Nota: los pesos objetivo son teóricos; "
            "el execution planner puede bloquearlos por guards de calidad.</i>"
        )

    h.append("")

    # ── RADAR EXTERNO — compacto dentro de /analisis ──────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>RADAR EXTERNO</b>")

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

        if strong:
            h.append("🟢🟢 <b>Compras fuertes</b>")
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

    h.append("")

    # ── VEREDICTO FINAL — derivado EXCLUSIVAMENTE del ExecutionPlan ───────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>VEREDICTO FINAL</b>")

    if plan:
        h.append(plan.verdict())
    elif gate == "BLOCKED":
        h.append("Sistema bloqueado por gate de riesgo — solo stops de emergencia.")
    else:
        h.append("Sin plan de ejecución disponible — mantener y observar.")

    h.append("")
    h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")

    return "\n".join(h)

# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def _load_portfolio(cfg):
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot()
        if not snap:
            logger.error("Sin snapshots en DB — correr scraper primero")
            sys.exit(1)
        positions = snap.get("positions", [])
        total_ars = float(snap.get("total_value_ars", 0))
        cash_ars  = float(snap.get("cash_ars", 0))
        history   = await db.get_portfolio_history(limit=60)
        return positions, total_ars, cash_ars, history
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
):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    # ── 1. Posiciones ──────────────────────────────────────────────────────────
    if tickers_override:
        positions = [{"ticker": t, "market_value": 0} for t in tickers_override]
        total_ars = cash_ars = 0.0
        history   = []
    else:
        positions, total_ars, cash_ars, history = await _load_portfolio(cfg)

    tickers = [p["ticker"] for p in positions]
    logger.info(f"Pipeline: {tickers} | periodo={period}")

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
            "Historial Cocos faltante para %s; usando fallback legacy temporal",
            missing_tickers,
        )
        tech_signals.extend(analyze_portfolio(missing_tickers, period=period))
    tech_map     = {s.ticker: s for s in tech_signals}
    prices_map   = {}
    for ticker in tickers:
        df = cocos_frames.get(ticker)
        if df is None:
            df = fetch_history(ticker, period=period)
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
    sentiment_map = {}
    if not no_sentiment:
        logger.info("Analizando sentiment...")
        for ticker in tickers:
            sentiment_map[ticker] = fetch_sentiment(ticker)
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
        )

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
    cocos_universe: list[str] = []
    cocos_universe_assets: list[dict] = []
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

        if universe_tickers:
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
                    "Historial Cocos faltante para universo %s; usando fallback legacy temporal",
                    missing_universe,
                )
                u_tech_signals.extend(analyze_portfolio(missing_universe, period=period))
            u_tech_map     = {s.ticker: s for s in u_tech_signals}

            u_sent_map = {}
            if not no_sentiment:
                strong_tech = [
                    s.ticker for s in u_tech_signals
                    if s.signal == "BUY" and s.strength > 0.40
                ]
                for ticker in strong_tech[:8]:
                    u_sent_map[ticker] = fetch_sentiment(ticker)

            for ticker in universe_tickers:
                u_tech = u_tech_map.get(ticker)
                if not u_tech:
                    continue
                u_macro_score, _ = score_macro_for_ticker(ticker, macro_snap)
                u_sent           = u_sent_map.get(ticker)
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
                )
                universe_results.append(u_result)

            n_strong = sum(
                1 for r in universe_results
                if r.decision in ("BUY", "ACCUMULATE") and r.final_score > 0.25
            )
            logger.info(f"Universo: {len(universe_results)} resultados, {n_strong} compras claras")

    except Exception as e:
        logger.warning(f"Análisis de universo falló (no crítico): {e}")

    # ── 8. Portfolio Optimizer ─────────────────────────────────────────────────
    rebalance_report = None
    if not no_optimizer and results:
        logger.info("Ejecutando Portfolio Optimizer...")
        rebalance_report = run_optimizer(
            current_positions   = positions,
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
                    if d.action not in (Action.HOLD,)
                )
            )

            execution_plan = reconcile_funding(
                decisions           = decisions,
                current_positions   = current_positions,
                cash_before         = cash_ars,
                portfolio_value_ars = total_ars,
                gate                = gate_state,
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
    if execution_plan and total_ars > 0:
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
            )
            logger.info(f"Eventos ExecutionPlan guardados en DB: ids={saved}")
        except Exception as e:
            logger.warning(f"No se pudieron guardar eventos ExecutionPlan (no crítico): {e}")
    else:
        logger.info("Paso 9.5: sin execution_plan o portfolio vacío — skip")

    # ── 10. Information Coefficient ────────────────────────────────────────────
    ic_metrics = await _compute_information_coefficient(
        cfg, tickers=tickers, lookback_days=180
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
        ic_metrics       = ic_metrics,
        execution_plan   = execution_plan,
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
    p.add_argument("--period",       default="6mo",
                   choices=["1mo", "3mo", "6mo", "1y", "2y"])
    p.add_argument("--no-telegram",  action="store_true")
    p.add_argument("--no-llm",       action="store_true")
    p.add_argument("--no-sentiment", action="store_true")
    p.add_argument("--no-optimizer", action="store_true")
    args = p.parse_args()
    asyncio.run(main(
        tickers_override = args.tickers,
        period           = args.period,
        no_telegram      = args.no_telegram,
        no_llm           = args.no_llm,
        no_sentiment     = args.no_sentiment,
        no_optimizer     = args.no_optimizer,
    ))
