"""
scripts/run_analysis.py — Pipeline cuantitativo completo.

Flujo:
  1. Carga posiciones desde DB (o override por CLI)
  2. Descarga macro (yfinance + APIs Argentina)
  3. Análisis técnico multicapa (todos los tickers)
  4. Risk engine por posición
  5. Sentiment (RSS, opcional)
  6. Síntesis: blend de capas → score + decisión + conviction
  7. LLM Ollama: razonamiento explicativo (no modifica decisión)
  8. Universo Cocos: escaneo de candidatos fuera de cartera
  9. Portfolio Optimizer (Black-Litterman / Min-Variance)
  10. Render HTML → stdout (Telegram lo captura)

Output limpio:
  - Todo el logging va a stderr (INFO, WARNING, ERROR)
  - Solo print(report) va a stdout — el bot captura esto

Uso:
  python scripts/run_analysis.py
  python scripts/run_analysis.py --tickers CVX NVDA
  python scripts/run_analysis.py --no-llm --no-sentiment
  python scripts/run_analysis.py --no-telegram
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.technical import analyze_portfolio, fetch_history
from src.analysis.macro import fetch_macro, score_macro_for_ticker, get_macro_regime
from src.analysis.sentiment import fetch_sentiment
from src.analysis.risk import build_portfolio_risk_report
from src.analysis.synthesis import SynthesisResult, LayerScore, blend_scores, synthesize_with_llm_local
from src.analysis.optimizer import run_optimizer
from src.analysis.decision_engine import make_decisions_from_results

import numpy as np
from html import escape
from datetime import datetime, timedelta

logger = get_logger(__name__)

LAYER_WEIGHTS = {"technical": 0.30, "macro": 0.30, "risk": 0.25, "sentiment": 0.15}


# ══════════════════════════════════════════════════════════════════════════════
# UTILIDADES DE RENDER
# ══════════════════════════════════════════════════════════════════════════════

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


def _conv_label(x: float) -> str:
    x = float(x or 0.0)
    if x >= 0.70: return "ALTA"
    if x >= 0.45: return "MEDIA"
    if x >= 0.25: return "BAJA"
    return "MUY BAJA"


def _bar(x: float) -> str:
    n = max(0, min(5, round(float(x or 0.0) * 5)))
    return "█" * n + "░" * (5 - n)


def _rankdata(values: np.ndarray) -> np.ndarray:
    """Ranking con promedio para empates (similar a scipy.stats.rankdata)."""
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
        avg_rank = (i + j + 2) / 2.0  # ranks 1..n
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


async def _compute_information_coefficient(cfg, tickers: list[str], lookback_days: int = 180) -> dict:
    """
    Calcula IC histórico usando decision_log:
      IC (Pearson) y Rank IC (Spearman) entre final_score y outcome_{5d,10d,20d}.
    """
    db = PortfolioDatabase(cfg.database.url)
    cutoff = datetime.now() - timedelta(days=lookback_days)
    horizons = ("5d", "10d", "20d")
    metrics = {
        "lookback_days": lookback_days,
        "by_horizon": {},
        "has_data": False,
    }

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
                    cutoff,
                    ticker_filter,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT ticker, final_score, outcome_5d, outcome_10d, outcome_20d
                    FROM decision_log
                    WHERE decided_at >= $1
                      AND decision != 'HOLD'
                    """,
                    cutoff,
                )

        for hz in horizons:
            k = f"outcome_{hz}"
            xs, ys = [], []
            covered = set()
            for r in rows:
                score = r["final_score"]
                out = r[k]
                if score is None or out is None:
                    continue
                score = float(score)
                out = float(out)
                if not np.isfinite(score) or not np.isfinite(out):
                    continue
                xs.append(score)
                ys.append(out)
                covered.add(str(r["ticker"]).upper())

            pearson = _safe_corr(xs, ys)
            rank_ic = _safe_corr(_rankdata(np.asarray(xs)), _rankdata(np.asarray(ys))) if len(xs) >= 5 else None
            metrics["by_horizon"][hz] = {
                "ic": pearson,
                "rank_ic": rank_ic,
                "n_obs": len(xs),
                "n_tickers": len(covered),
                "quality": _ic_label(pearson),
            }

        primary = metrics["by_horizon"].get("5d", {})
        metrics["primary_horizon"] = "5d"
        metrics["primary_ic"] = primary.get("ic")
        metrics["primary_rank_ic"] = primary.get("rank_ic")
        metrics["primary_n_obs"] = primary.get("n_obs", 0)
        metrics["has_data"] = any((v.get("n_obs", 0) >= 5) for v in metrics["by_horizon"].values())
        return metrics
    except Exception as e:
        logger.warning(f"IC: no se pudo calcular ({e})")
        return metrics
    finally:
        try:
            await db.close()
        except Exception:
            pass


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
    tech  = _layer_weighted(result, "technical")
    macro = _layer_weighted(result, "macro")
    sent  = _layer_weighted(result, "sentiment")

    positives, negatives = [], []
    if tech  > 0.02: positives.append("técnico")
    elif tech  < -0.02: negatives.append("técnico")
    if macro > 0.02: positives.append("macro")
    elif macro < -0.02: negatives.append("macro")
    if sent  > 0.02: positives.append("sentiment")
    elif sent  < -0.02: negatives.append("sentiment")

    if positives and negatives:
        lectura = f"{positives[0]} ayuda, pero {negatives[0]} frena"
    elif positives:
        lectura = f"{positives[0]} sostiene la señal"
    elif negatives:
        lectura = f"{negatives[0]} domina en contra"
    else:
        lectura = "señal plana sin ventaja clara"

    mags = {"technical": abs(tech), "macro": abs(macro), "sentiment": abs(sent)}
    top  = max(mags, key=mags.get)
    top_val = {"technical": tech, "macro": macro, "sentiment": sent}[top]
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


def _extract_trade_maps(rebalance_report) -> tuple[dict, set]:
    """
    Retorna (trade_map, blocked_buys).
    trade_map[ticker] = RebalanceTrade obj
    blocked_buys = set de tickers con compra bloqueada por el gate
    """
    trade_map    = {}
    blocked_buys = set()
    if not rebalance_report:
        return trade_map, blocked_buys

    trades = _get(rebalance_report, "trades", None) or \
             _get(_get(rebalance_report, "optimization", None), "trades", None) or []

    for tr in trades:
        ticker = str(_get(tr, "ticker", "") or "").upper()
        if ticker:
            trade_map[ticker] = tr

    blocked = (_get(rebalance_report, "blocked_trades", None) or
               _get(rebalance_report, "blocked", None) or
               _get(_get(rebalance_report, "optimization", None), "blocked_trades", None) or [])

    for b in blocked:
        ticker = str(_get(b, "ticker", "") or "").upper()
        action = str(_get(b, "action", "") or "").upper()
        if ticker and ("BUY" in action or "COMPRAR" in action or action == ""):
            blocked_buys.add(ticker)

    return trade_map, blocked_buys


def _target_weight(ticker: str, current_w: float, trade_map: dict) -> float:
    """
    Lee el peso objetivo desde el trade del optimizer.
    FIX: busca 'weight_optimal' PRIMERO (campo real de RebalanceTrade).
    """
    tr = trade_map.get(ticker)
    if not tr:
        return current_w
    # Campo real del optimizer (RebalanceTrade.weight_optimal)
    for key in ("weight_optimal", "target_weight", "new_weight", "after_weight",
                "weight_after", "to_weight"):
        val = _get(tr, key, None)
        if val is not None:
            val = float(val)
            return val / 100.0 if val > 1.5 else val
    for key in ("target_pct", "after_pct", "to_pct", "weight_pct_after"):
        val = _get(tr, key, None)
        if val is not None:
            return float(val) / 100.0
    delta = _get(tr, "delta_weight", _get(tr, "delta_pct", _get(tr, "delta", None)))
    if delta is not None:
        d = float(delta)
        return current_w + (d / 100.0 if abs(d) > 1.5 else d)
    return current_w


def _action_label(score: float, current_w: float,
                  target_w: float, blocked_buy: bool = False) -> tuple[str, str]:
    delta = float(target_w) - float(current_w)
    if blocked_buy and delta >= 0.03:
        return "NO AUMENTAR", "🟡"
    if float(target_w) <= 0.01 and float(score) <= -0.20:
        return "SALIR", "🔴"
    if delta <= -0.05:
        return "RECORTAR", "🔴"
    if delta >= 0.05:
        return "AUMENTAR", "🟢"
    return "MANTENER", "🟡"


def _normalize_conviction(x) -> float:
    try:
        if x is None:
            return 0.0
        x = float(x)
        return max(0.0, min(1.0, x / 100.0 if x > 1.0 else x))
    except Exception:
        return 0.0


def _extract_conviction(result) -> float:
    for key in ("conviction", "confidence", "confidence_pct", "conviction_pct"):
        val = _get(result, key, None)
        if val is not None:
            return _normalize_conviction(val)
    return 0.0


def _extract_position_size(result, portfolio_risk, ticker: str) -> float:
    for key in ("position_size", "suggested_size", "sizing", "size"):
        val = _get(result, key, None)
        if val is not None:
            v = float(val)
            return v / 100.0 if v > 1.5 else v
    positions = _get(portfolio_risk, "positions", []) or []
    for p in positions:
        if str(_get(p, "ticker", "") or "").upper() != ticker:
            continue
        for key in ("suggested_pct_adj", "suggested_pct", "target_pct", "position_size"):
            val = _get(p, key, None)
            if val is not None:
                v = float(val)
                return v / 100.0 if v > 1.5 else v
    return 0.0


def _extract_risk_gate(rebalance_report) -> str:
    """
    FIX: busca 'risk_gate_state' PRIMERO (campo real de RebalanceReport).
    """
    for path in [
        ("risk_gate_state",),
        ("risk_gate",),
        ("gate",),
        ("optimization", "risk_gate_state"),
        ("optimization", "risk_gate"),
        ("optimization", "gate"),
    ]:
        obj = rebalance_report
        for key in path:
            obj = _get(obj, key, None)
            if obj is None:
                break
        if obj and isinstance(obj, str):
            return obj.upper()
    return "NORMAL"


def _extract_cash_metrics(rebalance_report, cash_ars: float,
                           trade_map: dict, current_w: dict,
                           total_ars: float) -> tuple[float, float, float]:
    # Intentar desde campos directos del RebalanceReport
    sells = _get(rebalance_report, "total_sells_ars", _get(rebalance_report, "total_sell_ars", None))
    buys  = _get(rebalance_report, "total_buys_ars",  _get(rebalance_report, "total_buy_ars",  None))
    if sells is not None and buys is not None:
        s = float(sells); b = float(buys)
        return s, b, max(0.0, float(cash_ars) + s - b)

    # Fallback: reconstruir desde trades
    s = b = 0.0
    for ticker, tr in trade_map.items():
        action = str(_get(tr, "action", "") or "").upper()
        amount = None
        for key in ("amount_ars", "trade_amount_ars", "notional_ars", "amount"):
            v = _get(tr, key, None)
            if v is not None:
                amount = float(v); break
        if amount is None:
            bw = float(current_w.get(ticker, 0.0))
            aw = _target_weight(ticker, bw, trade_map)
            amount = abs(aw - bw) * float(total_ars)
        if "SELL" in action or "VENDER" in action or "REDUCIR" in action:
            s += amount
        elif "BUY" in action or "COMPRAR" in action or "NUEVO" in action:
            b += amount
        else:
            bw = float(current_w.get(ticker, 0.0))
            aw = _target_weight(ticker, bw, trade_map)
            (b if aw > bw else s).__add__(amount)
    return s, b, float(cash_ars) + s - b


# ══════════════════════════════════════════════════════════════════════════════
# RENDER PRINCIPAL — HTML para Telegram
# ══════════════════════════════════════════════════════════════════════════════

def _compute_rotation_plan(sells_ars: float, radar: list, rows: list,
                            gate: str, total_ars: float) -> list[dict]:
    """
    Dado un monto de ventas disponible, calcula cómo reasignarlo.
    Retorna lista de {ticker, ars, pct, is_external, score} o [] si el gate bloquea.
    """
    if sells_ars < 5_000 or gate in ("CAUTIOUS", "BLOCKED") or total_ars <= 0:
        return []

    candidates = []
    # Radar externo (compras fuertes/débiles)
    for x in radar:
        if x["decision"] in ("COMPRA FUERTE", "COMPRA DÉBIL"):
            priority = 0 if x["decision"] == "COMPRA FUERTE" else 1
            candidates.append({
                "ticker": x["ticker"], "score": x["score"],
                "conviction": x["conviction"], "priority": priority,
                "is_external": True,
            })
    # Posiciones internas con AUMENTAR
    for row in rows:
        if row["action"] == "AUMENTAR":
            candidates.append({
                "ticker": row["ticker"], "score": max(row["score"], 0.01),
                "conviction": row["conviction"], "priority": 2,
                "is_external": False,
            })

    if not candidates:
        return []

    candidates.sort(key=lambda x: (x["priority"], -x["conviction"] * max(x["score"], 0.01)))
    candidates = candidates[:4]

    # Límite por posición: 15% del portfolio, máximo 50% del cash disponible por pick
    max_per = min(total_ars * 0.15, sells_ars * 0.50)
    weights = [max(c["conviction"] * max(c["score"], 0.03), 0.01) for c in candidates]
    total_w = sum(weights) or 1.0

    plan, remaining = [], sells_ars
    for c, w in zip(candidates, weights):
        amount = min(sells_ars * (w / total_w), max_per, remaining)
        if amount >= 5_000:
            plan.append({
                "ticker": c["ticker"], "ars": amount,
                "pct": amount / total_ars,
                "is_external": c["is_external"],
                "score": c["score"],
            })
            remaining -= amount
    return plan


def render_report(results, macro_snap, total_ars: float, cash_ars: float,
                  portfolio_risk, rebalance_report, positions: list,
                  universe_results: list, ic_metrics: dict | None = None) -> str:
    """
    Genera el reporte semanal completo en HTML para Telegram.

    Fixes:
      - IC solo se muestra cuando hay datos reales (≥5 observaciones)
      - AUMENTAR/NO AUMENTAR es consistente con el gate activo
      - Plan de rotación muestra ARS concretos cuando hay ventas
      - Radar externo muestra monto sugerido en ARS
    """
    current_w    = _current_weights(positions, total_ars)
    trade_map, blocked_buys = _extract_trade_maps(rebalance_report)
    risk_gate    = _extract_risk_gate(rebalance_report)
    gate_blocks  = risk_gate in {"CAUTIOUS", "BLOCKED"}

    # ── Preparar filas por activo ──────────────────────────────────────────────
    rows = []
    for r in results or []:
        ticker     = str(getattr(r, "ticker", "")).upper()
        score      = float(_get(r, "final_score", _get(r, "score", 0.0)) or 0.0)
        conviction = _extract_conviction(r)
        cw         = float(current_w.get(ticker, 0.0))
        tw         = _target_weight(ticker, cw, trade_map)
        delta      = tw - cw

        # FIX: si gate bloquea compras, AUMENTAR pasa a NO AUMENTAR
        is_blocked = ticker in blocked_buys or (gate_blocks and delta >= 0.03)
        action, emoji = _action_label(score, cw, tw, is_blocked)
        lectura, motivo = _component_reason(r)
        sizing     = _extract_position_size(r, portfolio_risk, ticker)
        delta_ars  = delta * float(total_ars)

        if action == "RECORTAR":
            motivo = "concentración excesiva"
            lectura = f"{lectura}; peso actual demasiado alto"
        elif action == "NO AUMENTAR":
            motivo = f"gate {risk_gate} — esperar mejora del régimen"
            lectura = f"{lectura}; compra bloqueada temporalmente"
        elif action == "AUMENTAR":
            motivo = "señal favorable con espacio para subir"
        elif action == "MANTENER" and abs(score) < 0.10:
            motivo = "sin ventaja clara"

        rows.append({
            "ticker": ticker, "score": score, "conviction": conviction, "sizing": sizing,
            "current_w": cw, "target_w": tw, "action": action, "emoji": emoji,
            "lectura": lectura, "motivo": motivo, "delta_ars": delta_ars,
            "tech":  _layer_weighted(r, "technical"),
            "macro": _layer_weighted(r, "macro"),
            "sent":  _layer_weighted(r, "sentiment"),
        })

    priority  = {"SALIR": 0, "RECORTAR": 1, "AUMENTAR": 2, "NO AUMENTAR": 3, "MANTENER": 4}
    rows.sort(key=lambda x: (priority.get(x["action"], 9), x["ticker"]))
    reductions = [x for x in rows if x["action"] in {"SALIR", "RECORTAR"}]
    adds       = [x for x in rows if x["action"] == "AUMENTAR"]

    # ── Radar externo ─────────────────────────────────────────────────────────
    owned = {str(p.get("ticker", "")).upper() for p in positions or []}
    radar = []
    for r in universe_results or []:
        ticker     = str(getattr(r, "ticker", "")).upper()
        if not ticker or ticker in owned:
            continue
        score      = float(_get(r, "final_score", _get(r, "score", 0.0)) or 0.0)
        conviction = _extract_conviction(r)
        decision   = str(_get(r, "decision", "HOLD")).upper()
        lectura, motivo = _component_reason(r)

        if score >= 0.18 and conviction >= 0.50 and decision in ("BUY", "ACCUMULATE"):
            tier, label = 0, "COMPRA FUERTE"
        elif score >= 0.10 and conviction >= 0.35 and decision in ("BUY", "ACCUMULATE"):
            tier, label = 1, "COMPRA DÉBIL"
        elif score >= 0.03 and conviction >= 0.20:
            tier, label = 2, "OBSERVAR"
        else:
            continue
        radar.append({"ticker": ticker, "score": score, "conviction": conviction,
                      "decision": label, "lectura": lectura, "motivo": motivo, "tier": tier})

    radar.sort(key=lambda x: (x["tier"], -x["conviction"], -x["score"], x["ticker"]))
    radar = radar[:5]

    # ── Cash metrics ──────────────────────────────────────────────────────────
    total_sell, total_buy, cash_after = _extract_cash_metrics(
        rebalance_report, cash_ars, trade_map, current_w, total_ars
    )

    # ── Plan de rotación ──────────────────────────────────────────────────────
    rotation_plan = _compute_rotation_plan(
        sells_ars=total_sell, radar=radar, rows=rows,
        gate=risk_gate, total_ars=float(total_ars),
    )
    rotation_leftover = total_sell - sum(p["ars"] for p in rotation_plan)

    # ── Resumen ejecutivo ─────────────────────────────────────────────────────
    has_buys = any(x["decision"] in {"COMPRA FUERTE", "COMPRA DÉBIL"} for x in radar)
    summary = []
    if risk_gate == "BLOCKED":
        summary.append("🔴 Sistema en modo bloqueado — solo se ejecutan stops urgentes.")
    elif risk_gate in {"CAUTIOUS", "DEFENSIVE"}:
        summary.append("⚠️ Mercado en modo defensivo — VIX elevado o régimen risk-off.")
    else:
        summary.append("✅ Régimen operativo normal — sistema operando sin restricciones.")

    summary.append(
        "Hay candidatos comprables en el radar externo." if has_buys
        else "Sin compras fuertes fuera de cartera en este momento."
    )
    if reductions:
        summary.append("Prioridad: reducir exposición y ordenar concentración.")
    elif adds:
        summary.append("Prioridad: aumentar selectivamente donde el score lo justifica.")
    else:
        summary.append("Prioridad: mantener posiciones y esperar confirmación de señal.")

    # ── Acción principal ──────────────────────────────────────────────────────
    if reductions:
        m = reductions[0]
        ars_label = f" ({_money_ars(abs(m['delta_ars']))})" if abs(m['delta_ars']) > 1000 else ""
        main_title = f"{m['emoji']} <b>{m['action']} {m['ticker']}{ars_label}</b>"
        main_l1    = f"Peso actual: <b>{_pct(m['current_w'])}</b> → objetivo: <b>{_pct(m['target_w'])}</b>"
        main_l2    = f"Motivo: {escape(m['motivo'])}"
        main_l3    = f"Lectura: {escape(m['lectura'])}"
    elif adds:
        m = adds[0]
        ars_label = f" ({_money_ars(abs(m['delta_ars']))})" if abs(m['delta_ars']) > 1000 else ""
        main_title = f"{m['emoji']} <b>{m['action']} {m['ticker']}{ars_label}</b>"
        main_l1    = f"Peso actual: <b>{_pct(m['current_w'])}</b> → objetivo: <b>{_pct(m['target_w'])}</b>"
        main_l2    = f"Motivo: {escape(m['motivo'])}"
        main_l3    = f"Lectura: {escape(m['lectura'])}"
    else:
        main_title = "🟡 <b>MANTENER CARTERA</b>"
        main_l1    = "No hay ajustes activos habilitados por el sistema."
        main_l2    = "El gate de riesgo prioriza la preservación de capital."
        main_l3    = "Esperar señal más fuerte o mejora del régimen macro."

    # ── Macro lines ───────────────────────────────────────────────────────────
    macro_parts = []
    for attr, fmt in [("wti", "WTI ${:.1f}"), ("brent", "Brent ${:.1f}"),
                      ("dxy", "DXY {:.1f}"), ("vix", "VIX {:.1f}"), ("sp500", "SP500 {:,.0f}"),
                      ("merval", "Merval {:,.0f}")]:
        v = getattr(macro_snap, attr, None)
        if v is not None:
            macro_parts.append(fmt.format(float(v)).replace(",", "."))
    tnx = getattr(macro_snap, "tnx", getattr(macro_snap, "us10y", None))
    if tnx is not None:
        macro_parts.append(f"10Y {float(tnx):.2f}%")

    # ── Optimizer ─────────────────────────────────────────────────────────────
    opt = _get(rebalance_report, "optimization", rebalance_report)

    # ════════════════════════════════════════════
    # RENDER
    # ════════════════════════════════════════════
    h = []

    # Header
    h.append("🧠 <b>ANÁLISIS SEMANAL — SISTEMA CUANTITATIVO</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART")
    h.append(f"💼 Portfolio: <b>{_money_ars(total_ars)}</b>")
    h.append("")

    # Resumen ejecutivo
    h.append("<b>RESUMEN EJECUTIVO</b>")
    h.extend(summary)
    h.append("")

    # IC — solo se muestra cuando hay datos reales
    ic_data = ic_metrics or {}
    if ic_data.get("has_data"):
        h.append("<b>INFORMATION COEFFICIENT (IC)</b>")
        by_h = ic_data.get("by_horizon", {}) or {}
        for hz in ("5d", "10d", "20d"):
            v = by_h.get(hz, {})
            n_obs = int(v.get("n_obs", 0) or 0)
            n_tk  = int(v.get("n_tickers", 0) or 0)
            ic = v.get("ic", None)
            ric = v.get("rank_ic", None)
            if ic is None or n_obs < 5:
                continue
            q = v.get("quality", "NULO")
            h.append(
                f"{hz}: IC <code>{ic:+.3f}</code> | Rank IC <code>{(ric or 0.0):+.3f}</code> "
                f"| n={n_obs} ({n_tk} tickers) | <b>{q}</b>"
            )
        h.append("IC > 0 indica poder predictivo direccional.")
        h.append("")

    # Acción principal
    h.append("<b>ACCIÓN PRINCIPAL</b>")
    h.append(main_title)
    h.append(f"   {main_l1}")
    h.append(f"   {main_l2}")
    h.append(f"   {main_l3}")
    h.append("")

    # Resultado financiero
    h.append("💵 <b>Resultado esperado</b>")
    h.append(f"   Ventas: <b>{_money_ars(total_sell)}</b>")
    if gate_blocks and total_sell > 0:
        h.append(f"   Compras: <b>$0 ARS</b> (gate {risk_gate} — solo reducciones activas)")
    else:
        h.append(f"   Compras: <b>{_money_ars(total_buy)}</b>")
    h.append(f"   Cash luego del ajuste: <b>{_money_ars(cash_after)}</b>")
    h.append("")

    # Plan de rotación — solo si hay ventas y gate permite
    if rotation_plan:
        h.append("📋 <b>PLAN DE ROTACIÓN</b>")
        step = 1
        for red in reductions:
            h.append(f"   {step}. Vender <b>{red['ticker']}</b>: -{_money_ars(abs(red['delta_ars']))} ({_pct(red['current_w'])} → {_pct(red['target_w'])})")
            step += 1
        for buy in rotation_plan:
            tag = "🌍" if buy["is_external"] else "📈"
            h.append(f"   {step}. {tag} Comprar <b>{buy['ticker']}</b>: +{_money_ars(buy['ars'])} (~{_pct(buy['pct'])} portfolio) | score {buy['score']:+.3f}")
            step += 1
        if rotation_leftover > 5_000:
            h.append(f"   → Cash restante: {_money_ars(rotation_leftover)}")
        h.append("")
    elif total_sell > 5_000 and gate_blocks:
        h.append("📋 <b>PLAN DE ROTACIÓN</b>")
        h.append(f"   Gate {risk_gate} activo: los {_money_ars(total_sell)} de ventas quedan en cash.")
        if has_buys:
            picks = ", ".join(x["ticker"] for x in radar if x["tier"] <= 1)
            h.append(f"   Candidatos cuando mejore el régimen: <b>{picks}</b>")
        h.append("")

    # Estado por activo
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>CARTERA ACTUAL</b>")
    h.append("")
    for row in rows:
        c = row["conviction"]
        # Mostrar monto ARS delta cuando es relevante
        ars_str = ""
        if row["action"] in ("RECORTAR", "SALIR") and abs(row["delta_ars"]) > 1000:
            ars_str = f" → -{_money_ars(abs(row['delta_ars']))}"
        elif row["action"] == "AUMENTAR" and abs(row["delta_ars"]) > 1000:
            ars_str = f" → +{_money_ars(abs(row['delta_ars']))}"
        h.append(f"{row['emoji']} <b>{row['ticker']}</b> → <b>{row['action']}</b>{ars_str}")
        h.append(
            f"   Score: <code>{row['score']:+.3f}</code> | "
            f"Conv: <b>{_conv_label(c)}</b> ({round(c*100)}%) [{_bar(c)}] | "
            f"Peso: {_pct(row['current_w'])} → {_pct(row['target_w'])}"
        )
        h.append(f"   {escape(row['lectura'])}.")
        h.append(
            f"   <code>técnico {row['tech']:+.3f} | "
            f"macro {row['macro']:+.3f} | "
            f"sentiment {row['sent']:+.3f}</code>"
        )
        h.append("")

    # Contexto de mercado
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>CONTEXTO DE MERCADO</b>")
    h.append(" | ".join(macro_parts))
    ccl_v      = getattr(macro_snap, "ccl",         None)
    mep_v      = getattr(macro_snap, "mep",         None)
    reservas_v = getattr(macro_snap, "reservas",    None)
    riesgo_v   = getattr(macro_snap, "riesgo_pais", None)
    arg_parts  = []
    if ccl_v:      arg_parts.append(f"CCL ${ccl_v:,.0f}")
    if mep_v:      arg_parts.append(f"MEP ${mep_v:,.0f}")
    if reservas_v: arg_parts.append(f"Reservas ${reservas_v:,.0f}M")
    if riesgo_v:
        rp_icon = "🔴" if riesgo_v > 1000 else "🟡" if riesgo_v > 600 else "🟢"
        arg_parts.append(f"Riesgo País {rp_icon} {riesgo_v} pb")
    if arg_parts:
        h.append("🇦🇷 " + " | ".join(arg_parts))
    h.append(f"Gate actual: <b>{escape(risk_gate)}</b>")
    h.append("")

    # Optimizer
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>OPTIMIZER</b>")
    method  = escape(str(_get(opt, "method", "N/A")))
    obj_str = escape(str(_get(opt, "method_reason", _get(opt, "reason", "N/A"))))
    ret     = float(_get(opt, "expected_return_annual", 0.0) or 0.0)
    vol     = float(_get(opt, "expected_vol_annual",    0.0) or 0.0)
    sharpe  = float(_get(opt, "sharpe_ratio",           0.0) or 0.0)
    h.append(f"Método: <b>{method}</b> | {obj_str}")
    if 0 < ret < 2.0:
        h.append(f"Ret esperado: <b>{ret:.1%}</b> | Vol: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>")
    else:
        h.append(f"Vol estimada: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>")
    if trade_map:
        h.append("<b>Pesos objetivo:</b>")
        for ticker_t in sorted(current_w.keys()):
            cw_t  = current_w.get(ticker_t, 0.0)
            tw_t  = _target_weight(ticker_t, cw_t, trade_map)
            delta_t = tw_t - cw_t
            arrow_t = "📈" if delta_t > 0.03 else "📉" if delta_t < -0.03 else "➡️"
            h.append(f"  {arrow_t} <b>{ticker_t}</b>: {cw_t:.1%} → <b>{tw_t:.1%}</b>  ({delta_t:+.1%})")
    h.append("")

    # Radar externo
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>RADAR EXTERNO</b>")
    if not radar:
        h.append("Sin compras claras en el universo de Cocos.")
        if gate_blocks:
            h.append(f"Gate {risk_gate}: mantener en observación hasta mejora del régimen.")
        else:
            h.append("Esperar señales técnicas más definidas para actuar.")
    else:
        # Estimar cuánto asignar a cada radar pick si hay cash disponible
        available = cash_after if not gate_blocks else 0.0
        radar_alloc: dict[str, float] = {}
        if available > 5_000:
            radar_buys = [x for x in radar if x["tier"] <= 1]
            if radar_buys:
                ws = [x["conviction"] * max(x["score"], 0.03) for x in radar_buys]
                total_w = sum(ws) or 1.0
                for x, w in zip(radar_buys, ws):
                    alloc = min(available * (w / total_w), float(total_ars) * 0.12)
                    radar_alloc[x["ticker"]] = alloc

        strong = [x for x in radar if x["decision"] == "COMPRA FUERTE"]
        weak   = [x for x in radar if x["decision"] == "COMPRA DÉBIL"]
        watch  = [x for x in radar if x["decision"] == "OBSERVAR"]
        if strong:
            h.append("<b>🟢🟢 Compras fuertes</b>")
            for x in strong:
                alloc_str = f" → sugerido: <b>{_money_ars(radar_alloc[x['ticker']])}</b>" \
                            if x["ticker"] in radar_alloc else ""
                h.append(
                    f"   <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code> | "
                    f"conv. {round(x['conviction']*100)}%{alloc_str}"
                )
                h.append(f"   └ {escape(x['lectura'])}")
        if weak:
            h.append("<b>🟢 Compras débiles / tácticas</b>")
            for x in weak:
                alloc_str = f" → sugerido: <b>{_money_ars(radar_alloc[x['ticker']])}</b>" \
                            if x["ticker"] in radar_alloc else ""
                h.append(
                    f"   <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code> | "
                    f"conv. {round(x['conviction']*100)}%{alloc_str}"
                )
                h.append(f"   └ {escape(x['lectura'])}")
        if watch:
            h.append("<b>👁 En observación</b>")
            for x in watch:
                h.append(
                    f"   <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code> | "
                    f"{escape(x['lectura'])}"
                )
    h.append("")

    # Veredicto final
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>VEREDICTO FINAL</b>")
    if rotation_plan:
        h.append("Plan de rotación definido — ejecutar en el orden indicado.")
        h.append("Prioridad: liquidar concentración primero, luego reasignar.")
    elif reductions and gate_blocks:
        h.append(f"Reducir CVX es la única acción habilitada (gate {risk_gate}).")
        h.append("Los fondos quedan en cash hasta que el régimen mejore.")
    elif reductions and has_buys:
        h.append("Reducir concentración en posiciones débiles y rotar hacia mejores señales.")
    elif reductions:
        h.append("El sistema prioriza reducir riesgo. Sin compras claras disponibles.")
    elif adds and has_buys:
        h.append("Hay señales alineadas para aumentar exposición selectiva.")
        h.append("Actuar solo donde score, convicción y régimen coinciden.")
    else:
        h.append("No hay una ventaja operativa clara para actuar hoy.")
        h.append("Mantener, observar y esperar mejor contexto o señal más fuerte.")
    h.append("")
    h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")

    return "\n".join(h)


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE
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


async def main(tickers_override: list[str], period: str,
               no_telegram: bool, no_llm: bool,
               no_sentiment: bool, no_optimizer: bool = False):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    # ── 1. Posiciones ──────────────────────────────────────────────────────────
    if tickers_override:
        positions = [{"ticker": t, "market_value": 0} for t in tickers_override]
        total_ars = cash_ars = 0.0; history = []
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
    tech_signals = analyze_portfolio(tickers, period=period)
    tech_map     = {s.ticker: s for s in tech_signals}
    prices_map   = {}
    for ticker in tickers:
        df = fetch_history(ticker, period=period)
        if df is not None and "Close" in df.columns:
            prices_map[ticker] = df["Close"].squeeze()

    # ── 4. Risk ────────────────────────────────────────────────────────────────
    logger.info("Calculando riesgo...")
    portfolio_risk = build_portfolio_risk_report(
        positions=positions, prices_map=prices_map,
        total_ars=total_ars, cash_ars=cash_ars,
        history=history, vix=macro_snap.vix,
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
        sent  = sentiment_map.get(ticker)
        macro_score, macro_reasons = score_macro_for_ticker(ticker, macro_snap)

        if not tech:
            logger.warning(f"Sin datos técnicos para {ticker}")
            continue

        result = blend_scores(
            ticker=ticker,
            technical_signal=tech.signal,
            technical_strength=tech.strength,
            macro_score=macro_score,
            risk_position=risk_p,
            sentiment_score=sent.score if sent else 0.0,
            technical_score_raw=getattr(tech, "score_raw", 0.0),
        )

        if not no_llm:
            result = synthesize_with_llm_local(
                result=result,
                macro_snap=macro_snap,
                macro_reasons=macro_reasons,
                technical_reasons=tech.reasons,
                sentiment_headlines=sent.top_headlines if sent else [],
                risk_position=risk_p,
                portfolio_context={"total_ars": total_ars, "cash_ars": cash_ars,
                                   "regime": macro_regime},
            )
            # El LLM es SOLO display — no modifica score ni decisión

        results.append(result)


    # ── 6.5 Decision Engine — decisiones forzadas ─────────────────────────────
    logger.info("Generando decisiones forzadas...")
    decisions = make_decisions_from_results(results, macro_snap, macro_regime)
    actionable = [d for d in decisions if d.is_actionable()]
    logger.info(f"Decisiones: {len(actionable)} accionables de {len(decisions)}")
 
    # Guardar en DB (async, no bloquea el pipeline si falla)
    if actionable:
        try:
            db_dec = PortfolioDatabase(cfg.database.url)
            await db_dec.connect()
            saved_ids = []
            for dec in actionable:
                dec_id = await db_dec.save_decision(dec)
                if dec_id:
                    saved_ids.append(dec_id)
            await db_dec.close()
            logger.info(f"Decisiones guardadas en DB: ids={saved_ids}")
        except Exception as e:
            logger.warning(f"No se pudieron guardar decisiones en DB (no crítico): {e}")

    # ── 7. Universo Cocos ──────────────────────────────────────────────────────
    universe_results = []
    cocos_universe: list[str] = []
    try:
        db_u = PortfolioDatabase(cfg.database.url)
        await db_u.connect()
        cocos_universe = await db_u.get_cocos_universe()   # aplica YFINANCE_BLACKLIST
        await db_u.close()

        owned_set        = {t.upper() for t in tickers}
        universe_tickers = [t for t in cocos_universe if t.upper() not in owned_set]

        if not cocos_universe:
            logger.warning("Universo Cocos vacío — el scraper aún no pobló market_prices.")
        elif not universe_tickers:
            logger.info("Universo Cocos: todos los tickers ya están en cartera.")

        if universe_tickers:
            logger.info(f"Analizando universo: {len(universe_tickers)} tickers...")
            u_tech_signals = analyze_portfolio(universe_tickers, period=period)
            u_tech_map     = {s.ticker: s for s in u_tech_signals}

            # Sentiment solo para los que tienen señal técnica BUY clara
            u_sent_map = {}
            if not no_sentiment:
                strong_tech = [s.ticker for s in u_tech_signals
                               if s.signal == "BUY" and s.strength > 0.40]
                for ticker in strong_tech[:8]:
                    u_sent_map[ticker] = fetch_sentiment(ticker)

            for ticker in universe_tickers:
                u_tech = u_tech_map.get(ticker)
                if not u_tech:
                    continue
                u_macro_score, _ = score_macro_for_ticker(ticker, macro_snap)
                u_sent           = u_sent_map.get(ticker)
                u_result         = blend_scores(
                    ticker=ticker,
                    technical_signal=u_tech.signal,
                    technical_strength=u_tech.strength,
                    macro_score=u_macro_score,
                    risk_position={
                        "risk_level": "NORMAL", "warnings": [],
                        "suggested_pct_adj": 0.05, "current_pct": 0.0,
                        "volatility_annual": 0.0, "sharpe": 0.0, "action": "MANTENER",
                    },
                    sentiment_score=u_sent.score if u_sent else 0.0,
                    technical_score_raw=getattr(u_tech, "score_raw", 0.0),
                )
                universe_results.append(u_result)

            n_strong = sum(1 for r in universe_results
                           if r.decision in ("BUY", "ACCUMULATE") and r.final_score > 0.25)
            logger.info(f"Universo: {len(universe_results)} resultados, {n_strong} compras claras")

    except Exception as e:
        logger.warning(f"Análisis de universo falló (no crítico): {e}")

    # ── 8. Portfolio Optimizer ─────────────────────────────────────────────────
    rebalance_report = None
    if not no_optimizer and results:
        logger.info("Ejecutando Portfolio Optimizer...")
        rebalance_report = run_optimizer(
            current_positions=positions,
            portfolio_value_ars=total_ars,
            cash_ars=cash_ars,
            macro_regime=macro_regime,
            vix=macro_snap.vix,
            synthesis_results=results,
            market_assets=[{"ticker": t} for t in cocos_universe],
        )
        if rebalance_report:
            opt = rebalance_report.optimization
            # Este bloque va a stderr (logger), no a stdout
            logger.info(
                f"Optimizer [{opt.method}] gate={rebalance_report.risk_gate_state}: "
                f"{rebalance_report.n_trades} trades — "
                f"ventas ${rebalance_report.total_sells_ars:,.0f}  "
                f"compras ${rebalance_report.total_buys_ars:,.0f}"
            )
    else:
        logger.info("Optimizer omitido")

    # ── 9.5 Information Coefficient (histórico) ──────────────────────────────
    ic_metrics = await _compute_information_coefficient(cfg, tickers=tickers, lookback_days=180)
    p_h = ic_metrics.get("primary_horizon", "5d")
    p_ic = ic_metrics.get("primary_ic", None)
    p_n = int(ic_metrics.get("primary_n_obs", 0) or 0)
    if p_ic is None:
        logger.info(f"IC {p_h}: sin datos suficientes (n={p_n})")
    else:
        logger.info(f"IC {p_h}: {p_ic:+.3f} (n={p_n})")

    # ── 10. Render → stdout ────────────────────────────────────────────────────
    report = render_report(
        results=results,
        macro_snap=macro_snap,
        total_ars=total_ars,
        cash_ars=cash_ars,
        portfolio_risk=portfolio_risk,
        rebalance_report=rebalance_report,
        positions=positions,
        universe_results=universe_results,
        ic_metrics=ic_metrics,
    )
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
        tickers_override=args.tickers,
        period=args.period,
        no_telegram=args.no_telegram,
        no_llm=args.no_llm,
        no_sentiment=args.no_sentiment,
        no_optimizer=args.no_optimizer,
    ))