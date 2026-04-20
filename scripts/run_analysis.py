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
from src.analysis.technical import analyze_portfolio, fetch_history
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
    if a >= 0.10: return "FUERTE"
    if a >= 0.05: return "MODERADO"
    if a >= 0.02: return "DÉBIL"
    return "NULO"


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
    if tech  > 0.02:  positives.append("técnico")
    elif tech  < -0.02: negatives.append("técnico")
    if macro > 0.02:  positives.append("macro")
    elif macro < -0.02: negatives.append("macro")
    if sent  > 0.02:  positives.append("sentiment")
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
    if top_val > 0.02:   motivo = f"{top} favorable"
    elif top_val < -0.02: motivo = f"{top} en contra"
    else:                 motivo = "sin ventaja clara"
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
        if x is None: return 0.0
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
    from src.analysis.decision_engine import _normalize_regime, STOP_NORMAL, TARGET_RR, HORIZON_MED

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

        if vix_f > 30:
            stop = -0.05
        elif vix_f > 25 or is_defensive:
            stop = STOP_NORMAL * 1.25
        elif vol > 0.60:
            stop = -0.05
        elif vol > 0.40:
            stop = -0.07
        else:
            stop = STOP_NORMAL

        stop_pct   = stop if direction == "BUY" else abs(stop)
        target_pct = abs(stop) * TARGET_RR * (1 if direction == "BUY" else -1)
        rr         = abs(target_pct) / abs(stop_pct) if stop_pct else TARGET_RR

        trades_to_save.append({
            "ticker":        ticker,
            "direction":     direction,
            "score":         score,
            "conviction":    conv,
            "size_pct":      size_pct,
            "price":         float(price) if price else None,
            "stop_loss_pct": stop_pct if direction == "BUY" else -abs(stop_pct),
            "target_pct":    target_pct,
            "rr_ratio":      rr,
            "regime":        regime,
            "vix":           float(vix) if vix else None,
            "decided_at":    datetime.utcnow(),
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
                    _json.dumps({"source": "optimizer", "delta_pct": t["size_pct"]}),
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
    plan  = execution_plan
    gate  = plan.gate if plan else "NORMAL"
    h     = []

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
        # Warnings del plan (máx 3 para no saturar)
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
        for hz in ("5d", "10d", "20d"):
            v     = by_h.get(hz, {})
            n_obs = int(v.get("n_obs", 0) or 0)
            n_tk  = int(v.get("n_tickers", 0) or 0)
            ic    = v.get("ic", None)
            ric   = v.get("rank_ic", None)
            if ic is None or n_obs < 5:
                continue
            q = v.get("quality", "NULO")
            h.append(
                f"{hz}: IC <code>{ic:+.3f}</code> | Rank IC <code>{(ric or 0.0):+.3f}</code> "
                f"| n={n_obs} ({n_tk} tickers) | <b>{q}</b>"
            )
        h.append("IC > 0 indica poder predictivo direccional.")
        h.append("")

    # ── ACCIÓN PRINCIPAL — desde plan.main_action, NUNCA del optimizer ────────
    h.append("<b>ACCIÓN PRINCIPAL</b>")
    if plan and plan.main_action:
        main_order = plan.main_action
        verb       = "VENDER" if main_order.side.value == "SELL" else "COMPRAR"
        icon       = "🔴" if main_order.side.value == "SELL" else "🟢"
        partial_tag = " <i>(parcial)</i>" if main_order.partial else ""

        h.append(
            f"{icon} <b>{verb} {main_order.ticker} "
            f"({_money_ars(main_order.amount_ars)}){partial_tag}</b>"
        )
        # Razón de la orden
        h.append(f"   {escape(main_order.reason)}")

        # Delta de peso
        d = next((x for x in plan.decisions if x.ticker == main_order.ticker), None)
        if d:
            h.append(
                f"   Peso actual: <b>{_pct(d.current_weight)}</b> → "
                f"objetivo: <b>{_pct(d.target_weight)}</b>"
            )

        # Advertencia de parcialidad
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
                f"   Compras: <b>$0 ARS</b> (gate {gate} — solo reducciones activas)"
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
                h.append(f"      → Liquidación total (target 0%)")
            step += 1

        for o in sorted(plan.buy_orders, key=lambda x: x.priority):
            ext_icon    = "🌍" if o.priority >= 3 else "📈"
            partial_tag = " <i>(parcial)</i>" if o.partial else ""
            d           = next((x for x in plan.decisions if x.ticker == o.ticker), None)
            score_tag   = f" | score {d.score:+.3f}" if d and d.score is not None else ""
            h.append(
                f"   {step}. {ext_icon} Comprar <b>{o.ticker}</b>: "
                f"+{_money_ars(o.amount_ars)}{partial_tag}{score_tag}"
            )
            step += 1

        if plan.cash_after > 5_000:
            h.append(f"   → Cash remanente: {_money_ars(plan.cash_after)}")

        if plan.blocked_orders:
            h.append("")
            h.append("   🚫 <b>Bloqueadas por gate:</b>")
            for o in plan.blocked_orders[:3]:
                h.append(
                    f"      {o.ticker}: {_money_ars(o.theoretical_ars)} teórico — "
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

    current_w    = _current_weights(positions, total_ars)
    decision_map = {d.ticker: d for d in (plan.decisions if plan else [])}

    # Ordenar por action priority, luego por score desc
    action_priority = {
        Action.SELL_FULL.value:    0,
        Action.SELL_PARTIAL.value: 1,
        Action.BUY.value:          2,
        Action.BLOCKED.value:      3,
        Action.WATCH.value:        4,
        Action.HOLD.value:         5,
    }

    sorted_results = sorted(
        results or [],
        key=lambda r: (
            action_priority.get(
                decision_map.get(
                    str(getattr(r, "ticker", "")).upper(), None
                ) and decision_map[str(getattr(r, "ticker", "")).upper()].action.value or "HOLD",
                5
            ),
            -abs(float(getattr(r, "final_score", 0) or 0)),
        )
    )

    for r in sorted_results:
        ticker     = str(getattr(r, "ticker", "")).upper()
        score      = float(getattr(r, "final_score", getattr(r, "score", 0)) or 0)
        conviction = _extract_conviction(r)
        lectura, _ = _component_reason(r)
        d          = decision_map.get(ticker)

        cw          = float(current_w.get(ticker, 0.0))
        tw          = d.target_weight if d else cw
        action_str  = d.action.value if d else "HOLD"
        icon        = _action_icon(d.action if d else Action.HOLD)

        # Monto ejecutable real (no teórico)
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
        c = conviction
        h.append(
            f"   Score: <code>{score:+.3f}</code> | "
            f"Conv: <b>{_conv_label(c)}</b> ({round(c*100)}%) [{_bar(c)}] | "
            f"Peso: {_pct(cw)} → {_pct(tw)}"
        )
        # Razón de la decisión de cartera si difiere de la señal
        if d and d.reason_secondary:
            h.append(f"   {escape(d.reason_secondary)}.")
        else:
            h.append(f"   {escape(lectura)}.")
        h.append(
            f"   <code>técnico {_layer_weighted(r, 'technical'):+.3f} | "
            f"macro {_layer_weighted(r, 'macro'):+.3f} | "
            f"sentiment {_layer_weighted(r, 'sentiment'):+.3f}</code>"
        )
        h.append("")

    # ── CONTEXTO MACRO ────────────────────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>CONTEXTO DE MERCADO</b>")
    macro_parts = []
    for attr, fmt in [
        ("wti",    "WTI ${:.1f}"),
        ("brent",  "Brent ${:.1f}"),
        ("dxy",    "DXY {:.1f}"),
        ("vix",    "VIX {:.1f}"),
        ("sp500",  "SP500 {:,.0f}"),
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
    ccl_v      = getattr(macro_snap, "ccl", None)
    mep_v      = getattr(macro_snap, "mep", None)
    reservas_v = getattr(macro_snap, "reservas", None)
    riesgo_v   = getattr(macro_snap, "riesgo_pais", None)
    if ccl_v:      arg_parts.append(f"CCL ${ccl_v:,.0f}")
    if mep_v:      arg_parts.append(f"MEP ${mep_v:,.0f}")
    if reservas_v: arg_parts.append(f"Reservas ${reservas_v:,.0f}M")
    if riesgo_v:
        rp_icon = "🔴" if riesgo_v > 1000 else "🟡" if riesgo_v > 600 else "🟢"
        arg_parts.append(f"Riesgo País {rp_icon} {riesgo_v} pb")
    if arg_parts:
        h.append("🇦🇷 " + " | ".join(arg_parts))
    h.append(f"Gate actual: <b>{escape(gate)}</b>")
    h.append("")

    # ── OPTIMIZER — bloque INFORMATIVO (pesos teóricos, no operativos) ─────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>OPTIMIZER</b>")
    opt = _get(rebalance_report, "optimization", rebalance_report)
    if opt:
        method  = escape(str(_get(opt, "method", "N/A")))
        obj_str = escape(str(_get(opt, "method_reason", _get(opt, "reason", "N/A"))))
        ret     = float(_get(opt, "expected_return_annual", 0.0) or 0.0)
        vol     = float(_get(opt, "expected_vol_annual",    0.0) or 0.0)
        sharpe  = float(_get(opt, "sharpe_ratio",           0.0) or 0.0)
        h.append(f"Método: <b>{method}</b> | {obj_str}")
        if 0 < ret < 2.0:
            h.append(
                f"Ret esperado: <b>{ret:.1%}</b> | Vol: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>"
            )
        else:
            h.append(f"Vol estimada: {vol:.1%} | Sharpe: <b>{sharpe:.2f}</b>")

    # Pesos objetivo teóricos — claramente marcados como "teóricos"
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

    # ── RADAR EXTERNO ─────────────────────────────────────────────────────────
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>RADAR EXTERNO</b>")

    owned = {str(p.get("ticker", "")).upper() for p in positions or []}
    radar = []
    for r in universe_results or []:
        ticker     = str(getattr(r, "ticker", "")).upper()
        if not ticker or ticker in owned:
            continue
        score      = float(getattr(r, "final_score", getattr(r, "score", 0)) or 0)
        conviction = _extract_conviction(r)
        decision   = str(getattr(r, "decision", "HOLD")).upper()
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
            "ticker": ticker, "score": score, "conviction": conviction,
            "label": label, "tier": tier, "lectura": lectura,
        })

    radar.sort(key=lambda x: (x["tier"], -x["conviction"], -x["score"]))
    radar = radar[:6]

    if not radar:
        h.append("Sin compras claras en el universo de Cocos.")
        if gate in ("CAUTIOUS", "BLOCKED"):
            h.append(f"Gate {gate}: mantener en observación hasta mejora del régimen.")
        else:
            h.append("Esperar señales técnicas más definidas para actuar.")
    else:
        strong = [x for x in radar if x["tier"] == 0]
        watch  = [x for x in radar if x["tier"] == 1]
        obs    = [x for x in radar if x["tier"] == 2]

        if strong:
            h.append("🟢🟢 <b>Compras fuertes</b>")
            for x in strong:
                h.append(
                    f"   <b>{x['ticker']}</b>: score <code>{x['score']:+.3f}</code> "
                    f"| conv. {round(x['conviction']*100)}%"
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
    h.append("")

    # ── VEREDICTO FINAL — derivado EXCLUSIVAMENTE del ExecutionPlan ────────────
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
    try:
        db_u = PortfolioDatabase(cfg.database.url)
        await db_u.connect()
        cocos_universe = await db_u.get_cocos_universe()
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
            market_assets       = [{"ticker": t} for t in cocos_universe],
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

    # ── 9.5 Guardar trades en decision_log ────────────────────────────────────
    if rebalance_report and total_ars > 0:
        logger.info("Guardando trades del optimizer en decision_log...")
        try:
            saved = await _save_optimizer_trades(
                cfg              = cfg,
                rebalance_report = rebalance_report,
                current_w        = _current_weights(positions, total_ars),
                positions        = positions,
                results          = results,
                macro_snap       = macro_snap,
                macro_regime     = macro_regime,
                total_ars        = total_ars,
            )
            logger.info(f"Trades guardados en DB: ids={saved}")
        except Exception as e:
            logger.warning(f"No se pudieron guardar trades (no crítico): {e}")
    else:
        logger.info("Paso 9.5: sin optimizer o portfolio vacío — skip")

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