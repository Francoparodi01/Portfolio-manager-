"""
src/analysis/optimizer.py
Portfolio Optimizer — Black-Litterman + fallback numpy puro.

ARQUITECTURA NUEVA: Risk engine en SERIE antes del optimizer.
  1. _get_risk_gate_state() evalúa el estado global de risk
  2. El optimizer SOLO corre dentro del espacio permitido por el gate:
       NORMAL   → optimizer opera sin restricciones adicionales
       CAUTIOUS → STEP_MAX reducido 50%, solo se permiten reducciones de posiciones
                  con score negativo. No se permiten nuevas compras.
       BLOCKED  → no hay rebalanceo. Solo se ejecutan stops (drawdown crítico).
  3. El RebalanceReport expone risk_gate_state y risk_gate_reason para el reporte.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .macro import MacroSnapshot, score_macro_for_ticker

import numpy as np

logger = logging.getLogger(__name__)

# ── Parámetros globales ───────────────────────────────────────────────────────
W_MIN            = 0.02
W_MAX            = 0.40
REBALANCE_THRESH = 0.10
RF_ANNUAL        = 0.05
RISK_AVERSION    = 2.5
TAU              = 0.05
MIN_HISTORY_DAYS = 60
HISTORY_PERIOD   = "1y"

# Risk gate — umbrales para cambiar estado
VIX_CAUTIOUS = 28    # antes era 25 para MIN_VARIANCE — ahora modifica comportamiento
VIX_BLOCKED  = 38    # antes era 35 para RISK_PARITY — ahora bloquea
DD_CAUTIOUS  = -0.12 # drawdown > 12%: modo cauteloso
DD_BLOCKED   = -0.22 # drawdown > 22%: bloqueado


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    method: str
    method_reason: str
    weights: dict[str, float]
    expected_return_annual: float
    expected_vol_annual: float
    sharpe_ratio: float
    n_assets: int
    views_used: dict[str, float] = field(default_factory=dict)


@dataclass
class RebalanceTrade:
    ticker: str
    weight_current: float
    weight_optimal: float
    delta: float
    action: str
    amount_ars: float


@dataclass
class RebalanceReport:
    optimization: OptimizationResult
    trades: list[RebalanceTrade]
    total_sells_ars: float
    total_buys_ars: float
    net_cash_needed: float
    n_trades: int
    portfolio_value_ars: float
    threshold_used: float
    # Nuevos campos del risk gate
    risk_gate_state: str = "NORMAL"   # NORMAL | CAUTIOUS | BLOCKED
    risk_gate_reason: str = ""

    def to_telegram(self) -> str:
        method_icons = {
            "BLACK_LITTERMAN": "🧠", "MAX_SHARPE": "📈",
            "MIN_VARIANCE": "🛡️", "RISK_PARITY": "⚖️",
        }
        action_icons = {
            "COMPRAR": "🟢 COMPRAR", "NUEVO": "🟢 NUEVO",
            "REDUCIR": "🔴 REDUCIR", "VENDER": "🔴 VENDER",
            "MANTENER": "🟡 MANTENER",
        }
        gate_icons = {"NORMAL": "✅", "CAUTIOUS": "🟡", "BLOCKED": "🔴"}
        opt  = self.optimization
        icon = method_icons.get(opt.method, "📊")

        lines = [
            "━" * 38,
            f"{icon} <b>PORTFOLIO OPTIMIZER</b>",
            f"   Método: <b>{opt.method}</b>",
            f"   Motivo: <i>{opt.method_reason}</i>",
            f"   {gate_icons.get(self.risk_gate_state, '⚪')} Risk gate: "
            f"<b>{self.risk_gate_state}</b>"
            + (f" — <i>{self.risk_gate_reason}</i>" if self.risk_gate_reason else ""),
            f"   Portfolio: <b>${self.portfolio_value_ars:,.0f} ARS</b>",
            f"   Ret esperado: {opt.expected_return_annual:.1%}  "
            f"Vol: {opt.expected_vol_annual:.1%}  Sharpe: {opt.sharpe_ratio:.2f}",
        ]

        if opt.views_used:
            views_str = "  ".join(
                f"{t}:{v:+.2f}" for t, v in sorted(opt.views_used.items())
            )
            lines.append(f"   Views: <code>{views_str}</code>")

        lines.append("")
        lines.append("<b>Rebalanceo sugerido:</b>")

        for t in sorted(self.trades, key=lambda x: x.delta):
            if t.action == "MANTENER":
                continue
            lines.append(
                f"  {action_icons.get(t.action, t.action):15s} "
                f"<b>{t.ticker:6s}</b> "
                f"{t.weight_current:.1%} → {t.weight_optimal:.1%} "
                f"({t.delta:+.1%})  <code>${abs(t.amount_ars):,.0f}</code> ARS"
            )

        mantener = [t.ticker for t in self.trades if t.action == "MANTENER"]
        if mantener:
            lines.append(f"  🟡 MANTENER: {', '.join(mantener)}")

        lines += [
            "",
            f"   💰 Ventas:  <b>${self.total_sells_ars:,.0f} ARS</b>",
            f"   🛒 Compras: <b>${self.total_buys_ars:,.0f} ARS</b>",
        ]
        if self.net_cash_needed > 5_000:
            lines.append(f"   ⚠️ Cash adicional: ${self.net_cash_needed:,.0f} ARS")
        elif self.net_cash_needed < -5_000:
            lines.append(f"   ✅ Cash generado: ${abs(self.net_cash_needed):,.0f} ARS")

        return "\n".join(lines)


# ── Risk Gate — evaluación en serie ANTES del optimizer ──────────────────────

def _get_risk_gate_state(
    vix: Optional[float],
    portfolio_drawdown: float,
    macro_regime: dict,
) -> tuple[str, str]:
    """
    Devuelve (state, reason) donde state ∈ {NORMAL, CAUTIOUS, BLOCKED}.

    Este es el árbitro. El optimizer NO corre hasta que este gate define el espacio.
    BLOCKED: solo stops, no rebalanceo.
    CAUTIOUS: solo reducciones de posiciones negativas, no nuevas compras.
    NORMAL: optimizer opera libremente.
    """
    vix_val = vix or 20.0
    dd = portfolio_drawdown

    if vix_val > VIX_BLOCKED or dd <= DD_BLOCKED:
        reason_parts = []
        if vix_val > VIX_BLOCKED:
            reason_parts.append(f"VIX {vix_val:.0f} > {VIX_BLOCKED}")
        if dd <= DD_BLOCKED:
            reason_parts.append(f"drawdown {dd:.1%}")
        return "BLOCKED", " + ".join(reason_parts)

    if vix_val > VIX_CAUTIOUS or dd <= DD_CAUTIOUS or macro_regime.get("market") == "risk_off":
        reason_parts = []
        if vix_val > VIX_CAUTIOUS:
            reason_parts.append(f"VIX {vix_val:.0f} > {VIX_CAUTIOUS}")
        if dd <= DD_CAUTIOUS:
            reason_parts.append(f"drawdown {dd:.1%}")
        if macro_regime.get("market") == "risk_off":
            reason_parts.append("régimen risk_off")
        return "CAUTIOUS", " + ".join(reason_parts)

    return "NORMAL", ""


def _apply_risk_gate_to_trades(
    trades: list[RebalanceTrade],
    gate_state: str,
    score_map: dict[str, float],
) -> list[RebalanceTrade]:
    """
    Filtra los trades según el estado del risk gate.
    CAUTIOUS: bloquea compras de activos con score >= 0 (solo deja reducir negativos)
    BLOCKED: bloquea todo excepto los stops más urgentes (delta < -0.15)
    """
    if gate_state == "NORMAL":
        return trades

    filtered = []
    blocked_count = 0

    for t in trades:
        if t.action == "MANTENER":
            filtered.append(t)
            continue

        score = score_map.get(t.ticker, 0.0)

        if gate_state == "BLOCKED":
            # Solo dejar pasar stops urgentes
            if t.action in ("VENDER", "REDUCIR") and t.delta < -0.15:
                filtered.append(t)
            else:
                logger.info(f"  Gate BLOCKED: {t.action} {t.ticker} bloqueado")
                blocked_count += 1
                filtered.append(RebalanceTrade(
                    ticker=t.ticker, weight_current=t.weight_current,
                    weight_optimal=t.weight_current, delta=0.0,
                    action="MANTENER", amount_ars=0.0,
                ))

        elif gate_state == "CAUTIOUS":
            # Bloquear compras cuando score es negativo o señal débil
            # (score >= -0.05 con convicción baja se trata como "sin señal")
            should_block = t.action in ("COMPRAR", "NUEVO") and score > -0.10
            if should_block:
                logger.info(
                    f"  Gate CAUTIOUS: {t.action} {t.ticker} bloqueado "
                    f"(score={score:+.2f}, {t.weight_current:.1%}→{t.weight_optimal:.1%})"
                )
                blocked_count += 1
                filtered.append(RebalanceTrade(
                    ticker=t.ticker, weight_current=t.weight_current,
                    weight_optimal=t.weight_current, delta=0.0,
                    action="MANTENER", amount_ars=0.0,
                ))
            else:
                filtered.append(t)

    if blocked_count:
        logger.info(f"Risk gate {gate_state}: {blocked_count} trade(s) bloqueados")

    return filtered


# ── Carga de datos ────────────────────────────────────────────────────────────

def _fetch_returns(tickers: list[str]):
    try:
        import pandas as pd
        from src.analysis.technical import fetch_history

        data = {}
        for ticker in tickers:
            df = fetch_history(ticker, period=HISTORY_PERIOD)
            if df is None or "Close" not in df.columns:
                continue
            prices = df["Close"].squeeze()
            if len(prices) < MIN_HISTORY_DAYS:
                logger.warning(f"Optimizer: {ticker} excluido ({len(prices)} días)")
                continue
            data[ticker] = prices.pct_change().dropna()

        if not data:
            import pandas as pd
            return pd.DataFrame()

        import pandas as pd
        returns = pd.DataFrame(data).dropna(how="all").fillna(0)
        return returns
    except Exception as e:
        logger.error(f"Error descargando retornos: {e}")
        import pandas as pd
        return pd.DataFrame()


# ── Utilidades ────────────────────────────────────────────────────────────────

def _portfolio_stats(weights, mu, cov):
    w = np.array(weights)
    ret = float(w @ mu)
    vol = float(np.sqrt(w @ cov @ w))
    sharpe = (ret - RF_ANNUAL) / vol if vol > 1e-10 else 0.0
    return ret, vol, sharpe


def _project_simplex_bounds(w: np.ndarray, w_min: float = W_MIN,
                             w_max_arr: np.ndarray = None) -> np.ndarray:
    n = len(w)
    wmax = w_max_arr if w_max_arr is not None else np.full(n, W_MAX)
    w = np.clip(w, w_min, wmax)
    for _ in range(300):
        diff = w.sum() - 1.0
        if abs(diff) < 1e-10:
            break
        if diff > 0:
            red = w - w_min
            tot = red.sum()
            if tot > 1e-10:
                w -= red * (diff / tot)
        else:
            aum = wmax - w
            tot = aum.sum()
            if tot > 1e-10:
                w += aum * (-diff / tot)
        w = np.clip(w, w_min, wmax)
    return w


def _dynamic_w_max(
    score: float,
    w_current: float,
    gate_state: str,
    conviction: float = 0.5,
) -> float:
    """
    W_MAX con doble clamp: por score Y por convicción.

    Regla principal (score):
      score > 0.15  → W_MAX normal (o reducido si gate=CAUTIOUS)
      score > 0.0   → 35% (o 26% si CAUTIOUS)
      score > -0.15 → max(w_current, 25%) — no aumentar si conviction < 50%
      score > -0.40 → w_current * 0.7 — reducción suave forzada
      score ≤ -0.40 → W_MIN — salida forzada

    Clamp adicional por conviction baja:
      Si conviction < 0.40 Y score < 0: no aumentar sobre w_current.
      Esto evita que MIN_VARIANCE compre fuerte en activos con señal débil/negativa.
    """
    base_max = W_MAX if gate_state == "NORMAL" else W_MAX * 0.75

    if score > 0.15:
        cap = base_max
    elif score > 0.0:
        cap = min(0.35, base_max)
    elif score > -0.15:
        # Señal negativa débil: no aumentar si conviction baja
        if conviction < 0.40:
            cap = min(w_current, 0.25)   # congelar en peso actual o menos
        else:
            cap = min(0.25, base_max)
    elif score > -0.40:
        cap = max(w_current * 0.7, W_MIN * 2)  # reducción suave forzada
    else:
        cap = W_MIN  # salida forzada

    return round(float(cap), 4)


# ── Black-Litterman ───────────────────────────────────────────────────────────

def _optimize_black_litterman(returns, universe, score_map, tau=TAU,
                               risk_aversion=RISK_AVERSION, w_max_arr=None):
    try:
        from pypfopt import BlackLittermanModel, EfficientFrontier, risk_models, expected_returns

        mu_hist = expected_returns.mean_historical_return(
            returns, returns_data=True, compounding=True, frequency=252)
        cov_bl = risk_models.CovarianceShrinkage(
            returns, returns_data=True, frequency=252).ledoit_wolf()

        viewdict = {}
        view_conf = {}
        for ticker in universe:
            score = score_map.get(ticker, 0.0)
            viewdict[ticker] = RF_ANNUAL + score * 0.55
            view_conf[ticker] = min(0.35 + abs(score) * 0.85, 0.95)

        bl = BlackLittermanModel(
            cov_matrix=cov_bl, pi="market",
            absolute_views=viewdict,
            view_confidences=list(view_conf.values()),
            tau=tau, risk_aversion=risk_aversion,
        )
        bl_returns = bl.bl_returns()
        bl_cov = bl.bl_cov()

        ef = EfficientFrontier(bl_returns, bl_cov, weight_bounds=(W_MIN, W_MAX))
        if w_max_arr is not None:
            ef.weight_bounds = list(zip([W_MIN] * len(universe), w_max_arr.tolist()))
        ef.max_sharpe(risk_free_rate=RF_ANNUAL)
        cleaned = ef.clean_weights()

        weights = np.array([cleaned.get(t, W_MIN) for t in universe])
        weights = np.clip(weights, W_MIN, w_max_arr if w_max_arr is not None else np.full(len(universe), W_MAX))
        weights /= weights.sum()
        logger.info(f"BL weights: { {t: f'{v:.1%}' for t, v in zip(universe, weights)} }")
        return weights
    except Exception as e:
        logger.warning(f"Black-Litterman falló ({e}) — usando fallback")
        mu = returns.mean().values * 252
        cov = returns.cov().values * 252
        return _optimize_max_sharpe_np(mu, cov, universe, w_max_arr)


# ── Optimizadores numpy (fallback) ────────────────────────────────────────────

def _optimize_max_sharpe_np(mu, cov, tickers, w_max_arr=None):
    n = len(tickers)
    wmax = w_max_arr if w_max_arr is not None else np.full(n, W_MAX)
    w = np.clip(np.ones(n) / n, W_MIN, wmax); w /= w.sum()
    best_w, best_sr, lr = w.copy(), -np.inf, 0.01
    for i in range(6000):
        sigma = np.sqrt(max(w @ cov @ w, 1e-12))
        sr = (float(w @ mu) - RF_ANNUAL) / sigma
        grad = (mu - sr * (cov @ w) / sigma) / sigma
        w = _project_simplex_bounds(w + lr * grad, w_max_arr=wmax)
        if i % 1000 == 999: lr *= 0.7
        sr_new = (float(w @ mu) - RF_ANNUAL) / max(np.sqrt(w @ cov @ w), 1e-12)
        if sr_new > best_sr: best_sr, best_w = sr_new, w.copy()
    return best_w


def _optimize_min_variance_np(mu, cov, tickers, w_max_arr=None):
    n = len(tickers)
    wmax = w_max_arr if w_max_arr is not None else np.full(n, W_MAX)
    w = np.clip(np.ones(n) / n, W_MIN, wmax); w /= w.sum()
    best_w, best_var, lr = w.copy(), np.inf, 0.005
    for i in range(6000):
        w = _project_simplex_bounds(w - lr * 2.0 * (cov @ w), w_max_arr=wmax)
        if i % 1000 == 999: lr *= 0.8
        v = float(w @ cov @ w)
        if v < best_var: best_var, best_w = v, w.copy()
    return best_w


def _optimize_risk_parity_np(mu, cov, tickers, w_max_arr=None):
    n = len(tickers)
    wmax = w_max_arr if w_max_arr is not None else np.full(n, W_MAX)
    w = np.clip(np.ones(n) / n, W_MIN, wmax); w /= w.sum()
    target = np.ones(n) / n
    best_w, best_obj, lr = w.copy(), np.inf, 0.005
    for i in range(10000):
        w = np.maximum(w, 1e-8)
        sigma = max(np.sqrt(w @ cov @ w), 1e-12)
        mrc = cov @ w / sigma; rc = w * mrc / sigma
        w = _project_simplex_bounds(w - lr * 2.0 * (rc - target) * mrc / sigma, w_max_arr=wmax)
        if i % 2000 == 1999: lr *= 0.8
        obj = float(np.sum((rc - target) ** 2))
        if obj < best_obj: best_obj, best_w = obj, w.copy()
    return best_w


# ── Selección de método (ahora informativa — no controla comportamiento) ──────

def _select_method(macro_regime: dict, vix: Optional[float],
                   gate_state: str) -> tuple[str, str]:
    """
    Con el risk gate en serie, _select_method es solo una elección de
    función objetivo — ya no necesita cambiar el comportamiento global
    porque el gate ya definió el espacio de acción permitido.
    """
    vix_val = vix or 20.0
    market = macro_regime.get("market", "neutral")

    if gate_state == "CAUTIOUS" or vix_val > 25:
        return "MIN_VARIANCE", f"gate={gate_state}, VIX {vix_val:.0f} — minimizar volatilidad"
    if market in ("risk_off",):
        return "BLACK_LITTERMAN", f"risk_off + VIX {vix_val:.0f} — BL conservador (tau bajo)"
    return "BLACK_LITTERMAN", f"Régimen {market} — BL con views del pipeline"


# ── Universo ──────────────────────────────────────────────────────────────────

def _build_universe(current_positions, synthesis_results, market_assets) -> list[str]:
    universe = set()
    for pos in current_positions:
        t = pos.get("ticker", "").strip().upper()
        market_value = float(pos.get("market_value", 0) or 0)
        if t and market_value > 0:
            universe.add(t)
    if not universe:
        for r in synthesis_results:
            universe.add(r.ticker)
    logger.info(f"Universo optimizer: {len(universe)} activos — {sorted(universe)}")
    return sorted(universe)


# ── Pipeline principal ────────────────────────────────────────────────────────

def run_optimizer(
    current_positions: list[dict],
    portfolio_value_ars: float,
    cash_ars: float,
    macro_regime: dict,
    vix: Optional[float],
    synthesis_results: list,
    market_assets: list[dict],
    threshold: float = REBALANCE_THRESH,
    portfolio_drawdown: float = 0.0,
) -> Optional[RebalanceReport]:
    try:
        import pandas as pd

        # ── PASO 0: Risk gate — define el espacio antes de cualquier optimización ──
        gate_state, gate_reason = _get_risk_gate_state(vix, portfolio_drawdown, macro_regime)
        logger.info(f"Risk gate: {gate_state}" + (f" ({gate_reason})" if gate_reason else ""))

        if gate_state == "BLOCKED":
            logger.warning("Risk gate BLOCKED — optimizer no corre, solo stops")
            # Construir reporte vacío con el gate explicado
            empty_opt = OptimizationResult(
                method="BLOCKED", method_reason=gate_reason,
                weights={}, expected_return_annual=0.0,
                expected_vol_annual=0.0, sharpe_ratio=0.0, n_assets=0,
            )
            # Solo mantener todo
            trades = []
            for pos in current_positions:
                t = pos.get("ticker", "")
                w = float(pos.get("market_value", 0) or 0) / portfolio_value_ars if portfolio_value_ars > 0 else 0
                trades.append(RebalanceTrade(
                    ticker=t, weight_current=round(w, 4), weight_optimal=round(w, 4),
                    delta=0.0, action="MANTENER", amount_ars=0.0,
                ))
            return RebalanceReport(
                optimization=empty_opt, trades=trades,
                total_sells_ars=0.0, total_buys_ars=0.0,
                net_cash_needed=0.0, n_trades=0,
                portfolio_value_ars=portfolio_value_ars, threshold_used=threshold,
                risk_gate_state=gate_state, risk_gate_reason=gate_reason,
            )

        # ── PASO 1: Universo ──────────────────────────────────────────────────
        universe = _build_universe(current_positions, synthesis_results, market_assets)
        if len(universe) < 2:
            logger.warning("Optimizer: universo insuficiente")
            return None

        # ── PASO 2: Retornos históricos ───────────────────────────────────────
        returns = _fetch_returns(universe)
        if returns.empty or len(returns.columns) < 2:
            logger.warning("Optimizer: datos históricos insuficientes")
            return None
        universe = list(returns.columns)
        n = len(universe)

        # ── PASO 3: Estadísticas base ─────────────────────────────────────────
        mu_daily = returns.mean().values
        cov = returns.cov().values * 252
        mu_ann = mu_daily * 252

        # ── PASO 4: Scores y pesos actuales ──────────────────────────────────
        score_map = {r.ticker: r.final_score for r in synthesis_results}
        current_w_map: dict[str, float] = {}
        for pos in current_positions:
            t = pos.get("ticker", "").upper()
            mv = float(pos.get("market_value", 0) or 0)
            current_w_map[t] = mv / portfolio_value_ars if portfolio_value_ars > 0 else 0.0

        # ── PASO 5: W_MAX dinámico respetando gate + conviction ──────────────
        # conviction_map: conviction por ticker del pipeline de síntesis
        conviction_map = {r.ticker: getattr(r, 'conviction', r.confidence)
                          for r in synthesis_results}
        w_max_arr = np.array([
            _dynamic_w_max(
                score_map.get(t, 0.0),
                current_w_map.get(t, 0.0),
                gate_state,
                conviction=conviction_map.get(t, 0.5),
            )
            for t in universe
        ])
        logger.info(f"W_MAX ({gate_state}): { {t: f'{v:.0%}' for t, v in zip(universe, w_max_arr)} }")

        # ── PASO 6: Seleccionar método y optimizar ────────────────────────────
        method, reason = _select_method(macro_regime, vix, gate_state)

        if method == "BLACK_LITTERMAN":
            tau = TAU * 0.5 if macro_regime.get("market") == "risk_off" else TAU
            raw_weights = _optimize_black_litterman(
                returns, universe, score_map, tau=tau,
                risk_aversion=RISK_AVERSION, w_max_arr=w_max_arr,
            )
        elif method == "MIN_VARIANCE":
            raw_weights = _optimize_min_variance_np(mu_ann, cov, universe, w_max_arr)
        elif method == "RISK_PARITY":
            raw_weights = _optimize_risk_parity_np(mu_ann, cov, universe, w_max_arr)
        else:
            raw_weights = _optimize_max_sharpe_np(mu_ann, cov, universe, w_max_arr)

        weights_optimal = dict(zip(universe, raw_weights))

        # ── PASO 7: Stats del portfolio óptimo ───────────────────────────────
        w_arr = np.array([weights_optimal[t] for t in universe])
        exp_ret, exp_vol, sharpe = _portfolio_stats(w_arr, mu_ann, cov)

        opt_result = OptimizationResult(
            method=method, method_reason=reason,
            weights=weights_optimal,
            expected_return_annual=round(exp_ret, 4),
            expected_vol_annual=round(exp_vol, 4),
            sharpe_ratio=round(sharpe, 3),
            n_assets=n,
            views_used={t: round(score_map[t], 3) for t in universe if t in score_map},
        )

        # ── PASO 8: Calcular trades ───────────────────────────────────────────
        all_tickers = sorted(set(list(weights_optimal.keys()) + list(current_w_map.keys())))
        raw_trades: list[RebalanceTrade] = []

        for ticker in all_tickers:
            w_cur = current_w_map.get(ticker, 0.0)
            w_opt = weights_optimal.get(ticker, 0.0)
            delta = w_opt - w_cur
            amount_ars = abs(delta) * portfolio_value_ars

            if abs(delta) < threshold:
                action = "MANTENER"
            elif delta > 0:
                action = "NUEVO" if w_cur == 0.0 else "COMPRAR"
            elif delta > -0.10:
                action = "REDUCIR"
            else:
                action = "VENDER"

            raw_trades.append(RebalanceTrade(
                ticker=ticker, weight_current=round(w_cur, 4),
                weight_optimal=round(w_opt, 4), delta=round(delta, 4),
                action=action, amount_ars=round(amount_ars, 0),
            ))

        # ── PASO 9: Aplicar filtro del risk gate sobre los trades ─────────────
        trades = _apply_risk_gate_to_trades(raw_trades, gate_state, score_map)

        # ── PASO 10: Resumen financiero ───────────────────────────────────────
        sells    = sum(t.amount_ars for t in trades if t.action in ("VENDER", "REDUCIR"))
        buys     = sum(t.amount_ars for t in trades if t.action in ("COMPRAR", "NUEVO"))
        # net_cash_needed > 0: hay que poner plata adicional de afuera
        # net_cash_needed < 0: las ventas generan cash sobrante (autofinanciado)
        net      = buys - (cash_ars + sells)
        n_trades = sum(1 for t in trades if t.action != "MANTENER")
        logger.info(
            f"  Cash flow: ventas ${sells:,.0f} + cash ${cash_ars:,.0f} = "
            f"${sells+cash_ars:,.0f} disponible | compras ${buys:,.0f} | "
            f"net {'necesita' if net > 0 else 'genera'} ${abs(net):,.0f} ARS"
        )

        logger.info(
            f"Optimizer [{method}] gate={gate_state}: {n_trades} trades — "
            f"ventas ${sells:,.0f}  compras ${buys:,.0f}  net ${net:,.0f} ARS"
        )

        return RebalanceReport(
            optimization=opt_result, trades=trades,
            total_sells_ars=round(sells, 0),
            total_buys_ars=round(buys, 0),
            net_cash_needed=round(net, 0),
            n_trades=n_trades,
            portfolio_value_ars=portfolio_value_ars,
            threshold_used=threshold,
            risk_gate_state=gate_state,
            risk_gate_reason=gate_reason,
        )

    except Exception as e:
        logger.error(f"Optimizer falló: {e}", exc_info=True)
        return None


# ── Sugerencia de cash sobrante ───────────────────────────────────────────────

def suggest_cash_deployment(
    cash_sobrante: float,
    synthesis_results: list,
    market_assets: list[dict],
    snap: MacroSnapshot,
    max_suggestions: int = 3,
) -> list[dict]:
    if cash_sobrante < 10_000:
        return []

    candidates = []
    seen = set()

    for asset in market_assets:
        ticker = asset.get("ticker", "").strip().upper()
        if ticker in seen or ticker in {"CVX", "NVDA", "MU", "MELI", "CASH_ARS"}:
            continue
        seen.add(ticker)

        macro_score, _ = score_macro_for_ticker(ticker, snap)
        synth_score = next(
            (r.final_score for r in synthesis_results if r.ticker.upper() == ticker), 0.0
        )
        combined = macro_score * 0.6 + synth_score * 0.4

        if combined > 0.20:
            amount = round(cash_sobrante * 0.33, 0)
            candidates.append({
                "ticker": ticker,
                "combined_score": round(combined, 3),
                "suggested_amount_ars": int(amount),
                "reason": "mejor macro + synthesis",
            })

    return sorted(candidates, key=lambda x: x["combined_score"], reverse=True)[:max_suggestions]