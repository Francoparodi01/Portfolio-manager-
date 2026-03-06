"""
src/analysis/optimizer.py
Portfolio Optimizer — 3 métodos + selección automática por régimen macro.

Métodos:
  MAX_SHARPE   → maximiza retorno ajustado por riesgo
  MIN_VARIANCE → minimiza volatilidad total (defensivo)
  RISK_PARITY  → igual contribución de riesgo por activo (crisis)

Selección automática:
  risk_on  + VIX < 25 → MAX_SHARPE
  neutral  + VIX < 25 → MAX_SHARPE
  risk_off + VIX < 25 → MIN_VARIANCE
  VIX 25-35           → MIN_VARIANCE
  VIX > 35            → RISK_PARITY

Constraints:
  w_min = 2%   por activo incluido
  w_max = 40%  por activo
  sum   = 100%
  threshold rebalance = 3% (configurable)
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ── Parámetros ────────────────────────────────────────────────────────────────
W_MIN           = 0.02   # mínimo 2% por activo
W_MAX           = 0.40   # máximo 40% por activo
REBALANCE_THRESH = 0.03  # ignorar deltas < 3%
RF_ANNUAL       = 0.05   # tasa libre de riesgo (aprox USD)
MIN_HISTORY_DAYS = 60    # excluir activos con menos datos
HISTORY_PERIOD  = "1y"   # ventana para el optimizer


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    method: str                      # "MAX_SHARPE" | "MIN_VARIANCE" | "RISK_PARITY"
    method_reason: str               # por qué se eligió este método
    weights: dict[str, float]        # ticker → peso óptimo (suma = 1)
    expected_return_annual: float    # retorno anual esperado
    expected_vol_annual: float       # volatilidad anual esperada
    sharpe_ratio: float
    n_assets: int


@dataclass
class RebalanceTrade:
    ticker: str
    weight_current: float   # peso actual en el portfolio
    weight_optimal: float   # peso sugerido por el optimizer
    delta: float            # weight_optimal - weight_current
    action: str             # "COMPRAR" | "REDUCIR" | "VENDER" | "MANTENER" | "NUEVO"
    amount_ars: float       # monto absoluto en ARS


@dataclass
class RebalanceReport:
    optimization: OptimizationResult
    trades: list[RebalanceTrade]
    total_sells_ars: float
    total_buys_ars: float
    net_cash_needed: float    # >0 = necesita cash extra, <0 = genera cash
    n_trades: int
    portfolio_value_ars: float
    threshold_used: float

    def to_telegram(self) -> str:
        method_icons = {
            "MAX_SHARPE":   "📈",
            "MIN_VARIANCE": "🛡️",
            "RISK_PARITY":  "⚖️",
        }
        action_icons = {
            "COMPRAR": "🟢 COMPRAR",
            "NUEVO":   "🟢 NUEVO",
            "REDUCIR": "🔴 REDUCIR",
            "VENDER":  "🔴 VENDER",
            "MANTENER":"🟡 MANTENER",
        }

        opt = self.optimization
        icon = method_icons.get(opt.method, "📊")

        lines = [
            "━" * 38,
            f"{icon} <b>PORTFOLIO OPTIMIZER</b>",
            f"   Método: <b>{opt.method}</b>",
            f"   Motivo: <i>{opt.method_reason}</i>",
            f"   Portfolio: <b>${self.portfolio_value_ars:,.0f} ARS</b>",
            f"   Retorno esperado: {opt.expected_return_annual:.1%}  "
            f"Vol: {opt.expected_vol_annual:.1%}  Sharpe: {opt.sharpe_ratio:.2f}",
            "",
            "<b>Rebalanceo sugerido:</b>",
        ]

        # Tabla de trades
        for t in sorted(self.trades, key=lambda x: x.delta):
            if t.action == "MANTENER":
                continue
            delta_str = f"{t.delta:+.1%}"
            monto_str = f"${abs(t.amount_ars):,.0f}"
            lines.append(
                f"  {action_icons.get(t.action, t.action):15s} "
                f"<b>{t.ticker:6s}</b> "
                f"{t.weight_current:.1%} → {t.weight_optimal:.1%} "
                f"({delta_str})  {monto_str} ARS"
            )

        # Mantener (al final, sin monto)
        mantener = [t.ticker for t in self.trades if t.action == "MANTENER"]
        if mantener:
            lines.append(f"  🟡 MANTENER: {', '.join(mantener)}")

        lines += [
            "",
            f"   💰 Ventas estimadas:  <b>${self.total_sells_ars:,.0f} ARS</b>",
            f"   🛒 Compras estimadas: <b>${self.total_buys_ars:,.0f} ARS</b>",
        ]
        if self.net_cash_needed > 5000:
            lines.append(f"   ⚠️ Cash adicional necesario: ${self.net_cash_needed:,.0f} ARS")
        elif self.net_cash_needed < -5000:
            lines.append(f"   ✅ Cash generado: ${abs(self.net_cash_needed):,.0f} ARS")

        return "\n".join(lines)


# ── Carga de datos ────────────────────────────────────────────────────────────

def _fetch_returns(tickers: list[str]) -> "pd.DataFrame":
    """Descarga 1 año de retornos diarios para todos los tickers."""
    try:
        import pandas as pd
        from src.analysis.technical import fetch_history

        data = {}
        excluded = []
        for ticker in tickers:
            df = fetch_history(ticker, period=HISTORY_PERIOD)
            if df is None or "Close" not in df.columns:
                excluded.append(ticker)
                continue
            prices = df["Close"].squeeze()
            if len(prices) < MIN_HISTORY_DAYS:
                excluded.append(ticker)
                logger.warning(f"Optimizer: {ticker} excluido (solo {len(prices)} días)")
                continue
            data[ticker] = prices.pct_change().dropna()

        if excluded:
            logger.warning(f"Optimizer: excluidos por datos insuficientes: {excluded}")

        if not data:
            return pd.DataFrame()

        returns = pd.DataFrame(data).dropna(how="all").fillna(0)
        return returns

    except Exception as e:
        logger.error(f"Error descargando retornos: {e}")
        import pandas as pd
        return pd.DataFrame()


# ── Utilidades de portfolio ───────────────────────────────────────────────────

def _portfolio_stats(weights: np.ndarray, mu: np.ndarray, cov: np.ndarray, rf: float = RF_ANNUAL):
    """Retorno esperado, volatilidad y Sharpe anualizado."""
    ret = float(np.dot(weights, mu))
    vol = float(np.sqrt(weights @ cov @ weights))
    sharpe = (ret - rf) / vol if vol > 0 else 0.0
    return ret, vol, sharpe


def _apply_constraints(weights: np.ndarray, tickers: list[str]) -> np.ndarray:
    """Clipea pesos a [W_MIN, W_MAX] y renormaliza."""
    w = np.clip(weights, W_MIN, W_MAX)
    total = w.sum()
    return w / total if total > 0 else w


# ── Optimizadores ─────────────────────────────────────────────────────────────

def _optimize_max_sharpe(mu: np.ndarray, cov: np.ndarray, tickers: list[str]) -> np.ndarray:
    """Maximiza el Sharpe ratio usando scipy."""
    from scipy.optimize import minimize

    n = len(tickers)
    rf = RF_ANNUAL / 252  # diario

    def neg_sharpe(w):
        ret = np.dot(w, mu)
        vol = np.sqrt(w @ cov @ w)
        return -(ret - rf * 252) / (vol * np.sqrt(252)) if vol > 1e-10 else 0.0

    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1}]
    bounds = [(W_MIN, W_MAX)] * n
    w0 = np.ones(n) / n

    result = minimize(neg_sharpe, w0, method="SLSQP",
                      bounds=bounds, constraints=constraints,
                      options={"maxiter": 1000, "ftol": 1e-9})

    if result.success:
        return _apply_constraints(result.x, tickers)
    logger.warning("Max Sharpe no convergió — usando equal weight")
    return np.ones(n) / n


def _optimize_min_variance(mu: np.ndarray, cov: np.ndarray, tickers: list[str]) -> np.ndarray:
    """Minimiza la varianza del portfolio."""
    from scipy.optimize import minimize

    n = len(tickers)

    def portfolio_variance(w):
        return float(w @ cov @ w)

    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1}]
    bounds = [(W_MIN, W_MAX)] * n
    w0 = np.ones(n) / n

    result = minimize(portfolio_variance, w0, method="SLSQP",
                      bounds=bounds, constraints=constraints,
                      options={"maxiter": 1000, "ftol": 1e-12})

    if result.success:
        return _apply_constraints(result.x, tickers)
    logger.warning("Min Variance no convergió — usando equal weight")
    return np.ones(n) / n


def _optimize_risk_parity(mu: np.ndarray, cov: np.ndarray, tickers: list[str]) -> np.ndarray:
    """Risk Parity: igual contribución de riesgo por activo."""
    from scipy.optimize import minimize

    n = len(tickers)
    target_rc = np.ones(n) / n  # contribución objetivo = 1/n

    def risk_parity_objective(w):
        w = np.maximum(w, 1e-8)
        sigma = np.sqrt(w @ cov @ w)
        mrc = cov @ w / sigma          # marginal risk contribution
        rc  = w * mrc / sigma          # risk contribution relativa
        return float(np.sum((rc - target_rc) ** 2))

    constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1}]
    bounds = [(W_MIN, W_MAX)] * n
    w0 = np.ones(n) / n

    result = minimize(risk_parity_objective, w0, method="SLSQP",
                      bounds=bounds, constraints=constraints,
                      options={"maxiter": 2000, "ftol": 1e-10})

    if result.success:
        return _apply_constraints(result.x, tickers)
    logger.warning("Risk Parity no convergió — usando equal weight")
    return np.ones(n) / n


# ── Selección de método ───────────────────────────────────────────────────────

def _select_method(macro_regime: dict, vix: Optional[float]) -> tuple[str, str]:
    """
    Selecciona el método óptimo según régimen macro y VIX.
    Retorna (method, reason).
    """
    market  = macro_regime.get("market", "neutral")
    vix_val = vix or 20.0

    if vix_val > 35:
        return "RISK_PARITY", f"VIX extremo ({vix_val:.0f}) — máxima diversificación"
    if vix_val > 25:
        return "MIN_VARIANCE", f"VIX elevado ({vix_val:.0f}) — modo defensivo"
    if market == "risk_off":
        return "MIN_VARIANCE", f"Régimen risk_off — preservar capital"
    if market == "risk_on":
        return "MAX_SHARPE", f"Régimen risk_on — maximizar retorno ajustado"
    return "MAX_SHARPE", f"Régimen neutral — maximizar Sharpe"


# ── Filtro de universo ────────────────────────────────────────────────────────

def _build_universe(
    current_positions: list[dict],
    synthesis_results: list,          # list[SynthesisResult]
    market_assets: list[dict],        # activos scrapeados de Cocos
) -> list[str]:
    """
    Construye el universo de activos para el optimizer.

    Incluye:
      1. Posiciones actuales (siempre, para calcular delta de salida)
      2. Activos del mercado scrapeado con score synthesis > -0.15
         (no SELL/REDUCE confirmados)

    Excluye:
      - Activos sin ticker USD claro
      - Duplicados
    """
    universe = set()

    # Posiciones actuales — siempre incluidas
    for pos in current_positions:
        t = pos.get("ticker", "").strip().upper()
        if t:
            universe.add(t)

    # Scores del pipeline (solo los no-negativos)
    score_map = {r.ticker: r.final_score for r in synthesis_results}
    for ticker, score in score_map.items():
        if score > -0.15:
            universe.add(ticker)

    # Activos de mercado scrapeados — solo CEDEARs con ticker reconocible
    # Los CEDEARs de Cocos tienen el ticker del subyacente USD directamente
    for asset in market_assets:
        ticker = asset.get("ticker", "").strip().upper()
        if ticker and len(ticker) <= 5 and ticker.isalpha():
            # Filtrar por score si ya fue analizado
            if ticker in score_map and score_map[ticker] <= -0.15:
                continue
            universe.add(ticker)

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
) -> Optional[RebalanceReport]:
    """
    Pipeline completo del optimizer.

    Args:
        current_positions: posiciones actuales con 'ticker' y 'market_value'
        portfolio_value_ars: valor total del portfolio en ARS
        cash_ars: cash disponible en ARS
        macro_regime: dict del get_macro_regime()
        vix: VIX actual
        synthesis_results: list[SynthesisResult] del pipeline de scoring
        market_assets: activos scrapeados del mercado (CEDEARs + Acciones)
        threshold: delta mínimo para ejecutar un trade (default 3%)

    Returns:
        RebalanceReport o None si falla
    """
    try:
        import pandas as pd

        # 1. Universo
        universe = _build_universe(current_positions, synthesis_results, market_assets)
        if len(universe) < 2:
            logger.warning("Optimizer: universo insuficiente (<2 activos)")
            return None

        # 2. Retornos históricos
        returns = _fetch_returns(universe)
        if returns.empty or len(returns.columns) < 2:
            logger.warning("Optimizer: no hay suficientes datos históricos")
            return None

        # Recalcular universe con los que sí tienen datos
        universe = list(returns.columns)
        n = len(universe)

        # 3. Parámetros estadísticos
        mu  = returns.mean().values          # retorno diario medio
        cov = returns.cov().values * 252     # covarianza anualizada
        mu_annual = mu * 252                 # retorno anual

        # 4. Seleccionar método
        method, reason = _select_method(macro_regime, vix)

        # 5. Optimizar
        if method == "MAX_SHARPE":
            raw_weights = _optimize_max_sharpe(mu_annual, cov, universe)
        elif method == "MIN_VARIANCE":
            raw_weights = _optimize_min_variance(mu_annual, cov, universe)
        else:
            raw_weights = _optimize_risk_parity(mu_annual, cov, universe)

        weights_optimal = dict(zip(universe, raw_weights))

        # 6. Stats del portfolio óptimo
        w_arr = np.array([weights_optimal[t] for t in universe])
        exp_ret, exp_vol, sharpe = _portfolio_stats(w_arr, mu_annual, cov)

        opt_result = OptimizationResult(
            method=method,
            method_reason=reason,
            weights=weights_optimal,
            expected_return_annual=round(exp_ret, 4),
            expected_vol_annual=round(exp_vol, 4),
            sharpe_ratio=round(sharpe, 3),
            n_assets=n,
        )

        # 7. Pesos actuales
        current_weights: dict[str, float] = {}
        for pos in current_positions:
            t  = pos.get("ticker", "").upper()
            mv = float(pos.get("market_value", 0) or 0)
            current_weights[t] = mv / portfolio_value_ars if portfolio_value_ars > 0 else 0.0

        # 8. Calcular trades
        all_tickers = set(list(weights_optimal.keys()) + list(current_weights.keys()))
        trades: list[RebalanceTrade] = []

        for ticker in sorted(all_tickers):
            w_cur = current_weights.get(ticker, 0.0)
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

            trades.append(RebalanceTrade(
                ticker=ticker,
                weight_current=round(w_cur, 4),
                weight_optimal=round(w_opt, 4),
                delta=round(delta, 4),
                action=action,
                amount_ars=round(amount_ars, 0),
            ))

        # 9. Resumen financiero
        sells = sum(t.amount_ars for t in trades if t.action in ("VENDER", "REDUCIR"))
        buys  = sum(t.amount_ars for t in trades if t.action in ("COMPRAR", "NUEVO"))
        cash_total = cash_ars + sells
        net_cash_needed = buys - cash_total

        n_trades = sum(1 for t in trades if t.action != "MANTENER")

        logger.info(
            f"Optimizer [{method}]: {n_trades} trades — "
            f"ventas ${sells:,.0f} compras ${buys:,.0f} net ${net_cash_needed:,.0f} ARS"
        )

        return RebalanceReport(
            optimization=opt_result,
            trades=trades,
            total_sells_ars=round(sells, 0),
            total_buys_ars=round(buys, 0),
            net_cash_needed=round(net_cash_needed, 0),
            n_trades=n_trades,
            portfolio_value_ars=portfolio_value_ars,
            threshold_used=threshold,
        )

    except Exception as e:
        logger.error(f"Optimizer falló: {e}", exc_info=True)
        return None
