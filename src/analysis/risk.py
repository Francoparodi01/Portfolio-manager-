"""
src/analysis/risk.py

Motor de riesgo: position sizing, drawdown, volatilidad objetivo.

Formulas:
  Volatilidad anualizada = std(retornos_diarios) * sqrt(252)
  Kelly fraccionario     = (win_rate * avg_win - loss_rate * avg_loss) / avg_win
  Position size          = (vol_target / asset_vol) * kelly_fraction * capital
  Drawdown               = (valor_actual - pico_historico) / pico_historico

Filosofia:
  El sizing gana mas que el timing.
  Un sistema que apuesta demasiado quiebra incluso con edge positivo.
  Kelly completo es demasiado agresivo → usar 25% del Kelly optimo.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# ── Parametros globales del risk engine ─────────────────────────────────────
VOLATILITY_TARGET    = 0.15   # 15% vol anualizada objetivo del portfolio
MAX_POSITION_PCT     = 0.25   # max 25% por activo
MIN_POSITION_PCT     = 0.02   # min 2% (por debajo no vale la pena operar)
KELLY_FRACTION       = 0.25   # 25% del Kelly optimo (conservador)
DRAWDOWN_WARN        = 0.10   # -10%: reducir exposicion
DRAWDOWN_STOP        = 0.20   # -20%: salir a cash
VIX_HIGH_THRESHOLD   = 25     # VIX > 25: reducir sizing en 40%
VIX_EXTREME          = 35     # VIX > 35: reducir sizing en 70%


@dataclass
class RiskMetrics:
    ticker: str
    # Volatilidad
    volatility_annual: float = 0.0   # vol anualizada del activo
    volatility_20d: float    = 0.0   # vol 20 dias
    # Retornos historicos
    avg_daily_return: float  = 0.0
    sharpe_approx: float     = 0.0   # sharpe simplificado (sin risk-free)
    max_drawdown_6m: float   = 0.0   # max drawdown 6 meses
    # Position sizing
    kelly_fraction: float    = 0.0   # Kelly fraccionario calculado
    suggested_pct: float     = 0.0   # % del portfolio sugerido
    suggested_pct_adj: float = 0.0   # % ajustado por VIX y drawdown
    # Estado
    risk_level: str          = "NORMAL"  # LOW | NORMAL | HIGH | EXTREME
    warnings: list           = field(default_factory=list)


@dataclass
class PortfolioRisk:
    """Estado de riesgo del portfolio completo."""
    total_value_ars: float
    cash_pct: float
    drawdown_current: float   = 0.0   # vs pico historico
    drawdown_status: str      = "OK"  # OK | WARN | STOP
    vix_level: Optional[float] = None
    sizing_multiplier: float  = 1.0   # multiplicador global por condiciones
    positions: list           = field(default_factory=list)
    report: str               = ""

    def to_telegram(self) -> str:
        dd_icon = "🟢" if self.drawdown_status == "OK" else \
                  "🟡" if self.drawdown_status == "WARN" else "🔴"
        vix_str = f"VIX {self.vix_level:.1f}" if self.vix_level else ""
        lines = [
            f"⚖️ <b>RISK ENGINE</b>",
            f"{dd_icon} Drawdown: <b>{self.drawdown_current:.1%}</b> ({self.drawdown_status})",
            f"💵 Cash: <b>{self.cash_pct:.1%}</b> del portfolio",
        ]
        if vix_str:
            lines.append(f"📊 {vix_str} → sizing x{self.sizing_multiplier:.2f}")
        for p in self.positions[:6]:
            lines.append(
                f"  {p['ticker']:6s} "
                f"actual {p['current_pct']:.1%} → "
                f"sugerido {p['suggested_pct_adj']:.1%} "
                f"({p['risk_level']})"
            )
        return "\n".join(lines)


def compute_asset_risk(
    ticker: str,
    prices: "pd.Series",
    current_value: float,
    portfolio_total: float,
    vix: Optional[float] = None,
    portfolio_drawdown: float = 0.0,
) -> RiskMetrics:
    """
    Calcula metricas de riesgo y position sizing para un activo.

    Args:
        ticker:             simbolo del activo
        prices:             serie de precios historicos (Close)
        current_value:      valor actual de la posicion en ARS
        portfolio_total:    valor total del portfolio en ARS
        vix:                nivel actual del VIX (opcional)
        portfolio_drawdown: drawdown actual del portfolio (0 a -1)
    """
    m = RiskMetrics(ticker=ticker)

    if not HAS_DEPS or prices is None or len(prices) < 20:
        m.warnings.append("Datos insuficientes para calcular riesgo")
        m.suggested_pct = MAX_POSITION_PCT / 2
        m.suggested_pct_adj = m.suggested_pct
        return m

    try:
        returns = prices.pct_change().dropna()

        # ── Volatilidad ────────────────────────────────────────
        vol_daily    = float(returns.std())
        vol_annual   = vol_daily * (252 ** 0.5)
        vol_20d      = float(returns.tail(20).std()) * (252 ** 0.5)

        m.volatility_annual = round(vol_annual, 4)
        m.volatility_20d    = round(vol_20d, 4)

        # ── Retornos ───────────────────────────────────────────
        avg_ret = float(returns.mean())
        m.avg_daily_return = round(avg_ret, 6)
        m.sharpe_approx    = round((avg_ret / vol_daily) * (252 ** 0.5), 3) if vol_daily > 0 else 0.0

        # ── Max drawdown 6 meses ───────────────────────────────
        rolling_max = prices.cummax()
        dd_series   = (prices - rolling_max) / rolling_max
        m.max_drawdown_6m = round(float(dd_series.min()), 4)

        # ── Kelly fraccionario ─────────────────────────────────
        # Estimar win_rate y avg_win/loss de los retornos historicos
        wins   = returns[returns > 0]
        losses = returns[returns < 0]

        if len(wins) > 5 and len(losses) > 5:
            win_rate  = len(wins) / len(returns)
            loss_rate = 1 - win_rate
            avg_win   = float(wins.mean())
            avg_loss  = abs(float(losses.mean()))

            if avg_loss > 0:
                kelly_full = (win_rate * avg_win - loss_rate * avg_loss) / avg_win
                kelly_full = max(0.0, kelly_full)  # Kelly negativo = no operar
            else:
                kelly_full = 0.5

            m.kelly_fraction = round(kelly_full * KELLY_FRACTION, 4)
        else:
            m.kelly_fraction = 0.05  # default conservador

        # ── Position sizing base ───────────────────────────────
        # Vol targeting: cuanto % del portfolio para que contribuya
        # VOLATILITY_TARGET al riesgo total del portfolio
        if vol_annual > 0:
            size_by_vol = VOLATILITY_TARGET / vol_annual
        else:
            size_by_vol = 0.10

        # Combinar Kelly y vol-targeting (el mas conservador gana)
        suggested = min(m.kelly_fraction, size_by_vol)
        suggested = float(np.clip(suggested, MIN_POSITION_PCT, MAX_POSITION_PCT))
        m.suggested_pct = round(suggested, 4)

        # ── Ajustes por condiciones de mercado ─────────────────
        adj = suggested

        # Ajuste por VIX
        vix_mult = 1.0
        if vix:
            if vix > VIX_EXTREME:
                vix_mult = 0.30
                m.warnings.append(f"VIX extremo ({vix:.0f}) — sizing reducido 70%")
            elif vix > VIX_HIGH_THRESHOLD:
                vix_mult = 0.60
                m.warnings.append(f"VIX alto ({vix:.0f}) — sizing reducido 40%")

        # Ajuste por drawdown del portfolio
        dd_mult = 1.0
        if portfolio_drawdown <= -DRAWDOWN_STOP:
            dd_mult = 0.0
            m.warnings.append(f"Drawdown critico ({portfolio_drawdown:.1%}) — salir a cash")
            m.risk_level = "EXTREME"
        elif portfolio_drawdown <= -DRAWDOWN_WARN:
            dd_mult = 0.50
            m.warnings.append(f"Drawdown en zona de alerta ({portfolio_drawdown:.1%})")
            m.risk_level = "HIGH"

        adj = adj * vix_mult * dd_mult
        m.suggested_pct_adj = round(float(np.clip(adj, 0.0, MAX_POSITION_PCT)), 4)

        # ── Risk level ─────────────────────────────────────────
        if m.risk_level == "NORMAL":
            if vol_annual > 0.50:
                m.risk_level = "HIGH"
            elif vol_annual > 0.35:
                m.risk_level = "ELEVATED"
            elif vol_annual < 0.20:
                m.risk_level = "LOW"

    except Exception as e:
        logger.error(f"Error calculando riesgo para {ticker}: {e}", exc_info=True)
        m.warnings.append(f"Error en calculo: {e}")

    return m


def compute_portfolio_drawdown(history: list[dict]) -> float:
    """
    Calcula el drawdown actual del portfolio contra su pico historico.

    Args:
        history: lista de dicts con 'total_value_ars' ordenados por fecha asc.

    Returns:
        float entre -1.0 (todo perdido) y 0.0 (en el pico)
    """
    if not history:
        return 0.0
    values = [float(h.get("total_value_ars", 0) or 0) for h in history]
    values = [v for v in values if v > 0]
    if len(values) < 2:
        return 0.0
    peak    = max(values)
    current = values[-1]
    return (current - peak) / peak if peak > 0 else 0.0


def build_portfolio_risk_report(
    positions: list[dict],
    prices_map: dict,         # ticker → pd.Series de precios
    total_ars: float,
    cash_ars: float,
    history: list[dict],
    vix: Optional[float] = None,
) -> PortfolioRisk:
    """
    Construye el reporte de riesgo completo del portfolio.

    Args:
        positions:   lista de posiciones del snapshot
        prices_map:  {ticker: pd.Series} precios historicos USD
        total_ars:   valor total del portfolio
        cash_ars:    cash disponible
        history:     historial de snapshots para calcular drawdown
        vix:         nivel VIX actual
    """
    drawdown = compute_portfolio_drawdown(history)
    cash_pct = cash_ars / (total_ars + cash_ars) if (total_ars + cash_ars) > 0 else 0.0

    # Sizing multiplier global
    sizing_mult = 1.0
    if vix:
        if vix > VIX_EXTREME:
            sizing_mult = 0.30
        elif vix > VIX_HIGH_THRESHOLD:
            sizing_mult = 0.60

    if drawdown <= -DRAWDOWN_STOP:
        sizing_mult = 0.0
        dd_status = "STOP"
    elif drawdown <= -DRAWDOWN_WARN:
        sizing_mult = min(sizing_mult, 0.50)
        dd_status = "WARN"
    else:
        dd_status = "OK"

    pr = PortfolioRisk(
        total_value_ars=total_ars,
        cash_pct=cash_pct,
        drawdown_current=drawdown,
        drawdown_status=dd_status,
        vix_level=vix,
        sizing_multiplier=sizing_mult,
    )

    # Calcular riesgo por posicion
    for pos in positions:
        ticker = pos.get("ticker", "")
        prices = prices_map.get(ticker)
        mv     = float(pos.get("market_value", 0) or 0)

        metrics = compute_asset_risk(
            ticker=ticker,
            prices=prices,
            current_value=mv,
            portfolio_total=total_ars,
            vix=vix,
            portfolio_drawdown=drawdown,
        )

        current_pct = mv / total_ars if total_ars > 0 else 0.0

        # Recomendacion de ajuste
        delta = metrics.suggested_pct_adj - current_pct
        if delta > 0.03:
            action = "AUMENTAR"
        elif delta < -0.03:
            action = "REDUCIR"
        else:
            action = "MANTENER"

        pr.positions.append({
            "ticker":            ticker,
            "current_pct":       round(current_pct, 4),
            "suggested_pct":     metrics.suggested_pct,
            "suggested_pct_adj": metrics.suggested_pct_adj,
            "volatility_annual": metrics.volatility_annual,
            "sharpe":            metrics.sharpe_approx,
            "max_drawdown_6m":   metrics.max_drawdown_6m,
            "kelly":             metrics.kelly_fraction,
            "risk_level":        metrics.risk_level,
            "action":            action,
            "warnings":          metrics.warnings,
        })

        logger.info(
            f"Risk {ticker}: vol={metrics.volatility_annual:.0%} "
            f"sharpe={metrics.sharpe_approx:.2f} "
            f"sizing={metrics.suggested_pct_adj:.1%} ({action})"
        )

    return pr