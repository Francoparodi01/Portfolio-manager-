"""src/analysis/risk.py — Risk engine: volatilidad, Kelly, sizing, drawdown"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional
logger = logging.getLogger(__name__)
try:
    import numpy as np; import pandas as pd; HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# ── Parámetros calibrados para portfolio concentrado (4-8 posiciones) ─────────
VOL_TARGET   = 0.40   # 40% — permite posiciones en tech/growth de alta vol
MAX_POS      = 0.70   # 70% — portfolio concentrado, CVX puede ser 60%+
MIN_POS      = 0.03   # 3%  — mínimo útil
KELLY_FRAC   = 0.33   # 33% del Kelly óptimo (conservador pero no paralizante)

# Limites de drawdown del portfolio
DD_WARN  = 0.10   # -10%: warning
DD_STOP  = 0.20   # -20%: reducir todo

# VIX adjustments
VIX_HIGH    = 25
VIX_EXTREME = 35

@dataclass
class RiskMetrics:
    ticker: str
    volatility_annual: float = 0.0
    volatility_20d: float    = 0.0
    avg_daily_return: float  = 0.0
    sharpe_approx: float     = 0.0
    max_drawdown_6m: float   = 0.0
    kelly_fraction: float    = 0.0
    suggested_pct: float     = 0.0
    suggested_pct_adj: float = 0.0
    risk_level: str          = "NORMAL"
    warnings: list           = field(default_factory=list)


@dataclass
class PortfolioRisk:
    total_value_ars: float
    cash_pct: float
    drawdown_current: float = 0.0
    drawdown_status: str    = "OK"
    vix_level: Optional[float] = None
    sizing_multiplier: float = 1.0
    positions: list = field(default_factory=list)

    def to_telegram(self, opt_weights: dict = None) -> str:
        """
        opt_weights: dict ticker→weight_optimal del RebalanceReport.
        Si se pasa, el 'target' que se muestra es el del optimizer (fuente de verdad).
        El Risk Engine solo muestra métricas (vol, sharpe, drawdown).
        """
        dd_icon = "🟢" if self.drawdown_status == "OK" else "🟡" if self.drawdown_status == "WARN" else "🔴"
        lines = [
            "━" * 35,
            "⚖️  <b>RISK ENGINE</b>",
            f"{dd_icon} Drawdown actual: <b>{self.drawdown_current:.1%}</b> [{self.drawdown_status}]",
            f"💵 Cash disponible: <b>{self.cash_pct:.1%}</b> del portfolio",
        ]
        if self.vix_level:
            lines.append(f"   • VIX: {self.vix_level:.1f} → sizing ×{self.sizing_multiplier:.2f}")

        # Si tenemos pesos del optimizer, el target es el del optimizer
        using_optimizer = bool(opt_weights)
        header = "<b>Position sizing por activo:</b>" if not using_optimizer else \
                 "<b>Métricas de riesgo por activo:</b>"
        lines += ["", header]

        for p in self.positions:
            ticker = p["ticker"]
            cur = p["current_pct"]

            if using_optimizer and ticker in opt_weights:
                # Usar peso del optimizer como target
                target = opt_weights[ticker]
                delta  = target - cur
                delta_str = f"({delta:+.1%})" if abs(delta) > 0.02 else ""
                action_icon = "📈" if delta > 0.02 else "📉" if delta < -0.02 else "➡️"
                lines.append(
                    f"  {action_icon} <b>{ticker}</b>  "
                    f"actual {cur:.1%} → target <b>{target:.1%}</b> {delta_str}  "
                    f"vol={p['volatility_annual']:.0%}  sharpe={p['sharpe']:.2f}"
                )
            else:
                # Sin optimizer: mostrar suggested_pct_adj como antes
                target = p["suggested_pct_adj"]
                delta  = target - cur
                delta_str = f"({delta:+.1%})" if abs(delta) > 0.02 else ""
                action_icon = {"AUMENTAR": "📈", "REDUCIR": "📉", "MANTENER": "➡️"}.get(p["action"], "•")
                lines.append(
                    f"  {action_icon} <b>{ticker}</b>  "
                    f"actual {cur:.1%} → sugerido <b>{target:.1%}</b> {delta_str}  "
                    f"vol={p['volatility_annual']:.0%}  sharpe={p['sharpe']:.2f}"
                )
            for w in p.get("warnings", []):
                lines.append(f"    ⚠️ {w}")

        if using_optimizer:
            lines.append("   <i>↑ target = optimizer · métricas = risk engine</i>")

        return "\n".join(lines)


def compute_asset_risk(ticker, prices, current_value, portfolio_total, vix=None, portfolio_drawdown=0.0):
    m = RiskMetrics(ticker=ticker)
    if not HAS_DEPS or prices is None or len(prices) < 20:
        m.suggested_pct = 0.15
        m.suggested_pct_adj = m.suggested_pct
        return m

    try:
        returns = prices.pct_change().dropna()
        vol_d = float(returns.std())
        vol_a = vol_d * (252 ** 0.5)
        m.volatility_annual = round(vol_a, 4)
        m.volatility_20d    = round(float(returns.tail(20).std()) * (252 ** 0.5), 4)
        m.avg_daily_return  = round(float(returns.mean()), 6)
        m.sharpe_approx     = round((float(returns.mean()) / vol_d) * (252 ** 0.5), 3) if vol_d > 0 else 0.0

        rm = prices.cummax()
        m.max_drawdown_6m = round(float(((prices - rm) / rm).min()), 4)

        # ── Kelly fraccionario ─────────────────────────────────────────────
        wins   = returns[returns > 0]
        losses = returns[returns < 0]
        if len(wins) > 10 and len(losses) > 10:
            wr = len(wins) / len(returns)
            aw = float(wins.mean())
            al = abs(float(losses.mean()))
            raw_kelly = max(0.0, (wr * aw - (1 - wr) * al) / aw) if al > 0 else 0.20
            m.kelly_fraction = round(raw_kelly * KELLY_FRAC, 4)
        else:
            m.kelly_fraction = 0.10  # default conservador

        # ── Position sizing base ───────────────────────────────────────────
        # VOL_TARGET / asset_vol da el sizing para alcanzar la vol objetivo
        # Con VOL_TARGET=40% y CVX vol=25%, sugiere 40/25 = 1.6 → capped a MAX_POS
        vol_based = VOL_TARGET / vol_a if vol_a > 0 else 0.15

        # Tomar el menor entre Kelly y vol-based, pero mínimo MIN_POS
        sug = float(np.clip(min(m.kelly_fraction, vol_based), MIN_POS, MAX_POS))
        m.suggested_pct = round(sug, 4)

        # ── Clasificar nivel de riesgo ─────────────────────────────────────
        # Para tech/growth: vol 40-60% es NORMAL, no HIGH
        if vol_a > 0.80:
            m.risk_level = "EXTREME"
            m.warnings.append(f"Volatilidad extrema ({vol_a:.0%})")
        elif vol_a > 0.60:
            m.risk_level = "HIGH"
        elif vol_a > 0.45:
            m.risk_level = "ELEVATED"
        elif vol_a < 0.15:
            m.risk_level = "LOW"
        else:
            m.risk_level = "NORMAL"

        # ── Ajustes por VIX ────────────────────────────────────────────────
        vix_m = 1.0
        if vix and vix > VIX_EXTREME:
            vix_m = 0.50
            m.warnings.append(f"VIX extremo ({vix:.0f}) — sizing -50%")
        elif vix and vix > VIX_HIGH:
            vix_m = 0.75
            m.warnings.append(f"VIX alto ({vix:.0f}) — sizing -25%")

        # ── Ajustes por drawdown del portfolio ─────────────────────────────
        dd_m = 1.0
        if portfolio_drawdown <= -DD_STOP:
            dd_m = 0.0
            m.risk_level = "EXTREME"
            m.warnings.append(f"Drawdown crítico ({portfolio_drawdown:.1%}) — SALIR A CASH")
        elif portfolio_drawdown <= -DD_WARN:
            dd_m = 0.60
            m.warnings.append(f"Drawdown alerta ({portfolio_drawdown:.1%}) — reducir exposición")

        m.suggested_pct_adj = round(float(np.clip(sug * vix_m * dd_m, 0.0, MAX_POS)), 4)

    except Exception as e:
        logger.error(f"Risk {ticker}: {e}", exc_info=True)
        m.warnings.append(str(e))
        m.suggested_pct = 0.10
        m.suggested_pct_adj = 0.10

    return m


def compute_portfolio_drawdown(history) -> float:
    if not history:
        return 0.0
    vals = [float(h.get("total_value_ars", 0) or 0) for h in history
            if float(h.get("total_value_ars", 0) or 0) > 0]
    if len(vals) < 2:
        return 0.0
    peak = max(vals)
    cur  = vals[-1]
    return (cur - peak) / peak if peak > 0 else 0.0


def build_portfolio_risk_report(positions, prices_map, total_ars, cash_ars, history, vix=None):
    drawdown  = compute_portfolio_drawdown(history)
    total_all = total_ars + cash_ars
    cash_pct  = cash_ars / total_all if total_all > 0 else 0.0

    vix_m = 1.0
    if vix and vix > VIX_EXTREME:
        vix_m = 0.50
    elif vix and vix > VIX_HIGH:
        vix_m = 0.75

    if drawdown <= -DD_STOP:
        dd_s = "STOP"; vix_m = min(vix_m, 0.0)
    elif drawdown <= -DD_WARN:
        dd_s = "WARN"; vix_m = min(vix_m, 0.60)
    else:
        dd_s = "OK"

    pr = PortfolioRisk(
        total_value_ars=total_ars, cash_pct=cash_pct,
        drawdown_current=drawdown, drawdown_status=dd_s,
        vix_level=vix, sizing_multiplier=vix_m,
    )

    for pos in positions:
        ticker = pos.get("ticker", "")
        prices = prices_map.get(ticker)
        mv     = float(pos.get("market_value", 0) or 0)
        m      = compute_asset_risk(ticker, prices, mv, total_ars, vix, drawdown)

        current_pct = mv / total_ars if total_ars > 0 else 0.0
        delta       = m.suggested_pct_adj - current_pct
        action      = "AUMENTAR" if delta > 0.05 else "REDUCIR" if delta < -0.05 else "MANTENER"

        pr.positions.append({
            "ticker":           ticker,
            "current_pct":      round(current_pct, 4),
            "suggested_pct":    m.suggested_pct,
            "suggested_pct_adj": m.suggested_pct_adj,
            "volatility_annual": m.volatility_annual,
            "sharpe":           m.sharpe_approx,
            "max_drawdown_6m":  m.max_drawdown_6m,
            "kelly":            m.kelly_fraction,
            "risk_level":       m.risk_level,
            "action":           action,
            "warnings":         m.warnings,
        })
        logger.info(
            f"Risk {ticker}: vol={m.volatility_annual:.0%} sharpe={m.sharpe_approx:.2f} "
            f"sizing={m.suggested_pct_adj:.1%} ({action})"
        )

    return pr
