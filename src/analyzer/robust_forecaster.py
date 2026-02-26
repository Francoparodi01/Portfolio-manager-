import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class RobustForecaster:
    """
    Forecast profesional para 30–60 días históricos.

    - Log returns diarios
    - Volatilidad EWMA (RiskMetrics)
    - Monte Carlo geométrico
    - VaR paramétrico
    """

    def __init__(self, repository):
        self.repo = repository

    # =====================================================
    # MAIN
    # =====================================================
    def run_forecast(
        self,
        window_days: int = 60,
        horizon_days: int = 20,
        simulations: int = 5000,
        lambda_decay: float = 0.94
    ) -> Dict:

        history = self.repo.get_portfolio_history(days=window_days)

        if len(history) < 10:
            logger.warning("Datos insuficientes")
            return {}

        # -------------------------
        # DataFrame
        # -------------------------
        df = pd.DataFrame(history)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        df["date"] = df["timestamp"].dt.date
        df = df.groupby("date").last().reset_index()

        df["log_returns"] = np.log(
            df["total_value"] / df["total_value"].shift(1)
        )

        df = df.dropna()

        if len(df) < 10:
            logger.warning("Pocos retornos diarios")
            return {}

        current_value = float(df["total_value"].iloc[-1])

        returns = df["log_returns"].values

        # =====================================================
        # Drift robusto (media winsorizada)
        # =====================================================
        trimmed = np.clip(
            returns,
            np.percentile(returns, 5),
            np.percentile(returns, 95)
        )
        mu = np.mean(trimmed)

        # =====================================================
        # EWMA Volatility
        # =====================================================
        var = np.var(returns)
        for r in returns:
            var = lambda_decay * var + (1 - lambda_decay) * r**2

        sigma = np.sqrt(var)

        # =====================================================
        # Monte Carlo geométrico
        # =====================================================
        shocks = np.random.normal(
            mu,
            sigma,
            (simulations, horizon_days)
        )

        price_paths = current_value * np.exp(
            np.cumsum(shocks, axis=1)
        )

        final_prices = price_paths[:, -1]

        # =====================================================
        # Métricas
        # =====================================================
        expected_value = np.mean(final_prices)
        median_value = np.median(final_prices)
        p5 = np.percentile(final_prices, 5)
        p95 = np.percentile(final_prices, 95)

        var_95 = current_value - p5

        result = {
            "current_value": current_value,
            "expected_value": float(expected_value),
            "median_value": float(median_value),
            "p5": float(p5),
            "p95": float(p95),
            "probabilidad_perdida": float(
                np.mean(final_prices < current_value)
            ),
            "expected_return_pct": float(
                (expected_value / current_value - 1) * 100
            ),
            "volatilidad_ewma_anualizada": float(
                sigma * np.sqrt(252) * 100
            ),
            "VaR_95_monetario": float(var_95),
            "horizon_days": horizon_days,
            "simulations": simulations,
            "calculated_at": datetime.utcnow().isoformat()
        }

        logger.info("Forecast robusto completado")

        return result