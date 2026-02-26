import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict
import logging

try:
    from arch import arch_model
    ARCH_AVAILABLE = True
except:
    ARCH_AVAILABLE = False

logger = logging.getLogger(__name__)


class AdaptiveForecaster:

    def __init__(self, repository):
        self.repo = repository

    def run_forecast(
        self,
        window_days: int = 90,
        horizon_days: int = 20,
        simulations: int = 5000
    ) -> Dict:

        history = self.repo.get_portfolio_history(days=window_days)

        if len(history) < 15:
            logger.warning("Datos insuficientes")
            return {}

        df = pd.DataFrame(history)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        df["date"] = df["timestamp"].dt.date
        df = df.groupby("date").last().reset_index()

        df["log_returns"] = np.log(
            df["total_value"] / df["total_value"].shift(1)
        )

        df = df.dropna()

        if len(df) < 15:
            logger.warning("Pocos retornos diarios")
            return {}

        current_value = float(df["total_value"].iloc[-1])
        returns = df["log_returns"].values

        mu = np.mean(returns)

        # =====================================================
        # Volatility Estimation
        # =====================================================
        if len(df) >= 90 and ARCH_AVAILABLE:

            logger.info("Usando GARCH")

            returns_pct = returns * 100
            model = arch_model(returns_pct, vol="Garch", p=1, q=1)
            res = model.fit(disp="off")

            forecast = res.forecast(horizon=1)
            cond_var = forecast.variance.values[-1][0]
            sigma = np.sqrt(cond_var) / 100

        else:
            logger.info("Usando EWMA")

            lambda_decay = 0.94
            var = np.var(returns)

            for r in returns:
                var = lambda_decay * var + (1 - lambda_decay) * r**2

            sigma = np.sqrt(var)

        # =====================================================
        # Monte Carlo
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

        expected_value = np.mean(final_prices)
        p5 = np.percentile(final_prices, 5)
        p95 = np.percentile(final_prices, 95)

        result = {
            "modelo_usado": "GARCH" if len(df) >= 90 else "EWMA",
            "current_value": current_value,
            "expected_value": float(expected_value),
            "expected_return_pct": float(
                (expected_value / current_value - 1) * 100
            ),
            "p5": float(p5),
            "p95": float(p95),
            "probabilidad_perdida": float(
                np.mean(final_prices < current_value)
            ),
            "volatilidad_anualizada_pct": float(
                sigma * np.sqrt(252) * 100
            ),
            "horizon_days": horizon_days,
            "simulations": simulations,
            "calculated_at": datetime.utcnow().isoformat()
        }

        logger.info("Forecast adaptativo completado")

        return result
    
    # ==========================================================
# TEST DIRECTO
# ==========================================================
if __name__ == "__main__":
    import logging
    from src.storage.postgres_repository import PostgresRepository

    logging.basicConfig(level=logging.INFO)

    repo = PostgresRepository()
    model = AdaptiveForecaster(repo)

    result = model.run_forecast(
        window_days=60,
        horizon_days=20,
        simulations=3000
    )

    print("\nForecast resultado:")
    print(result)