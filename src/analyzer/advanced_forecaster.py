import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict
import logging

from arch import arch_model

logger = logging.getLogger(__name__)


class AdvancedForecaster:
    """
    Forecast profesional:
    - Consolidación diaria (último snapshot del día)
    - Log returns
    - Volatilidad condicional GARCH(1,1)
    - Simulación Monte Carlo
    """

    def __init__(self, repository):
        self.repo = repository

    # =========================
    # MAIN
    # =========================
    def run_forecast(
        self,
        window_days: int = 180,
        horizon_days: int = 20,
        simulations: int = 10000
    ) -> Dict:

        history = self.repo.get_portfolio_history(days=window_days)

        if len(history) < 5:
            logger.warning("Datos insuficientes para forecast robusto")
            return {}

        # -------------------------
        # Crear DataFrame
        # -------------------------
        df = pd.DataFrame(history)

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")

        # -------------------------
        # Consolidar a 1 snapshot por día
        # -------------------------
        df["date"] = df["timestamp"].dt.date
        df = df.groupby("date").last().reset_index()

        # -------------------------
        # Log returns
        # -------------------------
        df["log_returns"] = np.log(
            df["total_value"] / df["total_value"].shift(1)
        )

        df = df.dropna()

        if len(df) < 3:
            logger.warning("No hay suficientes retornos diarios")
            return {}

        current_value = float(df["total_value"].iloc[-1])

        # GARCH trabaja mejor en %
        returns = df["log_returns"] * 100

        # =========================
        # GARCH(1,1)
        # =========================
        try:
            model = arch_model(returns, vol="Garch", p=1, q=1)
            res = model.fit(disp="off")

            forecast = res.forecast(horizon=horizon_days)
            cond_var = forecast.variance.values[-1]
            cond_vol = np.sqrt(cond_var) / 100  # volver a escala normal

        except Exception as e:
            logger.warning(f"GARCH falló, usando std histórica: {e}")
            cond_vol = np.std(df["log_returns"])

        mu = df["log_returns"].mean()

        # =========================
        # Monte Carlo
        # =========================
        simulated_paths = np.zeros((simulations, horizon_days))

        for i in range(simulations):
            shocks = np.random.normal(0, cond_vol, horizon_days)
            path = np.exp((mu - 0.5 * cond_vol**2) + shocks)
            simulated_paths[i] = path.cumprod()

        final_prices = current_value * simulated_paths[:, -1]

        result = {
            "current_value": current_value,
            "expected_value": float(np.mean(final_prices)),
            "median_value": float(np.median(final_prices)),
            "p5": float(np.percentile(final_prices, 5)),
            "p95": float(np.percentile(final_prices, 95)),
            "probabilidad_perdida": float(
                np.mean(final_prices < current_value)
            ),
            "expected_return_pct": float(
                (np.mean(final_prices) / current_value - 1) * 100
            ),
            "horizon_days": horizon_days,
            "simulations": simulations,
            "calculated_at": datetime.utcnow().isoformat()
        }

        logger.info("Forecast Monte Carlo completado")

        return result


# ==========================================================
# TEST DIRECTO
# ==========================================================
if __name__ == "__main__":
    import logging
    from src.storage.postgres_repository import PostgresRepository

    logging.basicConfig(level=logging.INFO)

    repo = PostgresRepository()
    model = AdvancedForecaster(repo)

    result = model.run_forecast(
        window_days=180,
        horizon_days=20,
        simulations=3000
    )

    print("\nForecast resultado:")
    print(result)