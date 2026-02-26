import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ProjectionsCalculator:
    """
    Proyecciones multi-escenario profesionales
    Basadas en log returns diarios (cierre 17:00)
    Modelo geométrico consistente (drift + shock)
    """

    def __init__(self, repository):
        self.repo = repository

    # =========================
    # MAIN
    # =========================
    def generate_projections(
        self,
        weeks_ahead: List[int] = [4, 12],
        window_days: int = 90
    ) -> Dict:

        logger.info(f"Generando proyecciones profesionales para {weeks_ahead} semanas...")

        history = self.repo.get_portfolio_history(days=window_days)

        if len(history) < 30:
            logger.warning("Datos insuficientes para proyecciones robustas")
            return {}

        df = pd.DataFrame(history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Filtrar solo cierre diario (17:00)
        df = df[df['timestamp'].dt.hour == 17].copy()

        if len(df) < 30:
            logger.warning("Datos diarios insuficientes")
            return {}

        # Log returns diarios
        df['log_returns'] = np.log(
            df['total_value'] / df['total_value'].shift(1)
        )
        df = df.dropna()

        current_value = df['total_value'].iloc[-1]

        mean_return = df['log_returns'].mean()
        std_return = df['log_returns'].std()

        projections = {
            'current_value': float(current_value),
            'analysis_days': len(df),
            'mean_daily_log_return': float(mean_return),
            'std_daily_log_return': float(std_return),
            'scenarios': {},
            'calculated_at': datetime.utcnow().isoformat()
        }

        for weeks in weeks_ahead:
            trading_days_ahead = weeks * 5  # solo días hábiles

            scenarios = self._calculate_scenarios(
                current_value=current_value,
                days_ahead=trading_days_ahead,
                mean_return=mean_return,
                std_return=std_return
            )

            projections['scenarios'][f'{weeks}_semanas'] = scenarios

        logger.info("Proyecciones generadas correctamente")

        return projections

    # =========================
    # SCENARIOS (Modelo geométrico)
    # =========================
    def _calculate_scenarios(
        self,
        current_value: float,
        days_ahead: int,
        mean_return: float,
        std_return: float
    ) -> Dict:

        def projected_value(k_sigma: float):
            drift = mean_return * days_ahead
            shock = k_sigma * std_return * np.sqrt(days_ahead)
            return current_value * np.exp(drift + shock)

        def scenario(value, prob_label):
            return {
                'valor': float(value),
                'cambio_pct': float(((value - current_value) / current_value) * 100),
                'cambio_absoluto': float(value - current_value),
                'probabilidad_aprox': prob_label
            }

        return {
            'mejor_caso': scenario(projected_value(+2), '2.5%'),
            'optimista': scenario(projected_value(+1), '16%'),
            'base': scenario(projected_value(0), '50%'),
            'pesimista': scenario(projected_value(-1), '16%'),
            'estres': scenario(projected_value(-2), '2.5%')
        }