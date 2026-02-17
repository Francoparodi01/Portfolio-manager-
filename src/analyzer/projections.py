import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class ProjectionsCalculator:
    """
    Genera proyecciones multi-escenario
    
    Escenarios:
    - Mejor caso (+2σ)
    - Optimista (+1σ)
    - Base (tendencia)
    - Pesimista (-1σ)
    - Estrés (-2σ)
    
    Enfoque: Proyecciones como RANGOS, no puntos
    """
    
    def __init__(self, repository):
        self.repo = repository
    
    def generate_projections(
        self,
        weeks_ahead: List[int] = [4, 12],
        window_days: int = 90
    ) -> Dict:
        """
        Genera proyecciones a N semanas
        
        Args:
            weeks_ahead: Lista de horizontes (semanas)
            window_days: Ventana histórica para calibrar
            
        Returns:
            Dict con proyecciones por escenario
        """
        logger.info(f"Generando proyecciones para {weeks_ahead} semanas...")
        
        history = self.repo.get_portfolio_history(days=window_days)
        
        if len(history) < 30:
            logger.warning("Datos insuficientes para proyecciones robustas")
            return {}
        
        df = pd.DataFrame(history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['returns'] = df['total_value'].pct_change()
        
        current_value = df['total_value'].iloc[-1]
        
        # Estadísticas de retornos
        mean_return = df['returns'].mean()
        std_return = df['returns'].std()
        
        # Análisis de tendencia (regresión lineal)
        days = (df['timestamp'] - df['timestamp'].min()).dt.days
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            days, df['total_value']
        )
        
        projections = {
            'current_value': float(current_value),
            'analysis_period_days': len(df),
            'trend_slope_daily': float(slope),
            'trend_confidence': float(r_value ** 2),  # R²
            'mean_daily_return': float(mean_return),
            'std_daily_return': float(std_return),
            'scenarios': {}
        }
        
        # Generar escenarios para cada horizonte
        for weeks in weeks_ahead:
            days_ahead = weeks * 7
            
            scenarios = self._calculate_scenarios(
                current_value=current_value,
                days_ahead=days_ahead,
                mean_return=mean_return,
                std_return=std_return,
                slope=slope,
                intercept=intercept,
                days_max=days.max()
            )
            
            projections['scenarios'][f'{weeks}_semanas'] = scenarios
        
        logger.info(f"Proyecciones generadas para {len(weeks_ahead)} horizontes")
        
        return projections
    
    def _calculate_scenarios(
        self,
        current_value: float,
        days_ahead: int,
        mean_return: float,
        std_return: float,
        slope: float,
        intercept: float,
        days_max: int
    ) -> Dict:
        """
        Calcula 5 escenarios para un horizonte
        """
        # Base: Tendencia lineal histórica
        base_value = intercept + slope * (days_max + days_ahead)
        
        # Mejor caso: +2 desviaciones estándar
        best_return = mean_return + 2 * std_return
        best_value = current_value * (1 + best_return) ** days_ahead
        
        # Optimista: +1 desviación estándar
        opt_return = mean_return + std_return
        opt_value = current_value * (1 + opt_return) ** days_ahead
        
        # Pesimista: -1 desviación estándar
        pess_return = mean_return - std_return
        pess_value = current_value * (1 + pess_return) ** days_ahead
        
        # Estrés: -2 desviaciones estándar
        stress_return = mean_return - 2 * std_return
        stress_value = current_value * (1 + stress_return) ** days_ahead
        
        def _scenario_dict(value, prob_label):
            return {
                'valor': float(value),
                'cambio_pct': float(((value - current_value) / current_value) * 100),
                'cambio_absoluto': float(value - current_value),
                'probabilidad': prob_label
            }
        
        return {
            'mejor_caso': _scenario_dict(best_value, '2.5%'),
            'optimista': _scenario_dict(opt_value, '16%'),
            'base': _scenario_dict(base_value, '50%'),
            'pesimista': _scenario_dict(pess_value, '16%'),
            'estres': _scenario_dict(stress_value, '2.5%')
        }