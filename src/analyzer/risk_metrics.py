import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class RiskMetricsCalculator:
    """
    Calcula métricas de riesgo del portfolio
    
    Métricas:
    - Volatilidad anualizada
    - Max Drawdown
    - Sharpe Ratio
    - Value at Risk (VaR)
    """
    
    def __init__(self, repository):
        self.repo = repository

        
    def calculate_all(self, window_days: int = 90) -> Dict:
        logger.info(f"Calculando métricas de riesgo (ventana: {window_days} días)...")
        
        history = self.repo.get_portfolio_history(days=window_days)
        
        if len(history) < 10:
            logger.warning("Datos insuficientes para métricas robustas")
            return self._empty_metrics()
        
        df = pd.DataFrame(history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Filtrar solo cierre 17:00
        df_daily = df[df['timestamp'].dt.hour == 17].copy()

        if len(df_daily) < 10:
            logger.warning("Datos diarios insuficientes")
            return self._empty_metrics()

        metrics = {
            'window_days': window_days,
            'data_points': len(df_daily),
            'volatility': self._calculate_volatility(df),
            'max_drawdown': self._calculate_max_drawdown(df),
            'sharpe_ratio': self._calculate_sharpe(df),
            'var_95': self._calculate_var(df, confidence=0.95),
            'calculated_at': datetime.utcnow().isoformat()
        }
        
        logger.info(
            f"Métricas calculadas: "
            f"Vol={metrics['volatility']:.4f}%, "
            f"DD={metrics['max_drawdown']:.4f}%"
        )

        return metrics
    


    def _prepare_daily_returns(self, df: pd.DataFrame) -> pd.Series:
        # Usar solo cierre (17:00)
        df = df[df['timestamp'].dt.hour == 17].copy()
        df = df.sort_values('timestamp')

        df['returns'] = df['total_value'].pct_change()
        return df['returns'].dropna()


    def _calculate_volatility(self, df: pd.DataFrame) -> float:
        returns = self._prepare_daily_returns(df)

        if len(returns) < 10:
            return 0.0

        daily_std = returns.std()
        annual_vol = daily_std * np.sqrt(252) * 100
        return float(annual_vol)


    def _calculate_sharpe(self, df: pd.DataFrame, risk_free_rate: float = 0.0) -> float:
        returns = self._prepare_daily_returns(df)

        if len(returns) < 10:
            return 0.0

        mean_return = returns.mean()
        std_return = returns.std()

        if std_return == 0:
            return 0.0

        sharpe = (mean_return - risk_free_rate / 252) / std_return * np.sqrt(252)
        return float(sharpe)


    def _calculate_var(self, df: pd.DataFrame, confidence: float = 0.95) -> float:
        returns = self._prepare_daily_returns(df)

        if len(returns) < 10:
            return 0.0

        var = np.percentile(returns, (1 - confidence) * 100) * 100
        return float(var)
    
    def _calculate_max_drawdown(self, df: pd.DataFrame) -> float:
        df = df[df['timestamp'].dt.hour == 17].copy()
        df = df.sort_values('timestamp')

        df['cummax'] = df['total_value'].cummax()
        df['drawdown'] = (df['total_value'] - df['cummax']) / df['cummax'] * 100

        return float(df['drawdown'].min())