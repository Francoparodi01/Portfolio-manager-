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
        """
        Calcula todas las métricas de riesgo
        
        Args:
            window_days: Ventana de análisis
            
        Returns:
            Dict con métricas calculadas
        """
        logger.info(f"Calculando métricas de riesgo (ventana: {window_days} días)...")
        
        history = self.repo.get_portfolio_history(days=window_days)
        
        if len(history) < 10:
            logger.warning("Datos insuficientes para métricas robustas")
            return self._empty_metrics()
        
        df = pd.DataFrame(history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        df['returns'] = df['total_value'].pct_change()
        
        metrics = {
            'window_days': window_days,
            'data_points': len(df),
            'volatility': self._calculate_volatility(df),
            'max_drawdown': self._calculate_max_drawdown(df),
            'sharpe_ratio': self._calculate_sharpe(df),
            'var_95': self._calculate_var(df, confidence=0.95),
            'calculated_at': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Métricas calculadas: Vol={metrics['volatility']:.2f}%, DD={metrics['max_drawdown']:.2f}%")
        
        return metrics
    
    def _calculate_volatility(self, df: pd.DataFrame) -> float:
        """
        Volatilidad anualizada
        
        Formula: σ_daily * sqrt(252)
        """
        daily_std = df['returns'].std()
        annual_vol = daily_std * np.sqrt(252) * 100  # Porcentaje
        return float(annual_vol)
    
    def _calculate_max_drawdown(self, df: pd.DataFrame) -> float:
        """
        Maximum Drawdown
        
        Formula: (Trough - Peak) / Peak
        """
        df['cummax'] = df['total_value'].cummax()
        df['drawdown'] = (df['total_value'] - df['cummax']) / df['cummax'] * 100
        max_dd = df['drawdown'].min()
        return float(max_dd)
    
    def _calculate_sharpe(self, df: pd.DataFrame, risk_free_rate: float = 0.0) -> float:
        """
        Sharpe Ratio (anualizado)
        
        Formula: (R_portfolio - R_f) / σ_portfolio
        Asume rf = 0 para simplificar
        """
        mean_return = df['returns'].mean()
        std_return = df['returns'].std()
        
        if std_return == 0:
            return 0.0
        
        sharpe = (mean_return - risk_free_rate / 252) / std_return * np.sqrt(252)
        return float(sharpe)
    
    def _calculate_var(self, df: pd.DataFrame, confidence: float = 0.95) -> float:
        """
        Value at Risk (VaR)
        
        Args:
            confidence: Nivel de confianza (0.95 = 95%)
            
        Returns:
            Pérdida máxima esperada con X% confianza (valor negativo)
        """
        returns = df['returns'].dropna()
        var = np.percentile(returns, (1 - confidence) * 100) * 100
        return float(var)
    
    def _empty_metrics(self) -> Dict:
        """Retorna métricas vacías"""
        return {
            'window_days': 0,
            'data_points': 0,
            'volatility': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'var_95': 0.0,
            'calculated_at': datetime.utcnow().isoformat()
        }