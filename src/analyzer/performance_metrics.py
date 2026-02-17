from typing import Dict
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PerformanceMetricsCalculator:
    def __init__(self, repository):
        self.repo = repository

    def calculate(self, window_days: int = 90) -> Dict:
        history = self.repo.get_portfolio_history(days=window_days)
        if not history:
            return {'window_days': 0, 'annual_return': 0.0}

        start = history[0].get('total_value', 0) or 0
        end = history[-1].get('total_value', 0) or 0
        try:
            ret = (end / start - 1) * 100 if start else 0.0
        except Exception:
            ret = 0.0

        return {
            'window_days': window_days,
            'annual_return': float(ret),
            'calculated_at': datetime.utcnow().isoformat()
        }
