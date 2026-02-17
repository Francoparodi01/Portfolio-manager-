from typing import Dict
import logging

logger = logging.getLogger(__name__)


class ConcentrationMetricsCalculator:
    def __init__(self, repository):
        self.repo = repository

    def calculate_top_holdings(self, top_n: int = 5) -> Dict:
        positions = []
        try:
            positions = self.repo.get_current_positions() or []
        except Exception:
            positions = []

        if not positions:
            return {'top_holdings': []}

        sorted_pos = sorted(positions, key=lambda p: p.get('value', 0), reverse=True)[:top_n]
        return {'top_holdings': sorted_pos}
