from typing import Dict
import logging

logger = logging.getLogger(__name__)


class HealthScoreCalculator:
    def __init__(self, repository):
        self.repo = repository

    def calculate(self) -> Dict:
        try:
            positions = self.repo.get_current_positions() or []
        except Exception:
            positions = []

        if not positions:
            return {'score_total': 0.0, 'clasificacion': 'desconocido'}

        # Heurística simple: mientras más posiciones, mejor diversificado
        score = max(0.0, min(100.0, 60.0 + len(positions) * 2))
        clas = 'bueno' if score >= 75 else 'regular' if score >= 50 else 'malo'
        return {'score_total': float(score), 'clasificacion': clas}
