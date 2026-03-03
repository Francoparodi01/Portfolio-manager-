"""
src/collector/data/normalizer.py
Utilidades de normalizacion, parsing y confidence scoring.
"""
from __future__ import annotations
import hashlib
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional


def parse_decimal(value) -> Optional[Decimal]:
    """Parsea strings monetarios argentinos a Decimal. Soporta: 1.234,56 / 1234.56 / $ 1.234,56"""
    if not value:
        return None
    raw = str(value).strip()
    raw = re.sub(r'[$\s%]', '', raw)
    if ',' in raw and '.' in raw:
        if raw.index('.') < raw.index(','):
            raw = raw.replace('.', '').replace(',', '.')
        else:
            raw = raw.replace(',', '')
    elif ',' in raw:
        raw = raw.replace(',', '.')
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def normalize_ticker(raw: str) -> str:
    """Normaliza ticker a mayusculas, sin espacios, max 10 chars."""
    if not raw:
        return ''
    t = raw.strip().upper()
    t = re.sub(r'[^A-Z0-9]', '', t)
    return t[:10]


class DOMFingerprint:
    """Detecta cambios estructurales en el DOM entre sesiones."""

    @staticmethod
    def _extract_structure(html: str) -> list:
        return re.findall(r'<[a-zA-Z][^>]*>', html)

    @classmethod
    def compute(cls, html: str) -> str:
        structure = ' '.join(cls._extract_structure(html))
        return hashlib.sha256(structure.encode()).hexdigest()[:16]

    @staticmethod
    def raw_hash(html: str) -> str:
        return hashlib.sha256(html.encode()).hexdigest()[:16]

    @classmethod
    def similarity(cls, hash_a: str, hash_b: str) -> float:
        if hash_a == hash_b:
            return 1.0
        if len(hash_a) != len(hash_b) or not hash_a:
            return 0.0
        matches = sum(a == b for a, b in zip(hash_a, hash_b))
        return matches / len(hash_a)


@dataclass
class ConfidenceResult:
    score: float
    checks: list = field(default_factory=list)

    @classmethod
    def compute(cls, checks) -> 'ConfidenceResult':
        """checks = [(name, passed, weight), ...]"""
        total_weight = sum(w for _, _, w in checks)
        passed_weight = sum(w for _, passed, w in checks if passed)
        score = passed_weight / total_weight if total_weight > 0 else 0.0
        return cls(score=round(score, 4), checks=checks)

    def is_acceptable(self, min_score: float) -> bool:
        return self.score >= min_score

    def summary(self) -> str:
        lines = []
        for name, passed, weight in self.checks:
            icon = 'ok' if passed else 'FAIL'
            lines.append(f'  {icon} {name} (weight={weight})')
        return '\n'.join(lines)
