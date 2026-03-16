"""src/collector/data/normalizer.py — Parsing y normalización de datos del DOM."""
from __future__ import annotations
import hashlib
import re
from decimal import Decimal, InvalidOperation
from typing import Optional


def normalize_ticker(raw: str) -> str:
    return re.sub(r"[^A-Z0-9\.]", "", raw.strip().upper())[:10]


def parse_decimal(s: str) -> Optional[Decimal]:
    if not s:
        return None
    s = s.strip().replace("\xa0", "").replace(" ", "")
    # Formato argentino: 1.234,56 → 1234.56
    if re.search(r"\d\.\d{3},\d", s):
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return Decimal(s) if s else None
    except InvalidOperation:
        return None


class DOMFingerprint:
    @staticmethod
    def compute(html: str) -> str:
        tags = re.findall(r"<[a-z][^>]*>", html.lower())
        return hashlib.md5(" ".join(tags[:200]).encode()).hexdigest()

    @staticmethod
    def raw_hash(html: str) -> str:
        return hashlib.sha256(html.encode()).hexdigest()[:16]

    @staticmethod
    def similarity(h1: str, h2: str) -> float:
        return 1.0 if h1 == h2 else 0.5  # simplified


class ConfidenceResult:
    def __init__(self, score: float, details: list):
        self.score = score
        self.details = details

    def is_acceptable(self, min_score: float) -> bool:
        return self.score >= min_score

    def summary(self) -> str:
        return ", ".join(f"{k}={'OK' if v else 'FAIL'}" for k, v, _ in self.details)

    @classmethod
    def compute(cls, checks: list[tuple[str, bool, float]]) -> "ConfidenceResult":
        total_weight = sum(w for _, _, w in checks)
        earned = sum(w for _, ok, w in checks if ok)
        score = earned / total_weight if total_weight else 0.0
        return cls(score=round(score, 4), details=checks)