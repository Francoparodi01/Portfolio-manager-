"""Shadow-only macro exposure profiles.

These profiles are audit metadata. score_macro_for_ticker() remains the live source
until prospective evidence supports promotion.
"""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class MacroExposureProfile:
    name: str
    factors: tuple[str, ...]
    mode: str = "SHADOW_ONLY"

PROFILES = {
    "gold_miners": MacroExposureProfile("gold_miners", ("gold", "dxy", "tnx", "vix")),
    "semiconductors": MacroExposureProfile("semiconductors", ("sp500", "tnx", "vix", "semiconductor_cycle")),
    "oil": MacroExposureProfile("oil", ("wti", "brent")),
    "argentina_energy": MacroExposureProfile("argentina_energy", ("wti", "merval", "ccl", "riesgo_pais")),
}
TICKER_PROFILE = {
    "GDX": "gold_miners",
    "NVDA": "semiconductors", "AMD": "semiconductors", "MU": "semiconductors",
    "CVX": "oil", "XOM": "oil",
    "YPFD": "argentina_energy",
}

def shadow_macro_profile_for_ticker(ticker: str) -> MacroExposureProfile | None:
    name = TICKER_PROFILE.get(str(ticker or "").upper())
    return PROFILES.get(name) if name else None
