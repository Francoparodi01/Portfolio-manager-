"""Symbol normalization for sentiment/news ingestion.

The trading engine keeps portfolio symbols (BYMA/CEDEAR-facing names) while
news providers often index the US underlying/ADR symbol.  This module keeps the
mapping explicit and overrideable so sentiment evidence remains auditable.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache

# Conservative defaults only where the provider symbol differs from the symbol
# Quantia commonly stores. Same-symbol CEDEARs do not need an entry.
DEFAULT_UNDERLYING_MAP: dict[str, str] = {
    "YPFD": "YPF",
    "PAMP": "PAM",
    "BRKB": "BRK-B",
    "BRK.B": "BRK-B",
}

# Company aliases are used only to attribute a news item to a ticker.  They do
# not create a trading signal.
COMPANY_ALIASES: dict[str, tuple[str, ...]] = {
    "AAPL": ("apple",),
    "AMD": ("advanced micro devices", "amd"),
    "AMZN": ("amazon",),
    "CVX": ("chevron",),
    "GGAL": ("grupo financiero galicia", "banco galicia", "galicia"),
    "GOOGL": ("alphabet", "google"),
    "MELI": ("mercadolibre", "mercado libre"),
    "META": ("meta platforms", "facebook", "instagram"),
    "MSFT": ("microsoft",),
    "MU": ("micron", "micron technology"),
    "NVDA": ("nvidia",),
    "PAMP": ("pampa energia", "pampa energía"),
    "QCOM": ("qualcomm",),
    "TSLA": ("tesla",),
    "TSM": ("taiwan semiconductor", "tsmc"),
    "VIST": ("vista energy",),
    "YPFD": ("ypf",),
}


def _clean(value: str | None) -> str:
    return str(value or "").upper().strip()


@lru_cache(maxsize=1)
def underlying_map() -> dict[str, str]:
    """Return the default mapping plus optional JSON env overrides."""
    mapping = dict(DEFAULT_UNDERLYING_MAP)
    raw = os.getenv("SENTIMENT_UNDERLYING_MAP_JSON", "").strip()
    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                for key, value in payload.items():
                    k = _clean(key)
                    v = _clean(value)
                    if k and v:
                        mapping[k] = v
        except Exception:
            # Configuration validation belongs in diagnostics; normalization
            # stays fail-safe and falls back to the checked-in mapping.
            pass
    return mapping


def news_symbol_for_portfolio_ticker(ticker: str) -> str:
    ticker = _clean(ticker)
    return underlying_map().get(ticker, ticker)


def portfolio_ticker_for_news_symbol(symbol: str) -> str:
    symbol = _clean(symbol)
    for portfolio_ticker, news_symbol in underlying_map().items():
        if _clean(news_symbol) == symbol:
            return portfolio_ticker
    return symbol


def expand_news_symbols(tickers: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    """Return provider symbols while preserving deterministic order/deduping."""
    result: list[str] = []
    for raw in tickers:
        symbol = news_symbol_for_portfolio_ticker(str(raw))
        if symbol and symbol not in result:
            result.append(symbol)
    return result


def infer_portfolio_ticker(text: str, ticker_hint: str | None = None) -> str | None:
    """Resolve a portfolio ticker from provider hint first, then text aliases."""
    hint = _clean(ticker_hint)
    if hint:
        return portfolio_ticker_for_news_symbol(hint)

    normalized = str(text or "").lower()
    for ticker, aliases in COMPANY_ALIASES.items():
        if any(alias.lower() in normalized for alias in aliases):
            return ticker
    return None
