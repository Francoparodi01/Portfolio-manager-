"""
src/analysis/sentiment.py

Capa de sentimiento: noticias via RSS (sin API key).

Fuentes:
  - Yahoo Finance RSS por ticker
  - Reuters RSS (business/markets)
  - Seeking Alpha RSS (si disponible)

Scoring:
  Lexicon financiero propio + ponderacion por fuente y recencia.
  Score -1.0 (muy negativo) a +1.0 (muy positivo).
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

# ── Lexicon financiero ──────────────────────────────────────────────────────
POSITIVE_WORDS = {
    # Resultados
    "beat": 2.0, "beats": 2.0, "exceed": 1.5, "exceeds": 1.5, "surpass": 1.5,
    "record": 1.5, "growth": 1.2, "profit": 1.2, "revenue": 0.8, "upgrade": 2.0,
    "buy": 1.5, "outperform": 1.8, "bullish": 1.8, "rally": 1.5, "surge": 1.5,
    "strong": 1.2, "positive": 1.0, "recovery": 1.3, "rebound": 1.3,
    "dividend": 1.0, "buyback": 1.2, "acquisition": 0.8, "partnership": 0.8,
    "innovation": 0.8, "demand": 1.0, "expansion": 1.0, "milestone": 1.0,
    "confidence": 1.0, "optimistic": 1.2, "opportunity": 0.8, "gain": 1.2,
    # Oil-specific
    "supply cut": 1.5, "opec": 0.5, "geopolitical": 0.8, "tension": 0.5,
    "disruption": 1.0,
}

NEGATIVE_WORDS = {
    # Resultados
    "miss": -2.0, "misses": -2.0, "disappoint": -1.5, "disappoints": -1.5,
    "loss": -1.5, "losses": -1.5, "decline": -1.2, "downgrade": -2.0,
    "sell": -1.5, "underperform": -1.8, "bearish": -1.8, "crash": -2.0,
    "plunge": -1.8, "slump": -1.5, "weak": -1.2, "negative": -1.0,
    "recession": -2.0, "inflation": -1.0, "debt": -0.8, "lawsuit": -1.5,
    "investigation": -1.5, "fine": -1.0, "penalty": -1.2, "recall": -1.5,
    "layoff": -1.2, "layoffs": -1.2, "cut": -1.0, "concern": -0.8,
    "risk": -0.5, "uncertainty": -0.8, "volatile": -0.5, "drop": -1.2,
    "fall": -0.8, "fell": -0.8, "tumble": -1.5, "fear": -1.0, "warning": -1.2,
    # Macro
    "rate hike": -1.5, "tariff": -1.2, "sanction": -1.0, "ban": -1.2,
}

RSS_FEEDS = {
    # Yahoo Finance por ticker (dinamico, no necesita API)
    "yahoo_ticker": "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
    # Reuters markets
    "reuters_markets": "https://feeds.reuters.com/reuters/businessNews",
    # MarketWatch
    "marketwatch": "https://feeds.marketwatch.com/marketwatch/topstories/",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; portfolio-analyzer/1.0)"
}


@dataclass
class NewsItem:
    title: str
    summary: str
    published: str
    source: str
    ticker_relevance: float = 0.0
    sentiment_score: float  = 0.0


@dataclass
class SentimentResult:
    ticker: str
    score: float              = 0.0
    news_count: int           = 0
    positive_count: int       = 0
    negative_count: int       = 0
    top_headlines: list       = field(default_factory=list)
    fetched_at: datetime      = field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str]      = None


def _score_text(text: str) -> float:
    """Aplica el lexicon al texto y retorna score raw."""
    text_lower = text.lower()
    score = 0.0
    word_count = 0

    # Frases primero (mas especificas)
    for phrase, val in {**POSITIVE_WORDS, **NEGATIVE_WORDS}.items():
        if " " in phrase and phrase in text_lower:
            score += val
            word_count += 1

    # Palabras individuales
    words = re.findall(r"\b\w+\b", text_lower)
    for word in words:
        if word in POSITIVE_WORDS:
            score += POSITIVE_WORDS[word]
            word_count += 1
        elif word in NEGATIVE_WORDS:
            score += NEGATIVE_WORDS[word]
            word_count += 1

    # Normalizar por densidad de palabras relevantes
    if word_count > 0:
        total_words = max(len(words), 1)
        density = word_count / total_words
        score = score * min(density * 10, 1.0)

    return float(score)


def _parse_rss(url: str, timeout: int = 8) -> list[dict]:
    """Descarga y parsea un feed RSS. Retorna lista de items."""
    items = []
    try:
        req  = Request(url, headers=HEADERS)
        resp = urlopen(req, timeout=timeout)
        xml  = resp.read()
        root = ET.fromstring(xml)

        # Buscar items en namespace estandar o Atom
        for item in root.iter("item"):
            title   = (item.findtext("title") or "").strip()
            summary = (item.findtext("description") or
                       item.findtext("summary") or "").strip()
            pubdate = (item.findtext("pubDate") or
                       item.findtext("published") or "").strip()
            if title:
                items.append({"title": title, "summary": summary, "pubdate": pubdate})

    except URLError as e:
        logger.debug(f"RSS fetch error {url[:60]}: {e}")
    except ET.ParseError as e:
        logger.debug(f"RSS parse error {url[:60]}: {e}")
    except Exception as e:
        logger.debug(f"RSS error {url[:60]}: {e}")

    return items


def _ticker_relevance(text: str, ticker: str) -> float:
    """Que tan relevante es un texto para un ticker (0 a 1)."""
    text_lower = text.lower()
    ticker_lower = ticker.lower()

    # Mapeo de nombre completo del ticker
    company_names = {
        "cvx":  ["chevron", "cvx"],
        "nvda": ["nvidia", "nvda", "gpu", "ai chip"],
        "mu":   ["micron", " mu "],
        "meli": ["mercadolibre", "mercado libre", "meli"],
        "ggal": ["galicia", "ggal"],
        "ypfd": ["ypf", "petroleo"],
        "xom":  ["exxon", "xom"],
    }

    names = company_names.get(ticker_lower, [ticker_lower])

    for name in names:
        if name in text_lower:
            # Mencion directa = alta relevancia
            if ticker_lower == name:
                return 1.0
            return 0.8

    # Relevancia por sector
    sector_keywords = {
        "cvx":  ["oil", "energy", "crude", "opec", "barrel", "petroleum", "middle east", "iran"],
        "xom":  ["oil", "energy", "crude", "opec"],
        "nvda": ["semiconductor", "ai", "chip", "data center", "gpu", "nvidia"],
        "mu":   ["memory", "dram", "nand", "semiconductor", "micron"],
        "meli": ["ecommerce", "latin america", "brazil", "argentina", "fintech", "payments"],
    }

    keywords = sector_keywords.get(ticker_lower, [])
    for kw in keywords:
        if kw in text_lower:
            return 0.4

    return 0.0


def fetch_sentiment(ticker: str, max_articles: int = 30) -> SentimentResult:
    """
    Descarga noticias de RSS y calcula sentiment score para un ticker.

    Gratis, sin API key. Usa Yahoo Finance RSS + Reuters.
    """
    result = SentimentResult(ticker=ticker, score=0.0)
    all_items = []

    # 1. Yahoo Finance RSS especifico del ticker
    url_yahoo = RSS_FEEDS["yahoo_ticker"].format(ticker=ticker)
    items = _parse_rss(url_yahoo)
    for item in items:
        item["source"] = "yahoo"
        item["weight"] = 1.0   # alta relevancia (es el ticker exacto)
    all_items.extend(items[:15])

    # 2. Reuters markets (contexto general)
    items_reuters = _parse_rss(RSS_FEEDS["reuters_markets"])
    for item in items_reuters:
        item["source"] = "reuters"
        item["weight"] = 0.5
    all_items.extend(items_reuters[:20])

    if not all_items:
        result.error = "No se pudieron descargar noticias"
        logger.warning(f"Sentiment {ticker}: sin noticias disponibles")
        return result

    # Puntuar cada item
    scores = []
    headlines = []

    for item in all_items[:max_articles]:
        title   = item.get("title", "")
        summary = item.get("summary", "")
        source  = item.get("source", "unknown")
        weight  = item.get("weight", 0.5)
        text    = f"{title} {summary}"

        relevance = _ticker_relevance(text, ticker)
        if relevance < 0.1 and source != "yahoo":
            continue

        raw_score = _score_text(text)
        weighted  = raw_score * relevance * weight

        if abs(raw_score) > 0.1:
            scores.append(weighted)
            result.news_count += 1

            if raw_score > 0.2:
                result.positive_count += 1
            elif raw_score < -0.2:
                result.negative_count += 1

            if len(headlines) < 5 and relevance > 0.3:
                headlines.append({
                    "title":     title[:100],
                    "score":     round(raw_score, 2),
                    "source":    source,
                    "relevance": round(relevance, 2),
                })

    result.top_headlines = sorted(headlines, key=lambda x: abs(x["score"]), reverse=True)

    if scores:
        raw = sum(scores) / len(scores)
        # Normalizar a -1 / +1 con tanh (suaviza extremos)
        import math
        result.score = round(math.tanh(raw / 2), 4)
    else:
        result.score = 0.0

    logger.info(
        f"Sentiment {ticker}: score={result.score:.2f} "
        f"({result.positive_count}+ / {result.negative_count}-) "
        f"{result.news_count} articulos"
    )
    return result