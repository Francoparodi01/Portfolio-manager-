"""LLM scorer for persisted sentiment_raw items.

The scorer is intentionally non-blocking for trading: failures keep rows in
PENDING_SCORE and do not affect analysis/planner execution.
"""
from __future__ import annotations

import json
import logging
import os
import re
from html import unescape
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.getenv("SENTIMENT_OLLAMA_MODEL", "qwen2.5:3b")
DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
HEURISTIC_MODEL = "heuristic-v2"
HEURISTIC_FALLBACK_ENABLED = os.getenv("SENTIMENT_HEURISTIC_FALLBACK", "true").lower() in {
    "1",
    "true",
    "yes",
    "y",
}

VALID_IMPACT = {"low", "mid", "high"}
VALID_SCOPE = {"ticker", "sector", "macro", "unknown"}
VALID_HORIZON = {"intraday", "2d", "5d", "10d", "20d", "unknown"}

KNOWN_TICKERS = {
    "AAPL", "AMD", "AMZN", "ASTS", "BABA", "CVX", "GGAL", "GOOGL", "HMY",
    "MELI", "META", "MSFT", "MU", "NVDA", "PAMP", "QCOM", "RGTI", "SPCE",
    "TSLA", "TSM", "VIST", "YPFD",
}

POSITIVE_TERMS = {
    "acuerdo", "alza", "aumenta", "aumentó", "beneficio", "crece", "creció",
    "ganancia", "ganancias", "mejora", "mejoró", "positivo", "rebote",
    "record", "récord", "suba", "sube", "supera", "superó",
}

NEGATIVE_TERMS = {
    "baja", "bajó", "cae", "cayó", "caída", "cepo", "conflicto", "deuda",
    "default", "demanda", "desploma", "inflación", "juicio", "negativo",
    "pierde", "pérdida", "riesgo", "sanción", "tensión", "volatilidad",
}

HIGH_IMPACT_TERMS = {
    "bcra", "cepo", "ccl", "dólar", "dolar", "inflación", "fed", "fmi",
    "mep", "tasas", "guerra", "regulación", "regulacion",
}


POSITIVE_TERMS.update({
    "agreement", "beat", "beats", "ceasefire", "deal", "eases", "gain",
    "gains", "jump", "jumps", "peace", "rally", "record high", "rebound",
    "rises", "rose", "settlement", "soar", "soars", "surge", "surges",
    "surging",
})

NEGATIVE_TERMS.update({
    "attack", "blockade", "bomb", "conflict", "crisis", "escalation",
    "falls", "fear", "fears", "inflation", "missile", "plunge", "risk",
    "sanction", "sanctions", "selloff", "strike", "tension", "war",
})

HIGH_IMPACT_TERMS.update({
    "brent", "crude", "dow", "federal reserve", "hormuz", "iran", "nasdaq",
    "oil", "s&p", "sp500", "trump", "wall street", "wti",
})

POSITIVE_TERMS.update({
    "avanza", "avanzan", "recupera", "recuperan", "subas",
})

POSITIVE_PHRASES = {
    "acuerdo de paz": 3,
    "bonos soberanos anotan subas": 3,
    "bonos suben": 2,
    "dolar baja": 2,
    "dolar cae": 2,
    "end war": 2,
    "framework to end war": 3,
    "flirts with a $900 billion valuation": 2,
    "inflacion baja": 2,
    "market rally": 2,
    "markets rally": 2,
    "markets soar": 3,
    "oil falls as": 1,
    "oil falls as us and iran announce": 3,
    "oil prices hit three-month low and markets rally": 3,
    "oil tumbles on us-iran deal": 3,
    "peace deal": 3,
    "petroleo retrocede": 1,
    "riesgo pais baja": 3,
    "riesgo pais cae": 3,
    "stock is surging": 3,
    "stock jumps": 2,
    "stock markets soar": 3,
    "stocks rose": 2,
    "wall street avanza": 2,
}

NEGATIVE_PHRASES = {
    "conflicto escala": -3,
    "dolar salta": -2,
    "dolar sube": -2,
    "inflacion acelera": -2,
    "mercado cae": -2,
    "mercados caen": -2,
    "petroleo sube por guerra": -2,
    "riesgo pais aumenta": -3,
    "riesgo pais sube": -3,
    "wall street cae": -2,
}


@dataclass(frozen=True)
class SentimentScore:
    raw_id: int
    ticker: str | None
    asset_scope: str
    score: float
    impact: str
    confidence: float
    horizon: str
    event_type: str
    summary: str
    raw_response: dict[str, Any]


def _clamp(value: Any, lo: float, hi: float, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return default
    return max(lo, min(hi, parsed))


def _clean_token(value: Any, *, upper: bool = False, fallback: str = "") -> str:
    text = str(value or "").strip()
    if text.lower() in {"null", "none", "nan"}:
        text = ""
    text = re.sub(r"[^A-Za-z0-9_./ -]", "", text)[:80]
    if upper:
        text = text.upper()
    return text or fallback


def _extract_json(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("empty LLM response")
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("no JSON object in LLM response")
    parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise ValueError("JSON response is not an object")
    return parsed


def _normalize_score(raw_id: int, payload: dict[str, Any]) -> SentimentScore:
    ticker = _clean_token(payload.get("ticker"), upper=True)
    if ticker in {"", "NULL"}:
        ticker = None
    asset_scope = _clean_token(payload.get("asset_scope"), fallback="unknown").lower()
    if asset_scope not in VALID_SCOPE:
        asset_scope = "unknown"
    impact = _clean_token(payload.get("impact"), fallback="low").lower()
    if impact not in VALID_IMPACT:
        impact = "low"
    horizon = _clean_token(payload.get("horizon"), fallback="unknown").lower()
    if horizon not in VALID_HORIZON:
        horizon = "unknown"
    event_type = _clean_token(payload.get("event_type"), fallback="unknown").lower()
    summary = str(payload.get("summary") or "").strip()
    summary = re.sub(r"\s+", " ", summary)[:160]

    return SentimentScore(
        raw_id=int(raw_id),
        ticker=ticker,
        asset_scope=asset_scope,
        score=_clamp(payload.get("score"), -1.0, 1.0),
        impact=impact,
        confidence=_clamp(payload.get("confidence"), 0.0, 1.0),
        horizon=horizon,
        event_type=event_type,
        summary=summary,
        raw_response=payload,
    )


def build_prompt(row: dict[str, Any]) -> str:
    return (
        "Analiza esta noticia financiera para Argentina/CEDEARs.\n"
        "Devuelve SOLO JSON valido, sin markdown ni explicaciones.\n"
        "Campos obligatorios:\n"
        "{"
        '"ticker":"YPFD|GGAL|MELI|TSM|QCOM|MACRO|null",'
        '"asset_scope":"ticker|sector|macro|unknown",'
        '"score":float(-1 to 1),'
        '"impact":"low|mid|high",'
        '"confidence":float(0 to 1),'
        '"horizon":"intraday|2d|5d|10d|20d|unknown",'
        '"event_type":"earnings|regulation|macro|fx|company|commodity|rumor|unknown",'
        '"summary":"max 15 words"'
        "}\n\n"
        f"Fuente: {row.get('source')}\n"
        f"Fecha: {row.get('published_at') or row.get('fetched_at')}\n"
        f"Titulo: {row.get('headline')}\n"
        f"Texto: {row.get('body_snippet') or ''}"
    )


def score_with_heuristic(row: dict[str, Any]) -> SentimentScore:
    """Low-confidence deterministic fallback when the local LLM is unavailable."""
    raw_text = " ".join(
        str(row.get(key) or "") for key in ("headline", "body_snippet", "source")
    )
    text = unescape(raw_text)
    text = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    normalized = re.sub(r"\s+", " ", text).strip()
    lower = normalized.lower()
    lower_ascii = (
        lower
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    upper_tokens = set(re.findall(r"\b[A-Z]{2,5}\b", normalized.upper()))
    ticker = next((tk for tk in sorted(KNOWN_TICKERS) if tk in upper_tokens), None)

    positive = sum(1 for term in POSITIVE_TERMS if term in lower)
    negative = sum(1 for term in NEGATIVE_TERMS if term in lower)
    phrase_bias = sum(weight for phrase, weight in POSITIVE_PHRASES.items() if phrase in lower_ascii)
    phrase_bias += sum(weight for phrase, weight in NEGATIVE_PHRASES.items() if phrase in lower_ascii)
    raw_score = positive - negative + phrase_bias
    if raw_score == 0:
        score = 0.0
    else:
        score = max(-0.35, min(0.35, raw_score / 5.0))

    high_impact_hits = [term for term in HIGH_IMPACT_TERMS if term in lower]
    asset_scope = "ticker" if ticker else ("macro" if high_impact_hits else "unknown")
    impact = "high" if high_impact_hits else ("mid" if abs(score) >= 0.2 else "low")
    confidence = 0.35 if score else 0.18
    if ticker:
        confidence += 0.15
    if high_impact_hits:
        confidence += 0.10
    confidence = min(confidence, 0.65)

    summary = normalized[:120] or "Sin resumen"
    payload = {
        "ticker": ticker,
        "asset_scope": asset_scope,
        "score": score,
        "impact": impact,
        "confidence": confidence,
        "horizon": "5d" if asset_scope == "ticker" else "2d",
        "event_type": "macro" if asset_scope == "macro" else "unknown",
        "summary": summary,
        "method": "heuristic_fallback",
        "model": HEURISTIC_MODEL,
        "positive_hits": positive,
        "negative_hits": negative,
        "high_impact_hits": high_impact_hits,
    }
    return _normalize_score(int(row["id"]), payload)


def _is_scorer_unavailable(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.PoolTimeout,
            httpx.HTTPStatusError,
        ),
    )


async def score_with_ollama(
    row: dict[str, Any],
    *,
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout_seconds: float = 5.0,
) -> SentimentScore:
    prompt = build_prompt(row)
    url = ollama_url.rstrip("/")
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        resp = await client.post(
            f"{url}/api/chat",
            json={
                "model": model,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.0, "num_predict": 220},
                "messages": [
                    {
                        "role": "system",
                        "content": "Eres un clasificador financiero. Respondes solo JSON valido.",
                    },
                    {"role": "user", "content": prompt},
                ],
            },
        )
        resp.raise_for_status()
        data = resp.json()
    content = data.get("message", {}).get("content", "")
    payload = _extract_json(content)
    return _normalize_score(int(row["id"]), payload)


async def load_pending_raw_items(conn, *, limit: int = 25, max_attempts: int = 3) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT id, fetched_at, source, url, headline, body_snippet, published_at
        FROM sentiment_raw
        WHERE score_status = 'PENDING_SCORE'
          AND score_attempts < $1
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT $2
        """,
        int(max_attempts),
        int(limit),
    )
    return [dict(row) for row in rows]


async def mark_score_attempt(conn, raw_id: int, *, error: str | None = None) -> None:
    await conn.execute(
        """
        UPDATE sentiment_raw
        SET score_attempts = score_attempts + 1,
            last_score_attempt_at = NOW(),
            score_status = CASE
                WHEN score_attempts + 1 >= 3 THEN 'PENDING_SCORE'
                ELSE score_status
            END
        WHERE id = $1
        """,
        int(raw_id),
    )
    if error:
        logger.debug("sentiment raw_id=%s pending after scorer error: %s", raw_id, error)


async def save_sentiment_score(
    conn,
    item: SentimentScore,
    *,
    model: str = DEFAULT_MODEL,
    scorer: str = "ollama",
) -> int | None:
    row = await conn.fetchrow(
        """
        INSERT INTO sentiment_scored (
            raw_id, scorer, model, ticker, asset_scope, score, impact, confidence,
            horizon, event_type, summary, raw_response, status
        )
        VALUES (
            $1, $12, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11::jsonb, 'SCORED'
        )
        ON CONFLICT (raw_id, scorer, model) DO UPDATE SET
            scored_at = NOW(),
            ticker = EXCLUDED.ticker,
            asset_scope = EXCLUDED.asset_scope,
            score = EXCLUDED.score,
            impact = EXCLUDED.impact,
            confidence = EXCLUDED.confidence,
            horizon = EXCLUDED.horizon,
            event_type = EXCLUDED.event_type,
            summary = EXCLUDED.summary,
            raw_response = EXCLUDED.raw_response,
            status = 'SCORED',
            error = NULL
        RETURNING id
        """,
        item.raw_id,
        model,
        item.ticker,
        item.asset_scope,
        item.score,
        item.impact,
        item.confidence,
        item.horizon,
        item.event_type,
        item.summary,
        json.dumps(item.raw_response),
        scorer,
    )
    await conn.execute(
        """
        UPDATE sentiment_raw
        SET score_status = 'SCORED',
            last_score_attempt_at = NOW()
        WHERE id = $1
        """,
        item.raw_id,
    )
    return int(row["id"]) if row else None


async def score_pending_items(
    conn,
    *,
    limit: int = 25,
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout_seconds: float = 5.0,
    max_attempts: int = 3,
) -> dict[str, int]:
    pending = await load_pending_raw_items(conn, limit=limit, max_attempts=max_attempts)
    stats = {"pending": len(pending), "scored": 0, "failed": 0}
    use_heuristic_only = False
    for row in pending:
        raw_id = int(row["id"])
        try:
            if use_heuristic_only:
                scored = score_with_heuristic(row)
                await save_sentiment_score(conn, scored, model=HEURISTIC_MODEL, scorer="heuristic")
            else:
                scored = await score_with_ollama(
                    row,
                    model=model,
                    ollama_url=ollama_url,
                    timeout_seconds=timeout_seconds,
                )
                await save_sentiment_score(conn, scored, model=model)
            stats["scored"] += 1
        except Exception as exc:
            if HEURISTIC_FALLBACK_ENABLED:
                if _is_scorer_unavailable(exc) or isinstance(exc, ValueError):
                    use_heuristic_only = True
                fallback = score_with_heuristic(row)
                await save_sentiment_score(conn, fallback, model=HEURISTIC_MODEL, scorer="heuristic")
                stats["scored"] += 1
                logger.debug("sentiment raw_id=%s scored by heuristic fallback: %s", raw_id, str(exc)[:220])
                continue
            await mark_score_attempt(conn, raw_id, error=str(exc)[:300])
            stats["failed"] += 1
            if _is_scorer_unavailable(exc):
                remaining = max(0, len(pending) - stats["scored"] - stats["failed"])
                logger.warning(
                    "sentiment scorer unavailable; leaving %s pending items for next cycle",
                    remaining,
                )
                break
    return stats


async def rescore_recent_heuristic_items(
    conn,
    *,
    window_hours: int = 24,
    limit: int = 80,
) -> dict[str, int]:
    """Re-apply deterministic heuristic to recent heuristic rows.

    This is used after heuristic rule changes so current context reports do not
    keep stale polarity. It only updates sentiment_scored; it does not touch
    decision_log, outcomes or trading thresholds.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (sr.id)
            sr.id, sr.fetched_at, sr.source, sr.url, sr.headline,
            sr.body_snippet, sr.published_at,
            ss.model
        FROM sentiment_raw sr
        JOIN sentiment_scored ss ON ss.raw_id = sr.id
        WHERE ss.scorer = 'heuristic'
          AND COALESCE(sr.published_at, sr.fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')
        ORDER BY sr.id, ss.scored_at DESC
        LIMIT $2
        """,
        int(window_hours),
        int(limit),
    )
    stats = {"candidates": len(rows), "rescored": 0}
    for row in rows:
        payload = dict(row)
        scored = score_with_heuristic(payload)
        await save_sentiment_score(
            conn,
            scored,
            model=HEURISTIC_MODEL,
            scorer="heuristic",
        )
        stats["rescored"] += 1
    return stats
