"""Deterministic FinBERT scorer for persisted sentiment_raw items.

Sentiment is contextual evidence only. Scoring failures leave rows pending and
never block portfolio analysis/planner execution.

The previous Ollama/lexicon scorer is intentionally not used by this module.
Historical rows remain in Postgres for auditability, while aggregation selects
only the active FinBERT scorer/policy.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from .sentiment_symbols import infer_portfolio_ticker

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.getenv("SENTIMENT_FINBERT_MODEL", "ProsusAI/finbert")
DEFAULT_MODEL_REVISION = os.getenv(
    "SENTIMENT_FINBERT_REVISION",
    "db38d3727cbaed87c9aed72df7b3519e2ba5cca1",
)
DEFAULT_SCORER = "finbert"
DEFAULT_MAX_LENGTH = max(32, min(int(os.getenv("SENTIMENT_FINBERT_MAX_LENGTH", "384")), 512))
DEFAULT_TORCH_THREADS = max(1, int(os.getenv("SENTIMENT_FINBERT_TORCH_THREADS", "2")))

# Kept so older scheduler/CLI imports remain compatible during the cutover.
# Ollama is not used by sentiment scoring anymore.
DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
HEURISTIC_MODEL = "legacy-disabled"

HIGH_IMPACT_TERMS = {
    "earnings", "guidance", "profit warning", "bankruptcy", "default", "merger",
    "acquisition", "fed", "federal reserve", "rate cut", "rate hike", "inflation",
    "tariff", "sanction", "war", "ceasefire", "opec", "sec", "doj", "regulation",
    "resultados", "quiebra", "fusión", "adquisición", "inflación", "tasas",
    "regulación", "sanción",
}

EVENT_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("earnings", ("earnings", "revenue", "eps", "guidance", "quarter", "resultados", "trimestre")),
    ("regulation", ("regulation", "regulator", "sec ", "doj ", "lawsuit", "antitrust", "regulación", "juicio")),
    ("fx", ("dollar", "dólar", "ccl", "mep", "fx", "currency")),
    ("commodity", ("oil", "crude", "brent", "wti", "gold", "copper", "commodity", "petróleo")),
    ("macro", ("fed", "inflation", "rates", "recession", "gdp", "employment", "inflación", "tasas", "pbi")),
    ("company", ("company", "shares", "stock", "ceo", "deal", "partnership", "empresa", "acciones")),
)


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


class FinBertRuntime:
    """Lazy CPU runtime so importing Quantia does not download/load the model."""

    def __init__(self, model_name: str, revision: str):
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
        except Exception as exc:  # pragma: no cover - deployment diagnostic path
            raise RuntimeError(
                "FinBERT dependencies missing; install torch and transformers"
            ) from exc

        torch.set_num_threads(DEFAULT_TORCH_THREADS)
        self.torch = torch
        self.model_name = model_name
        self.revision = revision
        local_only = os.getenv("SENTIMENT_FINBERT_LOCAL_ONLY", "false").lower() in {
            "1", "true", "yes", "y"
        }
        kwargs = {
            "revision": revision,
            "local_files_only": local_only,
            "trust_remote_code": False,
        }
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, **kwargs)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name, **kwargs)
        self.model.eval()

    def classify(self, text: str) -> dict[str, float]:
        encoded = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=DEFAULT_MAX_LENGTH,
        )
        with self.torch.inference_mode():
            logits = self.model(**encoded).logits[0]
            probabilities = self.torch.softmax(logits, dim=-1).detach().cpu().tolist()

        labels: dict[str, float] = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
        id2label = getattr(self.model.config, "id2label", {}) or {}
        recognized = 0
        for idx, probability in enumerate(probabilities):
            label = str(id2label.get(idx, idx)).lower().strip()
            if label in labels:
                labels[label] = float(probability)
                recognized += 1

        # ProsusAI/finbert uses positive/negative/neutral in that order. This
        # fallback protects against config serialization that exposes LABEL_N.
        if recognized == 0 and len(probabilities) == 3:
            labels = {
                "positive": float(probabilities[0]),
                "negative": float(probabilities[1]),
                "neutral": float(probabilities[2]),
            }
        return labels


@lru_cache(maxsize=4)
def _runtime(model_name: str = DEFAULT_MODEL, revision: str = DEFAULT_MODEL_REVISION) -> FinBertRuntime:
    return FinBertRuntime(model_name, revision)


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _raw_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("raw_payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return payload if isinstance(payload, dict) else {}


def _event_type(text: str) -> str:
    lower = text.lower()
    for event_type, needles in EVENT_RULES:
        if any(needle in lower for needle in needles):
            return event_type
    return "unknown"


def _impact(text: str, confidence: float, score: float) -> str:
    lower = text.lower()
    if any(term in lower for term in HIGH_IMPACT_TERMS):
        return "high"
    if confidence >= 0.75 and abs(score) >= 0.35:
        return "mid"
    return "low"


def _horizon(event_type: str, impact: str) -> str:
    if event_type in {"earnings", "regulation"}:
        return "20d"
    if event_type in {"macro", "commodity", "fx"}:
        return "5d" if impact == "high" else "2d"
    if event_type == "company":
        return "10d"
    return "5d"


def _build_text(row: dict[str, Any]) -> str:
    headline = _clean_text(row.get("headline"))
    snippet = _clean_text(row.get("body_snippet"))
    return f"{headline}. {snippet}".strip(" .")


def score_with_finbert_sync(
    row: dict[str, Any],
    *,
    model: str = DEFAULT_MODEL,
    revision: str = DEFAULT_MODEL_REVISION,
) -> SentimentScore:
    """Classify one raw news row with FinBERT and preserve full probabilities."""
    text = _build_text(row)
    if not text:
        raise ValueError("empty sentiment text")

    probabilities = _runtime(model, revision).classify(text)
    positive = float(probabilities.get("positive", 0.0))
    negative = float(probabilities.get("negative", 0.0))
    neutral = float(probabilities.get("neutral", 0.0))
    probability_sum = positive + negative + neutral
    if probability_sum <= 0:
        raise RuntimeError("FinBERT returned no recognized class probabilities")
    positive /= probability_sum
    negative /= probability_sum
    neutral /= probability_sum

    score = max(-1.0, min(1.0, positive - negative))
    confidence = max(positive, negative, neutral)
    label = max(
        {"positive": positive, "negative": negative, "neutral": neutral},
        key={"positive": positive, "negative": negative, "neutral": neutral}.get,
    )

    raw_payload = _raw_payload(row)
    ticker_hint = raw_payload.get("ticker_hint")
    ticker = infer_portfolio_ticker(text, ticker_hint=ticker_hint)
    event_type = _event_type(text)
    scope = "ticker" if ticker else "macro" if event_type in {"macro", "fx", "commodity"} else "unknown"
    impact = _impact(text, confidence, score)
    summary = _clean_text(row.get("headline"))[:160] or text[:160]

    response = {
        "method": "finbert_local",
        "model": model,
        "revision": revision,
        "label": label,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "score_formula": "positive_probability-negative_probability",
        "ticker_hint": ticker_hint,
        "resolved_ticker": ticker,
        "max_length": DEFAULT_MAX_LENGTH,
    }
    return SentimentScore(
        raw_id=int(row["id"]),
        ticker=ticker,
        asset_scope=scope,
        score=score,
        impact=impact,
        confidence=confidence,
        horizon=_horizon(event_type, impact),
        event_type=event_type,
        summary=summary,
        raw_response=response,
    )


async def score_with_finbert(
    row: dict[str, Any],
    *,
    model: str = DEFAULT_MODEL,
    revision: str = DEFAULT_MODEL_REVISION,
) -> SentimentScore:
    return await asyncio.to_thread(
        score_with_finbert_sync,
        row,
        model=model,
        revision=revision,
    )


async def load_pending_raw_items(conn, *, limit: int = 25, max_attempts: int = 3) -> list[dict]:
    rows = await conn.fetch(
        """
        SELECT id, fetched_at, source, url, headline, body_snippet, published_at, raw_payload
        FROM sentiment_raw
        WHERE score_status = 'PENDING_SCORE'
          AND score_attempts < $1
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT $2
        """,
        int(max_attempts),
        max(0, int(limit)),
    )
    return [dict(row) for row in rows]


async def mark_score_attempt(conn, raw_id: int, *, error: str | None = None) -> None:
    await conn.execute(
        """
        UPDATE sentiment_raw
        SET score_attempts = score_attempts + 1,
            last_score_attempt_at = NOW(),
            score_status = 'PENDING_SCORE'
        WHERE id = $1
        """,
        int(raw_id),
    )
    if error:
        logger.warning("FinBERT raw_id=%s left pending: %s", raw_id, error[:300])


async def save_sentiment_score(
    conn,
    item: SentimentScore,
    *,
    model: str = DEFAULT_MODEL,
    scorer: str = DEFAULT_SCORER,
) -> int | None:
    revision = str(item.raw_response.get("revision") or DEFAULT_MODEL_REVISION)
    model_version = f"{model}@{revision}" if scorer == DEFAULT_SCORER else model
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
        model_version,
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
    ollama_url: str = DEFAULT_OLLAMA_URL,  # compatibility; intentionally ignored
    timeout_seconds: float = 5.0,  # compatibility; intentionally ignored
    max_attempts: int = 3,
    revision: str = DEFAULT_MODEL_REVISION,
) -> dict[str, int | str]:
    del ollama_url, timeout_seconds
    pending = await load_pending_raw_items(conn, limit=limit, max_attempts=max_attempts)
    stats: dict[str, int | str] = {
        "pending": len(pending),
        "scored": 0,
        "failed": 0,
        "backend": DEFAULT_SCORER,
    }
    for row in pending:
        raw_id = int(row["id"])
        try:
            scored = await score_with_finbert(row, model=model, revision=revision)
            await save_sentiment_score(conn, scored, model=model, scorer=DEFAULT_SCORER)
            stats["scored"] = int(stats["scored"]) + 1
        except Exception as exc:
            await mark_score_attempt(conn, raw_id, error=str(exc))
            stats["failed"] = int(stats["failed"]) + 1
            # Missing runtime/model is systemic; avoid hammering every row.
            if isinstance(exc, (RuntimeError, OSError, ImportError)):
                break
    return stats


async def rescore_recent_items(
    conn,
    *,
    window_hours: int = 72,
    limit: int = 500,
    model: str = DEFAULT_MODEL,
    revision: str = DEFAULT_MODEL_REVISION,
) -> dict[str, int | str]:
    """Bootstrap/cutover: score recent raw evidence with the active FinBERT model."""
    rows = await conn.fetch(
        """
        SELECT id, fetched_at, source, url, headline, body_snippet, published_at, raw_payload
        FROM sentiment_raw
        WHERE COALESCE(published_at, fetched_at) <= NOW()
          AND COALESCE(published_at, fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT $2
        """,
        max(1, int(window_hours)),
        max(1, int(limit)),
    )
    stats: dict[str, int | str] = {
        "candidates": len(rows),
        "rescored": 0,
        "failed": 0,
        "backend": DEFAULT_SCORER,
    }
    for row in rows:
        try:
            scored = await score_with_finbert(dict(row), model=model, revision=revision)
            await save_sentiment_score(conn, scored, model=model, scorer=DEFAULT_SCORER)
            stats["rescored"] = int(stats["rescored"]) + 1
        except Exception as exc:
            stats["failed"] = int(stats["failed"]) + 1
            logger.warning("FinBERT rescore failed raw_id=%s: %s", row["id"], str(exc)[:300])
            if isinstance(exc, (RuntimeError, OSError, ImportError)):
                break
    return stats


async def rescore_recent_heuristic_items(
    conn,
    *,
    window_hours: int = 24,
    limit: int = 80,
) -> dict[str, int | str]:
    """Deprecated compatibility hook used by run_market_context.

    Historical heuristic rows are deliberately not re-scored on every report.
    Production cutover uses the explicit rescore_recent_items/--rescore-hours path.
    """
    del conn, window_hours, limit
    return {
        "candidates": 0,
        "rescored": 0,
        "failed": 0,
        "backend": DEFAULT_SCORER,
        "compatibility": "noop",
    }
