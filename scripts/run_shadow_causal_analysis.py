"""Audit shadow projections against fresh macro/geopolitical evidence.

The command reads shadow forecasts and sentiment evidence but writes only to
shadow_thesis_causal_analysis. It never calls the production scorer and never
touches decision_log, planner, optimizer, forecasts or outcomes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.macro import fetch_macro
from src.analysis.shadow_causal import (
    DEFAULT_MODEL,
    DEFAULT_OLLAMA_URL,
    DEFAULT_TIMEOUT_SECONDS,
    CausalAnalysisInput,
    ShadowProjection,
    analyze_with_ollama,
)
from src.analysis.shadow_causal_store import (
    ShadowCausalAnalysisStore,
    ticker_aliases,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)
DEFAULT_TICKERS = ("CVX", "AMD", "YPF")


def _owner_chat_id(explicit: int | None) -> int:
    return int(explicit) if explicit is not None else 0


def _load_inputs(path: str) -> list[CausalAnalysisInput]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("examples", []) if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        raise ValueError("input JSON must contain a non-empty list or an 'examples' list")
    return [CausalAnalysisInput.from_mapping(row) for row in rows]


async def _build_inputs_from_database(
    store: ShadowCausalAnalysisStore,
    *,
    owner_chat_id: int,
    tickers: Sequence[str],
    horizon_sessions: int,
) -> tuple[list[CausalAnalysisInput], list[str]]:
    projections = await store.latest_projections(
        owner_chat_id=owner_chat_id,
        tickers=tickers,
        horizon_sessions=horizon_sessions,
    )
    found_aliases = {
        alias
        for row in projections
        for alias in ticker_aliases(str(row.get("ticker") or ""))
    }
    missing = [
        ticker
        for ticker in tickers
        if not found_aliases.intersection(ticker_aliases(ticker))
    ]
    if not projections:
        return [], missing

    macro = await asyncio.to_thread(fetch_macro)
    macro_context = macro.to_dict()
    context_as_of = macro.fetched_at or datetime.now(timezone.utc)
    macro_news = await store.recent_macro_news(limit=8, lookback_days=7)

    inputs: list[CausalAnalysisInput] = []
    for row in projections:
        ticker = str(row["ticker"]).upper()
        ticker_news = await store.recent_ticker_news(
            ticker=ticker,
            limit=3,
            lookback_days=14,
        )
        inputs.append(
            CausalAnalysisInput(
                projection=ShadowProjection(
                    forecast_id=int(row["forecast_id"]),
                    ticker=ticker,
                    expected_return=float(row["expected_return"]),
                    probability_up=float(row["probability_up"]),
                    horizon_sessions=int(row["horizon_sessions"]),
                    as_of_ts=row["as_of_ts"],
                ),
                macro_context=macro_context,
                macro_news=macro_news,
                ticker_news=ticker_news,
                context_as_of=context_as_of,
            )
        )
    return inputs, missing


async def _analyze_inputs(
    inputs: Sequence[CausalAnalysisInput],
    *,
    store: ShadowCausalAnalysisStore | None,
    owner_chat_id: int,
    model: str,
    ollama_url: str,
    timeout_seconds: float,
    persist: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for item in inputs:
        ticker = item.projection.ticker
        try:
            analysis = await analyze_with_ollama(
                item,
                model=model,
                ollama_url=ollama_url,
                timeout_seconds=timeout_seconds,
            )
            row_id = None
            if persist:
                if store is None:
                    raise RuntimeError("persistence requested without a database store")
                row_id = await store.save_analysis(
                    owner_chat_id=owner_chat_id,
                    input_data=item,
                    analysis=analysis,
                )
            results.append(
                {
                    "id": row_id,
                    "projection": item.projection.to_dict(),
                    "analysis": analysis.to_dict(),
                }
            )
        except Exception as exc:
            logger.error("Causal audit failed for %s: %s", ticker, exc)
            errors.append({"ticker": ticker, "error": f"{type(exc).__name__}: {exc}"})
    return results, errors


async def main(args: argparse.Namespace) -> int:
    cfg = get_config()
    owner_chat_id = _owner_chat_id(args.owner_chat_id)
    db: PortfolioDatabase | None = None
    store: ShadowCausalAnalysisStore | None = None
    missing: list[str] = []

    try:
        if args.input_json and args.no_persist:
            inputs = _load_inputs(args.input_json)
        else:
            db = PortfolioDatabase(cfg.database.url)
            await db.connect()
            pool = await db.get_pool()
            if pool is None:
                raise RuntimeError("database pool unavailable")
            store = ShadowCausalAnalysisStore(pool)
            if args.input_json:
                inputs = _load_inputs(args.input_json)
            else:
                inputs, missing = await _build_inputs_from_database(
                    store,
                    owner_chat_id=owner_chat_id,
                    tickers=args.tickers,
                    horizon_sessions=args.horizon,
                )

        results, errors = await _analyze_inputs(
            inputs,
            store=store,
            owner_chat_id=owner_chat_id,
            model=args.model,
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout,
            persist=not args.no_persist,
        )
        output = {
            "audit_layer": "shadow_thesis_causal_analysis",
            "persisted": not args.no_persist,
            "model": args.model,
            "missing_projections": missing,
            "results": results,
            "errors": errors,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 0 if results and not errors and not missing else 1
    finally:
        if db is not None:
            await db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-chat-id", type=int)
    parser.add_argument("--tickers", nargs="+", default=list(DEFAULT_TICKERS))
    parser.add_argument("--horizon", type=int, default=20)
    parser.add_argument(
        "--input-json",
        help="Explicit input examples; with --no-persist this does not require PostgreSQL.",
    )
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(_parse_args())))
