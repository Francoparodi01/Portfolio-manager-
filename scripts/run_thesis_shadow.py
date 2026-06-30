"""Generate and evaluate independent shadow price theses.

This command writes only to shadow_thesis_* tables. It never creates execution
plans, portfolio weights, broker orders, or decision_log rows.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from uuid import uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.thesis_shadow import (
    MIN_INPUT_SESSIONS,
    ShadowContext,
    ShadowThesis,
    build_shadow_thesis,
    mature_forecast,
    partition_fresh_theses,
    render_shadow_report,
    render_shadow_ticker_telegram_report,
    render_shadow_telegram_report,
)
from src.analysis.macro import fetch_macro, get_macro_regime, score_macro_for_ticker
from src.analysis.signal_aggregator import load_sentiment_contexts
from src.analysis.thesis_shadow_store import ShadowThesisStore
from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)
HISTORY_LIMIT = 180
OUTCOME_HISTORY_LIMIT = 500


def _owner_chat_id(cfg, explicit: int | None) -> int:
    if explicit is not None:
        return int(explicit)
    # The regular analysis runner uses the latest portfolio when no owner is
    # explicitly requested. Keep the same legacy/system scope here instead of
    # assuming that TELEGRAM_CHAT_ID matches portfolio_snapshots.owner_chat_id.
    return 0


def _current_position_tickers(positions: list[dict]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for position in positions:
        ticker = str(position.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        explicit_quantity = next(
            (
                position.get(key)
                for key in ("quantity", "nominals", "qty")
                if position.get(key) is not None
            ),
            None,
        )
        if explicit_quantity is not None:
            try:
                if float(explicit_quantity) <= 0.0:
                    continue
            except (TypeError, ValueError):
                continue
        else:
            try:
                if float(position.get("market_value") or 0.0) <= 0.0:
                    continue
            except (TypeError, ValueError):
                continue
        result[ticker] = position
    return result


async def _build_theses(
    db: PortfolioDatabase,
    *,
    positions: dict[str, dict],
    cash_ars: float = 0.0,
    include_candidates: bool,
    ticker_override: list[str],
    max_assets: int,
) -> tuple[list[ShadowThesis], list[str]]:
    universe_assets = await db.get_cocos_universe_assets()
    assets_by_ticker = {
        str(asset.get("ticker") or "").upper(): asset
        for asset in universe_assets
        if asset.get("ticker")
    }
    skipped: list[str] = []
    if ticker_override:
        requested = sorted({ticker.upper() for ticker in ticker_override})
        selected = [ticker for ticker in requested if ticker in assets_by_ticker]
        stale_or_missing = sorted(set(requested) - set(selected))
        if stale_or_missing:
            logger.warning(
                "shadow freshness: omitidos sin precio fresco en market_prices: %s",
                stale_or_missing,
            )
            skipped.extend(stale_or_missing)
    else:
        selected = [ticker for ticker in sorted(positions) if ticker in assets_by_ticker]
        stale_positions = sorted(set(positions) - set(selected))
        if stale_positions:
            logger.warning(
                "shadow freshness: posiciones omitidas sin precio fresco en market_prices: %s",
                stale_positions,
            )
            skipped.extend(stale_positions)
        if include_candidates:
            selected.extend(
                ticker
                for ticker in sorted(assets_by_ticker)
                if ticker not in positions
            )
    selected = list(dict.fromkeys(selected))
    if max_assets > 0:
        selected = selected[:max_assets]

    invested_ars = sum(float(position.get("market_value", 0) or 0) for position in positions.values())
    total_ars = invested_ars + max(float(cash_ars or 0.0), 0.0)
    cash_pct = (max(float(cash_ars or 0.0), 0.0) / total_ars) if total_ars > 0 else None
    position_weights = {
        ticker: (float(position.get("market_value", 0) or 0) / total_ars)
        for ticker, position in positions.items()
        if total_ars > 0
    }
    max_position_weight = max(position_weights.values(), default=None)

    macro_snap = None
    macro_regime = {}
    try:
        macro_snap = fetch_macro()
        macro_regime = get_macro_regime(macro_snap)
    except Exception as exc:
        logger.warning("shadow context: macro no disponible; sigo price-only para macro: %s", exc)

    sentiment_contexts = {}
    try:
        pool = await db.get_pool()
        if pool:
            async with pool.acquire() as conn:
                sentiment_contexts = await load_sentiment_contexts(conn, selected)
    except Exception as exc:
        logger.warning("shadow context: sentiment no disponible; sigo sin overlay sentiment: %s", exc)

    theses: list[ShadowThesis] = []
    for ticker in selected:
        role = "POSITION" if ticker in positions else "CANDIDATE"
        metadata = positions.get(ticker) or assets_by_ticker.get(ticker) or {}
        macro_score = None
        macro_reasons: tuple[str, ...] = ()
        if macro_snap is not None:
            try:
                macro_score_raw, macro_reason_raw = score_macro_for_ticker(ticker, macro_snap)
                macro_score = float(macro_score_raw)
                macro_reasons = tuple(str(item) for item in macro_reason_raw[:3])
            except Exception as exc:
                logger.debug("shadow context macro omitido para %s: %s", ticker, exc)

        sentiment = sentiment_contexts.get(ticker)
        context = ShadowContext(
            macro_score=macro_score,
            macro_regime=str(macro_regime.get("market") or "") or None,
            macro_reasons=macro_reasons,
            sentiment_score=(
                float(getattr(sentiment, "score", 0.0))
                if sentiment is not None
                else None
            ),
            sentiment_confidence=(
                float(getattr(sentiment, "confidence", 0.0))
                if sentiment is not None
                else None
            ),
            sentiment_event_count=(
                int(getattr(sentiment, "event_count", 0))
                if sentiment is not None
                else 0
            ),
            sentiment_high_impact_count=(
                int(getattr(sentiment, "high_impact_count", 0))
                if sentiment is not None
                else 0
            ),
            sentiment_summary=(
                str(getattr(sentiment, "top_summary", "") or "")
                if sentiment is not None
                else ""
            ),
            cash_pct=cash_pct,
            current_weight=position_weights.get(ticker),
            max_position_weight=max_position_weight,
        )
        rows = await db.get_market_candles(
            ticker,
            asset_type=metadata.get("asset_type") or None,
            limit=HISTORY_LIMIT,
        )
        if len(rows) < MIN_INPUT_SESSIONS:
            skipped.append(ticker)
            continue
        try:
            theses.append(
                build_shadow_thesis(
                    ticker,
                    rows,
                    universe_role=role,
                    context=context,
                )
            )
        except ValueError as exc:
            logger.debug("shadow thesis omitida para %s: %s", ticker, exc)
            skipped.append(ticker)
    fresh, stale = partition_fresh_theses(theses)
    skipped.extend(item.ticker for item in stale)
    return fresh, sorted(set(skipped))


async def _mature_pending_outcomes(
    db: PortfolioDatabase,
    store: ShadowThesisStore,
    *,
    owner_chat_id: int,
) -> int:
    pending = await store.pending_outcomes(owner_chat_id=owner_chat_id)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in pending:
        grouped[str(row["ticker"]).upper()].append(row)

    filled = 0
    for ticker, forecasts in grouped.items():
        candles = await db.get_market_candles(ticker, limit=OUTCOME_HISTORY_LIMIT)
        for row in forecasts:
            outcome = mature_forecast(
                as_of_ts=row["as_of_ts"],
                reference_price=float(row["reference_price"]),
                horizon_sessions=int(row["horizon_sessions"]),
                expected_return=float(row["expected_return"]),
                future_candles=candles,
            )
            if outcome is None:
                continue
            filled += await store.save_outcome(
                forecast_id=int(row["id"]),
                outcome=outcome,
            )
    return filled


async def main(args: argparse.Namespace) -> int:
    cfg = get_config()
    owner_chat_id = _owner_chat_id(cfg, args.owner_chat_id)
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        if pool is None:
            raise RuntimeError("database pool unavailable")
        store = ShadowThesisStore(pool)
        if args.latest_report:
            theses = await store.latest_theses(owner_chat_id=owner_chat_id)
            metrics = await store.evaluation_metrics(owner_chat_id=owner_chat_id)
            if args.tickers:
                ticker = str(args.tickers[0]).upper()
                if args.telegram_format:
                    print(render_shadow_ticker_telegram_report(theses, ticker, metrics=metrics))
                else:
                    filtered = [item for item in theses if item.ticker.upper() == ticker]
                    print(render_shadow_report(filtered, metrics=metrics))
            elif args.telegram_format:
                print(render_shadow_telegram_report(theses, metrics=metrics))
            else:
                print(render_shadow_report(theses, metrics=metrics))
            return 0

        snapshot = await db.get_latest_snapshot(
            owner_chat_id=owner_chat_id if args.owner_chat_id is not None else None
        )
        positions = _current_position_tickers((snapshot or {}).get("positions", []))
        cash_ars = float((snapshot or {}).get("cash_ars", 0) or 0)

        matured = 0
        if not args.no_outcomes and not args.no_persist:
            matured = await _mature_pending_outcomes(
                db,
                store,
                owner_chat_id=owner_chat_id,
            )

        theses, skipped = await _build_theses(
            db,
            positions=positions,
            cash_ars=cash_ars,
            include_candidates=not args.positions_only,
            ticker_override=args.tickers,
            max_assets=args.max_assets,
        )
        inserted = 0
        stored_run_id = None
        if theses and not args.no_persist:
            stored_run_id, inserted = await store.save_theses(
                run_id=uuid4(),
                owner_chat_id=owner_chat_id,
                theses=theses,
            )
        metrics = (
            await store.evaluation_metrics(owner_chat_id=owner_chat_id)
            if not args.no_persist
            else []
        )

        if args.json:
            print(
                json.dumps(
                    {
                        "run_id": str(stored_run_id) if stored_run_id else None,
                        "inserted_forecasts": inserted,
                        "matured_outcomes": matured,
                        "skipped_tickers": skipped,
                        "theses": [item.to_dict() for item in theses],
                        "metrics": metrics,
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
        else:
            renderer = render_shadow_telegram_report if args.telegram_format else render_shadow_report
            print(renderer(theses, metrics=metrics))
            print(
                f"\nAudit: forecasts_inserted={inserted} "
                f"outcomes_matured={matured} skipped={len(skipped)}"
            )
        return 0
    finally:
        await db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-chat-id", type=int)
    parser.add_argument("--tickers", nargs="*", default=[])
    parser.add_argument("--positions-only", action="store_true")
    parser.add_argument(
        "--max-assets",
        type=int,
        default=0,
        help="Cap de diagnóstico; 0 evalúa todo el universo Cocos disponible.",
    )
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument("--no-outcomes", action="store_true")
    parser.add_argument(
        "--latest-report",
        action="store_true",
        help="Muestra la última corrida persistida sin recalcular forecasts.",
    )
    parser.add_argument(
        "--telegram-format",
        action="store_true",
        help="Renderiza un reporte HTML compacto para Telegram.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(_parse_args())))
