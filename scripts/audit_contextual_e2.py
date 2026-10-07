"""Read-only E2 audit for one recorded decision and its stored market series."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import urlsplit

from src.analysis.contextual_market import build_contextual_snapshot, render_contextual_diagnostic
from src.collector.cocos_history import candles_to_frame


async def audit(
    database_url: str,
    *,
    decision_id: int,
    benchmark_ticker: str = "SPY",
    asset_long_ticker: str | None = None,
    benchmark_long_ticker: str | None = None,
    diagnostic_path: Path | None = None,
) -> dict:
    import asyncpg

    parsed = urlsplit(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("read-only contextual audit requires a loopback database")
    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with conn.transaction(readonly=True):
            decision = await conn.fetchrow(
                """SELECT d.id,d.owner_chat_id,d.run_id,d.decided_at,d.ticker,
                          d.decision,d.final_score,d.confidence,
                          i.execution_plan_id AS plan_id
                   FROM decision_log d
                   LEFT JOIN order_intents i ON i.decision_log_id=d.id
                   WHERE d.id=$1""",
                decision_id,
            )
            if not decision:
                raise ValueError(f"decision {decision_id} not found")
            columns = {
                row["column_name"] for row in await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='market_candles'"
                )
            }

            async def choose_symbol(ticker: str, requested: str | None) -> str | None:
                if requested:
                    return requested
                return await conn.fetchval(
                    """SELECT long_ticker FROM market_candles
                       WHERE ticker=$1 AND interval='1d' AND ts <= $2 AND scraped_at <= $2
                       GROUP BY long_ticker ORDER BY count(*) DESC,long_ticker LIMIT 1""",
                    ticker, decision["decided_at"],
                )

            async def load(ticker: str, long_ticker: str | None):
                if not long_ticker:
                    return []
                optional = {
                    "bar_start": "bar_start" if "bar_start" in columns else "NULL::timestamptz AS bar_start",
                    "bar_end": "bar_end" if "bar_end" in columns else "NULL::timestamptz AS bar_end",
                    "available_at": "available_at" if "available_at" in columns else "NULL::timestamptz AS available_at",
                    "is_closed": "is_closed" if "is_closed" in columns else "NULL::boolean AS is_closed",
                    "volume_unit": "volume_unit" if "volume_unit" in columns else "NULL::text AS volume_unit",
                    "calendar": "calendar" if "calendar" in columns else "NULL::text AS calendar",
                    "calendar_validation": (
                        "calendar_validation" if "calendar_validation" in columns
                        else "NULL::text AS calendar_validation"
                    ),
                    "adjustment_policy": (
                        "adjustment_policy" if "adjustment_policy" in columns
                        else "NULL::text AS adjustment_policy"
                    ),
                    "depositary_ratio": (
                        "depositary_ratio" if "depositary_ratio" in columns
                        else "NULL::text AS depositary_ratio"
                    ),
                }
                rows = await conn.fetch(
                    f"""SELECT ts,ticker,long_ticker,asset_type,currency,venue,interval,
                               open_price,high_price,low_price,close_price,volume,source,scraped_at,
                               {','.join(optional.values())}
                        FROM market_candles
                        WHERE ticker=$1 AND long_ticker=$2 AND interval='1d'
                          AND ts <= $3 AND scraped_at <= $3
                        ORDER BY ts""",
                    ticker, long_ticker, decision["decided_at"],
                )
                return [dict(row) for row in rows]

            asset_symbol = await choose_symbol(decision["ticker"], asset_long_ticker)
            benchmark_symbol = await choose_symbol(benchmark_ticker, benchmark_long_ticker)
            asset_rows = await load(decision["ticker"], asset_symbol)
            benchmark_rows = await load(benchmark_ticker, benchmark_symbol)

        asset_frame = candles_to_frame(asset_rows)
        benchmark_frame = candles_to_frame(benchmark_rows) if benchmark_rows else None
        benchmarks = {benchmark_ticker: benchmark_frame} if benchmark_frame is not None else {}
        snapshot = build_contextual_snapshot(
            asset_frame,
            cutoff=decision["decided_at"],
            signal_action=decision["decision"],
            benchmarks=benchmarks,
            general_benchmark=benchmark_ticker,
        )
        diagnostic = {"status": "NOT_GENERATED", "reason": "NO_PIT_ELIGIBLE_BARS"}
        if diagnostic_path and snapshot.timestamps["last_asset_bar"]:
            render_contextual_diagnostic(asset_frame, snapshot, diagnostic_path)
            diagnostic = {"status": "GENERATED", "path": str(diagnostic_path)}
        required_columns = {
            "bar_start", "bar_end", "available_at", "is_closed", "volume_unit",
            "calendar", "calendar_validation", "adjustment_policy", "depositary_ratio",
        }
        return {
            "audit_mode": "READ_ONLY",
            "decision": {
                "id": decision["id"], "owner": decision["owner_chat_id"],
                "run_id": str(decision["run_id"]) if decision["run_id"] else None,
                "plan_id": str(decision["plan_id"]) if decision["plan_id"] else None,
                "decided_at": decision["decided_at"].isoformat(),
                "ticker": decision["ticker"], "signal": decision["decision"],
                "score": decision["final_score"], "conviction": decision["confidence"],
            },
            "series": {
                "asset_provider_symbol": asset_symbol,
                "benchmark_ticker": benchmark_ticker,
                "benchmark_provider_symbol": benchmark_symbol,
                "asset_rows_known_at_cutoff": len(asset_rows),
                "benchmark_rows_known_at_cutoff": len(benchmark_rows),
            },
            "database_capabilities": {
                "market_candle_columns": sorted(columns),
                "missing_context_columns": sorted(required_columns - columns),
            },
            "snapshot": snapshot.to_dict(),
            "diagnostic": diagnostic,
            "gaps": sorted({
                *(f"MISSING_MARKET_CANDLE_COLUMN:{name}" for name in required_columns - columns),
                *snapshot.missingness,
                *([] if snapshot.timestamps["last_asset_bar"] else ["NO_PIT_ELIGIBLE_ASSET_BAR"]),
            }),
        }
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--decision-id", required=True, type=int)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--asset-long-ticker")
    parser.add_argument("--benchmark-long-ticker")
    parser.add_argument("--diagnostic", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(audit(
        args.database_url,
        decision_id=args.decision_id,
        benchmark_ticker=args.benchmark,
        asset_long_ticker=args.asset_long_ticker,
        benchmark_long_ticker=args.benchmark_long_ticker,
        diagnostic_path=args.diagnostic,
    ))
    payload = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
