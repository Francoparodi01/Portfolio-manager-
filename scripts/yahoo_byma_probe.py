"""Read-only Yahoo Finance coverage probe for BYMA symbols.

This script is intentionally isolated from the production ingestion path. It
does not import the scheduler/scraper and does not write to the database. Input
is a JSON inventory exported from the live DB; output is CSV/JSON/Markdown under
the selected report directory.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_SEARCH = "https://query2.finance.yahoo.com/v1/finance/search"
USER_AGENT = "Mozilla/5.0 (compatible; cocos-copilot-yahoo-byma-probe/1.0)"
DEFAULT_START_DATE = "2000-01-01"


def _load_universe(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    by_ticker: dict[str, dict[str, Any]] = {}

    def _item(ticker: str) -> dict[str, Any]:
        clean = str(ticker or "").upper().strip()
        return by_ticker.setdefault(
            clean,
            {
                "ticker": clean,
                "asset_types": set(),
                "sources": set(),
                "db_last_price": None,
                "db_last_ts": None,
                "db_price_rows": 0,
                "candle_rows": 0,
                "first_candle_date": None,
                "last_candle_date": None,
                "decision_rows": 0,
                "ever_position": False,
            },
        )

    for row in raw.get("market_prices", []):
        item = _item(row.get("ticker"))
        item["sources"].add("market_prices")
        item["asset_types"].add(str(row.get("asset_type") or "UNKNOWN").upper())
        item["db_last_price"] = _to_float(row.get("last_price"))
        item["db_last_ts"] = str(row.get("ts") or "")
        item["db_price_rows"] = max(int(row.get("price_rows") or 0), int(item["db_price_rows"] or 0))

    for row in raw.get("market_candles", []):
        item = _item(row.get("ticker"))
        item["sources"].add("market_candles")
        item["asset_types"].add(str(row.get("asset_type") or "UNKNOWN").upper())
        item["candle_rows"] = max(int(row.get("candle_rows") or 0), int(item["candle_rows"] or 0))
        item["first_candle_date"] = row.get("first_candle_date") or item.get("first_candle_date")
        item["last_candle_date"] = row.get("last_candle_date") or item.get("last_candle_date")

    for row in raw.get("positions", []):
        item = _item(row.get("ticker"))
        item["sources"].add("positions")
        item["ever_position"] = True

    for row in raw.get("decision_log", []):
        item = _item(row.get("ticker"))
        item["sources"].add("decision_log")
        item["decision_rows"] = int(row.get("decision_rows") or 0)

    result = []
    for item in by_ticker.values():
        if not item["ticker"]:
            continue
        item["asset_type"] = "/".join(sorted(item.pop("asset_types"))) or "UNKNOWN"
        item["sources"] = ",".join(sorted(item["sources"]))
        result.append(item)
    return sorted(result, key=lambda value: value["ticker"])


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
        if math.isfinite(parsed):
            return parsed
    except Exception:
        return None
    return None


def _epoch(date_text: str) -> int:
    return int(dt.datetime.fromisoformat(date_text).replace(tzinfo=dt.UTC).timestamp())


def _candidate_symbols(ticker: str) -> list[str]:
    base = str(ticker or "").upper().strip()
    candidates = [f"{base}.BA"]
    if "." in base:
        candidates.extend(
            [
                f"{base.replace('.', '-')}.BA",
                f"{base.replace('.', '')}.BA",
            ]
        )
    return list(dict.fromkeys(candidates))


def _fetch_json(url: str, *, timeout: int) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def _chart(symbol: str, *, period1: int, period2: int, timeout: int) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
    )
    url = YAHOO_CHART.format(symbol=urllib.parse.quote(symbol, safe="")) + "?" + query
    data = _fetch_json(url, timeout=timeout)
    chart = data.get("chart") or {}
    result = chart.get("result") or []
    if not result:
        err = chart.get("error") or {}
        raise RuntimeError(err.get("description") or err.get("code") or "empty chart result")
    return result[0]


def _fallback_period1_values(period1: int, period2: int) -> list[int]:
    """Yahoo sometimes returns fewer rows for very long BYMA requests.

    Keep the explicit long request first, then retry shorter explicit windows
    and keep the richest result.
    """
    end = dt.datetime.fromtimestamp(period2, tz=dt.UTC)
    candidates = [
        period1,
        int((end - dt.timedelta(days=365 * 20 + 10)).timestamp()),
        int((end - dt.timedelta(days=365 * 10 + 10)).timestamp()),
        int((end - dt.timedelta(days=365 * 5 + 10)).timestamp()),
    ]
    return list(dict.fromkeys(max(0, value) for value in candidates))


def _chart_summary_with_fallback(
    symbol: str,
    *,
    period1: int,
    period2: int,
    timeout: int,
) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    errors: list[str] = []
    for candidate_period1 in _fallback_period1_values(period1, period2):
        try:
            summary = _summarise_chart(
                symbol,
                _chart(symbol, period1=candidate_period1, period2=period2, timeout=timeout),
            )
            summaries.append(summary)
            if int(summary.get("non_null_close_rows") or 0) >= 80:
                break
        except Exception as exc:
            errors.append(f"{dt.datetime.fromtimestamp(candidate_period1, tz=dt.UTC).date()}: {exc}")
    if summaries:
        best = max(summaries, key=lambda item: int(item.get("non_null_close_rows") or 0))
        if errors:
            best["fallback_errors"] = " || ".join(errors[-3:])
        return best
    raise RuntimeError("fallback chart failed: " + " || ".join(errors[-3:]))


def _search_symbols(ticker: str, *, timeout: int) -> list[str]:
    query = urllib.parse.urlencode({"q": f"{ticker}.BA", "quotesCount": 10, "newsCount": 0})
    try:
        data = _fetch_json(YAHOO_SEARCH + "?" + query, timeout=timeout)
    except Exception:
        return []
    symbols = []
    for quote in data.get("quotes") or []:
        symbol = str(quote.get("symbol") or "").upper()
        exchange = str(quote.get("exchange") or quote.get("exchDisp") or "").upper()
        quote_type = str(quote.get("quoteType") or "").upper()
        if symbol.endswith(".BA") and ("BUE" in exchange or "BUENOS" in exchange or quote_type):
            symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def _summarise_chart(symbol: str, result: dict[str, Any]) -> dict[str, Any]:
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote = (indicators.get("quote") or [{}])[0]
    adjclose = (indicators.get("adjclose") or [{}])[0].get("adjclose") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []

    valid_close = [(ts, close) for ts, close in zip(timestamps, closes) if close is not None]
    valid_volume = [v for v in volumes if v not in (None, 0)]
    null_close_count = max(len(timestamps) - len(valid_close), 0)
    first_date = dt.datetime.fromtimestamp(valid_close[0][0], tz=dt.UTC).date().isoformat() if valid_close else ""
    last_date = dt.datetime.fromtimestamp(valid_close[-1][0], tz=dt.UTC).date().isoformat() if valid_close else ""
    latest_close = float(valid_close[-1][1]) if valid_close else None

    return {
        "yahoo_symbol": symbol,
        "found": True,
        "exchange": meta.get("exchangeName") or meta.get("fullExchangeName") or "",
        "currency": meta.get("currency") or "",
        "instrument_type": meta.get("instrumentType") or "",
        "timezone": meta.get("timezone") or "",
        "first_date": first_date,
        "last_date": last_date,
        "rows": len(timestamps),
        "non_null_close_rows": len(valid_close),
        "null_close_rows": null_close_count,
        "null_close_pct": (null_close_count / len(timestamps)) if timestamps else None,
        "non_zero_volume_rows": len(valid_volume),
        "latest_close": latest_close,
        "regular_market_price": _to_float(meta.get("regularMarketPrice")),
        "has_ohlc": bool(closes and highs and lows),
        "has_adjclose": bool(adjclose),
    }


def probe_one(asset: dict[str, Any], *, period1: int, period2: int, timeout: int) -> dict[str, Any]:
    ticker = asset["ticker"]
    attempted: list[str] = []
    errors: list[str] = []
    candidates = _candidate_symbols(ticker)

    for symbol in candidates:
        attempted.append(symbol)
        try:
            summary = _chart_summary_with_fallback(symbol, period1=period1, period2=period2, timeout=timeout)
            return _merge(asset, summary, attempted, errors)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            errors.append(f"{symbol}: {type(exc).__name__}: {exc}")

    for symbol in _search_symbols(ticker, timeout=timeout):
        if symbol in attempted:
            continue
        attempted.append(symbol)
        try:
            summary = _chart_summary_with_fallback(symbol, period1=period1, period2=period2, timeout=timeout)
            return _merge(asset, summary, attempted, errors)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            errors.append(f"{symbol}: {type(exc).__name__}: {exc}")

    return _merge(
        asset,
        {
            "yahoo_symbol": "",
            "found": False,
            "exchange": "",
            "currency": "",
            "instrument_type": "",
            "timezone": "",
            "first_date": "",
            "last_date": "",
            "rows": 0,
            "non_null_close_rows": 0,
            "null_close_rows": 0,
            "null_close_pct": None,
            "non_zero_volume_rows": 0,
            "latest_close": None,
            "regular_market_price": None,
            "has_ohlc": False,
            "has_adjclose": False,
        },
        attempted,
        errors,
    )


def _merge(asset: dict[str, Any], summary: dict[str, Any], attempted: list[str], errors: list[str]) -> dict[str, Any]:
    result = dict(asset)
    result.update(summary)
    latest = result.get("latest_close") or result.get("regular_market_price")
    db_price = result.get("db_last_price")
    if latest and db_price:
        result["price_diff_pct_vs_db"] = latest / db_price - 1.0
    else:
        result["price_diff_pct_vs_db"] = None
    result["attempted_symbols"] = "|".join(attempted)
    result["errors"] = " || ".join(errors[-3:])
    result["quality_bucket"] = _quality_bucket(result)
    return result


def _quality_bucket(row: dict[str, Any]) -> str:
    if not row.get("found"):
        return "NO_YAHOO"
    if row.get("exchange") not in {"BUE", "Buenos Aires"}:
        return "NON_BUE"
    if row.get("currency") != "ARS":
        return "NON_ARS"
    if int(row.get("non_null_close_rows") or 0) < 80:
        return "TOO_SHORT"
    if row.get("null_close_pct") is not None and float(row["null_close_pct"]) > 0.02:
        return "HAS_GAPS"
    if row.get("last_date"):
        last = dt.date.fromisoformat(row["last_date"])
        if (dt.date.today() - last).days > 7:
            return "STALE"
    return "OK"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "ticker",
        "asset_type",
        "yahoo_symbol",
        "quality_bucket",
        "found",
        "exchange",
        "currency",
        "instrument_type",
        "first_date",
        "last_date",
        "rows",
        "non_null_close_rows",
        "null_close_pct",
        "non_zero_volume_rows",
        "db_last_price",
        "latest_close",
        "regular_market_price",
        "price_diff_pct_vs_db",
        "candle_rows",
        "first_candle_date",
        "last_candle_date",
        "db_price_rows",
        "decision_rows",
        "ever_position",
        "sources",
        "attempted_symbols",
        "errors",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _pct(value: Any) -> str:
    if value is None:
        return "n/d"
    try:
        return f"{float(value):.1%}"
    except Exception:
        return "n/d"


def _coverage_markdown(rows: list[dict[str, Any]], *, started_at: str, input_path: Path) -> str:
    total = len(rows)
    by_quality = Counter(row["quality_bucket"] for row in rows)
    by_asset_quality: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        by_asset_quality[row["asset_type"]][row["quality_bucket"]] += 1

    ok_rows = [row for row in rows if row["quality_bucket"] == "OK"]
    non_null_counts = [int(row.get("non_null_close_rows") or 0) for row in ok_rows]
    median_rows = int(statistics.median(non_null_counts)) if non_null_counts else 0
    ge_5y = sum(1 for row in ok_rows if int(row.get("non_null_close_rows") or 0) >= 1000)
    ge_1y = sum(1 for row in ok_rows if int(row.get("non_null_close_rows") or 0) >= 240)
    diffs = [
        abs(float(row["price_diff_pct_vs_db"]))
        for row in rows
        if row.get("price_diff_pct_vs_db") is not None and row.get("quality_bucket") == "OK"
    ]
    median_diff = statistics.median(diffs) if diffs else None
    diff_gt_5 = sum(1 for value in diffs if value > 0.05)

    lines = [
        "# Yahoo BYMA probe",
        "",
        f"- Started at UTC: `{started_at}`",
        f"- Input universe: `{input_path}`",
        f"- Total tickers probed: **{total}**",
        f"- Yahoo OK: **{by_quality.get('OK', 0)}** ({_pct(by_quality.get('OK', 0) / total if total else None)})",
        f"- >= 1y daily rows among OK: **{ge_1y}/{len(ok_rows)}**",
        f"- >= ~5y daily rows among OK: **{ge_5y}/{len(ok_rows)}**",
        f"- Median non-null daily rows among OK: **{median_rows}**",
        f"- Median abs latest price diff vs DB among OK: **{_pct(median_diff)}**",
        f"- Abs latest price diff > 5% among OK: **{diff_gt_5}**",
        "",
        "## Quality buckets",
        "",
        "| bucket | count | pct |",
        "|---|---:|---:|",
    ]
    for bucket, count in by_quality.most_common():
        lines.append(f"| {bucket} | {count} | {_pct(count / total if total else None)} |")

    lines.extend(["", "## By asset type", "", "| asset_type | OK | NO_YAHOO | other issues | total |", "|---|---:|---:|---:|---:|"])
    for asset_type, counts in sorted(by_asset_quality.items()):
        subtotal = sum(counts.values())
        other = subtotal - counts.get("OK", 0) - counts.get("NO_YAHOO", 0)
        lines.append(f"| {asset_type} | {counts.get('OK', 0)} | {counts.get('NO_YAHOO', 0)} | {other} | {subtotal} |")

    problem_rows = [row for row in rows if row["quality_bucket"] != "OK"]
    lines.extend(["", "## Non-OK tickers", "", "| ticker | type | bucket | attempted | error |", "|---|---|---|---|---|"])
    for row in problem_rows[:120]:
        err = str(row.get("errors") or "").replace("|", "/")[:180]
        attempted = str(row.get("attempted_symbols") or "").replace("|", ", ")
        lines.append(f"| {row['ticker']} | {row['asset_type']} | {row['quality_bucket']} | {attempted} | {err} |")
    if len(problem_rows) > 120:
        lines.append(f"| ... | ... | ... | ... | {len(problem_rows) - 120} more |")

    large_diff = [
        row for row in rows
        if row.get("price_diff_pct_vs_db") is not None and abs(float(row["price_diff_pct_vs_db"])) > 0.05
    ]
    large_diff.sort(key=lambda row: abs(float(row["price_diff_pct_vs_db"])), reverse=True)
    lines.extend(["", "## Latest price diff > 5% vs DB", "", "| ticker | type | yahoo | db | diff | last_date |", "|---|---|---:|---:|---:|---|"])
    for row in large_diff[:80]:
        lines.append(
            f"| {row['ticker']} | {row['asset_type']} | {row.get('latest_close') or row.get('regular_market_price')} | "
            f"{row.get('db_last_price')} | {_pct(row.get('price_diff_pct_vs_db'))} | {row.get('last_date')} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation guardrails",
            "",
            "- This is a read-only probe. It does not prove Yahoo is production-safe by itself.",
            "- `period1/period2` explicit daily requests are used; `range=max` was avoided because it returned fewer rows for some BYMA symbols.",
            "- Price diffs compare Yahoo latest close/regular price vs the latest DB market price. Differences can reflect Cocos intraday timestamps, stale DB rows, splits, market holidays, or Yahoo adjustments.",
            "- Bonds are only included if they exist in the exported project universe. This project universe currently comes from Cocos ACCIONES/CEDEARS market tables.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("reports/yahoo_byma_probe"))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    assets = _load_universe(args.input)
    if args.limit > 0:
        assets = assets[: args.limit]
    args.output_dir.mkdir(parents=True, exist_ok=True)

    period1 = _epoch(args.start_date)
    period2 = int(time.time())
    started_at = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                probe_one,
                asset,
                period1=period1,
                period2=period2,
                timeout=args.timeout,
            ): asset
            for asset in assets
        }
        for index, future in enumerate(as_completed(futures), start=1):
            row = future.result()
            rows.append(row)
            print(
                f"{index}/{len(futures)} {row['ticker']} {row['asset_type']} "
                f"{row['quality_bucket']} {row.get('yahoo_symbol') or '-'} rows={row.get('non_null_close_rows')}"
            )

    rows.sort(key=lambda row: (row["asset_type"], row["ticker"]))
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = args.output_dir / f"yahoo_byma_probe_{stamp}.csv"
    json_path = args.output_dir / f"yahoo_byma_probe_{stamp}.json"
    md_path = args.output_dir / f"yahoo_byma_probe_{stamp}.md"
    latest_md = args.output_dir / "latest.md"

    _write_csv(csv_path, rows)
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    markdown = _coverage_markdown(rows, started_at=started_at, input_path=args.input)
    md_path.write_text(markdown, encoding="utf-8")
    latest_md.write_text(markdown, encoding="utf-8")

    print(f"WROTE {csv_path}")
    print(f"WROTE {json_path}")
    print(f"WROTE {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
