"""Read-only, local dashboard of Quantia formal bot plans.

Run: DATABASE_URL=... OWNER_CHAT_ID=... python server.py
No writes to PostgreSQL. Outcomes are recomputed from market_candles, not decision_log.
"""
from __future__ import annotations

import asyncio
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import asyncpg

ART = ZoneInfo("America/Argentina/Buenos_Aires")
HORIZONS = (5, 10, 20, 40)
HERE = Path(__file__).resolve().parent


def as_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def compute(rows, candles, *, cost_bps=75.0):
    """One plan intent per ART date/ticker/side; returns by later BYMA sessions.

    Entry is next daily candle's open. H-day exit is the close of the Hth
    session including entry. SELL measures avoidance versus holding to exit.
    These are hypothetical directional returns, not an executable portfolio.
    """
    quality = Counter()
    usable = defaultdict(dict)
    conflicted = set()
    for row in candles:
        ticker = str(row["ticker"]).upper()
        # The project's candle joins use the UTC date of daily candle timestamps.
        d = row["ts"].astimezone(timezone.utc).date()
        opening, closing = as_float(row["open_price"]), as_float(row["close_price"])
        if not opening or not closing or opening <= 0 or closing <= 0:
            quality["invalid_candles"] += 1
            usable[ticker][d] = None
            continue
        if d in usable[ticker]:
            prior = usable[ticker][d]
            if prior != (opening, closing):
                usable[ticker][d] = None
                conflicted.add((ticker, d))
        else:
            usable[ticker][d] = (opening, closing)
    series = {}
    quality["conflicting_candle_days"] = len(conflicted)
    for ticker, days in usable.items():
        dates = sorted(days)
        series[ticker] = (dates, [days[d] for d in dates])

    seen = set()
    selected = []
    for row in sorted(rows, key=lambda r: (r["created_at"], r["intent_id"])):
        quality["raw_intents"] += 1
        if not row["feasible"] or not row["is_executable"] or row["was_blocked"]:
            quality["ineligible_intents"] += 1
            continue
        side = str(row["side"]).upper()
        if side not in ("BUY", "SELL"):
            quality["ineligible_intents"] += 1
            continue
        day = row["created_at"].astimezone(ART).date()
        key = (day, str(row["ticker"]).upper(), side)
        if key in seen:
            quality["same_day_duplicates"] += 1
            continue
        seen.add(key)
        selected.append(row)
    quality["unique_signals"] = len(selected)

    evaluated = []
    for row in selected:
        ticker, side = str(row["ticker"]).upper(), str(row["side"]).upper()
        dates, prices = series.get(ticker, ([], []))
        decision_date = row["created_at"].astimezone(ART).date()
        idx = bisect_right(dates, decision_date)
        item = {"date": decision_date.isoformat(), "ticker": ticker, "side": side,
                "run_id": str(row["run_id"]) if row["run_id"] else None,
                "returns": {}}
        if idx >= len(dates):
            quality["no_next_session"] += 1
        elif prices[idx] is None:
            quality["ambiguous_entry_price"] += 1
        else:
            opening = prices[idx][0]
            for horizon in HORIZONS:
                end = idx + horizon - 1
                if end >= len(dates) or prices[end] is None:
                    item["returns"][str(horizon)] = None
                    continue
                gross = (prices[end][1] / opening - 1) * (1 if side == "BUY" else -1)
                item["returns"][str(horizon)] = gross - cost_bps / 10000
        evaluated.append(item)

    metrics = {}
    for horizon in HORIZONS:
        values = [s["returns"].get(str(horizon)) for s in evaluated]
        vals = [v for v in values if v is not None]
        metrics[str(horizon)] = {
            "n": len(vals), "pending": len(values) - len(vals),
            "mean_pct": round(100 * sum(vals) / len(vals), 3) if vals else None,
            "median_pct": round(100 * (sorted(vals)[(len(vals)-1)//2] + sorted(vals)[len(vals)//2]) / 2, 3) if vals else None,
            "win_pct": round(100 * sum(v > 0 for v in vals) / len(vals), 1) if vals else None,
        }
    weekly = defaultdict(lambda: {"n": 0, "sum": 0.0})
    for item in evaluated:
        value = item["returns"].get("5")
        if value is None:
            continue
        day = datetime.fromisoformat(item["date"]).date()
        week = (day - timedelta(days=day.weekday())).isoformat()
        weekly[week]["n"] += 1
        weekly[week]["sum"] += value
    trends = [{"week": week, "n": v["n"], "mean_pct": round(100*v["sum"]/v["n"], 3)}
              for week, v in sorted(weekly.items())]
    return {"metrics": metrics, "quality": dict(quality), "weekly_5d": trends,
            "signals": evaluated[-100:][::-1]}


async def load(owner_id, days):
    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn, timeout=10,
        server_settings={"default_transaction_read_only": "on", "statement_timeout": "15000"})
    try:
        async with conn.transaction(readonly=True):
            rows = await conn.fetch("""
                SELECT p.id AS plan_id, p.run_id, p.created_at, p.feasible,
                       i.id AS intent_id, i.ticker, i.side, i.is_executable, i.was_blocked
                FROM execution_plans p JOIN order_intents i ON i.execution_plan_id = p.id
                WHERE p.owner_chat_id = $1 AND p.created_at >= NOW() - $2::integer * INTERVAL '1 day'
                ORDER BY p.created_at DESC, i.id DESC
                LIMIT 25001
            """, owner_id, days)
            tickers = sorted({str(r["ticker"]).upper() for r in rows})
            candles = await conn.fetch("""
                SELECT ticker, ts, open_price, close_price
                FROM market_candles
                WHERE ticker = ANY($1::text[]) AND interval = '1d'
                  AND currency = 'ARS' AND venue = 'BYMA'
                  AND ts >= NOW() - ($2::integer + 120) * INTERVAL '1 day'
                ORDER BY ticker, ts
                LIMIT 250001
            """, tickers, days) if tickers else []
            db_time = await conn.fetchval("SELECT NOW()")
        return [dict(r) for r in rows], [dict(r) for r in candles], db_time
    finally:
        await conn.close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        uri = urlsplit(self.path)
        if uri.path in ("/", "/index.html"):
            body = (HERE / "index.html").read_bytes()
            kind = "text/html; charset=utf-8"
        elif uri.path == "/api/stats":
            try:
                q = parse_qs(uri.query)
                days = int(q.get("days", ["180"])[0])
                cost = float(q.get("cost_bps", ["75"])[0])
                if not 30 <= days <= 730 or not 0 <= cost <= 400:
                    raise ValueError("days 30–730; cost_bps 0–400")
                rows, candles, db_time = asyncio.run(load(int(os.environ["OWNER_CHAT_ID"]), days))
                if len(rows) > 25000 or len(candles) > 250000:
                    raise ValueError("La ventana excede el límite de extracción; elegí menos días.")
                result = compute(rows, candles, cost_bps=cost)
                result.update({"as_of":db_time.isoformat(), "days":days, "cost_bps":cost,
                               "raw_candles":len(candles), "raw_intents":len(rows)})
                body = json.dumps(result, ensure_ascii=False).encode()
                kind = "application/json; charset=utf-8"
            except (KeyError, ValueError) as exc:
                self.send_error(400, str(exc)); return
            except Exception as exc:
                # Never echo DSN or database error text to a browser.
                self.log_message("Database request failed: %s", type(exc).__name__)
                self.send_error(503, "Base de datos no disponible o esquema incompatible"); return
        else:
            self.send_error(404); return
        self.send_response(200)
        self.send_header("Content-Type", kind)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    if not os.environ.get("DATABASE_URL") or not os.environ.get("OWNER_CHAT_ID"):
        raise SystemExit("Definí DATABASE_URL y OWNER_CHAT_ID en el entorno; no pegues claves en el código.")
    server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
    print("Dashboard local: http://127.0.0.1:8765")
    server.serve_forever()


if __name__ == "__main__":
    main()
