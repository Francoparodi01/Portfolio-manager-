"""Cartera sombra: what the account would look like following the bot's real plans.

The shadow starts as a clone of the real ARS portfolio (positions + cash) at
the first snapshot on or after ``since`` and then applies every production
ExecutionPlan the bot actually emitted, with the backtest's fictional ledger:
fills at the first recorded quote >= 60 s after the plan, adverse slippage,
explicit fees, no shorts, no negative cash.

Plans are sized for the real account, which may have deviated from the
shadow, and the planner repeats the same recommendation run after run. So each
order is translated into a target weight (current weight +/- amount / plan
NAV) and the shadow trades only the difference to that weight: a repeated
plan is idempotent instead of selling twice.

Baselines over the same window: HOLD (the cloned portfolio never trades) and
the starting NAV invested in the benchmark. One path, no significance test:
this is a description, not evidence of edge. It does not rerun the engine
(no look-ahead from today's rules), reads only, and never touches orders.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal
from html import escape
from typing import Optional
from zoneinfo import ZoneInfo

import asyncpg

from src.backtesting.ledger import Ledger

ART = ZoneInfo("America/Argentina/Buenos_Aires")
SESSION_CLOSE = time(17, 0)
VARIANTS = ("all", "sells_only")
# Ignore re-trades smaller than this share of the shadow NAV: a repeated plan
# lands on the same weight give or take fees, which must not churn the book.
MIN_TRADE_FRACTION = Decimal("0.005")
CASH = "_CASH"


@dataclass(frozen=True)
class ShadowConfig:
    database_url: str
    since: datetime
    until: Optional[datetime] = None
    benchmark: str = "SPY"
    fee_rate: str = "0.006"
    slippage_rate: str = "0.0015"
    owner_chat_id: Optional[int] = None


@dataclass
class ShadowPath:
    name: str
    daily: list[dict]
    fills: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)

    @property
    def final(self) -> Optional[dict]:
        return self.daily[-1] if self.daily else None


@dataclass
class ShadowReport:
    start: dict
    paths: dict[str, ShadowPath]
    benchmark: Optional[list[dict]]
    plans_used: int
    plans_skipped: int
    excluded_positions: list[str]
    warnings: list[str]
    order_outcomes: dict = field(default_factory=dict)


# ── Inputs ──────────────────────────────────────────────────────────────────


async def load_shadow_inputs(config: ShadowConfig) -> dict:
    dsn = config.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn, server_settings={"default_transaction_read_only": "on"})
    owner = config.owner_chat_id
    try:
        snapshot = await conn.fetchrow(
            """
            SELECT snapshot_id, scraped_at, total_value_ars::float8 AS total, cash_ars::float8 AS cash
            FROM portfolio_snapshots
            WHERE scraped_at >= $1 AND ($2::bigint IS NULL OR owner_chat_id IS NULL OR owner_chat_id = $2)
            ORDER BY scraped_at ASC LIMIT 1
            """,
            config.since,
            owner,
        )
        if snapshot is None:
            raise ValueError("No hay snapshot de cartera desde esa fecha.")
        positions = await conn.fetch(
            """
            SELECT ticker, asset_type, currency, quantity::float8 AS quantity, market_value::float8 AS market_value
            FROM positions WHERE snapshot_id = $1 AND quantity > 0
            """,
            snapshot["snapshot_id"],
        )
        until = config.until or datetime.now(tz=timezone.utc)
        orders = await conn.fetch(
            """
            SELECT p.id AS plan_id, p.created_at, oi.ticker, oi.side, oi.amount_ars::float8 AS amount_ars,
                   oi.quantity_est::float8 AS quantity_est, oi.reason,
                   d.current_weight::float8 AS current_weight, d.target_weight::float8 AS target_weight
            FROM execution_plans p
            JOIN order_intents oi ON oi.execution_plan_id = p.id
            LEFT JOIN decision_log d ON d.id = oi.decision_log_id
            WHERE p.created_at > $1 AND p.created_at <= $2 AND p.feasible
              AND oi.is_executable AND NOT oi.was_blocked
              AND ($3::bigint IS NULL OR p.owner_chat_id IS NULL OR p.owner_chat_id = $3)
            ORDER BY p.created_at, oi.sequence_no
            """,
            snapshot["scraped_at"],
            until,
            owner,
        )
        navs = await conn.fetch(
            """
            SELECT scraped_at, (COALESCE(total_value_ars, 0) + COALESCE(cash_ars, 0))::float8 AS nav
            FROM portfolio_snapshots
            WHERE scraped_at >= $1::timestamptz - INTERVAL '3 days' AND scraped_at <= $2
              AND ($3::bigint IS NULL OR owner_chat_id IS NULL OR owner_chat_id = $3)
            ORDER BY scraped_at
            """,
            snapshot["scraped_at"],
            until,
            owner,
        )
        real_rows = await conn.fetch(
            """
            WITH daily AS (
                SELECT DISTINCT ON ((scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date)
                       snapshot_id, scraped_at, COALESCE(cash_ars, 0)::float8 AS cash
                FROM portfolio_snapshots
                WHERE scraped_at >= $1 AND scraped_at <= $2
                  AND ($3::bigint IS NULL OR owner_chat_id IS NULL OR owner_chat_id = $3)
                ORDER BY (scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date, scraped_at DESC
            )
            SELECT d.scraped_at, d.cash, p.ticker, p.market_value::float8 AS market_value
            FROM daily d LEFT JOIN positions p ON p.snapshot_id = d.snapshot_id AND p.quantity > 0
            ORDER BY d.scraped_at
            """,
            snapshot["scraped_at"],
            until,
            owner,
        )
        tickers = sorted(
            {r["ticker"] for r in positions} | {r["ticker"] for r in orders} | {config.benchmark.upper()}
        )
        intraday = await conn.fetch(
            """
            SELECT ts, ticker, asset_type, currency, last_price::float8 AS last_price
            FROM market_prices
            WHERE ts >= $1::timestamptz - INTERVAL '7 days' AND ts <= $2::timestamptz AND currency = 'ARS' AND last_price > 0
              AND ticker = ANY($3::text[])
            ORDER BY ts, ticker, asset_type
            """,
            snapshot["scraped_at"],
            until,
            tickers,
        )
        candles = await conn.fetch(
            """
            SELECT ts, ticker, asset_type, currency, close_price::float8 AS close_price, source, scraped_at
            FROM market_candles
            WHERE ts >= $1::timestamptz - INTERVAL '15 days' AND ts <= $2::timestamptz AND currency = 'ARS' AND interval = '1d'
              AND ticker = ANY($3::text[])
            ORDER BY ticker, ts
            """,
            snapshot["scraped_at"],
            until,
            tickers,
        )
    finally:
        await conn.close()
    return {
        "snapshot": dict(snapshot),
        "positions": [dict(r) for r in positions],
        "orders": [dict(r) for r in orders],
        "navs": [dict(r) for r in navs],
        "real_weights": real_weights_by_day([dict(r) for r in real_rows]),
        "intraday_points": [dict(r) for r in intraday],
        "daily_candles": [dict(r) for r in candles],
        "until": until,
        "benchmark": config.benchmark.upper(),
    }


# ── Simulation ──────────────────────────────────────────────────────────────


def real_weights_by_day(rows: list[dict]) -> dict[str, dict[str, float]]:
    """Weights of the real account at the last snapshot of each local day (cash included in NAV)."""
    days: dict[str, dict] = {}
    for row in rows:
        day = row["scraped_at"].astimezone(ART).date().isoformat()
        entry = days.setdefault(day, {"cash": float(row.get("cash") or 0), "values": {}})
        if row.get("ticker") and row.get("market_value"):
            entry["values"][row["ticker"]] = entry["values"].get(row["ticker"], 0.0) + float(row["market_value"])
    out = {}
    for day, entry in days.items():
        nav = entry["cash"] + sum(entry["values"].values())
        if nav > 0:
            out[day] = {t: v / nav for t, v in entry["values"].items()}
            out[day][CASH] = entry["cash"] / nav
    return out


def overlap(shadow: dict[str, float], real: dict[str, float]) -> float:
    """Share of the book both accounts hold: sum of min(weight) over tickers and cash (1 = identical)."""
    return sum(min(w, real.get(t, 0.0)) for t, w in shadow.items())


def _shadow_weights(value: dict) -> dict[str, float]:
    nav = value["nav_ars"]
    if nav <= 0:
        return {}
    weights = {a["ticker"]: a["value_ars"] / nav for a in value["assets"] if a["quantity"]}
    weights[CASH] = value["cash_ars"] / nav
    return weights


def _session_end(moment: datetime) -> datetime:
    local = moment.astimezone(ART)
    return datetime.combine(local.date(), SESSION_CLOSE, ART)


def _plan_nav(navs: list[dict], at: datetime) -> Optional[float]:
    times = [r["scraped_at"] for r in navs]
    index = bisect_right(times, at) - 1
    if index < 0:
        return None
    nav = navs[index]["nav"]
    return nav if nav and nav > 0 else None


def _group_plans(orders: list[dict]) -> list[dict]:
    plans: dict = {}
    for row in orders:
        plan = plans.setdefault(row["plan_id"], {"created_at": row["created_at"], "orders": []})
        plan["orders"].append(row)
    return sorted(plans.values(), key=lambda p: p["created_at"])


def _target_weight(order: dict, plan_nav: Optional[float]) -> Optional[float]:
    current = order.get("current_weight")
    amount = order.get("amount_ars")
    if current is None or plan_nav is None or amount is None:
        return None
    sign = -1.0 if str(order["side"]).upper() == "SELL" else 1.0
    return max(0.0, float(current) + sign * float(amount) / plan_nav)


def _clone(inputs: dict, tickers: list[str], fee: str, slip: str) -> tuple[Ledger, list[str]]:
    rows = {k: inputs[k] for k in ("intraday_points", "daily_candles")}
    snapshot = inputs["snapshot"]
    ledger = Ledger(rows, tickers, initial_cash=Decimal(str(snapshot["cash"] or 0)), fee_rate=fee, slippage_rate=slip)
    excluded = []
    start = snapshot["scraped_at"]
    for position in inputs["positions"]:
        ticker = position["ticker"]
        quantity = int(Decimal(str(position["quantity"])).to_integral_value(rounding=ROUND_FLOOR))
        if position.get("currency") != "ARS" or ticker not in ledger.qty or quantity <= 0:
            excluded.append(ticker)
            continue
        try:
            price, _meta = ledger.mark(ticker, start)
        except (ValueError, KeyError):
            excluded.append(ticker)
            continue
        ledger.qty[ticker] = quantity
        ledger.basis[ticker] = price * quantity
    # Returns are measured against the cloned NAV, not against the cash alone.
    ledger.initial_cash = ledger.cash + sum(ledger.basis.values())
    return ledger, excluded


def _session_days(inputs: dict, start: datetime, until: datetime) -> list[date]:
    days = sorted(
        {
            r["ts"].astimezone(ART).date()
            for r in inputs["intraday_points"]
            if start <= r["ts"] <= until
        }
    )
    return days


def _value(ledger: Ledger, asof: datetime) -> Optional[dict]:
    try:
        return ledger.valuation(asof)
    except ValueError:
        return None


def simulate_shadow(inputs: dict, *, fee_rate: str = "0.006", slippage_rate: str = "0.0015") -> ShadowReport:
    snapshot = inputs["snapshot"]
    start = snapshot["scraped_at"]
    until = inputs["until"]
    benchmark = inputs["benchmark"]
    tickers = sorted({p["ticker"] for p in inputs["positions"]} | {o["ticker"] for o in inputs["orders"]} | {benchmark})
    warnings: list[str] = []
    plans = _group_plans(inputs["orders"])
    days = _session_days(inputs, start, until)

    paths: dict[str, ShadowPath] = {}
    excluded: list[str] = []
    used = skipped = 0
    # What happened to each order of the bot in the "all" variant.
    outcomes = {"applied": 0, "at_target": 0, "no_position": 0, "no_price": 0, "untranslatable": 0}
    real_weights = inputs.get("real_weights") or {}
    for variant in ("hold",) + VARIANTS:
        ledger, excluded = _clone(inputs, tickers, fee_rate, slippage_rate)
        if _value(ledger, start) is None:
            raise ValueError("No se pudo valuar la cartera inicial.")
        path = ShadowPath(name=variant, daily=[])
        plan_index = 0
        for day in days:
            close = datetime.combine(day, SESSION_CLOSE, ART)
            while variant != "hold" and plan_index < len(plans) and plans[plan_index]["created_at"] <= close:
                plan = plans[plan_index]
                plan_index += 1
                cutoff = plan["created_at"]
                following = plans[plan_index]["created_at"] if plan_index < len(plans) else None
                end = min(_session_end(cutoff), following) if following else _session_end(cutoff)
                shadow = _value(ledger, cutoff)
                plan_nav = _plan_nav(inputs["navs"], cutoff)
                sell_orders, buy_orders = [], []
                for order in plan["orders"]:
                    side = str(order["side"]).upper()
                    if variant == "sells_only" and side == "BUY":
                        continue
                    target = _target_weight(order, plan_nav)
                    ticker = order["ticker"]
                    tally = variant == "all"
                    if target is None or shadow is None:
                        if tally:
                            skipped += 1
                            outcomes["untranslatable"] += 1
                        continue
                    if side == "SELL" and ledger.qty[ticker] == 0:
                        # The plan sells something the real account holds and the shadow does not.
                        if tally:
                            outcomes["no_position"] += 1
                        continue
                    try:
                        price, _meta = ledger.mark(ticker, cutoff)
                    except ValueError:
                        if tally:
                            outcomes["no_price"] += 1
                        continue
                    desired = int((Decimal(str(target)) * Decimal(str(shadow["nav_ars"])) / price).to_integral_value(rounding=ROUND_FLOOR))
                    delta = desired - ledger.qty[ticker]
                    small = abs(delta) * price < MIN_TRADE_FRACTION * Decimal(str(shadow["nav_ars"]))
                    # Only move in the direction the bot asked; never buy on a sell signal.
                    if not small and side == "SELL" and delta < 0:
                        sell_orders.append({"ticker": ticker, "quantity_est": -delta, "reference_price": float(price), "reason": order.get("reason")})
                    elif not small and side == "BUY" and delta > 0:
                        buy_orders.append({"ticker": ticker, "quantity_est": delta, "reference_price": float(price), "reason": order.get("reason")})
                    else:
                        if tally:
                            outcomes["at_target"] += 1
                        continue
                    if tally:
                        outcomes["applied"] += 1
                if variant == "all":
                    used += 1
                if sell_orders or buy_orders:
                    ledger.execute({"feasible": True, "sell_orders": sell_orders, "buy_orders": buy_orders}, cutoff, end)
            value = _value(ledger, close)
            if value is not None:
                row = {"day": day.isoformat(), "nav_ars": value["nav_ars"], "return_pct": value["return_pct"]}
                real = real_weights.get(day.isoformat())
                if real is not None:
                    row["overlap"] = overlap(_shadow_weights(value), real)
                path.daily.append(row)
        path.fills = list(ledger.fills)
        path.orders = list(ledger.orders)
        paths[variant] = path

    benchmark_path = _benchmark_path(inputs, benchmark, start, days)
    if benchmark_path is None:
        warnings.append(f"Sin precios de {benchmark} CEDEAR ARS en la ventana: no hay comparación con el mercado.")
    if excluded:
        warnings.append("Posiciones iniciales sin precio ARS, fuera de la sombra: " + ", ".join(sorted(set(excluded))))
    if skipped:
        warnings.append(f"{skipped} órdenes sin peso o NAV del plan: no se pudieron traducir.")
    return ShadowReport(
        order_outcomes=outcomes,
        start={"at": start, "nav_ars": paths["hold"].daily[0]["nav_ars"] if paths["hold"].daily else None},
        paths=paths,
        benchmark=benchmark_path,
        plans_used=used,
        plans_skipped=skipped,
        excluded_positions=sorted(set(excluded)),
        warnings=warnings,
    )


def _benchmark_path(inputs: dict, benchmark: str, start: datetime, days: list[date]) -> Optional[list[dict]]:
    ledger = Ledger({k: inputs[k] for k in ("intraday_points", "daily_candles")}, [benchmark])
    try:
        base, _ = ledger.mark(benchmark, start)
    except ValueError:
        return None
    out = []
    for day in days:
        try:
            price, _ = ledger.mark(benchmark, datetime.combine(day, SESSION_CLOSE, ART))
        except ValueError:
            continue
        out.append({"day": day.isoformat(), "return_pct": float(price / base - 1) * 100})
    return out or None


# ── Metrics & rendering ─────────────────────────────────────────────────────


def max_drawdown_pct(daily: list[dict]) -> Optional[float]:
    if not daily:
        return None
    peak, worst = None, 0.0
    for row in daily:
        nav = row["nav_ars"]
        peak = nav if peak is None else max(peak, nav)
        worst = min(worst, (nav / peak - 1) * 100)
    return worst


def overlap_stats(daily: list[dict]) -> Optional[dict]:
    rows = [r for r in daily if "overlap" in r]
    if not rows:
        return None
    below = next((r["day"] for r in rows if r["overlap"] < 0.5), None)
    return {
        "start": rows[0]["overlap"],
        "end": rows[-1]["overlap"],
        "min": min(r["overlap"] for r in rows),
        "below_half_since": date.fromisoformat(below).strftime("%d/%m") if below else None,
    }


def _pct(value: Optional[float]) -> str:
    return "-" if value is None else f"{value:+.1f}%"


def _money(value: Optional[float]) -> str:
    return "-" if value is None else "$" + f"{value:,.0f}".replace(",", ".")


def render_shadow(report: ShadowReport, *, benchmark: str = "SPY") -> str:
    start = report.start
    hold = report.paths["hold"].final
    lines = [
        "<b>🪞 Cartera sombra · seguir al bot al 100%</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        (
            f"Arranca igual que tu cartera real del {start['at'].astimezone(ART):%d/%m/%Y} "
            f"({_money(start['nav_ars'])}) y aplica los planes reales del bot."
        ),
        f"Planes aplicados: {report.plans_used} · costo 0.60% + 0.15% por lado",
        "",
    ]
    labels = {"all": "🤖 Todo lo del bot", "sells_only": "✂️ Solo sus ventas", "hold": "🧊 Sin operar"}
    for name in ("all", "sells_only", "hold"):
        path = report.paths[name]
        final = path.final
        if final is None:
            lines.append(f"{labels[name]}: sin valuación")
            continue
        fees = sum(f["fee_ars"] for f in path.fills)
        lines.append(
            f"{labels[name]}: <b>{_pct(final['return_pct'])}</b> · {_money(final['nav_ars'])} · "
            f"DD {_pct(max_drawdown_pct(path.daily))}"
            + (f" · {len(path.fills)} operaciones · comisiones {_money(fees)}" if path.fills else "")
        )
    if report.benchmark:
        lines.append(f"📈 {escape(benchmark)} CEDEAR: <b>{_pct(report.benchmark[-1]['return_pct'])}</b>")
    if hold and report.paths["all"].final:
        diff = report.paths["all"].final["return_pct"] - hold["return_pct"]
        lines += ["", f"Bot menos no operar: <b>{_pct(diff)}</b>"]
        if report.benchmark:
            lines.append(f"Bot menos {escape(benchmark)}: <b>{_pct(report.paths['all'].final['return_pct'] - report.benchmark[-1]['return_pct'])}</b>")
    outcomes = report.order_outcomes or {}
    total = sum(outcomes.values())
    if total:
        lines += [
            "",
            "<b>¿Cuánto del bot se pudo aplicar?</b>",
            (
                f"Órdenes del bot: {total} · aplicadas {outcomes.get('applied', 0)} · ya en el peso {outcomes.get('at_target', 0)} · "
                f"<b>sin la posición en la sombra {outcomes.get('no_position', 0)}</b> · sin precio {outcomes.get('no_price', 0)} · "
                f"sin traducir {outcomes.get('untranslatable', 0)}"
            ),
        ]
    stats = overlap_stats(report.paths["all"].daily)
    if stats:
        lines.append(
            f"Parecido con tu cartera real: {stats['start']:.0%} al inicio → {stats['end']:.0%} al final (mínimo {stats['min']:.0%})"
        )
        if stats.get("below_half_since"):
            lines.append(
                f"⚠️ Desde el {stats['below_half_since']} comparten menos de la mitad: los planes del bot "
                "ya no hablan de lo que tiene la sombra."
            )
    days = len(report.paths["hold"].daily)
    lines += ["", f"<i>{days} ruedas valuadas al cierre. Un solo camino: describe, no prueba edge.</i>"]
    lines += [f"⚠️ {escape(w)}" for w in report.warnings]
    lines.append(
        "<i>Las órdenes se traducen a peso objetivo: un plan repetido no vende dos veces. "
        "Fills simulados con la primera cotización registrada; sin dividendos ni impuestos.</i>"
    )
    return "\n".join(lines)
