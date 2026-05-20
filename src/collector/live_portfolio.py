"""Build intraday live portfolio views from snapshots plus latest market prices."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from typing import Optional

from src.collector.portfolio_quality import (
    PRICE_STATUS_FRESH,
    enrich_positions_with_market_metadata,
)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


@dataclass(frozen=True)
class PortfolioMoveAlert:
    ticker: str
    level: str
    direction: str
    change_pct_1d: float
    weight_live: float
    market_value: float


def build_live_portfolio(
    snapshot: dict,
    latest_prices: list[dict],
    *,
    generated_at: Optional[datetime] = None,
) -> dict:
    generated_at = generated_at or datetime.now(timezone.utc)
    price_map = {
        str(row.get("ticker", "")).upper(): row
        for row in latest_prices or []
        if row.get("ticker")
    }
    enriched_snapshot_positions = enrich_positions_with_market_metadata(
        snapshot.get("positions") or [],
        latest_prices or [],
    )

    positions: list[dict] = []
    covered_positions = 0

    for raw in enriched_snapshot_positions:
        ticker = str(raw.get("ticker", "")).upper()
        if not ticker:
            continue

        latest = price_map.get(ticker) or {}
        quantity = _safe_float(raw.get("quantity"))
        fallback_price = _safe_float(raw.get("current_price"))
        latest_price = _safe_float(latest.get("last_price"))
        price_is_fresh = str(raw.get("market_data_status")) == PRICE_STATUS_FRESH
        price = latest_price if latest_price > 0 and price_is_fresh else fallback_price
        market_value = quantity * price if quantity > 0 and price > 0 else _safe_float(raw.get("market_value"))
        change_pct_1d = latest.get("change_pct_1d")
        market_price_ts = latest.get("ts")
        if hasattr(market_price_ts, "isoformat"):
            market_price_ts = market_price_ts.isoformat()

        if latest_price > 0 and price_is_fresh:
            covered_positions += 1

        position = dict(raw)
        position.update(
            ticker=ticker,
            current_price=price,
            market_value=market_value,
            change_pct_1d=_safe_float(change_pct_1d) if change_pct_1d is not None else None,
            price_source="market_prices" if latest_price > 0 and price_is_fresh else "snapshot",
            market_price_ts=market_price_ts,
        )
        positions.append(position)

    invested_ars = sum(_safe_float(p.get("market_value")) for p in positions)
    cash_ars = _safe_float(snapshot.get("cash_ars"))
    total_value_ars = invested_ars + cash_ars

    for position in positions:
        position["weight_in_portfolio"] = (
            _safe_float(position.get("market_value")) / invested_ars
            if invested_ars > 0 else 0.0
        )

    return {
        "snapshot_id": snapshot.get("snapshot_id"),
        "scraped_at": snapshot.get("scraped_at"),
        "generated_at": generated_at.isoformat(),
        "valuation_mode": "live_market_prices",
        "cash_ars": cash_ars,
        "invested_ars": invested_ars,
        "total_value_ars": total_value_ars,
        "positions_count": len(positions),
        "price_coverage_count": covered_positions,
        "positions": positions,
    }


def select_portfolio_move_alerts(
    live_portfolio: dict,
    *,
    major_abs_pct: float = 0.03,
    weighted_abs_pct: float = 0.02,
    min_weight: float = 0.10,
) -> list[PortfolioMoveAlert]:
    alerts: list[PortfolioMoveAlert] = []

    for position in live_portfolio.get("positions") or []:
        change = position.get("change_pct_1d")
        if change is None:
            continue

        change = _safe_float(change)
        weight = _safe_float(position.get("weight_in_portfolio"))
        abs_change = abs(change)

        if abs_change >= major_abs_pct:
            level = "MAJOR"
        elif abs_change >= weighted_abs_pct and weight >= min_weight:
            level = "WEIGHTED"
        else:
            continue

        alerts.append(
            PortfolioMoveAlert(
                ticker=str(position.get("ticker", "")).upper(),
                level=level,
                direction="UP" if change > 0 else "DOWN",
                change_pct_1d=change,
                weight_live=weight,
                market_value=_safe_float(position.get("market_value")),
            )
        )

    return sorted(
        alerts,
        key=lambda alert: (abs(alert.change_pct_1d), alert.weight_live),
        reverse=True,
    )


def render_live_portfolio_alert(
    alerts: list[PortfolioMoveAlert],
    live_portfolio: dict,
) -> str:
    total = _safe_float(live_portfolio.get("total_value_ars"))
    invested = _safe_float(live_portfolio.get("invested_ars"))
    cash = _safe_float(live_portfolio.get("cash_ars"))
    positions = sorted(
        live_portfolio.get("positions") or [],
        key=lambda p: _safe_float(p.get("market_value")),
        reverse=True,
    )

    lines = [
        "📣 <b>Movimiento relevante en cartera</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for alert in alerts:
        icon = "🟢" if alert.direction == "UP" else "🔴"
        lines.append(
            f"{icon} <b>{escape(alert.ticker)}</b> "
            f"{alert.change_pct_1d:+.2%} hoy · peso {_safe_float(alert.weight_live):.1%}"
        )

    lines += [
        "",
        f"💰 Total live: <b>${total:,.0f} ARS</b>".replace(",", "."),
        f"📈 Invertido: <b>${invested:,.0f} ARS</b>".replace(",", "."),
        f"💵 Cash: <b>${cash:,.0f} ARS</b>".replace(",", "."),
        "",
        "<b>Portfolio actualizado</b>",
    ]

    for position in positions:
        ticker = escape(str(position.get("ticker", "")).upper())
        value = _safe_float(position.get("market_value"))
        weight = _safe_float(position.get("weight_in_portfolio"))
        change = position.get("change_pct_1d")
        change_txt = f" · {change:+.2%}" if change is not None else ""
        lines.append(
            f"• <b>{ticker}</b>: ${value:,.0f} ARS · {weight:.1%}{change_txt}".replace(",", ".")
        )

    lines.append("")
    lines.append("<i>Valuación live estimada con market_prices; posiciones/cash desde último snapshot real.</i>")
    return "\n".join(lines)


def render_opening_portfolio_report(live_portfolio: dict) -> str:
    """Render a daily opening portfolio mark after the first market scrape."""
    total = _safe_float(live_portfolio.get("total_value_ars"))
    invested = _safe_float(live_portfolio.get("invested_ars"))
    cash = _safe_float(live_portfolio.get("cash_ars"))
    covered = int(_safe_float(live_portfolio.get("price_coverage_count")))
    positions_count = int(_safe_float(live_portfolio.get("positions_count")))
    positions = sorted(
        live_portfolio.get("positions") or [],
        key=lambda p: _safe_float(p.get("market_value")),
        reverse=True,
    )

    lines = [
        "APERTURA DE MERCADO - PORTFOLIO ACTUALIZADO",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total apertura: <b>${total:,.0f} ARS</b>".replace(",", "."),
        f"Invertido: <b>${invested:,.0f} ARS</b>".replace(",", "."),
        f"Cash: <b>${cash:,.0f} ARS</b>".replace(",", "."),
        f"Cobertura precios: <b>{covered}/{positions_count}</b>",
        "",
        "<b>Posiciones</b>",
    ]

    for position in positions:
        ticker = escape(str(position.get("ticker", "") or "").upper())
        value = _safe_float(position.get("market_value"))
        weight = _safe_float(position.get("weight_in_portfolio"))
        price = _safe_float(position.get("current_price"))
        change = position.get("change_pct_1d")
        source = str(position.get("price_source") or "snapshot")
        change_txt = f" · {change:+.2%}" if change is not None else ""
        source_txt = "mkt" if source == "market_prices" else "snap"
        lines.append(
            f"• <b>{ticker}</b>: ${value:,.0f} ARS · {weight:.1%} "
            f"· ${price:,.2f}{change_txt} · {source_txt}"
            .replace(",", ".")
        )

    lines.append("")
    lines.append("<i>Precios desde market_prices de apertura; posiciones/cash desde snapshot real Cocos.</i>")
    return "\n".join(lines)
