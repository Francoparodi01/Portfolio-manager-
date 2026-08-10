"""Build intraday live portfolio views from snapshots plus latest market prices."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from typing import Optional

from src.analysis.corporate_actions import (
    CorporateActionEffect,
    PriceQualityStatus,
    assess_live_price,
    effects_by_ticker,
    rebase_position_view,
)
from src.collector.portfolio_quality import (
    PRICE_STATUS_FRESH,
    enrich_positions_with_market_metadata,
)
from src.core.telegram_format import header as tg_header, note as tg_note, section as tg_section


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def _fmt_ars(value, digits: int = 0, signed: bool = False) -> str:
    value_f = _safe_float(value)
    sign = ""
    if signed:
        sign = "+" if value_f >= 0 else "-"
        value_f = abs(value_f)
    text = f"{value_f:,.{digits}f}"
    text = text.replace(",", "_").replace(".", ",").replace("_", ".")
    return f"{sign}${text}"


def _fmt_price_ars(value) -> str:
    return _fmt_ars(value, digits=2)


def _fmt_pct(value, *, signed: bool = True, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    sign = "+" if signed else ""
    return f"{_safe_float(value):{sign}.{digits}%}"


def _price_source_label(source: str) -> str:
    return "mercado" if source == "market_prices" else "snapshot"


def _opening_state(day_change, covered: int, positions_count: int) -> str:
    coverage_ratio = (covered / positions_count) if positions_count else 0.0
    if positions_count and coverage_ratio < 0.8:
        return "REVISION: cobertura incompleta"
    if day_change is None:
        return "SIN VARIACION COMPARABLE"
    change = _safe_float(day_change)
    if change >= 0.01:
        return "POSITIVO"
    if change <= -0.01:
        return "NEGATIVO"
    return "ESTABLE"


def _compact_event_reason(value: str, max_len: int = 190) -> str:
    clean = " ".join(str(value or "").split())
    if not clean:
        return ""
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 1].rstrip() + "…"


@dataclass(frozen=True)
class PortfolioMoveAlert:
    ticker: str
    level: str
    direction: str
    change_pct_1d: float
    weight_live: float
    market_value: float
    alert_type: str = "PRICE_MOVE"
    reason: str = ""


def build_live_portfolio(
    snapshot: dict,
    latest_prices: list[dict],
    *,
    generated_at: Optional[datetime] = None,
    corporate_action_effects: list[CorporateActionEffect] | None = None,
) -> dict:
    generated_at = generated_at or datetime.now(timezone.utc)
    grouped_effects = effects_by_ticker(corporate_action_effects or [])
    snapshot_at_raw = snapshot.get("scraped_at")
    if isinstance(snapshot_at_raw, datetime):
        snapshot_at = snapshot_at_raw
    else:
        try:
            snapshot_at = datetime.fromisoformat(str(snapshot_at_raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            snapshot_at = generated_at
    if snapshot_at.tzinfo is None:
        snapshot_at = snapshot_at.replace(tzinfo=timezone.utc)
    price_map = {
        str(row.get("ticker", "")).upper(): row
        for row in latest_prices or []
        if row.get("ticker")
    }
    enriched_snapshot_positions = enrich_positions_with_market_metadata(
        snapshot.get("positions") or [],
        latest_prices or [],
        reference_at=generated_at,
    )

    positions: list[dict] = []
    covered_positions = 0
    price_quality_flags: list[dict] = []
    corporate_action_applications: list[dict] = []

    for source_raw in enriched_snapshot_positions:
        raw = dict(source_raw)
        ticker = str(raw.get("ticker", "")).upper()
        if not ticker:
            continue

        raw, position_applications = rebase_position_view(
            raw,
            snapshot_at=snapshot_at,
            as_of=generated_at,
            effects=grouped_effects.get(ticker, ()),
        )
        corporate_action_applications.extend(
            {
                "event_id": application.event_id,
                "instrument_effect_id": application.instrument_effect_id,
                "component": application.component,
                "application_status": application.application_status,
                "idempotency_key": application.idempotency_key,
                "before_state": application.before_state,
                "after_state": application.after_state,
                "invariant_checks": application.invariant_checks,
            }
            for application in position_applications
        )

        latest = price_map.get(ticker) or {}
        quantity = _safe_float(raw.get("quantity"))
        fallback_price = _safe_float(raw.get("current_price"))
        latest_price = _safe_float(latest.get("last_price"))
        price_is_fresh = str(raw.get("market_data_status")) == PRICE_STATUS_FRESH
        price = latest_price if latest_price > 0 and price_is_fresh else fallback_price
        market_value = quantity * price if quantity > 0 and price > 0 else _safe_float(raw.get("market_value"))
        change_pct_1d = latest.get("change_pct_1d")
        previous_close_price = _safe_float(latest.get("previous_close_price"))
        # Cocos exposes Var% in percentage points (2.8 means 2.8%, not 280%).
        # A stale quote must never contribute a daily return or trigger an alert.
        change_pct_1d_f = (
            _safe_float(change_pct_1d) / 100.0
            if change_pct_1d is not None and price_is_fresh
            else None
        )
        if previous_close_price > 0 and latest_price > 0 and price_is_fresh:
            change_pct_1d_f = (latest_price - previous_close_price) / previous_close_price
        assessment = None
        if previous_close_price > 0 and latest_price > 0 and price_is_fresh:
            assessment = assess_live_price(
                ticker=ticker,
                reference_price=previous_close_price,
                current_price=latest_price,
                observed_at=generated_at,
                effects=grouped_effects.get(ticker, ()),
            )
            if assessment.flag is not None:
                price_quality_flags.append(assessment.flag.to_dict())
            if assessment.status == PriceQualityStatus.RECONCILED.value:
                change_pct_1d_f = assessment.normalized_change
            elif assessment.status in {
                PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
                PriceQualityStatus.DATA_QUALITY_BLOCK.value,
            }:
                change_pct_1d_f = None
        day_pnl_ars = None
        if change_pct_1d_f is not None and change_pct_1d_f > -0.99 and market_value:
            prev_value = market_value / (1.0 + change_pct_1d_f)
            day_pnl_ars = market_value - prev_value
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
            change_pct_1d=change_pct_1d_f,
            day_pnl_ars=day_pnl_ars,
            previous_close_price=previous_close_price if previous_close_price > 0 else None,
            raw_change_pct_1d=(assessment.raw_change if assessment is not None else change_pct_1d_f),
            price_quality_status=(
                assessment.status if assessment is not None else PriceQualityStatus.COMPARABLE.value
            ),
            price_quality_reason=(
                assessment.flag.reason
                if assessment is not None and assessment.flag is not None
                else ""
            ),
            price_source="market_prices" if latest_price > 0 and price_is_fresh else "snapshot",
            market_price_ts=market_price_ts,
        )
        positions.append(position)

    invested_ars = sum(_safe_float(p.get("market_value")) for p in positions)
    day_pnl_ars = sum(
        _safe_float(p.get("day_pnl_ars"))
        for p in positions
        if p.get("day_pnl_ars") is not None
    )
    cash_ars = _safe_float(snapshot.get("cash_ars"))
    total_value_ars = invested_ars + cash_ars
    previous_invested_ars = invested_ars - day_pnl_ars
    has_complete_price_coverage = bool(positions) and covered_positions == len(positions)
    day_change_pct = (
        day_pnl_ars / previous_invested_ars
        if has_complete_price_coverage and previous_invested_ars > 0
        else None
    )

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
        "day_pnl_ars": day_pnl_ars,
        "day_change_pct": day_change_pct,
        "positions_count": len(positions),
        "price_coverage_count": covered_positions,
        "price_quality_flags": price_quality_flags,
        "corporate_action_applications": corporate_action_applications,
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
        quality_status = str(position.get("price_quality_status") or "").upper()
        if quality_status in {
            PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
            PriceQualityStatus.DATA_QUALITY_BLOCK.value,
        }:
            alerts.append(
                PortfolioMoveAlert(
                    ticker=str(position.get("ticker", "")).upper(),
                    level="DATA_QUALITY",
                    direction="NONE",
                    change_pct_1d=_safe_float(position.get("raw_change_pct_1d")),
                    weight_live=_safe_float(position.get("weight_in_portfolio")),
                    market_value=_safe_float(position.get("market_value")),
                    alert_type="PRICE_NOT_COMPARABLE",
                    reason=str(position.get("price_quality_reason") or ""),
                )
            )
            continue
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
    manual_event_risk = {
        str(ticker or "").upper(): str(reason or "")
        for ticker, reason in dict(
            live_portfolio.get("manual_event_risk_by_ticker") or {}
        ).items()
    }
    lines = tg_header("📣 Movimiento relevante en cartera", subtitle="Alerta intradía sobre valuación estimada")

    for alert in alerts:
        if alert.alert_type == "PRICE_NOT_COMPARABLE":
            lines.append(
                f"<b>{escape(alert.ticker)}</b> precio no comparable "
                f"(movimiento raw {alert.change_pct_1d:+.2%})."
            )
            if alert.reason:
                lines.append(f"   {escape(alert.reason)}")
            lines.append("   Senal intradia suspendida; requiere confirmacion o reconciliacion.")
            continue
        icon = "🟢" if alert.direction == "UP" else "🔴"
        lines.append(
            f"{icon} <b>{escape(alert.ticker)}</b> "
            f"{alert.change_pct_1d:+.2%} hoy · peso {_safe_float(alert.weight_live):.1%} "
            f"· valor {_fmt_ars(alert.market_value)} ARS"
        )
        event_reason = _compact_event_reason(manual_event_risk.get(alert.ticker))
        if event_reason:
            lines.append(f"   ⚠️ EVENT_RISK activo: {escape(event_reason)}")

    lines += [
        "",
        f"💰 Total live: <b>${total:,.0f} ARS</b>".replace(",", "."),
        f"📈 Invertido: <b>${invested:,.0f} ARS</b>".replace(",", "."),
        f"💵 Cash: <b>${cash:,.0f} ARS</b>".replace(",", "."),
        "",
        tg_note("Mensaje compacto: solo muestra tickers afectados. Para cartera completa usa /portfolio o el monitor."),
    ]

    lines.append("")
    lines.append(tg_note("Valuación live estimada con market_prices; posiciones/cash desde último snapshot real. No confirma fills."))
    return "\n".join(lines)


def render_opening_portfolio_report(
    live_portfolio: dict,
    *,
    title: str = "POST OPEN - PORTFOLIO ACTUALIZADO",
) -> str:
    """Render a daily opening portfolio mark after the first market scrape."""
    total = _safe_float(live_portfolio.get("total_value_ars"))
    invested = _safe_float(live_portfolio.get("invested_ars"))
    cash = _safe_float(live_portfolio.get("cash_ars"))
    day_pnl = _safe_float(live_portfolio.get("day_pnl_ars"))
    day_change = live_portfolio.get("day_change_pct")
    covered = int(_safe_float(live_portfolio.get("price_coverage_count")))
    positions_count = int(_safe_float(live_portfolio.get("positions_count")))
    positions = sorted(
        live_portfolio.get("positions") or [],
        key=lambda p: _safe_float(p.get("market_value")),
        reverse=True,
    )
    state = _opening_state(day_change, covered, positions_count)
    quality_positions = [
        position
        for position in positions
        if str(position.get("price_quality_status") or "").upper()
        in {
            PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
            PriceQualityStatus.DATA_QUALITY_BLOCK.value,
        }
    ]
    if quality_positions:
        state = "REVISION: PRECIO NO COMPARABLE"
    coverage_ratio = (covered / positions_count) if positions_count else 0.0
    top_movers = sorted(
        [p for p in positions if p.get("change_pct_1d") is not None],
        key=lambda p: abs(_safe_float(p.get("change_pct_1d"))),
        reverse=True,
    )[:3]

    lines = tg_header(title, subtitle="Marca post-open de la cartera; no confirma operaciones") + [
        tg_section("Lectura rapida"),
        f"Estado: <b>{state}</b>",
        (
            f"Movimiento cartera: <b>{_fmt_pct(day_change)}</b> "
            f"(<b>{_fmt_ars(day_pnl, signed=True)} ARS</b>)"
            if day_change is not None
            else "Movimiento cartera: <b>N/A</b>"
        ),
        f"Precios de mercado: <b>{covered}/{positions_count}</b> ({coverage_ratio:.0%})",
        "",
        tg_section("Resumen"),
        f"Total: <b>{_fmt_ars(total)} ARS</b>",
        f"Invertido: <b>{_fmt_ars(invested)} ARS</b>",
        f"Cash: <b>{_fmt_ars(cash)} ARS</b>",
    ]

    if top_movers:
        lines += ["", tg_section("Movimientos relevantes")]
        for position in top_movers:
            ticker = escape(str(position.get("ticker", "") or "").upper())
            change = position.get("change_pct_1d")
            day_pnl_pos = position.get("day_pnl_ars")
            lines.append(
                f"- <b>{ticker}</b>: {_fmt_pct(change)} "
                f"({_fmt_ars(day_pnl_pos, signed=True)} ARS)"
            )

    if quality_positions:
        lines += ["", tg_section("Calidad de precio")]
        for position in quality_positions:
            ticker = escape(str(position.get("ticker") or "").upper())
            raw_change = position.get("raw_change_pct_1d")
            reason = escape(str(position.get("price_quality_reason") or ""))
            lines.append(
                f"- <b>{ticker}</b>: raw {_fmt_pct(raw_change)} | PRICE_NOT_COMPARABLE"
            )
            if reason:
                lines.append(f"  {reason}")

    lines += [
        "",
        tg_section("Posiciones"),
        "Peso / dia / valor / fuente",
    ]

    for position in positions:
        ticker = escape(str(position.get("ticker", "") or "").upper())
        value = _safe_float(position.get("market_value"))
        weight = _safe_float(position.get("weight_in_portfolio"))
        change = position.get("change_pct_1d")
        day_pnl_pos = position.get("day_pnl_ars")
        source = str(position.get("price_source") or "snapshot")
        source_txt = _price_source_label(source)
        pnl_txt = f" ({_fmt_ars(day_pnl_pos, signed=True)} ARS)" if day_pnl_pos is not None else ""
        lines.append(
            f"- <b>{ticker}</b>: {weight:.1%} | dia {_fmt_pct(change)}{pnl_txt} "
            f"| {_fmt_ars(value)} ARS | {source_txt}"
        )

    lines.append("")
    warning = str(live_portfolio.get("post_open_warning") or "").strip()
    if warning:
        lines.append(f"<b>Advertencia:</b> {escape(warning)}")
    lines.append(tg_note("Plan EOD = proxima rueda. Este reporte marca cartera post-open con precios operables; no confirma operaciones."))
    return "\n".join(lines)
