"""Build evidence packets for the local Qwen narrative layer.

The builders in this module are pure transformations. They do not read the
database, call Ollama, publish messages, or mutate trading state.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from src.analysis.llm_narratives import MarketNarrative


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def build_market_report_packet(
    live_portfolio: Mapping[str, Any],
    *,
    packet_id: str | None = None,
    run_id: str = "manual_qwen_preview",
    as_of: datetime | None = None,
    policy_version: str = "market_report_v1",
    max_positions: int = 10,
    max_top_movers: int = 5,
) -> dict[str, Any]:
    """Convert a live portfolio payload into a bounded MarketReportPacket."""
    as_of = _coerce_datetime(as_of or live_portfolio.get("generated_at") or datetime.now(timezone.utc))
    as_of_art = as_of.astimezone(ART_TZ)
    positions = sorted(
        [dict(position) for position in live_portfolio.get("positions") or []],
        key=lambda position: _safe_float(position.get("market_value")),
        reverse=True,
    )
    positions_count = int(_safe_float(live_portfolio.get("positions_count"), len(positions)))
    covered_count = int(_safe_float(live_portfolio.get("price_coverage_count")))
    coverage_ratio = (covered_count / positions_count) if positions_count else 0.0
    priced_weight_pct = sum(
        _safe_float(position.get("weight_in_portfolio"))
        for position in positions
        if str(position.get("price_source") or "") == "market_prices"
    )
    missing_tickers = [
        str(position.get("ticker") or "").upper()
        for position in positions
        if str(position.get("price_source") or "") != "market_prices"
    ]

    evidence: list[dict[str, Any]] = []
    evidence.extend(_portfolio_statement_facts(live_portfolio, positions, coverage_ratio, priced_weight_pct, as_of=as_of))
    evidence.extend(
        _numeric_facts(
            [
                ("portfolio.total_value_ars", "Valor total de cartera", live_portfolio.get("total_value_ars"), "ARS", "portfolio"),
                ("portfolio.invested_ars", "Capital invertido", live_portfolio.get("invested_ars"), "ARS", "portfolio"),
                ("portfolio.cash_ars", "Cash disponible", live_portfolio.get("cash_ars"), "ARS", "portfolio"),
                ("portfolio.day_pnl_ars", "PnL estimado del dia", live_portfolio.get("day_pnl_ars"), "ARS", "portfolio"),
                ("portfolio.day_change_pct", "Variacion diaria estimada", live_portfolio.get("day_change_pct"), "ratio", "portfolio"),
                ("coverage.priced_positions_ratio", "Ratio de posiciones con precio de mercado", coverage_ratio, "ratio", "market_data"),
                ("coverage.priced_weight_pct", "Peso valorizado con precio de mercado", priced_weight_pct, "ratio", "market_data"),
            ],
            as_of=as_of,
        )
    )

    top_movers = sorted(
        [position for position in positions if position.get("change_pct_1d") is not None],
        key=lambda position: abs(_safe_float(position.get("change_pct_1d"))),
        reverse=True,
    )[:max_top_movers]
    for position in top_movers:
        ticker = _ticker_id(position.get("ticker"))
        if not ticker:
            continue
        evidence.extend(
            _numeric_facts(
                [
                    (f"position.{ticker}.weight", f"Peso de {ticker.upper()}", position.get("weight_in_portfolio"), "ratio", "portfolio"),
                    (f"position.{ticker}.day_change_pct", f"Variacion diaria de {ticker.upper()}", position.get("change_pct_1d"), "ratio", "market_data"),
                    (f"position.{ticker}.day_pnl_ars", f"PnL diario de {ticker.upper()}", position.get("day_pnl_ars"), "ARS", "portfolio"),
                    (f"position.{ticker}.market_value_ars", f"Valor de mercado de {ticker.upper()}", position.get("market_value"), "ARS", "portfolio"),
                ],
                as_of=as_of,
            )
        )

    warning = str(live_portfolio.get("post_open_warning") or "").strip()
    if warning:
        evidence.append(
            {
                "kind": "event",
                "fact_id": "quality.post_open_warning",
                "title": warning,
                "impact": "high" if coverage_ratio < 0.80 else "medium",
                "source": "internal",
                "event_time": as_of.isoformat(),
            }
        )

    return {
        "schema_version": "1.0.0",
        "packet_type": "market_report",
        "packet_id": packet_id or f"mr_{as_of_art:%Y-%m-%d_%H%M}_qwen_preview",
        "run_id": run_id,
        "as_of": as_of.isoformat(),
        "timezone": "America/Argentina/Buenos_Aires",
        "locale": "es-AR",
        "data_completeness": _decimal_string(max(0.0, min(1.0, coverage_ratio))),
        "policy_version": policy_version,
        "source_snapshot_ids": [str(live_portfolio.get("snapshot_id"))] if live_portfolio.get("snapshot_id") else [],
        "coverage": {
            "priced_weight_pct": _decimal_string(max(0.0, min(1.0, priced_weight_pct))),
            "priced_positions_count": covered_count,
            "positions_count": positions_count,
            "missing_tickers": missing_tickers,
        },
        "top_level_reason_codes": _market_reason_codes(
            day_change=live_portfolio.get("day_change_pct"),
            coverage_ratio=coverage_ratio,
            warning=warning,
        ),
        "evidence_items": _dedupe_by_fact_id(evidence),
    }


def render_market_narrative_preview(narrative: MarketNarrative, packet: Mapping[str, Any]) -> str:
    """Render a human-readable preview without publishing it anywhere."""
    lines = [
        "QWEN DAILY MARKET PREVIEW",
        "=========================",
        "Modo: preview manual read-only; no confirma operaciones.",
        f"Packet: {packet.get('packet_id')} | Modelo: {narrative.model}",
        "",
        narrative.headline,
        "",
        narrative.executive_summary,
    ]
    for section in narrative.sections:
        lines.extend(["", section.title, "-" * min(60, max(3, len(section.title)))])
        lines.append(section.paragraph)
        if section.supporting_fact_ids:
            lines.append("Facts: " + ", ".join(section.supporting_fact_ids))
    if narrative.caveats:
        lines.extend(["", "Caveats", "-------"])
        lines.extend(f"- {item}" for item in narrative.caveats)
    if narrative.insufficiency_flag:
        lines.extend(["", "Estado: REVISION - datos insuficientes o cobertura baja."])
    return "\n".join(lines)


def render_market_packet_statement_preview(packet: Mapping[str, Any]) -> str:
    """Render deterministic statement facts from a MarketReportPacket."""
    lines = [
        "QUANTIA MARKET PREVIEW - FALLBACK DETERMINISTICO",
        "==============================================",
        "Modo: read-only; no confirma operaciones.",
        f"Packet: {packet.get('packet_id')}",
        "",
    ]
    statements = [
        item
        for item in packet.get("evidence_items") or []
        if isinstance(item, Mapping) and item.get("kind") == "statement"
    ]
    if not statements:
        lines.append("Sin statements disponibles en el packet.")
    else:
        lines.extend(f"- {item.get('text')}" for item in statements if item.get("text"))
    return "\n".join(lines)


def _market_reason_codes(*, day_change: Any, coverage_ratio: float, warning: str) -> list[str]:
    codes: list[str] = []
    if coverage_ratio >= 0.95:
        codes.append("coverage_complete")
    elif coverage_ratio >= 0.80:
        codes.append("coverage_partial")
    else:
        codes.append("coverage_low")

    if day_change is None:
        codes.append("portfolio_no_comparable_change")
    else:
        change = _safe_float(day_change)
        if change >= 0.005:
            codes.append("portfolio_positive_open")
        elif change <= -0.005:
            codes.append("portfolio_negative_open")
        else:
            codes.append("portfolio_stable_open")
    if warning:
        codes.append("quality_warning")
    return codes


def _numeric_facts(
    values: list[tuple[str, str, Any, str, str]],
    *,
    as_of: datetime,
) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for fact_id, label, value, unit, source in values:
        if value is None:
            continue
        facts.append(
            {
                "kind": "numeric",
                "fact_id": fact_id,
                "label": label,
                "value": _decimal_string(value),
                "display_value": _display_value(value, unit=unit, fact_id=fact_id),
                "unit": unit,
                "source": source,
                "as_of": as_of.isoformat(),
            }
        )
    return facts


def _portfolio_statement_facts(
    live_portfolio: Mapping[str, Any],
    positions: list[dict[str, Any]],
    coverage_ratio: float,
    priced_weight_pct: float,
    *,
    as_of: datetime,
) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    total = _display_value(live_portfolio.get("total_value_ars"), unit="ARS", fact_id="portfolio.total_value_ars")
    invested = _display_value(live_portfolio.get("invested_ars"), unit="ARS", fact_id="portfolio.invested_ars")
    cash = _display_value(live_portfolio.get("cash_ars"), unit="ARS", fact_id="portfolio.cash_ars")
    day_pnl = _display_value(live_portfolio.get("day_pnl_ars"), unit="ARS", fact_id="portfolio.day_pnl_ars")
    day_change = _display_value(live_portfolio.get("day_change_pct"), unit="ratio", fact_id="portfolio.day_change_pct")
    facts.append(
        _statement_fact(
            "statement.portfolio.overview",
            (
                f"Cartera: total {total}, invertido {invested}, cash {cash}, "
                f"movimiento diario {day_change} ({day_pnl})."
            ),
            supporting_fact_ids=[
                "portfolio.total_value_ars",
                "portfolio.invested_ars",
                "portfolio.cash_ars",
                "portfolio.day_pnl_ars",
                "portfolio.day_change_pct",
            ],
            as_of=as_of,
        )
    )
    facts.append(
        _statement_fact(
            "statement.coverage",
            (
                "Cobertura de precios: "
                f"{coverage_ratio:.0%} de posiciones y {priced_weight_pct:.0%} del peso valorizado "
                "con precios de mercado."
            ),
            supporting_fact_ids=[
                "coverage.priced_positions_ratio",
                "coverage.priced_weight_pct",
            ],
            as_of=as_of,
        )
    )
    top_movers = sorted(
        [position for position in positions if position.get("change_pct_1d") is not None],
        key=lambda position: abs(_safe_float(position.get("change_pct_1d"))),
        reverse=True,
    )[:5]
    for position in top_movers:
        ticker = _ticker_id(position.get("ticker"))
        if not ticker:
            continue
        facts.append(
            _statement_fact(
                f"statement.position.{ticker}.move",
                (
                    f"{ticker.upper()}: movimiento diario "
                    f"{_display_value(position.get('change_pct_1d'), unit='ratio', fact_id=f'position.{ticker}.day_change_pct')} "
                    f"({_display_value(position.get('day_pnl_ars'), unit='ARS', fact_id=f'position.{ticker}.day_pnl_ars')}), "
                    f"peso {_display_value(position.get('weight_in_portfolio'), unit='ratio', fact_id=f'position.{ticker}.weight')}, "
                    f"valor {_display_value(position.get('market_value'), unit='ARS', fact_id=f'position.{ticker}.market_value_ars')}."
                ),
                supporting_fact_ids=[
                    f"position.{ticker}.day_change_pct",
                    f"position.{ticker}.day_pnl_ars",
                    f"position.{ticker}.weight",
                    f"position.{ticker}.market_value_ars",
                ],
                as_of=as_of,
            )
        )
    return facts


def _statement_fact(
    fact_id: str,
    text: str,
    *,
    supporting_fact_ids: list[str],
    as_of: datetime,
) -> dict[str, Any]:
    return {
        "kind": "statement",
        "fact_id": fact_id,
        "text": text,
        "supporting_fact_ids": supporting_fact_ids,
        "source": "portfolio",
        "as_of": as_of.isoformat(),
    }


def _dedupe_by_fact_id(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        fact_id = str(item.get("fact_id") or "").strip()
        if not fact_id or fact_id in seen:
            continue
        seen.add(fact_id)
        out.append(item)
    return out


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


def _decimal_string(value: Any) -> str:
    if value is None:
        return "0"
    try:
        number = float(value)
    except Exception:
        return str(value)
    return f"{number:.10f}".rstrip("0").rstrip(".") or "0"


def _display_value(value: Any, *, unit: str, fact_id: str) -> str:
    try:
        number = float(value)
    except Exception:
        return str(value)
    if unit == "ARS":
        signed = "pnl" in fact_id.lower()
        sign = "+" if signed and number >= 0 else "-" if signed and number < 0 else ""
        amount = abs(number) if signed else number
        return f"{sign}${amount:,.0f} ARS".replace(",", ".")
    if unit == "ratio":
        signed = "change" in fact_id.lower() or "pnl" in fact_id.lower()
        sign = "+" if signed and number >= 0 else ""
        text = f"{number:{sign}.2%}"
        return text.replace(".", ",")
    return _decimal_string(value)


def _ticker_id(value: Any) -> str:
    return re.sub(r"[^a-z0-9_.-]", "", str(value or "").lower())[:32]


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return _coerce_datetime(value).isoformat()
    except Exception:
        return str(value)
