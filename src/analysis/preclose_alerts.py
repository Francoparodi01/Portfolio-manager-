"""Pre-close intraday alerts.

This layer is intentionally parallel to planner/optimizer/decision_log. It
answers a narrower operational question: before the market closes, is there a
position whose next-session gap risk or chase risk deserves attention now?
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from typing import Any

from src.core.telegram_format import header as tg_header, note as tg_note, section as tg_section


PRE_CLOSE_ALERTS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS intraday_preclose_alerts (
    id              BIGSERIAL PRIMARY KEY,
    alert_ts        TIMESTAMPTZ NOT NULL,
    business_date   DATE        NOT NULL,
    slot            TEXT        NOT NULL,
    ticker          TEXT        NOT NULL,
    alert_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    current_price   FLOAT,
    reference_price FLOAT,
    change_pct      FLOAT,
    current_weight  FLOAT,
    reason          TEXT,
    evidence        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'OPEN',
    source          TEXT        NOT NULL DEFAULT 'preclose_v1',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (business_date, slot, ticker, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_intraday_preclose_alerts_lookup
    ON intraday_preclose_alerts (business_date DESC, ticker, alert_type);
"""


@dataclass(frozen=True)
class PrecloseAlert:
    ticker: str
    alert_type: str
    severity: str
    current_price: float
    reference_price: float | None
    change_pct: float
    current_weight: float | None = None
    reason: str = ""
    action: str = ""
    price_ts: datetime | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_record(self, *, alert_ts: datetime, business_date, slot: str) -> dict[str, Any]:
        return {
            "alert_ts": alert_ts,
            "business_date": business_date,
            "slot": slot,
            "ticker": self.ticker,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "current_price": self.current_price,
            "reference_price": self.reference_price,
            "change_pct": self.change_pct,
            "current_weight": self.current_weight,
            "reason": self.reason,
            "evidence": {
                **(self.evidence or {}),
                "action": self.action,
                "price_ts": self.price_ts.isoformat() if self.price_ts else None,
            },
        }


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _normalize_change_pct(value) -> float | None:
    raw = _safe_float(value)
    if raw is None:
        return None
    if abs(raw) > 1.5:
        return raw / 100.0
    return raw


def _parse_ts(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _sentiment_payload(ctx) -> dict[str, Any]:
    if not ctx:
        return {
            "score": 0.0,
            "confidence": 0.0,
            "event_count": 0,
            "high_impact_count": 0,
            "top_summary": "",
        }
    return {
        "score": float(getattr(ctx, "score", 0.0) or 0.0),
        "confidence": float(getattr(ctx, "confidence", 0.0) or 0.0),
        "event_count": int(getattr(ctx, "event_count", 0) or 0),
        "high_impact_count": int(getattr(ctx, "high_impact_count", 0) or 0),
        "top_summary": str(getattr(ctx, "top_summary", "") or "")[:220],
    }


def _price_change(
    ticker: str,
    latest: dict,
    previous_closes: dict[str, float],
) -> tuple[float | None, float | None]:
    current_price = _safe_float(latest.get("last_price"))
    if not current_price or current_price <= 0:
        return None, None

    previous_close = _safe_float(previous_closes.get(ticker))
    if previous_close and previous_close > 0:
        return previous_close, (current_price / previous_close) - 1.0

    change = _normalize_change_pct(latest.get("change_pct_1d"))
    if change is None:
        return None, None
    reference = current_price / (1.0 + change) if abs(1.0 + change) > 0.0001 else None
    return reference, change


def build_preclose_alerts(
    *,
    positions: list[dict],
    latest_prices: list[dict],
    previous_closes: dict[str, float] | None = None,
    total_ars: float | None = None,
    sentiment_contexts: dict[str, Any] | None = None,
    manual_event_risk_by_ticker: dict[str, str] | None = None,
    max_price_age_seconds: int = 20 * 60,
    now: datetime | None = None,
) -> list[PrecloseAlert]:
    """Build pre-close alerts from fresh, auditable inputs.

    Rules are deliberately simple and visible. This is not a score and does not
    create orders.
    """
    now = now or datetime.now()
    previous_closes = previous_closes or {}
    sentiment_contexts = sentiment_contexts or {}
    manual_event_risk_by_ticker = {
        str(k or "").upper(): str(v or "")
        for k, v in (manual_event_risk_by_ticker or {}).items()
        if str(k or "").strip()
    }
    latest_by_ticker = {
        str(row.get("ticker") or "").upper(): row
        for row in latest_prices or []
        if str(row.get("ticker") or "").strip()
    }
    invested = sum(float(p.get("market_value", 0) or 0) for p in positions or [])
    denominator = max(float(total_ars or 0.0), invested, 0.0)

    alerts: list[PrecloseAlert] = []
    for position in positions or []:
        ticker = str(position.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        quantity = _safe_float(position.get("quantity"), 0.0) or 0.0
        market_value = _safe_float(position.get("market_value"), 0.0) or 0.0
        if quantity <= 0 or market_value <= 0:
            continue

        latest = latest_by_ticker.get(ticker)
        if not latest:
            continue

        price_ts = _parse_ts(latest.get("ts"))
        if price_ts and now.tzinfo and price_ts.tzinfo:
            age_seconds = (now - price_ts.astimezone(now.tzinfo)).total_seconds()
            if age_seconds < 0 or age_seconds > max_price_age_seconds:
                continue

        current_price = _safe_float(latest.get("last_price"))
        reference_price, change_pct = _price_change(ticker, latest, previous_closes)
        if current_price is None or change_pct is None:
            continue

        weight = market_value / denominator if denominator > 0 else None
        sentiment = _sentiment_payload(sentiment_contexts.get(ticker))
        manual_event = manual_event_risk_by_ticker.get(ticker, "")
        reasons: list[str] = []
        evidence = {
            "market_value": market_value,
            "quantity": quantity,
            "price_change_source": "previous_close" if reference_price else "change_pct_1d",
            "sentiment": sentiment,
            "manual_event_risk": manual_event,
        }

        score = float(sentiment["score"])
        confidence = float(sentiment["confidence"])
        high_impact = int(sentiment["high_impact_count"])
        concentrated = bool(weight is not None and weight >= 0.20)

        if change_pct <= -0.04:
            reasons.append("caída intradía fuerte antes del cierre")
        elif change_pct <= -0.025 and concentrated:
            reasons.append("posición concentrada con deterioro intradía")
        if score <= -0.12 and confidence >= 0.20:
            reasons.append("sentiment/catalyst negativo reciente")
        if manual_event and change_pct <= -0.01:
            reasons.append("catalyst manual activo con precio débil")

        if reasons:
            severity = "HIGH" if change_pct <= -0.06 or high_impact > 0 or score <= -0.25 else "MEDIUM"
            alerts.append(
                PrecloseAlert(
                    ticker=ticker,
                    alert_type="PRE_CIERRE_SELL_WATCH",
                    severity=severity,
                    current_price=float(current_price),
                    reference_price=reference_price,
                    change_pct=float(change_pct),
                    current_weight=weight,
                    reason="; ".join(reasons),
                    action="Evaluar salida/reducción antes del cierre; no es orden automática.",
                    price_ts=price_ts,
                    evidence=evidence,
                )
            )
            continue

        positive_reasons: list[str] = []
        if 0.015 <= change_pct <= 0.05 and score >= 0.12 and confidence >= 0.20:
            positive_reasons.append("aceleración positiva con sentiment confirmado")
        elif 0.02 <= change_pct <= 0.045 and high_impact > 0:
            positive_reasons.append("evento de alto impacto con precio confirmando")

        if positive_reasons:
            alerts.append(
                PrecloseAlert(
                    ticker=ticker,
                    alert_type="PRE_CIERRE_BUY_WATCH",
                    severity="MEDIUM",
                    current_price=float(current_price),
                    reference_price=reference_price,
                    change_pct=float(change_pct),
                    current_weight=weight,
                    reason="; ".join(positive_reasons),
                    action="Evaluar mantener/agregar solo si la tesis sigue vigente; evitar perseguir gaps.",
                    price_ts=price_ts,
                    evidence=evidence,
                )
            )
            continue

        if change_pct >= 0.06:
            alerts.append(
                PrecloseAlert(
                    ticker=ticker,
                    alert_type="NO_PERSEGUIR",
                    severity="LOW",
                    current_price=float(current_price),
                    reference_price=reference_price,
                    change_pct=float(change_pct),
                    current_weight=weight,
                    reason="suba intradía extendida; riesgo de comprar tarde",
                    action="No perseguir precio al cierre; esperar revalidación.",
                    price_ts=price_ts,
                    evidence=evidence,
                )
            )

    priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    return sorted(
        alerts,
        key=lambda alert: (priority.get(alert.severity, 9), -abs(alert.change_pct)),
    )


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")


def render_preclose_alerts(alerts: list[PrecloseAlert], *, slot: str) -> str:
    lines = tg_header(
        "⚠️ Alerta pre-cierre",
        subtitle=f"{slot} ART · auditoría paralela · no modifica planner",
    )
    if not alerts:
        lines.append("Sin señales pre-cierre relevantes con precio fresco.")
        lines.append("")
        lines.append(tg_note("No se persistieron alertas. Esto no reemplaza el análisis EOD."))
        return "\n".join(lines)

    lines += [
        "Lectura: señal accionable antes de que cierre el mercado. No es orden automática.",
        "",
    ]
    for alert in alerts[:5]:
        weight = f" | peso {alert.current_weight:.1%}" if alert.current_weight is not None else ""
        price_time = alert.price_ts.strftime("%H:%M") if alert.price_ts else "N/A"
        lines += [
            tg_section(alert.ticker),
            f"Tipo: <b>{escape(alert.alert_type)}</b> | severidad <b>{escape(alert.severity)}</b>",
            f"Movimiento: <b>{alert.change_pct:+.2%}</b>{weight}",
            (
                f"Precio: <b>{_fmt_price(alert.current_price)}</b> "
                f"vs ref <b>{_fmt_price(alert.reference_price)}</b> ({escape(price_time)})"
            ),
            f"Motivo: {escape(alert.reason)}",
            f"Acción: <b>{escape(alert.action)}</b>",
        ]
        sentiment = (alert.evidence or {}).get("sentiment") or {}
        summary = str(sentiment.get("top_summary") or "").strip()
        if summary:
            lines.append(f"Contexto: {escape(summary[:180])}")
        lines.append("")

    omitted = max(0, len(alerts) - 5)
    if omitted:
        lines.append(f"+{omitted} alerta(s) omitidas.")
        lines.append("")

    lines.append(tg_note("Se audita contra apertura/cierre siguiente y outcomes 1D/3D/5D. No toca decision_log."))
    return "\n".join(lines)
