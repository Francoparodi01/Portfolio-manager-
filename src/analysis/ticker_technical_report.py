"""Ticker-scoped technical report and chart rendering.

This module is intentionally read-only. It reuses the existing technical
signal engine and renders a Telegram-safe report plus an optional PNG chart.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any
import math
import re

from src.analysis.technical import Signal, analyze_ticker_from_frame, fetch_history
from src.collector.cocos_history import candles_to_frame


MIN_TECHNICAL_CANDLES = 60
DEFAULT_CANDLE_LIMIT = 260


@dataclass(frozen=True)
class TickerPositionContext:
    quantity: float | None = None
    current_price: float | None = None
    market_value_ars: float | None = None
    portfolio_weight: float | None = None
    snapshot_at: Any = None


@dataclass(frozen=True)
class TickerDecisionContext:
    decision_id: int | None = None
    decided_at: Any = None
    decision: str | None = None
    status: str | None = None
    final_score: float | None = None
    source: str | None = None


@dataclass
class TickerTechnicalReport:
    ticker: str
    signal: Signal
    frame: Any
    data_source: str
    asset_type: str | None = None
    currency: str | None = None
    position: TickerPositionContext | None = None
    latest_decision: TickerDecisionContext | None = None
    warnings: list[str] = field(default_factory=list)
    generated_at: datetime | None = None

    @property
    def candle_count(self) -> int:
        return int(len(self.frame) if self.frame is not None else 0)

    @property
    def as_of(self) -> Any:
        if self.frame is None or len(self.frame) == 0:
            return None
        try:
            return self.frame.index[-1]
        except Exception:
            return None


def normalize_ticker(raw: str) -> str:
    ticker = re.sub(r"[^A-Za-z0-9.\-]", "", str(raw or "")).upper().strip()
    if not ticker:
        raise ValueError("ticker vacio")
    if len(ticker) > 24:
        raise ValueError("ticker demasiado largo")
    return ticker


def build_ticker_technical_report(
    ticker: str,
    frame: Any,
    *,
    data_source: str = "market_candles",
    asset_type: str | None = None,
    currency: str | None = None,
    position: TickerPositionContext | None = None,
    latest_decision: TickerDecisionContext | None = None,
    warnings: list[str] | None = None,
) -> TickerTechnicalReport:
    clean_ticker = normalize_ticker(ticker)
    if frame is None or len(frame) < MIN_TECHNICAL_CANDLES:
        raise ValueError(
            f"{clean_ticker}: datos insuficientes "
            f"({len(frame) if frame is not None else 0} velas)"
        )

    signal = analyze_ticker_from_frame(clean_ticker, frame)
    if signal is None:
        raise ValueError(f"{clean_ticker}: no se pudo calcular la senal tecnica")

    if data_source == "yfinance":
        signal.candle_source_mode = "external"
        signal.candle_sources = ("yfinance",)
        signal.candle_source_counts = {"yfinance": int(len(frame))}

    return TickerTechnicalReport(
        ticker=clean_ticker,
        signal=signal,
        frame=frame,
        data_source=data_source,
        asset_type=asset_type,
        currency=currency,
        position=position,
        latest_decision=latest_decision,
        warnings=list(warnings or []),
        generated_at=signal.generated_at,
    )


async def build_ticker_technical_report_from_db(
    db: Any,
    ticker: str,
    *,
    owner_chat_id: int | None = None,
    candle_limit: int = DEFAULT_CANDLE_LIMIT,
    allow_yfinance_fallback: bool = True,
) -> TickerTechnicalReport:
    clean_ticker = normalize_ticker(ticker)
    warnings: list[str] = []

    rows = await db.get_market_candles(clean_ticker, limit=int(candle_limit))
    frame = candles_to_frame(rows)
    data_source = "market_candles"
    asset_type = _first_row_value(rows, "asset_type")
    currency = _first_row_value(rows, "currency")

    if len(frame) < MIN_TECHNICAL_CANDLES and allow_yfinance_fallback:
        warnings.append(
            f"market_candles tiene {len(frame)} velas; se uso fallback externo."
        )
        fallback = fetch_history(clean_ticker, period="1y")
        if fallback is not None and len(fallback) >= MIN_TECHNICAL_CANDLES:
            frame = fallback
            data_source = "yfinance"
            asset_type = None
            currency = "USD"

    position = await _load_position_context(db, clean_ticker, owner_chat_id=owner_chat_id)
    latest_decision = await _load_latest_decision_context(
        db,
        clean_ticker,
        owner_chat_id=owner_chat_id,
    )
    return build_ticker_technical_report(
        clean_ticker,
        frame,
        data_source=data_source,
        asset_type=asset_type,
        currency=currency,
        position=position,
        latest_decision=latest_decision,
        warnings=warnings,
    )


def render_ticker_telegram_report(report: TickerTechnicalReport) -> str:
    signal = report.signal
    stats = _frame_stats(report.frame)
    verdict_title, verdict_detail = _verdict(report, stats)
    operative_regime = _operative_regime(stats, signal)
    horizon_lines = _horizon_lines(stats)
    level_lines = _level_lines(stats)
    scenario_lines = _scenario_lines(report, stats)
    position_lines = _position_lines(report.position)
    decision_lines = _decision_lines(report.latest_decision)
    warning_lines = _data_caveat_lines(report)

    reasons = [
        f"   - {escape(str(reason))}"
        for reason in (signal.reasons or [])[:5]
    ]
    if not reasons:
        reasons = ["   - Sin razon tecnica principal."]

    lines = [
        f"<b>{escape(report.ticker)} - {escape(verdict_title)}</b>",
        "----------------------------",
        escape(verdict_detail),
        "",
        "<b>Lectura operativa</b>",
        f"   Precio: <b>{_fmt_price(signal.price_usd)}</b>",
        f"   Senal tecnica: <b>{escape(signal.signal)}</b> | Score: <code>{signal.score_raw:+.2f}</code>",
        f"   Intensidad tecnica: <b>{signal.strength:.0%}</b> (no es probabilidad de acierto)",
        f"   Regimen cuantitativo: <b>{escape(signal.technical_regime)}</b>",
        f"   Interpretacion: <b>{escape(operative_regime)}</b>",
        f"   Trend: <code>{signal.trend_score:+.3f}</code> | Reversion: <code>{signal.reversion_score:+.3f}</code>",
        "",
        "<b>Lectura por horizonte</b>",
        *horizon_lines,
        "",
        "<b>Niveles a mirar</b>",
        *level_lines,
        "",
        "<b>Razones tecnicas</b>",
        *reasons,
    ]

    if scenario_lines:
        lines += ["", "<b>Escenarios</b>", *scenario_lines]

    if position_lines:
        lines += ["", "<b>Contexto cartera</b>", *position_lines]
    if decision_lines:
        lines += ["", "<b>Ultima decision registrada</b>", *decision_lines]
    if warning_lines:
        lines += ["", "<b>Advertencias</b>", *warning_lines]

    source = _source_label(report.signal, report.data_source)
    lines += [
        "",
        "<b>Datos</b>",
        f"   Fuente: <b>{escape(source)}</b>",
        f"   Velas: <b>{report.candle_count}</b> | Hasta: <b>{escape(_fmt_dt(report.as_of))}</b>",
        "",
        "<i>Read-only: no genera ordenes, no persiste decision_log y no cambia thresholds.</i>",
    ]
    return "\n".join(lines)


def render_ticker_technical_chart(
    report: TickerTechnicalReport,
    output_path: str | Path,
) -> Path:
    from PIL import Image, ImageDraw

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    frame = _chart_frame(report.frame)
    if len(frame) < 2:
        raise ValueError(f"{report.ticker}: no hay datos suficientes para graficar")

    width, height = 1280, 920
    bg = "#0b1117"
    panel = "#111b24"
    text = "#eef6fb"
    muted = "#9fb0bd"
    grid = "#243442"
    close_color = "#f2f5f7"
    sma20_color = "#4cb3ff"
    sma50_color = "#ffb86b"
    sma200_color = "#b88cff"
    volume_color = "#4a677a"
    rsi_color = "#6ee7a8"
    warn_color = "#ffd166"

    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)
    fonts = _chart_fonts()

    signal_color = {
        "BUY": "#6ee7a8",
        "SELL": "#ff6b6b",
        "HOLD": "#ffd166",
    }.get(str(report.signal.signal).upper(), muted)

    chart_verdict, _ = _verdict(report, _frame_stats(report.frame))
    draw.text((54, 36), f"{report.ticker} memo tecnico", fill=text, font=fonts["title"])
    draw.text(
        (56, 88),
        f"{chart_verdict} | score {report.signal.score_raw:+.2f}",
        fill=signal_color,
        font=fonts["body"],
    )
    draw.text(
        (56, 120),
        f"{report.candle_count} candles | as of {_fmt_dt(report.as_of)} | {report.data_source}",
        fill=muted,
        font=fonts["small"],
    )

    price_box = (64, 166, 1216, 570)
    vol_box = (64, 610, 1216, 736)
    rsi_box = (64, 776, 1216, 870)

    _panel(draw, price_box, panel)
    _panel(draw, vol_box, panel)
    _panel(draw, rsi_box, panel)

    close = frame["Close"]
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    rsi = _rsi(close)
    volume = frame["Volume"].fillna(0)

    price_values = []
    for series in (close, sma20, sma50, sma200):
        price_values.extend(
            float(v) for v in series.dropna().tolist() if math.isfinite(float(v))
        )
    min_price, max_price = _scale(price_values, pad=0.08)
    _draw_y_grid(draw, price_box, min_price, max_price, grid, muted, fonts["mono"], _fmt_price)

    _draw_line(draw, price_box, close, min_price, max_price, close_color, width=4)
    _draw_line(draw, price_box, sma20, min_price, max_price, sma20_color, width=2)
    _draw_line(draw, price_box, sma50, min_price, max_price, sma50_color, width=2)
    _draw_line(draw, price_box, sma200, min_price, max_price, sma200_color, width=2)
    _draw_legend(
        draw,
        (88, 184),
        [
            ("Close", close_color),
            ("SMA20", sma20_color),
            ("SMA50", sma50_color),
            ("SMA200", sma200_color),
        ],
        fonts["small"],
        muted,
    )

    _draw_volume(draw, vol_box, volume, volume_color)
    draw.text((88, vol_box[1] + 14), "Volume", fill=muted, font=fonts["small"])

    _draw_rsi(draw, rsi_box, rsi, rsi_color, warn_color, grid, muted, fonts["mono"])
    draw.text((88, rsi_box[1] + 10), "RSI 14", fill=muted, font=fonts["small"])

    image.save(path, "PNG")
    return path


def _frame_stats(frame: Any) -> dict[str, float | None]:
    close = frame["Close"].dropna()
    if close.empty:
        return {}
    high = frame["High"].dropna() if "High" in frame else close
    low = frame["Low"].dropna() if "Low" in frame else close

    def ret(period: int) -> float | None:
        if len(close) <= period:
            return None
        prev = float(close.iloc[-period - 1])
        if prev == 0:
            return None
        return (float(close.iloc[-1]) / prev) - 1.0

    last = float(close.iloc[-1])

    def avg(period: int) -> float | None:
        if len(close) < period:
            return None
        value = float(close.rolling(period).mean().iloc[-1])
        if not math.isfinite(value):
            return None
        return value

    def dist(period: int) -> float | None:
        value = avg(period)
        if value is None or value == 0:
            return None
        return (last / value) - 1.0

    ema12 = _ema_value(close, 12)
    ema26 = _ema_value(close, 26)
    macd_hist, macd_hist_prev = _macd_hist_values(close)
    rsi14 = _rsi_value(close)
    support_low, support_high = _support_zone(low, close)
    resistance_low, resistance_high = _resistance_zone(high, last, avg(20), avg(50))

    return {
        "last": last,
        "ret_5": ret(5),
        "ret_20": ret(20),
        "ret_60": ret(60),
        "sma_20": avg(20),
        "sma_50": avg(50),
        "sma_200": avg(200),
        "dist_sma20": dist(20),
        "dist_sma50": dist(50),
        "dist_sma200": dist(200),
        "ema_12": ema12,
        "ema_26": ema26,
        "macd_hist": macd_hist,
        "macd_hist_prev": macd_hist_prev,
        "rsi_14": rsi14,
        "support_low": support_low,
        "support_high": support_high,
        "resistance_low": resistance_low,
        "resistance_high": resistance_high,
    }


def _ema_value(series: Any, span: int) -> float | None:
    try:
        value = float(series.ewm(span=span, adjust=False).mean().iloc[-1])
        return value if math.isfinite(value) else None
    except Exception:
        return None


def _macd_hist_values(close: Any) -> tuple[float | None, float | None]:
    try:
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        hist = macd - signal
        last = float(hist.dropna().iloc[-1])
        prev = float(hist.dropna().iloc[-2]) if len(hist.dropna()) >= 2 else None
        return (
            last if math.isfinite(last) else None,
            prev if prev is not None and math.isfinite(prev) else None,
        )
    except Exception:
        return None, None


def _rsi_value(close: Any, period: int = 14) -> float | None:
    try:
        value = float(_rsi(close, period).dropna().iloc[-1])
        return value if math.isfinite(value) else None
    except Exception:
        return None


def _support_zone(low: Any, close: Any) -> tuple[float | None, float | None]:
    try:
        recent_low = low.tail(40).dropna()
        recent_close = close.tail(40).dropna()
        if recent_low.empty or recent_close.empty:
            return None, None
        zone_low = float(recent_low.quantile(0.10))
        zone_high = float(recent_close.quantile(0.25))
        if not math.isfinite(zone_low) or not math.isfinite(zone_high):
            return None, None
        if zone_low > zone_high:
            zone_low, zone_high = zone_high, zone_low
        return zone_low, zone_high
    except Exception:
        return None, None


def _resistance_zone(
    high: Any,
    last: float,
    sma20: float | None,
    sma50: float | None,
) -> tuple[float | None, float | None]:
    candidates = [
        value
        for value in (sma20, sma50)
        if value is not None and math.isfinite(value) and value > last
    ]
    if candidates:
        return min(candidates), max(candidates)
    try:
        recent_high = float(high.tail(20).quantile(0.75))
        if math.isfinite(recent_high) and recent_high > last:
            candidates.append(recent_high)
    except Exception:
        pass
    if not candidates:
        return None, None
    return min(candidates), max(candidates)


def _verdict(
    report: TickerTechnicalReport,
    stats: dict[str, float | None],
) -> tuple[str, str]:
    signal = str(report.signal.signal or "").upper()
    has_position = _has_position(report.position)
    medium = _medium_state(stats)
    short = _short_state(stats)
    last_sell_executed = (
        report.latest_decision is not None
        and str(report.latest_decision.decision or "").upper().startswith("SELL")
        and str(report.latest_decision.status or "").upper() == "EXECUTED"
    )

    if has_position:
        if signal == "SELL":
            title = "reducir o salir segun plan"
            detail = "La senal tecnica esta deteriorada para una posicion abierta."
        elif signal == "BUY":
            title = "mantener; agregar solo con plan"
            detail = "La estructura tecnica acompana, pero el tamano debe decidirse fuera de este reporte."
        else:
            title = "mantener sin agregar"
            detail = "La accion no tiene confirmacion suficiente para aumentar exposicion."
    else:
        if signal == "BUY":
            title = "evaluar entrada controlada"
            detail = "Hay senal tecnica favorable, pero debe validarse contra cartera, liquidez y riesgo."
        elif signal == "SELL":
            title = "evitar entrada / no reingresar"
            detail = "La accion no esta en cartera y la senal tecnica sigue negativa."
        elif medium == "Correctivo" or short == "Bajista":
            title = "esperar / no abrir posicion todavia"
            detail = "La tendencia estructural puede seguir viva, pero el timing de corto plazo no confirma entrada."
        else:
            title = "esperar confirmacion"
            detail = "No hay senal operativa clara para abrir posicion."

    if last_sell_executed and not has_position:
        detail += " La ultima venta registrada queda alineada con control de riesgo."
    return title, detail


def _operative_regime(stats: dict[str, float | None], signal: Signal) -> str:
    long_state = _long_state(stats)
    medium_state = _medium_state(stats)
    short_state = _short_state(stats)
    ret60 = _num(stats.get("ret_60")) or 0.0
    dist200 = _num(stats.get("dist_sma200")) or 0.0
    ret20 = _num(stats.get("ret_20")) or 0.0

    if long_state == "Alcista" and medium_state == "Correctivo" and short_state == "Bajista":
        return "Correccion de alta volatilidad dentro de tendencia estructural alcista."
    if long_state == "Alcista" and ret60 > 0.30 and dist200 > 0.30 and ret20 < 0:
        return "Correccion profunda despues de una suba extraordinaria."
    if long_state == "Alcista" and short_state == "Alcista":
        return "Tendencia alcista con momentum favorable."
    if long_state == "Bajista" or (medium_state == "Bajista" and short_state == "Bajista"):
        return "Tendencia bajista; priorizar preservacion de capital."
    if str(signal.technical_regime or "").upper() == "RANGE":
        return "Rango operativo; exigir ruptura o piso confirmado."
    return "Transicion tecnica; esperar confirmacion."


def _horizon_lines(stats: dict[str, float | None]) -> list[str]:
    return [
        f"   Largo plazo: <b>{_long_state(stats)}</b> - SMA200 {_fmt_pct(stats.get('dist_sma200'))}, 60r {_fmt_pct(stats.get('ret_60'))}.",
        f"   Medio plazo: <b>{_medium_state(stats)}</b> - SMA20 {_fmt_pct(stats.get('dist_sma20'))}, SMA50 {_fmt_pct(stats.get('dist_sma50'))}, 20r {_fmt_pct(stats.get('ret_20'))}.",
        f"   Corto plazo: <b>{_short_state(stats)}</b> - {_ema_relation(stats)}, MACD {_macd_label(stats)}, RSI {_fmt_number(stats.get('rsi_14'), decimals=1)}.",
    ]


def _level_lines(stats: dict[str, float | None]) -> list[str]:
    lines = []
    support = _fmt_range(stats.get("support_low"), stats.get("support_high"))
    resistance = _fmt_range(stats.get("resistance_low"), stats.get("resistance_high"))
    if resistance != "N/A":
        lines.append(f"   Resistencia / recuperacion: <b>{resistance}</b>")
    else:
        lines.append("   Resistencia / recuperacion: <b>sin techo inmediato por medias</b>")
    if support != "N/A":
        lines.append(f"   Soporte observado: <b>{support}</b>")
    else:
        lines.append("   Soporte observado: <b>sin zona clara en ultimas ruedas</b>")
    lines.append(
        f"   Medias: SMA20 {_fmt_price(stats.get('sma_20'))} | "
        f"SMA50 {_fmt_price(stats.get('sma_50'))} | "
        f"SMA200 {_fmt_price(stats.get('sma_200'))}"
    )
    return lines


def _scenario_lines(
    report: TickerTechnicalReport,
    stats: dict[str, float | None],
) -> list[str]:
    resistance = _fmt_range(stats.get("resistance_low"), stats.get("resistance_high"))
    support = _fmt_range(stats.get("support_low"), stats.get("support_high"))
    has_position = _has_position(report.position)
    lines: list[str] = []

    if not has_position:
        if resistance != "N/A":
            lines.append(f"   Entrada por momentum: recuperar y sostener <b>{resistance}</b>.")
        if support != "N/A":
            lines.append(f"   Entrada agresiva: piso confirmado sobre <b>{support}</b>, con volumen comprador.")
            lines.append(f"   Invalidacion: perdida clara de <b>{support}</b>.")
        if not lines:
            lines.append("   Permanecer fuera hasta que aparezca ruptura o piso medible.")
    else:
        if resistance != "N/A":
            lines.append(f"   Agregar: solo si recupera <b>{resistance}</b> con momentum.")
        if support != "N/A":
            lines.append(f"   Riesgo: revisar exposicion si pierde <b>{support}</b>.")
        if not lines:
            lines.append("   Mantener tamano; no hay nivel operativo claro para agregar.")

    return lines


def _data_caveat_lines(report: TickerTechnicalReport) -> list[str]:
    lines = [f"   - {escape(w)}" for w in report.warnings[:4]]
    asset_type = str(report.asset_type or "").upper()
    currency = str(report.currency or "").upper()
    if asset_type == "CEDEAR":
        lines.append(
            "   - CEDEAR/precio local: este reporte no separa subyacente USD, CCL y liquidez todavia."
        )
    elif currency and currency != "ARS":
        lines.append(f"   - Moneda de la serie: {escape(currency)}.")
    if report.signal.has_reconstructed_candles:
        lines.append("   - La serie mezcla velas oficiales con velas internas reconstruidas.")
    return lines


def _long_state(stats: dict[str, float | None]) -> str:
    dist200 = _num(stats.get("dist_sma200"))
    ret60 = _num(stats.get("ret_60"))
    if dist200 is not None and dist200 > 0.15 and (ret60 is None or ret60 > 0):
        return "Alcista"
    if dist200 is not None and dist200 < -0.08:
        return "Bajista"
    return "Neutral"


def _medium_state(stats: dict[str, float | None]) -> str:
    ret20 = _num(stats.get("ret_20"))
    dist20 = _num(stats.get("dist_sma20"))
    dist50 = _num(stats.get("dist_sma50"))
    if (ret20 is not None and ret20 < -0.10) or (
        dist20 is not None and dist20 < -0.03 and dist50 is not None and dist50 < 0
    ):
        return "Correctivo"
    if ret20 is not None and ret20 > 0.05 and dist20 is not None and dist20 > 0:
        return "Alcista"
    if dist20 is not None and dist20 < -0.08 and dist50 is not None and dist50 < -0.08:
        return "Bajista"
    return "Mixto"


def _short_state(stats: dict[str, float | None]) -> str:
    ema12 = _num(stats.get("ema_12"))
    ema26 = _num(stats.get("ema_26"))
    hist = _num(stats.get("macd_hist"))
    if ema12 is not None and ema26 is not None and ema12 < ema26 and (hist is None or hist < 0):
        return "Bajista"
    if ema12 is not None and ema26 is not None and ema12 > ema26 and (hist is None or hist > 0):
        return "Alcista"
    return "Mixto"


def _ema_relation(stats: dict[str, float | None]) -> str:
    ema12 = _num(stats.get("ema_12"))
    ema26 = _num(stats.get("ema_26"))
    if ema12 is None or ema26 is None:
        return "N/A"
    return "EMA12 > EMA26" if ema12 > ema26 else "EMA12 < EMA26"


def _macd_label(stats: dict[str, float | None]) -> str:
    hist = _num(stats.get("macd_hist"))
    prev = _num(stats.get("macd_hist_prev"))
    if hist is None:
        return "N/A"
    direction = "positivo" if hist > 0 else "negativo" if hist < 0 else "neutral"
    if prev is None:
        return direction
    accel = "mejorando" if hist > prev else "deteriorando" if hist < prev else "estable"
    return f"{direction}, {accel}"


def _has_position(position: TickerPositionContext | None) -> bool:
    if position is None:
        return False
    qty = _num(position.quantity)
    value = _num(position.market_value_ars)
    return bool((qty is not None and qty > 0) or (value is not None and value > 0))


def _first_row_value(rows: list[dict], key: str) -> str | None:
    for row in rows or []:
        value = row.get(key)
        if value is not None:
            return str(value)
    return None


async def _load_position_context(
    db: Any,
    ticker: str,
    *,
    owner_chat_id: int | None,
) -> TickerPositionContext | None:
    try:
        snap = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
    except Exception:
        return None
    if not snap:
        return None
    positions = snap.get("positions") or []
    match = next(
        (
            p for p in positions
            if str(p.get("ticker") or "").upper().strip() == ticker
        ),
        None,
    )
    if not match:
        return None
    market_value = _num(match.get("market_value"))
    total = _num(snap.get("total_value_ars"))
    weight = None
    if market_value is not None and total and total > 0:
        weight = market_value / total
    return TickerPositionContext(
        quantity=_num(
            match.get("quantity")
            if match.get("quantity") is not None
            else match.get("nominals")
        ),
        current_price=_num(match.get("current_price")),
        market_value_ars=market_value,
        portfolio_weight=weight,
        snapshot_at=snap.get("scraped_at") or snap.get("timestamp"),
    )


async def _load_latest_decision_context(
    db: Any,
    ticker: str,
    *,
    owner_chat_id: int | None,
) -> TickerDecisionContext | None:
    try:
        pool = await db.get_pool()
    except Exception:
        return None
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, decided_at, decision, status, final_score, source
                FROM decision_log
                WHERE ticker = $1
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                ORDER BY decided_at DESC, id DESC
                LIMIT 1
                """,
                ticker,
                owner_chat_id,
            )
    except Exception:
        return None
    if not row:
        return None
    return TickerDecisionContext(
        decision_id=int(row["id"]) if row["id"] is not None else None,
        decided_at=row["decided_at"],
        decision=str(row["decision"]) if row["decision"] is not None else None,
        status=str(row["status"]) if row["status"] is not None else None,
        final_score=_num(row["final_score"]),
        source=str(row["source"]) if row["source"] is not None else None,
    )


def _position_lines(position: TickerPositionContext | None) -> list[str]:
    if position is None:
        return ["   No esta en el ultimo snapshot de cartera."]
    lines = []
    if position.quantity is not None:
        lines.append(f"   Cantidad: <b>{position.quantity:g}</b>")
    if position.current_price is not None:
        lines.append(f"   Precio cartera: <b>{_fmt_price(position.current_price)}</b>")
    if position.market_value_ars is not None:
        weight = f" ({_fmt_pct(position.portfolio_weight)})" if position.portfolio_weight is not None else ""
        lines.append(f"   Valor: <b>{_fmt_money(position.market_value_ars)}</b>{weight}")
    if position.snapshot_at:
        lines.append(f"   Snapshot: <b>{escape(_fmt_dt(position.snapshot_at))}</b>")
    return lines or ["   Posicion detectada sin valores normalizados."]


def _decision_lines(decision: TickerDecisionContext | None) -> list[str]:
    if decision is None:
        return []
    chunks = []
    if decision.decision_id is not None:
        chunks.append(f"#{decision.decision_id}")
    if decision.decision:
        chunks.append(escape(decision.decision))
    if decision.status:
        chunks.append(escape(decision.status))
    if decision.final_score is not None:
        chunks.append(f"score {decision.final_score:+.3f}")
    if decision.source:
        chunks.append(escape(decision.source))
    line = " | ".join(chunks) if chunks else "sin detalle"
    return [
        f"   {line}",
        f"   Fecha: <b>{escape(_fmt_dt(decision.decided_at))}</b>",
    ]


def _source_label(signal: Signal, fallback: str) -> str:
    counts = signal.candle_source_counts or {}
    if counts:
        detail = ", ".join(
            f"{escape(str(source))} {int(count)}"
            for source, count in sorted(counts.items())
        )
        return f"{signal.candle_source_mode} ({detail})"
    return fallback


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except Exception:
        return None


def _fmt_price(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    text = f"{number:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"${text}"


def _fmt_money(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    return f"${number:,.0f} ARS".replace(",", ".")


def _fmt_pct(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    return f"{number:+.1%}"


def _fmt_number(value: Any, *, decimals: int = 2) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    return f"{number:.{decimals}f}"


def _fmt_range(low: Any, high: Any) -> str:
    lo = _num(low)
    hi = _num(high)
    if lo is None or hi is None:
        return "N/A"
    if lo > hi:
        lo, hi = hi, lo
    if abs(hi - lo) <= max(1.0, abs(lo) * 0.002):
        return _fmt_price((lo + hi) / 2.0)
    return f"{_fmt_price(lo)} - {_fmt_price(hi)}"


def _fmt_dt(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return str(value)


def _chart_fonts() -> dict[str, object]:
    from PIL import ImageFont

    regular = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ]
    bold = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    mono = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationMono-Regular.ttf",
        "C:/Windows/Fonts/consola.ttf",
    ]

    def load(size: int, candidates: list[str]):
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
        try:
            return ImageFont.load_default(size=size)
        except TypeError:
            return ImageFont.load_default()

    return {
        "title": load(40, bold),
        "body": load(24, bold),
        "small": load(18, regular),
        "mono": load(16, mono),
    }


def _chart_frame(frame: Any) -> Any:
    out = frame.copy()
    out = out[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    return out.tail(180)


def _panel(draw: Any, box: tuple[int, int, int, int], fill: str) -> None:
    draw.rounded_rectangle(box, radius=12, fill=fill)


def _scale(values: list[float], *, pad: float = 0.05) -> tuple[float, float]:
    finite = [v for v in values if math.isfinite(v)]
    if not finite:
        return 0.0, 1.0
    lo = min(finite)
    hi = max(finite)
    if abs(hi - lo) < 1e-9:
        return lo - 1.0, hi + 1.0
    margin = (hi - lo) * pad
    return lo - margin, hi + margin


def _map_point(
    box: tuple[int, int, int, int],
    index: int,
    total: int,
    value: float,
    min_value: float,
    max_value: float,
) -> tuple[int, int]:
    x1, y1, x2, y2 = box
    width = max(1, x2 - x1 - 48)
    height = max(1, y2 - y1 - 58)
    left = x1 + 24
    top = y1 + 38
    n = max(1, total - 1)
    span = max(max_value - min_value, 1e-9)
    x = left + int((index / n) * width)
    y = top + int((1.0 - ((value - min_value) / span)) * height)
    return x, y


def _draw_line(
    draw: Any,
    box: tuple[int, int, int, int],
    series: Any,
    min_value: float,
    max_value: float,
    color: str,
    *,
    width: int,
) -> None:
    values = [float(v) if _num(v) is not None else math.nan for v in series.tolist()]
    points: list[tuple[int, int]] = []
    for idx, value in enumerate(values):
        if math.isfinite(value):
            points.append(_map_point(box, idx, len(values), value, min_value, max_value))
            continue
        if len(points) >= 2:
            draw.line(points, fill=color, width=width, joint="curve")
        points = []
    if len(points) >= 2:
        draw.line(points, fill=color, width=width, joint="curve")


def _draw_y_grid(
    draw: Any,
    box: tuple[int, int, int, int],
    min_value: float,
    max_value: float,
    grid: str,
    muted: str,
    font: Any,
    formatter,
) -> None:
    x1, y1, x2, y2 = box
    for step in range(5):
        ratio = step / 4
        y = y1 + 38 + int(ratio * (y2 - y1 - 58))
        draw.line((x1 + 24, y, x2 - 24, y), fill=grid, width=1)
        value = max_value - ratio * (max_value - min_value)
        draw.text((x2 - 145, y - 18), formatter(value), fill=muted, font=font)


def _draw_legend(
    draw: Any,
    xy: tuple[int, int],
    items: list[tuple[str, str]],
    font: Any,
    muted: str,
) -> None:
    x, y = xy
    for label, color in items:
        draw.rounded_rectangle((x, y + 5, x + 24, y + 17), radius=4, fill=color)
        draw.text((x + 32, y), label, fill=muted, font=font)
        x += 128


def _draw_volume(
    draw: Any,
    box: tuple[int, int, int, int],
    volume: Any,
    color: str,
) -> None:
    values = [max(float(v), 0.0) if _num(v) is not None else 0.0 for v in volume.tolist()]
    max_value = max(values) if values else 0.0
    if max_value <= 0:
        return
    x1, y1, x2, y2 = box
    left = x1 + 24
    bottom = y2 - 22
    width = max(1, x2 - x1 - 48)
    bar_w = max(1, width // max(1, len(values)))
    for idx, value in enumerate(values):
        x = left + int((idx / max(1, len(values))) * width)
        h = int((value / max_value) * (y2 - y1 - 58))
        draw.rectangle((x, bottom - h, x + bar_w, bottom), fill=color)


def _rsi(close: Any, period: int = 14) -> Any:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))


def _draw_rsi(
    draw: Any,
    box: tuple[int, int, int, int],
    rsi: Any,
    color: str,
    warn_color: str,
    grid: str,
    muted: str,
    font: Any,
) -> None:
    x1, y1, x2, y2 = box
    for level in (30, 50, 70):
        y = y1 + 28 + int((1.0 - level / 100.0) * (y2 - y1 - 46))
        draw.line((x1 + 24, y, x2 - 24, y), fill=warn_color if level in (30, 70) else grid, width=1)
        draw.text((x2 - 58, y - 12), str(level), fill=muted, font=font)
    _draw_line(draw, box, rsi, 0.0, 100.0, color, width=3)
