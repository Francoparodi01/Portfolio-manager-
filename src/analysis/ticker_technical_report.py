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

    if len(frame) < MIN_TECHNICAL_CANDLES and allow_yfinance_fallback:
        warnings.append(
            f"market_candles tiene {len(frame)} velas; se uso fallback externo."
        )
        fallback = fetch_history(clean_ticker, period="1y")
        if fallback is not None and len(fallback) >= MIN_TECHNICAL_CANDLES:
            frame = fallback
            data_source = "yfinance"

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
        position=position,
        latest_decision=latest_decision,
        warnings=warnings,
    )


def render_ticker_telegram_report(report: TickerTechnicalReport) -> str:
    signal = report.signal
    stats = _frame_stats(report.frame)
    position_lines = _position_lines(report.position)
    decision_lines = _decision_lines(report.latest_decision)
    warning_lines = [f"   - {escape(w)}" for w in report.warnings[:4]]

    reasons = [
        f"   - {escape(str(reason))}"
        for reason in (signal.reasons or [])[:5]
    ]
    if not reasons:
        reasons = ["   - Sin razon tecnica principal."]

    lines = [
        f"<b>Analisis por accion: {escape(report.ticker)}</b>",
        "----------------------------",
        f"Senal: <b>{escape(signal.signal)}</b> | Score tecnico: <code>{signal.score_raw:+.2f}</code>",
        f"Precio: <b>{_fmt_price(signal.price_usd)}</b> | Fuerza: <b>{signal.strength:.0%}</b>",
        f"Regimen: <b>{escape(signal.technical_regime)}</b> | Trend: <code>{signal.trend_score:+.3f}</code> | Reversion: <code>{signal.reversion_score:+.3f}</code>",
        "",
        "<b>Retornos y medias</b>",
        f"   5r {_fmt_pct(stats.get('ret_5'))} | 20r {_fmt_pct(stats.get('ret_20'))} | 60r {_fmt_pct(stats.get('ret_60'))}",
        f"   vs SMA20 {_fmt_pct(stats.get('dist_sma20'))} | SMA50 {_fmt_pct(stats.get('dist_sma50'))} | SMA200 {_fmt_pct(stats.get('dist_sma200'))}",
        "",
        "<b>Razones tecnicas</b>",
        *reasons,
    ]

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

    draw.text((54, 36), f"{report.ticker} technical report", fill=text, font=fonts["title"])
    draw.text(
        (56, 88),
        f"{report.signal.signal} | score {report.signal.score_raw:+.2f} | {report.signal.technical_regime}",
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

    def ret(period: int) -> float | None:
        if len(close) <= period:
            return None
        prev = float(close.iloc[-period - 1])
        if prev == 0:
            return None
        return (float(close.iloc[-1]) / prev) - 1.0

    last = float(close.iloc[-1])

    def dist(period: int) -> float | None:
        if len(close) < period:
            return None
        avg = float(close.rolling(period).mean().iloc[-1])
        if avg == 0 or not math.isfinite(avg):
            return None
        return (last / avg) - 1.0

    return {
        "ret_5": ret(5),
        "ret_20": ret(20),
        "ret_60": ret(60),
        "dist_sma20": dist(20),
        "dist_sma50": dist(50),
        "dist_sma200": dist(200),
    }


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
