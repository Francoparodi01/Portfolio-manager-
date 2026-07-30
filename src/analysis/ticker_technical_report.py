"""Ticker-scoped technical report and chart rendering.

This module is intentionally read-only. It reuses the existing technical
signal engine and renders a Telegram-safe report plus optional PNG charts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any
import logging
import math
import re

from src.analysis.technical import Signal, analyze_ticker_from_frame, fetch_history
from src.collector.cocos_history import candles_to_frame


MIN_TECHNICAL_CANDLES = 60
DEFAULT_CANDLE_LIMIT = 260
PRICE_LEVEL_LABEL_COLLISION_PCT = 0.15
PRICE_LEVEL_LABEL_MIN_AXIS_GAP = 0.12
PRICE_LEVEL_LABEL_MAX_AXIS_GAP = 0.16
SMA_OVERLAY_ALPHA = 0.52
SMA_OVERLAY_ZORDER = 0.8

logger = logging.getLogger(__name__)


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

    _log_volume_quality(clean_ticker, frame, data_source=data_source)

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
    operative_signal = _operative_signal_label(report)
    recovery = _fmt_level_range(stats.get("resistance_low"), stats.get("resistance_high"))
    support_immediate, support_critical = _support_level_labels(stats)
    scenario = _scenario_sentence(report, stats)
    horizon_lines = _horizon_lines(stats)
    position_lines = _position_lines(report.position)
    decision_lines = _decision_lines(report.latest_decision)
    warning_lines = _data_caveat_lines(report)
    thesis_lines = _thesis_lines(stats)
    opinion_change_lines = _opinion_change_lines(stats)
    risk_lines = _risk_lines(report, stats)
    confidence_lines = _confidence_lines(report, stats)

    lines = [
        f"<b>{escape(report.ticker)} - {escape(verdict_title)}</b>",
        escape(verdict_detail),
        "",
        f"Precio: <b>{_fmt_level(signal.price_usd)}</b>",
        f"Señal operativa: <b>{escape(operative_signal)}</b>",
        f"Score técnico: <code>{signal.score_raw:+.2f}</code> | "
        f"Intensidad: <b>{signal.strength:.0%}</b> <i>(no es probabilidad)</i>",
        "",
        f"Recuperación: <b>{recovery}</b>",
        f"Soporte inmediato: <b>{support_immediate}</b>",
        f"Soporte crítico: <b>{support_critical}</b>",
        "",
        f"Escenario: {escape(scenario)}",
    ]

    if thesis_lines:
        lines += ["", "<b>Tesis</b>", *thesis_lines]
    if opinion_change_lines:
        lines += ["", "<b>Qué cambiaría la visión</b>", *opinion_change_lines]
    if risk_lines:
        lines += ["", "<b>Riesgos actuales</b>", *risk_lines]

    lines += [
        "",
        "<b>Datos del análisis</b>",
        *horizon_lines,
        f"   Señal técnica interna: <b>{escape(signal.signal)}</b> | "
        f"Régimen cuantitativo: <b>{escape(signal.technical_regime)}</b>",
        f"   Trend: <code>{signal.trend_score:+.3f}</code> | "
        f"Reversión: <code>{signal.reversion_score:+.3f}</code>",
        f"   Medias: SMA20 {_fmt_level(stats.get('sma_20'))} | "
        f"SMA50 {_fmt_level(stats.get('sma_50'))} | "
        f"SMA200 {_fmt_level(stats.get('sma_200'))}",
    ]

    if position_lines:
        lines += ["", "<b>Contexto cartera</b>", *position_lines]
    if decision_lines:
        lines += ["", "<b>Última decisión registrada</b>", *decision_lines]
    if warning_lines:
        lines += ["", "<b>Advertencias</b>", *warning_lines]

    source = _source_label(report.signal, report.data_source)
    lines += [
        "",
        "<b>Datos</b>",
        f"   Fuente: <b>{escape(source)}</b>",
        f"   Velas: <b>{report.candle_count}</b> | Hasta: <b>{escape(_fmt_dt(report.as_of))}</b>",
        "   Modo: <b>read-only</b> - no genera órdenes, no persiste decision_log y no cambia thresholds.",
    ]
    if confidence_lines:
        lines += ["", "<b>Confianza argumentada</b>", *confidence_lines]
    return "\n".join(lines)


def render_ticker_technical_chart(
    report: TickerTechnicalReport,
    output_path: str | Path,
) -> Path:
    return render_ticker_technical_charts(report, output_path)[0]


def render_ticker_technical_charts(
    report: TickerTechnicalReport,
    output_path: str | Path,
) -> list[Path]:
    price_path = _chart_output_path(output_path)
    momentum_path = _chart_output_path(output_path, suffix="momentum")
    _render_price_volume_chart(report, price_path)
    _render_momentum_chart(report, momentum_path)
    return [price_path, momentum_path]


def _chart_output_path(output_path: str | Path, *, suffix: str | None = None) -> Path:
    path = Path(output_path)
    if not path.suffix:
        path = path.with_suffix(".png")
    if suffix:
        path = path.with_name(f"{path.stem}_{suffix}{path.suffix}")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _render_price_volume_chart(report: TickerTechnicalReport, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import mplfinance as mpf
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FuncFormatter

    frame = _chart_frame(report.frame, limit=260)
    if len(frame) < 2:
        raise ValueError(f"{report.ticker}: no hay datos suficientes para graficar")

    stats = _frame_stats(report.frame)
    signal_color = _signal_color(report.signal.signal)
    sma20_color = "#4cb3ff"
    sma50_color = "#ffb86b"
    sma200_color = "#b88cff"

    sma_specs = [
        (20, "SMA20", sma20_color, 1.05, "--"),
        (50, "SMA50", sma50_color, 1.05, "-"),
        (200, "SMA200", sma200_color, 1.8, "-"),
    ]
    add_plots = [
        mpf.make_addplot(
            series,
            color=color,
            width=width,
            alpha=SMA_OVERLAY_ALPHA,
            linestyle=linestyle,
            label=label,
        )
        for period, label, color, width, linestyle in sma_specs
        for series in [_moving_average_series(frame, period)]
        if not series.dropna().empty
    ]

    fig, axes = mpf.plot(
        frame,
        type="candle",
        style=_mpf_style(),
        addplot=add_plots,
        volume=True,
        panel_ratios=(4.2, 1.15),
        figratio=(16, 10),
        figscale=1.18,
        datetime_format="%d/%m/%y",
        xrotation=0,
        tight_layout=False,
        returnfig=True,
        warn_too_much_data=10000,
    )
    fig.set_dpi(155)
    fig.set_facecolor("#0b1117")
    fig.subplots_adjust(left=0.08, right=0.955, top=0.775, bottom=0.095, hspace=0.08)
    fig.suptitle(
        f"{report.ticker} memo técnico",
        x=0.035,
        y=0.982,
        ha="left",
        color="#eef6fb",
        fontsize=20,
        fontweight="bold",
    )
    fig.text(
        0.035,
        0.925,
        f"{_verdict(report, stats)[0]} | score {report.signal.score_raw:+.2f}",
        color=signal_color,
        fontsize=12,
        fontweight="bold",
    )
    fig.text(
        0.035,
        0.895,
        f"{len(frame)} velas | {_fmt_dt(frame.index[0])} a {_fmt_dt(frame.index[-1])} | {report.data_source}",
        color="#9fb0bd",
        fontsize=9,
    )

    price_ax = axes[0]
    volume_ax = axes[2] if len(axes) > 2 else axes[-1]
    for ax in (price_ax, volume_ax):
        _style_mpl_axis(ax)
    _style_moving_average_lines(price_ax, {label for _period, label, _color, _width, _linestyle in sma_specs})
    price_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_axis_money(value)))
    volume_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: _fmt_axis_volume(value)))
    price_ax.set_ylabel("Precio", color="#9fb0bd")
    volume_ax.set_ylabel("Volumen", color="#9fb0bd")
    legend_handles = [
        Line2D(
            [0],
            [0],
            color=color,
            lw=max(1.5, width),
            alpha=0.9,
            linestyle=linestyle,
            label=_sma_legend_label(frame, period, label),
        )
        for period, label, color, width, linestyle in sma_specs
        if not _moving_average_series(frame, period).dropna().empty
    ]
    if legend_handles:
        price_ax.legend(
            handles=legend_handles,
            loc="upper left",
            bbox_to_anchor=(0.01, 0.98),
            ncol=min(3, len(legend_handles)),
            facecolor="#101820",
            edgecolor="#22313c",
            labelcolor="#d9e6ee",
            framealpha=0.9,
            fontsize=7.8,
            borderpad=0.35,
            handlelength=1.8,
            columnspacing=1.0,
        )

    _annotate_price_levels(
        price_ax,
        [
            (stats.get("support_low"), "soporte crítico", "#ff6b6b"),
            (stats.get("resistance_low"), "recuperación", "#ffd166"),
        ],
    )
    _annotate_volume_gap(volume_ax, stats)

    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)


def _mpf_style() -> Any:
    import mplfinance as mpf

    market_colors = mpf.make_marketcolors(
        up="#6ee7a8",
        down="#ff6b6b",
        edge="inherit",
        wick="inherit",
        volume={"up": "#376f62", "down": "#744755"},
    )
    return mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=market_colors,
        facecolor="#0f1a23",
        figcolor="#0b1117",
        gridcolor="#263541",
        gridstyle="-",
        y_on_right=False,
        rc={
            "axes.labelcolor": "#9fb0bd",
            "font.size": 9,
            "text.color": "#eef6fb",
            "xtick.color": "#9fb0bd",
            "ytick.color": "#9fb0bd",
        },
    )


def _signal_color(signal: str) -> str:
    return {
        "BUY": "#6ee7a8",
        "SELL": "#ff6b6b",
        "HOLD": "#ffd166",
    }.get(str(signal or "").upper(), "#9fb0bd")


def _style_mpl_axis(ax: Any) -> None:
    ax.set_facecolor("#0f1a23")
    ax.set_axisbelow(True)
    ax.grid(True, color="#263541", linewidth=0.7, alpha=0.85, zorder=0)
    ax.tick_params(colors="#9fb0bd", labelsize=8.5)
    for spine in ax.spines.values():
        spine.set_color("#22313c")


def _style_moving_average_lines(ax: Any, labels: set[str]) -> None:
    for line in getattr(ax, "lines", []):
        if str(line.get_label()) not in labels:
            continue
        line.set_zorder(SMA_OVERLAY_ZORDER)
        line.set_alpha(SMA_OVERLAY_ALPHA)


def _moving_average_series(frame: Any, period: int) -> Any:
    close = frame["Close"].astype("float64")
    series = close.rolling(period, min_periods=period).mean()
    return series.where(close.notna())


def _last_finite_value(series: Any) -> float | None:
    try:
        clean = series.dropna()
    except Exception:
        return None
    if clean.empty:
        return None
    value = _num(clean.iloc[-1])
    return value if value is not None else None


def _sma_legend_label(frame: Any, period: int, label: str) -> str:
    value = _last_finite_value(_moving_average_series(frame, period))
    if value is None:
        return label
    return f"{label} {_fmt_axis_money(value)}"


def _level_label_layout(
    levels: list[tuple[float, str, str]],
    ylim: tuple[float, float],
) -> list[tuple[float, float, str, str]]:
    if not levels:
        return []
    low, high = sorted((float(ylim[0]), float(ylim[1])))
    span = max(high - low, 1.0)
    min_axis_gap = span * PRICE_LEVEL_LABEL_MIN_AXIS_GAP
    cleaned = sorted(levels, key=lambda item: item[0])
    label_y = [float(level[0]) for level in cleaned]

    for idx in range(1, len(label_y)):
        previous_level = float(cleaned[idx - 1][0])
        current_level = float(cleaned[idx][0])
        relative_gap = abs(current_level - previous_level) / max(abs(current_level), 1.0)
        if relative_gap < PRICE_LEVEL_LABEL_COLLISION_PCT:
            value_gap = max(abs(previous_level), abs(current_level), 1.0) * PRICE_LEVEL_LABEL_COLLISION_PCT
            required_gap = max(min_axis_gap, min(value_gap, span * PRICE_LEVEL_LABEL_MAX_AXIS_GAP))
        else:
            required_gap = min_axis_gap * 0.7
        if label_y[idx] - label_y[idx - 1] < required_gap:
            label_y[idx] = label_y[idx - 1] + required_gap

    overflow = label_y[-1] - (high - span * 0.02)
    if overflow > 0:
        label_y = [value - overflow for value in label_y]
    underflow = (low + span * 0.02) - label_y[0]
    if underflow > 0:
        label_y = [value + underflow for value in label_y]

    return [
        (float(level[0]), float(adjusted), level[1], level[2])
        for level, adjusted in zip(cleaned, label_y)
    ]


def _fmt_axis_money(value: Any) -> str:
    number = _num(value)
    if number is None:
        return ""
    if abs(number) >= 1_000_000:
        return f"${number / 1_000_000:.1f}M"
    if abs(number) >= 1_000:
        return f"${number / 1_000:.0f}k"
    return f"${number:.0f}"


def _fmt_axis_volume(value: Any) -> str:
    number = _num(value)
    if number is None:
        return ""
    if abs(number) >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if abs(number) >= 1_000:
        return f"{number / 1_000:.0f}k"
    return f"{number:.0f}"


def _annotate_price_level(ax: Any, value: Any, label: str, color: str) -> None:
    number = _num(value)
    if number is None:
        return
    ax.axhline(number, color=color, linewidth=1.0, linestyle="--", alpha=0.75)
    ax.text(
        0.995,
        number,
        f" {label} {_fmt_level(number)}",
        transform=ax.get_yaxis_transform(),
        color=color,
        fontsize=8.3,
        ha="right",
        va="bottom",
        bbox={
            "facecolor": "#0b1117",
            "edgecolor": color,
            "alpha": 0.72,
            "boxstyle": "round,pad=0.25",
        },
    )


def _annotate_price_levels(
    ax: Any,
    levels: list[tuple[Any, str, str]],
) -> None:
    cleaned: list[tuple[float, str, str]] = []
    for value, label, color in levels:
        number = _num(value)
        if number is None:
            continue
        cleaned.append((number, label, color))
        ax.axhline(number, color=color, linewidth=1.0, linestyle="--", alpha=0.75)

    for number, label_y, label, color in _level_label_layout(cleaned, ax.get_ylim()):
        va = "center" if abs(label_y - number) > 1e-9 else "bottom"
        ax.text(
            0.995,
            label_y,
            f" {label} {_fmt_level(number)}",
            transform=ax.get_yaxis_transform(),
            color=color,
            fontsize=8.1,
            ha="right",
            va=va,
            bbox={
                "facecolor": "#0b1117",
                "edgecolor": color,
                "alpha": 0.74,
                "boxstyle": "round,pad=0.24",
            },
        )


def _annotate_volume_gap(ax: Any, stats: dict[str, float | None]) -> None:
    missing = int(_num(stats.get("trailing_missing_volume")) or 0)
    if missing < 3:
        return
    ax.text(
        0.985,
        0.88,
        f"{missing} velas sin volumen",
        transform=ax.transAxes,
        ha="right",
        va="top",
        color="#ffd166",
        fontsize=8.5,
        bbox={
            "facecolor": "#101820",
            "edgecolor": "#375061",
            "alpha": 0.88,
            "boxstyle": "round,pad=0.35",
        },
    )


def _macd_series(close: Any) -> tuple[Any, Any, Any]:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal, macd - signal


def _render_momentum_chart(report: TickerTechnicalReport, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    frame = _chart_frame(report.frame, limit=260)
    if len(frame) < 2:
        raise ValueError(f"{report.ticker}: no hay datos suficientes para graficar")

    close = frame["Close"]
    dates = frame.index
    rsi = _rsi(close)
    macd_line, macd_signal, macd_hist = _macd_series(close)

    fig, (rsi_ax, macd_ax) = plt.subplots(
        2,
        1,
        sharex=True,
        figsize=(13.2, 8.0),
        dpi=155,
        gridspec_kw={"height_ratios": [1.0, 1.15], "hspace": 0.14},
    )
    fig.set_facecolor("#0b1117")
    for ax in (rsi_ax, macd_ax):
        _style_mpl_axis(ax)

    fig.suptitle(
        f"{report.ticker} momentum",
        x=0.045,
        y=0.975,
        ha="left",
        color="#eef6fb",
        fontsize=20,
        fontweight="bold",
    )
    fig.text(
        0.045,
        0.932,
        f"RSI {_fmt_number(_rsi_value(close), decimals=1)} | MACD {_macd_label(_frame_stats(frame))}",
        color="#9fb0bd",
        fontsize=10,
    )

    rsi_ax.plot(dates, rsi, color="#6ee7a8", linewidth=1.7, label="RSI 14")
    rsi_ax.fill_between(dates, 70, 100, color="#ff6b6b", alpha=0.08)
    rsi_ax.fill_between(dates, 0, 30, color="#4cb3ff", alpha=0.08)
    rsi_ax.set_ylim(0, 100)
    rsi_ax.set_ylabel("RSI", color="#9fb0bd")
    for level, color in ((70, "#ffd166"), (50, "#6c7c88"), (30, "#ffd166")):
        rsi_ax.axhline(level, color=color, linewidth=0.85, alpha=0.95)
        rsi_ax.text(
            1.006,
            level,
            str(level),
            transform=rsi_ax.get_yaxis_transform(),
            color="#c6d2dc",
            fontsize=8.5,
            va="center",
            ha="left",
        )
    rsi_ax.legend(loc="upper left", facecolor="#101820", edgecolor="#22313c", labelcolor="#d9e6ee")

    hist_colors = ["#6ee7a8" if value >= 0 else "#ff6b6b" for value in macd_hist.fillna(0)]
    macd_ax.bar(dates, macd_hist, color=hist_colors, alpha=0.45, width=0.85, label="Histograma")
    macd_ax.plot(dates, macd_line, color="#4cb3ff", linewidth=1.5, label="MACD")
    macd_ax.plot(dates, macd_signal, color="#ffb86b", linewidth=1.3, label="Señal")
    macd_ax.axhline(0, color="#6c7c88", linewidth=0.9)
    macd_ax.set_ylabel("MACD", color="#9fb0bd")
    macd_ax.legend(
        loc="upper left",
        ncol=3,
        facecolor="#101820",
        edgecolor="#22313c",
        labelcolor="#d9e6ee",
        fontsize=8.5,
    )
    macd_ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=9))
    macd_ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m/%y"))
    macd_ax.tick_params(axis="x", rotation=0)

    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.18)
    plt.close(fig)


def _frame_stats(frame: Any) -> dict[str, float | None]:
    close = frame["Close"].dropna()
    if close.empty:
        return {}
    high = frame["High"].dropna() if "High" in frame else close
    low = frame["Low"].dropna() if "Low" in frame else close
    volume = frame["Volume"] if "Volume" in frame else None

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
        "trailing_missing_volume": _trailing_missing_volume(volume),
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


def _trailing_missing_volume(volume: Any) -> float:
    if volume is None:
        return 0.0
    try:
        count = 0
        for value in reversed(volume.tolist()):
            number = _num(value)
            if number is not None and number > 0:
                break
            count += 1
        return float(count)
    except Exception:
        return 0.0


def _log_volume_quality(ticker: str, frame: Any, *, data_source: str) -> None:
    stats = _frame_stats(frame)
    missing = int(_num(stats.get("trailing_missing_volume")) or 0)
    if missing < 3:
        return
    source_counts = getattr(frame, "attrs", {}).get("candle_source_counts", {}) or {}
    logger.warning(
        "%s: %d trailing candles without reported volume in %s; sources=%s. "
        "No automatic volume fallback applied because external volume may not match local CEDEAR/ARS prices.",
        ticker,
        missing,
        data_source,
        source_counts,
    )


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
            title = "reducir o salir según plan"
            detail = "La señal técnica está deteriorada para una posición abierta."
        elif signal == "BUY":
            title = "mantener; agregar solo con plan"
            detail = "La estructura técnica acompaña, pero el tamaño debe decidirse fuera de este reporte."
        else:
            title = "mantener sin agregar"
            detail = "La acción no tiene confirmación suficiente para aumentar exposición."
    else:
        if signal == "BUY":
            title = "evaluar entrada controlada"
            detail = "Hay señal técnica favorable, pero debe validarse contra cartera, liquidez y riesgo."
        elif signal == "SELL":
            title = "evitar entrada / no reingresar"
            detail = "La acción no está en cartera y la señal técnica sigue negativa."
        elif medium == "Correctivo" or short == "Bajista":
            title = "esperar / no abrir posición"
            if _long_state(stats) == "Alcista":
                detail = (
                    "La tendencia estructural sigue alcista, pero el corto plazo "
                    "permanece bajista. No hay confirmación de entrada."
                )
            else:
                detail = "El corto plazo no confirma entrada."
        else:
            title = "esperar confirmación"
            detail = "No hay señal operativa clara para abrir posición."

    if last_sell_executed and not has_position:
        detail += " La última venta registrada queda alineada con control de riesgo."
    return title, detail


def _operative_signal_label(report: TickerTechnicalReport) -> str:
    signal = str(report.signal.signal or "").upper()
    has_position = _has_position(report.position)
    if has_position:
        if signal == "SELL":
            return "REDUCIR / SALIR"
        if signal == "BUY":
            return "MANTENER / EVALUAR AGREGAR"
        return "MANTENER"
    if signal == "BUY":
        return "ENTRADA A EVALUAR"
    return "SIN ENTRADA"


def _support_level_labels(stats: dict[str, float | None]) -> tuple[str, str]:
    critical = _num(stats.get("support_low"))
    observed_high = _num(stats.get("support_high"))
    if critical is None or observed_high is None:
        return "N/A", "N/A"
    immediate_low = max(critical, _round_level(observed_high * 0.97))
    return _fmt_level_range(immediate_low, observed_high), _fmt_level(critical)


def _scenario_sentence(
    report: TickerTechnicalReport,
    stats: dict[str, float | None],
) -> str:
    recovery = _fmt_level_range(stats.get("resistance_low"), stats.get("resistance_high"))
    _support_immediate, support_critical = _support_level_labels(stats)
    has_position = _has_position(report.position)
    if not has_position:
        if recovery != "N/A" and support_critical != "N/A":
            return (
                "considerar entrada si recupera las medias o confirma un piso "
                f"con volumen. Invalidación: cierre sostenido debajo de {support_critical}."
            )
        if recovery != "N/A":
            return "considerar entrada solo si recupera las medias con volumen."
        return "permanecer fuera hasta que aparezca ruptura o piso medible."
    if support_critical != "N/A":
        return (
            "mantener tamaño mientras respete soporte; revisar exposición ante "
            f"cierre sostenido debajo de {support_critical}."
        )
    return "mantener tamaño; no hay nivel operativo claro para agregar."


def _horizon_lines(stats: dict[str, float | None]) -> list[str]:
    return [
        f"   Largo plazo: <b>{_long_state(stats)}</b> - SMA200 {_fmt_pct(stats.get('dist_sma200'))}, 60r {_fmt_pct(stats.get('ret_60'))}.",
        f"   Medio plazo: <b>{_medium_state(stats)}</b> - SMA20 {_fmt_pct(stats.get('dist_sma20'))}, SMA50 {_fmt_pct(stats.get('dist_sma50'))}, 20r {_fmt_pct(stats.get('ret_20'))}.",
        f"   Corto plazo: <b>{_short_state(stats)}</b> - {escape(_ema_relation(stats))}, MACD {escape(_macd_label(stats))}, RSI {_fmt_number(stats.get('rsi_14'), decimals=1)}.",
    ]


def _thesis_lines(stats: dict[str, float | None]) -> list[str]:
    long_state = _long_state(stats)
    medium_state = _medium_state(stats)
    short_state = _short_state(stats)
    last = _num(stats.get("last"))
    support = _num(stats.get("support_low"))

    lines: list[str] = []
    if long_state == "Alcista":
        lines.append("   ✓ Tendencia primaria intacta")
    elif long_state == "Bajista":
        lines.append("   △ Tendencia primaria deteriorada")
    else:
        lines.append("   • Tendencia primaria neutral")

    if medium_state == "Correctivo" and long_state == "Alcista":
        lines.append("   ✓ Corrección dentro de estructura")
    elif medium_state == "Correctivo":
        lines.append("   △ Corrección de medio plazo")
    elif medium_state == "Alcista":
        lines.append("   ✓ Medio plazo acompaña")
    else:
        lines.append("   • Medio plazo mixto")

    if short_state == "Bajista":
        lines.append("   △ Momentum corto plazo deteriorado")
    elif short_state == "Alcista":
        lines.append("   ✓ Momentum corto plazo favorable")
    else:
        lines.append("   • Momentum corto plazo mixto")

    if last is not None and support is not None and last > support:
        lines.append("   ✓ No hay evidencia de cambio estructural")
    elif last is not None and support is not None:
        lines.append("   △ Soporte crítico bajo presión")
    return lines


def _opinion_change_lines(stats: dict[str, float | None]) -> list[str]:
    last = _num(stats.get("last"))
    support = _num(stats.get("support_low"))
    resistance = _num(stats.get("resistance_low"))
    if last is None:
        return []

    lines: list[str] = []
    if resistance is not None and resistance > last:
        lines.append(
            f"   ▲ Recuperar {_fmt_level(resistance)} "
            f"({_fmt_signed_money(resistance - last)}, {_fmt_pct((resistance / last) - 1.0)})"
        )
    elif resistance is not None:
        lines.append(f"   ▲ Sostener precio sobre {_fmt_level(resistance)}")

    if lines and support is not None and support < last:
        lines.append("   o")

    if support is not None and support < last:
        lines.append(
            f"   ▼ Perder {_fmt_level(support)} "
            f"({_fmt_signed_money(support - last)}, {_fmt_pct((support / last) - 1.0)})"
        )
    elif support is not None:
        lines.append(f"   ▼ Recuperar soporte crítico {_fmt_level(support)}")

    return lines


def _risk_lines(report: TickerTechnicalReport, stats: dict[str, float | None]) -> list[str]:
    risks: list[str] = []
    if _short_state(stats) == "Bajista":
        risks.append("   • Momentum negativo")
    if _medium_state(stats) == "Correctivo":
        risks.append("   • Corrección de medio plazo")
    if int(_num(stats.get("trailing_missing_volume")) or 0) >= 3:
        risks.append("   • Falta volumen reciente")
    if str(report.asset_type or "").upper() == "CEDEAR":
        risks.append("   • CEDEAR: no separa subyacente USD, CCL y liquidez todavía")
    if not risks:
        risks.append("   • Sin riesgo técnico dominante detectado")
    return risks[:4]


def _confidence_lines(
    report: TickerTechnicalReport,
    stats: dict[str, float | None],
) -> list[str]:
    result = _result_label(report)
    follow_up = _follow_up_sentence(report, stats)
    lines = ["¿Por qué el sistema piensa esto?"]

    if _long_state(stats) == "Alcista":
        lines.append("   ✓ Tendencia primaria alcista")
    elif _long_state(stats) == "Bajista":
        lines.append("   △ Tendencia primaria bajista")
    else:
        lines.append("   • Tendencia primaria neutral")

    if _medium_state(stats) == "Correctivo" and _long_state(stats) == "Alcista":
        lines.append("   ✓ Corrección dentro de estructura")
    elif _medium_state(stats) == "Alcista":
        lines.append("   ✓ Medio plazo constructivo")
    elif _medium_state(stats) == "Correctivo":
        lines.append("   △ Corrección de medio plazo")

    if _short_state(stats) == "Bajista":
        lines.append("   △ Momentum corto plazo deteriorado")
    elif _short_state(stats) == "Alcista":
        lines.append("   ✓ Momentum corto plazo favorable")

    if int(_num(stats.get("trailing_missing_volume")) or 0) >= 3:
        lines.append("   △ Volumen reciente incompleto")

    lines += [
        f"Resultado: <b>{escape(result)}</b>.",
        escape(follow_up),
    ]
    return lines


def _result_label(report: TickerTechnicalReport) -> str:
    signal = str(report.signal.signal or "").upper()
    has_position = _has_position(report.position)
    if has_position:
        if signal == "SELL":
            return "Reducir o salir según plan"
        if signal == "BUY":
            return "Mantener; agregar solo con plan"
        return "Mantener"
    if signal == "BUY":
        return "Evaluar entrada controlada"
    return "No abrir posición"


def _follow_up_sentence(
    report: TickerTechnicalReport,
    stats: dict[str, float | None],
) -> str:
    has_position = _has_position(report.position)
    support = _fmt_level(stats.get("support_low"))
    recovery = _fmt_level_range(stats.get("resistance_low"), stats.get("resistance_high"))
    if not has_position:
        if recovery != "N/A":
            return "Agregar solo si recupera medias o confirma piso con volumen."
        return "Esperar confirmación antes de tomar riesgo."
    if support != "N/A":
        return f"Mantener mientras respete {support}; revisar exposición si lo pierde."
    return "Mantener tamaño y reevaluar si cambia el momentum."


def _data_caveat_lines(report: TickerTechnicalReport) -> list[str]:
    lines = [f"   - {escape(w)}" for w in report.warnings[:4]]
    asset_type = str(report.asset_type or "").upper()
    currency = str(report.currency or "").upper()
    stats = _frame_stats(report.frame)
    trailing_missing_volume = int(_num(stats.get("trailing_missing_volume")) or 0)
    if asset_type == "CEDEAR":
        lines.append(
            "   - CEDEAR/precio local: este reporte no separa subyacente USD, CCL y liquidez todavía."
        )
    elif currency and currency != "ARS":
        lines.append(f"   - Moneda de la serie: {escape(currency)}.")
    if report.signal.has_reconstructed_candles:
        lines.append("   - La serie mezcla velas oficiales con velas internas reconstruidas.")
    if trailing_missing_volume >= 3:
        lines.append(
            f"   - Volumen: faltan datos informados en las últimas {trailing_missing_volume} velas."
        )
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
        return ["   No está en el último snapshot de cartera."]
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


def _fmt_signed_money(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    sign = "+" if number >= 0 else "-"
    return f"{sign}${abs(number):,.0f}".replace(",", ".")


def _fmt_level(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "N/A"
    return f"${number:,.0f}".replace(",", ".")


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


def _fmt_level_range(low: Any, high: Any) -> str:
    lo = _num(low)
    hi = _num(high)
    if lo is None or hi is None:
        return "N/A"
    if lo > hi:
        lo, hi = hi, lo
    if abs(hi - lo) <= max(1.0, abs(lo) * 0.002):
        return _fmt_level((lo + hi) / 2.0)
    return f"{_fmt_level(lo)}–{_fmt_level(hi)}"


def _round_level(value: float) -> float:
    number = _num(value)
    if number is None:
        return value
    if abs(number) >= 100_000:
        return round(number / 1_000.0) * 1_000.0
    if abs(number) >= 10_000:
        return round(number / 100.0) * 100.0
    return round(number)


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


def _chart_frame(frame: Any, *, limit: int = 260) -> Any:
    out = frame.copy()
    if "Close" not in out:
        raise ValueError("frame sin columna Close")
    for column in ("Open", "High", "Low"):
        if column not in out:
            out[column] = out["Close"]
        else:
            out[column] = out[column].fillna(out["Close"])
    if "Volume" not in out:
        out["Volume"] = 0.0
    else:
        out["Volume"] = out["Volume"].fillna(0.0)
    out = out[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    try:
        out = out.sort_index()
    except Exception:
        pass
    return out.tail(int(limit))


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
    muted: str,
    warn_color: str,
    font: Any,
) -> None:
    values = [max(float(v), 0.0) if _num(v) is not None else 0.0 for v in volume.tolist()]
    max_value = max(values) if values else 0.0
    if max_value <= 0:
        x1, y1, _x2, _y2 = box
        draw.text((x1 + 24, y1 + 52), "Sin volumen informado.", fill=muted, font=font)
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

    missing = int(_trailing_missing_volume(volume))
    if missing >= 3:
        start_idx = max(0, len(values) - missing)
        x_start = left + int((start_idx / max(1, len(values))) * width)
        draw.rectangle((x_start, y1 + 34, x2 - 24, bottom), outline="#375061", width=1)
        draw.text(
            (max(x_start + 8, x2 - 270), y1 + 44),
            f"{missing} velas sin volumen",
            fill=warn_color,
            font=font,
        )


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
    inner_top = y1 + 34
    inner_bottom = y2 - 22
    plot_height = max(1, inner_bottom - inner_top)
    for level in (30, 50, 70):
        y = inner_top + int((1.0 - level / 100.0) * plot_height)
        draw.line((x1 + 24, y, x2 - 24, y), fill=warn_color if level in (30, 70) else grid, width=1)
        draw.text((x2 - 64, y - 8), str(level), fill=muted, font=font)
    _draw_line(draw, box, rsi, 0.0, 100.0, color, width=3)
