"""
scripts/weekly_summary.py
Genera un resumen semanal del portfolio comparando snapshots de la semana.

Uso:
    python scripts/weekly_summary.py                  # semana actual
    python scripts/weekly_summary.py --weeks-ago 1    # semana pasada
    python scripts/weekly_summary.py --no-telegram    # solo stdout
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)

TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _ars(x: float) -> str:
    try:
        return f"${float(x):,.0f}".replace(",", ".")
    except Exception:
        return "$0"


def _pct(x: float) -> str:
    try:
        return f"{float(x) * 100:+.2f}%"
    except Exception:
        return "0.00%"


def _arrow(x: float) -> str:
    if x > 0.005:  return "📈"
    if x < -0.005: return "📉"
    return "➡️"


def _week_range(weeks_ago: int = 0) -> tuple[datetime, datetime]:
    today  = datetime.now(tz=TZ).date()
    monday = today - timedelta(days=today.weekday()) - timedelta(weeks=weeks_ago)
    friday = monday + timedelta(days=4)
    start  = datetime(monday.year, monday.month, monday.day, 0,  0,  0,  tzinfo=TZ)
    end    = datetime(friday.year, friday.month, friday.day, 23, 59, 59, tzinfo=TZ)
    return start, end


def _week_label(weeks_ago: int = 0) -> str:
    start, end = _week_range(weeks_ago)
    return f"{start.strftime('%d/%m')} – {end.strftime('%d/%m/%Y')}"


def _parse_ts(ts) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=TZ) if ts.tzinfo is None else ts.astimezone(TZ)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.replace(tzinfo=TZ) if dt.tzinfo is None else dt.astimezone(TZ)
    except Exception:
        return None


async def _get_week_snapshots(db: PortfolioDatabase, weeks_ago: int = 0) -> list[dict]:
    start, end = _week_range(weeks_ago)
    history    = await db.get_portfolio_history(limit=200)
    week_snaps = []
    for snap in history:
        ts = _parse_ts(snap.get("timestamp") or snap.get("scraped_at") or snap.get("created_at"))
        if ts and start <= ts <= end:
            snap["_ts"] = ts
            week_snaps.append(snap)
    week_snaps.sort(key=lambda s: s["_ts"])
    return week_snaps


def _pos_map(snapshot: dict) -> dict[str, dict]:
    return {
        str(p.get("ticker", "")).upper(): p
        for p in (snapshot.get("positions") or [])
        if p.get("ticker")
    }


def _compare(snap_start: dict, snap_end: dict) -> dict:
    total_s = float(snap_start.get("total_value_ars", 0) or 0)
    total_e = float(snap_end.get("total_value_ars",   0) or 0)
    cash_e  = float(snap_end.get("cash_ars",          0) or 0)
    total_return = (total_e - total_s) / total_s if total_s > 0 else 0.0
    total_delta  = total_e - total_s

    pos_s = _pos_map(snap_start)
    pos_e = _pos_map(snap_end)
    positions = []

    for ticker in sorted(set(pos_s) | set(pos_e)):
        ps = pos_s.get(ticker, {})
        pe = pos_e.get(ticker, {})
        px_s  = float(ps.get("current_price", 0) or 0)
        px_e  = float(pe.get("current_price", 0) or 0)
        val_s = float(ps.get("market_value",  0) or 0)
        val_e = float(pe.get("market_value",  0) or 0)
        qty_s = float(ps.get("quantity",      0) or 0)
        qty_e = float(pe.get("quantity",      0) or 0)
        price_ret = (px_e - px_s) / px_s if px_s > 0 else 0.0

        if ticker not in pos_s:     status = "NUEVA"
        elif ticker not in pos_e:   status = "CERRADA"
        elif qty_e > qty_s * 1.02:  status = "AUMENTADA"
        elif qty_e < qty_s * 0.98:  status = "REDUCIDA"
        else:                        status = "SIN CAMBIO"

        positions.append({
            "ticker":    ticker,
            "px_s":      px_s,
            "px_e":      px_e,
            "price_ret": price_ret,
            "val_s":     val_s,
            "val_e":     val_e,
            "val_delta": val_e - val_s,
            "qty_s":     qty_s,
            "qty_e":     qty_e,
            "status":    status,
            "weight":    val_e / total_e if total_e > 0 else 0.0,
        })

    positions.sort(key=lambda x: abs(x["val_delta"]), reverse=True)
    active = [p for p in positions if p["status"] != "CERRADA" and p["px_s"] > 0]
    best   = max(active, key=lambda x: x["price_ret"]) if active else None
    worst  = min(active, key=lambda x: x["price_ret"]) if active else None

    return {
        "total_s": total_s, "total_e": total_e,
        "total_delta": total_delta, "total_return": total_return,
        "cash_e": cash_e, "positions": positions,
        "best": best, "worst": worst,
        "ts_start": snap_start.get("_ts"), "ts_end": snap_end.get("_ts"),
    }


def _render(c: dict, week_label: str, n_snaps: int) -> str:
    h   = []
    ret = c["total_return"]
    icon = "🟢" if ret > 0.01 else ("🔴" if ret < -0.01 else "🟡")
    sign = "+" if c["total_delta"] >= 0 else ""

    h.append(f"📅 <b>RESUMEN SEMANAL — {week_label}</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"🕐 {datetime.now(tz=TZ).strftime('%d/%m/%Y %H:%M')} ART")
    ts_s, ts_e = c.get("ts_start"), c.get("ts_end")
    if ts_s and ts_e:
        h.append(f"📌 {ts_s.strftime('%a %d/%m %H:%M')} → {ts_e.strftime('%a %d/%m %H:%M')}")
    h.append(f"🔍 Snapshots de la semana: {n_snaps}")
    h.append("")

    h.append("<b>RESULTADO DE LA SEMANA</b>")
    h.append(f"{icon} Portfolio: <b>{_ars(c['total_e'])}</b> ({_pct(ret)} | {sign}{_ars(c['total_delta'])})")
    h.append(f"   Inicio: {_ars(c['total_s'])}")
    h.append(f"   Cash:   {_ars(c['cash_e'])}")
    h.append("")

    best, worst = c.get("best"), c.get("worst")
    if best or worst:
        h.append("<b>DESTACADOS</b>")
        if best and best["price_ret"] > 0:
            h.append(f"🏆 Mejor: <b>{best['ticker']}</b> {_pct(best['price_ret'])} ({_ars(best['px_s'])} → {_ars(best['px_e'])})")
        if worst and worst["price_ret"] < 0:
            h.append(f"💀 Peor:  <b>{worst['ticker']}</b> {_pct(worst['price_ret'])} ({_ars(worst['px_s'])} → {_ars(worst['px_e'])})")
        h.append("")

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>DETALLE POR POSICIÓN</b>")
    h.append("")

    status_icon = {"NUEVA": "🆕", "CERRADA": "❌", "AUMENTADA": "⬆️", "REDUCIDA": "⬇️", "SIN CAMBIO": ""}
    for p in c["positions"]:
        tag = status_icon.get(p["status"], "")
        h.append(f"{_arrow(p['price_ret'])} <b>{p['ticker']}</b> {tag}")
        if p["px_s"] > 0 and p["px_e"] > 0:
            h.append(f"   Precio: {_ars(p['px_s'])} → <b>{_ars(p['px_e'])}</b> ({_pct(p['price_ret'])})")
        if p["val_e"] > 0:
            d_str = f" ({'+' if p['val_delta'] >= 0 else ''}{_ars(p['val_delta'])})" if p["val_s"] > 0 else ""
            h.append(f"   Valor: <b>{_ars(p['val_e'])}</b>{d_str} | Peso: {p['weight']:.1%}")
        if p["status"] == "CERRADA":
            h.append("   <i>Posición cerrada esta semana</i>")
        h.append("")

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if ret > 0.02:
        h.append("✅ <b>Semana positiva.</b>")
    elif ret < -0.02:
        h.append("⚠️ <b>Semana negativa.</b> Revisar posiciones con mayor drawdown.")
    else:
        h.append("➡️ <b>Semana lateral.</b> Variación dentro del ruido normal.")
    h.append("")
    h.append("<i>Resumen automático — Cocos Copilot</i>")
    return "\n".join(h)


async def generate_weekly_summary(weeks_ago: int = 0, no_telegram: bool = False) -> str:
    cfg = get_config()
    db  = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        week_snaps = await _get_week_snapshots(db, weeks_ago=weeks_ago)

        if not week_snaps:
            logger.warning("Sin snapshots esta semana — usando historial reciente")
            history = await db.get_portfolio_history(limit=50)
            if len(history) < 2:
                return "⚠️ Sin suficientes snapshots. Corré un scrape primero."
            now = datetime.now(tz=TZ)
            ref = None
            for snap in reversed(history):
                ts = _parse_ts(snap.get("timestamp") or snap.get("scraped_at"))
                if ts and (now - ts).days >= 5:
                    snap["_ts"] = ts
                    ref = snap
                    break
            snap_start = ref or history[-1]
            snap_end   = history[0]
            snap_end["_ts"]   = now
            snap_start.setdefault("_ts", now - timedelta(days=7))
            week_snaps = [snap_start, snap_end]

        elif len(week_snaps) == 1:
            history = await db.get_portfolio_history(limit=50)
            ref_ts  = week_snaps[0]["_ts"]
            for snap in history:
                ts = _parse_ts(snap.get("timestamp") or snap.get("scraped_at"))
                if ts and ts < ref_ts:
                    snap["_ts"] = ts
                    week_snaps.insert(0, snap)
                    break

        comparison = _compare(week_snaps[0], week_snaps[-1])
        week_label = _week_label(weeks_ago)
        report     = _render(comparison, week_label, n_snaps=len(week_snaps))

        logger.info(f"Resumen semanal {week_label} — retorno {comparison['total_return']:+.2%} | {len(week_snaps)} snapshots")

        if not no_telegram and cfg.scraper.telegram_enabled:
            TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(report)

        return report
    finally:
        await db.close()


async def main() -> None:
    p = argparse.ArgumentParser(description="Resumen semanal del portfolio")
    p.add_argument("--weeks-ago",   type=int, default=0)
    p.add_argument("--no-telegram", action="store_true")
    args = p.parse_args()
    print(await generate_weekly_summary(weeks_ago=args.weeks_ago, no_telegram=args.no_telegram))


if __name__ == "__main__":
    asyncio.run(main())