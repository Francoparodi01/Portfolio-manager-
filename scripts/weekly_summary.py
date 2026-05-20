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
        value = float(x)
        sign = "-" if value < 0 else ""
        return f"{sign}${abs(value):,.0f}".replace(",", ".")
    except Exception:
        return "$0"


def _pct(x: float) -> str:
    try:
        return f"{float(x) * 100:+.2f}%"
    except Exception:
        return "0.00%"


def _arrow(x: float) -> str:
    if x > 0.005:
        return "📈"
    if x < -0.005:
        return "📉"
    return "➡️"


def _week_range(weeks_ago: int = 0) -> tuple[datetime, datetime]:
    today = datetime.now(tz=TZ).date()
    monday = today - timedelta(days=today.weekday()) - timedelta(weeks=weeks_ago)
    friday = monday + timedelta(days=4)

    start = datetime(monday.year, monday.month, monday.day, 0, 0, 0, tzinfo=TZ)
    end = datetime(friday.year, friday.month, friday.day, 23, 59, 59, tzinfo=TZ)

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


async def _get_week_snapshots(
    db: PortfolioDatabase,
    weeks_ago: int = 0,
    owner_chat_id: int | None = None,
) -> list[dict]:
    start, end = _week_range(weeks_ago)
    history = await db.get_portfolio_history(limit=200, owner_chat_id=owner_chat_id)

    week_snaps = []

    for snap in history:
        ts = _parse_ts(
            snap.get("timestamp")
            or snap.get("scraped_at")
            or snap.get("created_at")
        )

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
    """
    Compara dos snapshots.

    IMPORTANTE:
    - gross_total_delta / gross_total_return:
      variación bruta de cartera. Incluye cambios de cantidad, cash,
      compras, ventas, retiros o snapshots parciales.
    - price_pnl / price_return:
      estimación del P&L puro por movimiento de precios.
      Esto evita interpretar una venta parcial como pérdida de mercado.
    """
    total_s = float(snap_start.get("total_value_ars", 0) or 0)
    total_e = float(snap_end.get("total_value_ars", 0) or 0)

    cash_s = float(snap_start.get("cash_ars", 0) or 0)
    cash_e = float(snap_end.get("cash_ars", 0) or 0)

    gross_total_delta = total_e - total_s
    gross_total_return = gross_total_delta / total_s if total_s > 0 else 0.0
    cash_delta = cash_e - cash_s

    pos_s = _pos_map(snap_start)
    pos_e = _pos_map(snap_end)

    positions = []
    total_price_pnl = 0.0
    total_quantity_effect = 0.0
    changed_positions = 0

    for ticker in sorted(set(pos_s) | set(pos_e)):
        ps = pos_s.get(ticker, {})
        pe = pos_e.get(ticker, {})

        px_s = float(ps.get("current_price", 0) or 0)
        px_e = float(pe.get("current_price", 0) or 0)

        val_s = float(ps.get("market_value", 0) or 0)
        val_e = float(pe.get("market_value", 0) or 0)

        qty_s = float(ps.get("quantity", 0) or 0)
        qty_e = float(pe.get("quantity", 0) or 0)

        price_ret = (px_e - px_s) / px_s if px_s > 0 and px_e > 0 else 0.0

        if ticker not in pos_s:
            status = "NUEVA"
        elif ticker not in pos_e:
            status = "CERRADA"
        elif qty_e > qty_s * 1.02:
            status = "AUMENTADA"
        elif qty_e < qty_s * 0.98:
            status = "REDUCIDA"
        else:
            status = "SIN CAMBIO"

        qty_delta = qty_e - qty_s
        gross_value_delta = val_e - val_s

        # P&L puro por precio:
        # Si la posición existe al final, usamos qty final.
        # Si fue cerrada, usamos qty inicial como estimación.
        # Sin trades_log no sabemos el precio real de venta.
        qty_for_price_pnl = qty_e if qty_e > 0 else qty_s

        if px_s > 0 and px_e > 0:
            price_pnl = (px_e - px_s) * qty_for_price_pnl
        else:
            price_pnl = 0.0

        # Efecto cantidad:
        # Representa capital movido por compras/ventas/cambios de quantity.
        # No debe tratarse como P&L puro de mercado.
        if px_s > 0:
            quantity_effect = qty_delta * px_s
        else:
            quantity_effect = gross_value_delta - price_pnl

        if abs(qty_delta) > max(0.0001, abs(qty_s) * 0.02):
            changed_positions += 1

        total_price_pnl += price_pnl
        total_quantity_effect += quantity_effect

        positions.append({
            "ticker": ticker,
            "px_s": px_s,
            "px_e": px_e,
            "price_ret": price_ret,

            "val_s": val_s,
            "val_e": val_e,

            # Antes esto se imprimía como P&L. Ahora queda como delta bruto.
            "gross_value_delta": gross_value_delta,

            # Nuevo: P&L estimado por movimiento de precio.
            "price_pnl": price_pnl,

            # Nuevo: efecto de compra/venta/cambio de cantidad.
            "quantity_effect": quantity_effect,
            "qty_delta": qty_delta,

            "qty_s": qty_s,
            "qty_e": qty_e,
            "status": status,
            "weight": val_e / total_e if total_e > 0 else 0.0,
        })

    # Ordenamos por impacto real de precio, no por delta bruto de market value.
    positions.sort(key=lambda x: abs(x["price_pnl"]), reverse=True)

    active = [
        p for p in positions
        if p["status"] != "CERRADA" and p["px_s"] > 0 and p["px_e"] > 0
    ]

    best = max(active, key=lambda x: x["price_ret"]) if active else None
    worst = min(active, key=lambda x: x["price_ret"]) if active else None

    price_return = total_price_pnl / total_s if total_s > 0 else 0.0

    return {
        "total_s": total_s,
        "total_e": total_e,

        # Variación bruta: valor final - valor inicial.
        # Incluye compras/ventas/cash/cambios de cantidad.
        "gross_total_delta": gross_total_delta,
        "gross_total_return": gross_total_return,

        # Métrica principal del semanal.
        # Estima cuánto se ganó/perdió por movimiento de precios.
        "price_pnl": total_price_pnl,
        "price_return": price_return,

        "quantity_effect": total_quantity_effect,
        "cash_s": cash_s,
        "cash_e": cash_e,
        "cash_delta": cash_delta,

        "changed_positions": changed_positions,
        "positions": positions,
        "best": best,
        "worst": worst,
        "ts_start": snap_start.get("_ts"),
        "ts_end": snap_end.get("_ts"),
    }


def _render(c: dict, week_label: str, n_snaps: int) -> str:
    h = []

    ret = c["price_return"]
    icon = "🟢" if ret > 0.01 else ("🔴" if ret < -0.01 else "🟡")

    sign_price = "+" if c["price_pnl"] >= 0 else ""
    sign_gross = "+" if c["gross_total_delta"] >= 0 else ""
    sign_cash = "+" if c["cash_delta"] >= 0 else ""

    h.append(f"📅 <b>RESUMEN SEMANAL — {week_label}</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"🕐 {datetime.now(tz=TZ).strftime('%d/%m/%Y %H:%M')} ART")

    ts_s, ts_e = c.get("ts_start"), c.get("ts_end")
    if ts_s and ts_e:
        h.append(f"📌 {ts_s.strftime('%a %d/%m %H:%M')} → {ts_e.strftime('%a %d/%m %H:%M')}")

    h.append(f"🔍 Snapshots de la semana: {n_snaps}")
    h.append("")

    h.append("<b>RESULTADO DE LA SEMANA</b>")
    h.append(f"{icon} Portfolio actual: <b>{_ars(c['total_e'])}</b>")
    h.append(
        f"📊 P&L estimado por precio: "
        f"<b>{sign_price}{_ars(c['price_pnl'])}</b> ({_pct(c['price_return'])})"
    )
    h.append(
        f"📦 Variación bruta de cartera: "
        f"{sign_gross}{_ars(c['gross_total_delta'])} ({_pct(c['gross_total_return'])})"
    )
    h.append(f"   Inicio: {_ars(c['total_s'])}")
    h.append(f"   Cash:   {_ars(c['cash_s'])} → {_ars(c['cash_e'])} ({sign_cash}{_ars(c['cash_delta'])})")

    if c.get("changed_positions", 0) > 0:
        h.append("")
        h.append("⚠️ <b>Se detectaron cambios de cantidad.</b>")
        h.append("La variación bruta incluye compras/ventas. No debe leerse como rendimiento puro.")

    h.append("")

    best, worst = c.get("best"), c.get("worst")
    if best or worst:
        h.append("<b>DESTACADOS POR PRECIO</b>")
        if best and best["price_ret"] > 0:
            h.append(
                f"🏆 Mejor: <b>{best['ticker']}</b> {_pct(best['price_ret'])} "
                f"({_ars(best['px_s'])} → {_ars(best['px_e'])})"
            )
        if worst and worst["price_ret"] < 0:
            h.append(
                f"💀 Peor:  <b>{worst['ticker']}</b> {_pct(worst['price_ret'])} "
                f"({_ars(worst['px_s'])} → {_ars(worst['px_e'])})"
            )
        h.append("")

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<b>DETALLE POR POSICIÓN</b>")
    h.append("")

    status_icon = {
        "NUEVA": "🆕",
        "CERRADA": "❌",
        "AUMENTADA": "⬆️",
        "REDUCIDA": "⬇️",
        "SIN CAMBIO": "",
    }

    for p in c["positions"]:
        tag = status_icon.get(p["status"], "")
        h.append(f"{_arrow(p['price_ret'])} <b>{p['ticker']}</b> {tag}")

        if p["px_s"] > 0 and p["px_e"] > 0:
            h.append(
                f"   Precio: {_ars(p['px_s'])} → "
                f"<b>{_ars(p['px_e'])}</b> ({_pct(p['price_ret'])})"
            )

        if p["val_e"] > 0:
            h.append(
                f"   Valor actual: <b>{_ars(p['val_e'])}</b> "
                f"| Peso: {p['weight']:.1%}"
            )

        sign_pnl = "+" if p["price_pnl"] >= 0 else ""
        h.append(f"   P&L por precio: {sign_pnl}{_ars(p['price_pnl'])}")

        if (
            p["status"] in ("AUMENTADA", "REDUCIDA", "NUEVA", "CERRADA")
            or abs(p["qty_delta"]) > 0.0001
        ):
            sign_qty = "+" if p["qty_delta"] >= 0 else ""
            sign_gross = "+" if p["gross_value_delta"] >= 0 else ""

            h.append(
                f"   Cantidad: {p['qty_s']:.2f} → {p['qty_e']:.2f} "
                f"({sign_qty}{p['qty_delta']:.2f})"
            )
            h.append(
                f"   Delta bruto de valor: "
                f"{sign_gross}{_ars(p['gross_value_delta'])}"
            )
            h.append(
                "   <i>Nota: el delta bruto incluye cambio de cantidad; "
                "no es P&L puro.</i>"
            )

        if p["status"] == "CERRADA":
            h.append("   <i>Posición cerrada esta semana.</i>")

        h.append("")

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if ret > 0.02:
        h.append("✅ <b>Semana positiva por movimiento de precios.</b>")
    elif ret < -0.02:
        h.append("⚠️ <b>Semana negativa por movimiento de precios.</b> Revisar posiciones con mayor drawdown.")
    else:
        h.append("➡️ <b>Semana lateral.</b> Variación de precios dentro del ruido normal.")

    h.append("")
    h.append("<i>Resumen automático — Cocos Copilot</i>")

    return "\n".join(h)


async def generate_weekly_summary(
    weeks_ago: int = 0,
    no_telegram: bool = False,
    owner_chat_id: int | None = None,
) -> str:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)

    await db.connect()

    try:
        week_snaps = await _get_week_snapshots(
            db,
            weeks_ago=weeks_ago,
            owner_chat_id=owner_chat_id,
        )

        if not week_snaps:
            logger.warning("Sin snapshots esta semana — usando historial reciente")

            history = await db.get_portfolio_history(limit=50, owner_chat_id=owner_chat_id)

            if len(history) < 2:
                return (
                    "⚠️ Todavía no hay suficientes snapshots para comparar una semana.\n"
                    "Necesito al menos dos capturas privadas en días distintos."
                )

            now = datetime.now(tz=TZ)
            ref = None

            for snap in reversed(history):
                ts = _parse_ts(snap.get("timestamp") or snap.get("scraped_at"))
                if ts and (now - ts).days >= 5:
                    snap["_ts"] = ts
                    ref = snap
                    break

            snap_start = ref or history[-1]
            snap_end = history[0]

            snap_end["_ts"] = now
            snap_start.setdefault("_ts", now - timedelta(days=7))

            week_snaps = [snap_start, snap_end]

        elif len(week_snaps) == 1:
            history = await db.get_portfolio_history(limit=50, owner_chat_id=owner_chat_id)
            ref_ts = week_snaps[0]["_ts"]

            for snap in history:
                ts = _parse_ts(snap.get("timestamp") or snap.get("scraped_at"))

                if ts and ts < ref_ts:
                    snap["_ts"] = ts
                    week_snaps.insert(0, snap)
                    break

        comparison = _compare(week_snaps[0], week_snaps[-1])
        week_label = _week_label(weeks_ago)
        report = _render(comparison, week_label, n_snaps=len(week_snaps))

        logger.info(
            f"Resumen semanal {week_label} — "
            f"retorno precio {comparison['price_return']:+.2%} "
            f"| variación bruta {comparison['gross_total_return']:+.2%} "
            f"| {len(week_snaps)} snapshots"
        )

        if not no_telegram and cfg.scraper.telegram_enabled:
            TelegramNotifier(
                cfg.scraper.telegram_bot_token,
                cfg.scraper.telegram_chat_id,
            ).send_raw(report)

        return report

    finally:
        await db.close()


async def main() -> None:
    p = argparse.ArgumentParser(description="Resumen semanal del portfolio")
    p.add_argument("--weeks-ago", type=int, default=0)
    p.add_argument("--no-telegram", action="store_true")
    p.add_argument("--owner-chat-id", type=int, default=None)

    args = p.parse_args()

    print(
        await generate_weekly_summary(
            weeks_ago=args.weeks_ago,
            no_telegram=args.no_telegram,
            owner_chat_id=args.owner_chat_id,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
