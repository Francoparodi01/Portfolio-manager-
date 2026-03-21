"""
scripts/run_performance.py
──────────────────────────
Calcula y muestra el performance REAL del sistema de decisiones.

Output (para Telegram y stdout):

    📊 PERFORMANCE DEL SISTEMA
    ──────────────────────────
    Win rate:   54%
    Avg win:    +6.2%
    Avg loss:   -3.1%
    EV:         +1.7%   ← la métrica que importa
    Trades:     73
    Pendientes: 12

Uso:
  python scripts/run_performance.py
  python scripts/run_performance.py --days 60
  python scripts/run_performance.py --no-telegram
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)


def _pct(x) -> str:
    if x is None:
        return "N/A"
    return f"{float(x):+.1%}"


def _ev_label(ev: float | None) -> str:
    if ev is None:
        return "SIN DATOS"
    if ev > 0.02:
        return "✅ POSITIVO — el sistema tiene edge real"
    if ev > 0:
        return "🟡 MARGINAL — edge pequeño, seguir midiendo"
    return "❌ NEGATIVO — el sistema no tiene edge demostrado"


def render_performance_report(stats: dict) -> str:
    """Genera reporte de performance en HTML para Telegram."""
    total   = stats.get("total_trades", 0)
    pending = stats.get("pending", 0)
    days    = stats.get("lookback_days", 90)

    if total == 0:
        lines = [
            "📊 <b>PERFORMANCE DEL SISTEMA</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"⚠️ Sin trades completados en los últimos {days} días.",
            "",
        ]
        if pending > 0:
            lines.append(f"📋 {pending} decisiones pendientes de outcome (esperando ≥5 días).")
            lines.append("Corriendo <code>python scripts/update_outcomes.py</code> se llena la DB.")
        else:
            lines.append("El sistema aún no tiene decisiones guardadas.")
            lines.append("Verificá que <b>decision_engine</b> esté integrado en run_analysis.py.")
        return "\n".join(lines)

    win_rate = stats.get("win_rate")
    avg_win  = stats.get("avg_win_5d")
    avg_loss = stats.get("avg_loss_5d")
    ev       = stats.get("ev")
    winners  = stats.get("winners", 0)
    losers   = stats.get("losers", 0)

    lines = [
        "📊 <b>PERFORMANCE DEL SISTEMA</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Últimos {days} días | {datetime.now().strftime('%d/%m/%Y %H:%M')} ART",
        "",
        "<b>MÉTRICAS PRINCIPALES (horizonte 5d)</b>",
        f"   Win rate:   <b>{win_rate:.0%}</b>  ({winners}W / {losers}L)"
            if win_rate is not None else "   Win rate:   <b>N/A</b>",
        f"   Avg win:    <b>{_pct(avg_win)}</b>",
        f"   Avg loss:   <b>{_pct(avg_loss)}</b>",
        "",
        f"   <b>EV: {_pct(ev)}</b>",
        f"   {_ev_label(ev)}",
        "",
        "<b>RETORNOS POR HORIZONTE</b>",
        f"   5d:   {_pct(stats.get('avg_return_5d'))}",
        f"   10d:  {_pct(stats.get('avg_return_10d'))}",
        f"   20d:  {_pct(stats.get('avg_return_20d'))}",
        "",
        f"   Mejor trade:  {_pct(stats.get('best_trade'))}",
        f"   Peor trade:   {_pct(stats.get('worst_trade'))}",
        "",
        f"   Total trades: <b>{total}</b>",
        f"   Pendientes:   {pending} (sin outcome aún)",
    ]

    # Por ticker
    ticker_stats = stats.get("ticker_stats", [])
    if ticker_stats:
        lines += ["", "<b>POR TICKER</b>"]
        for ts in ticker_stats[:6]:
            t         = ts.get("ticker", "?")
            trades    = int(ts.get("trades") or 0)
            wins      = int(ts.get("wins") or 0)
            avg_ret   = ts.get("avg_return")
            wr        = wins / trades if trades > 0 else 0
            icon      = "🟢" if (avg_ret or 0) > 0 else "🔴"
            lines.append(
                f"   {icon} <b>{t}</b>: {trades} trades | WR {wr:.0%} | avg {_pct(avg_ret)}"
            )

    # Últimas decisiones
    recent = stats.get("recent", [])
    if recent:
        lines += ["", "<b>ÚLTIMAS DECISIONES</b>"]
        for r in recent:
            ticker    = r.get("ticker", "?")
            direction = r.get("decision", "?")
            score     = float(r.get("final_score") or 0.0)
            outcome   = r.get("outcome_5d")
            correct   = r.get("was_correct")
            decided   = r.get("decided_at")
            date_str  = decided.strftime("%d/%m") if decided else "?"
            if correct is True:
                res = f"✅ {_pct(outcome)}"
            elif correct is False:
                res = f"❌ {_pct(outcome)}"
            else:
                res = "⏳ pendiente"
            lines.append(
                f"   {date_str} <b>{direction} {ticker}</b> "
                f"score <code>{score:+.3f}</code> → {res}"
            )

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>EV = (win_rate × avg_win) − (loss_rate × avg_loss)</i>",
        "<i>Si EV &gt; 0 → el sistema tiene edge real. Si no → es ruido.</i>",
    ]

    return "\n".join(lines)


async def main(lookback_days: int, no_telegram: bool) -> None:
    cfg      = get_config()
    db       = PortfolioDatabase(cfg.database.url)
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    try:
        await db.connect()

        # Primero actualizar outcomes pendientes
        logger.info("Actualizando outcomes pendientes...")
        updated = await db.update_outcomes(lookback_days=lookback_days)
        if updated:
            logger.info(f"{updated} outcomes actualizados")

        # Luego calcular stats
        logger.info("Calculando performance...")
        stats  = await db.get_performance_stats(lookback_days=lookback_days)
        report = render_performance_report(stats)

        print(report)

        if not no_telegram and cfg.scraper.telegram_enabled:
            notifier.send_raw(report)
            logger.info("Reporte enviado a Telegram")
        else:
            logger.info("Telegram omitido")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        await db.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Performance real del sistema de decisiones")
    p.add_argument("--days",        type=int, default=90,
                   help="Lookback en días (default: 90)")
    p.add_argument("--no-telegram", action="store_true",
                   help="No enviar a Telegram")
    args = p.parse_args()
    asyncio.run(main(lookback_days=args.days, no_telegram=args.no_telegram))
