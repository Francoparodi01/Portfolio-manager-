"""
scripts/run_performance.py
──────────────────────────
Calcula y muestra el performance del sistema de decisiones.

Incluye:
    - Performance histórica agregada.
    - Actualización de outcomes pendientes.
    - Estado del dataset operativo por source/status/decision_type.

Esto permite distinguir:
    optimizer / THEORETICAL  → ideas teóricas del optimizer
    execution_plan / APPROVED → planes aprobados por el planner
    execution_plan / EXECUTED → fills reales confirmados del broker
    execution_plan / BLOCKED  → señales bloqueadas por guards/funding

Output:
    📊 PERFORMANCE DEL SISTEMA
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    DATASET OPERATIVO
    optimizer / THEORETICAL / theoretical / BUY: ...
    execution_plan / APPROVED / executable / SELL: ...
    execution_plan / BLOCKED / blocked / BUY: ...

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
from html import escape

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.decision_engine import directional_return

logger = get_logger(__name__)


def directional_return_for_report(entry_price: float, exit_price: float, decision: str) -> float:
    """# CONVENTION: SELL returns are positive-up."""
    return directional_return(entry_price, exit_price, decision)


def _pct(x) -> str:
    if x is None:
        return "N/A"
    return f"{float(x):+.1%}"


def _ev_label(ev: float | None, *, historical_only: bool = False) -> str:
    if ev is None:
        return "SIN DATOS"
    if ev > 0.02:
        if historical_only:
            return "✅ POSITIVO — histórico favorable, ejecución aún por validar"
        return "✅ POSITIVO — el sistema tiene edge real"
    if ev > 0:
        if historical_only:
            return "🟡 MARGINAL — histórico levemente favorable, seguir midiendo"
        return "🟡 MARGINAL — edge pequeño, seguir midiendo"
    if historical_only:
        return "❌ NEGATIVO — histórico sin edge demostrado"
    return "❌ NEGATIVO — el sistema no tiene edge demostrado"


def _dataset_label(row: dict) -> str:
    source = str(row.get("source") or "sin_source")
    status = str(row.get("status") or "UNKNOWN")
    decision_type = str(row.get("decision_type") or "unknown")
    decision = str(row.get("decision") or "?")

    return f"{source} / {status} / {decision_type} / {decision}"


def _dataset_group_note(dataset_stats: list[dict]) -> str:
    """
    Mensaje corto para evitar interpretar el EV agregado como performance pura
    del Execution Planner cuando todavía no tiene outcomes.
    """
    if not dataset_stats:
        return "Sin eventos de decision_log para este período."

    executed_with_outcome = 0
    approved_with_outcome = 0
    blocked_with_outcome = 0
    optimizer_with_outcome = 0

    for row in dataset_stats:
        source = str(row.get("source") or "").lower()
        status = str(row.get("status") or "").upper()
        decision_type = str(row.get("decision_type") or "").lower()
        con_5d = int(row.get("con_5d") or 0)

        if source == "execution_plan" and status == "EXECUTED":
            executed_with_outcome += con_5d

        if source == "execution_plan" and status == "APPROVED":
            approved_with_outcome += con_5d

        if source == "execution_plan" and (status == "BLOCKED" or decision_type == "blocked"):
            blocked_with_outcome += con_5d

        if source == "optimizer" or status == "THEORETICAL" or decision_type == "theoretical":
            optimizer_with_outcome += con_5d

    if executed_with_outcome == 0 and approved_with_outcome > 0:
        return (
            "Lectura: ya hay outcomes de planes aprobados, "
            "pero todavía no son fills reales validados por broker."
        )

    if executed_with_outcome == 0 and optimizer_with_outcome > 0:
        return (
            "Lectura: el EV actual corresponde principalmente a histórico/optimizer. "
            "El Execution Audit todavía está acumulando outcomes."
        )

    if executed_with_outcome > 0:
        return (
            "Lectura: ya hay outcomes de fills reales confirmados. "
            "El Execution Audit empieza a medir performance operativa validada."
        )

    if blocked_with_outcome > 0:
        return (
            "Lectura: ya hay outcomes de bloqueos. "
            "El Blocked Audit empieza a medir si los guards protegen o frenan demasiado."
        )

    return "Lectura: dataset operativo iniciado; todavía faltan outcomes 5D/10D/20D."


def _ev_scope(dataset_stats: list[dict]) -> tuple[str, str]:
    """
    Distingue si el EV agregado todavía es principalmente histórico o si ya
    cuenta con outcomes operativos aprobados.
    """
    executed_with_outcome = 0
    optimizer_with_outcome = 0

    for row in dataset_stats:
        source = str(row.get("source") or "").lower()
        status = str(row.get("status") or "").upper()
        decision_type = str(row.get("decision_type") or "").lower()
        con_5d = int(row.get("con_5d") or 0)

        if source == "execution_plan" and status == "EXECUTED":
            executed_with_outcome += con_5d

        if source == "optimizer" or status == "THEORETICAL" or decision_type == "theoretical":
            optimizer_with_outcome += con_5d

    if executed_with_outcome == 0 and optimizer_with_outcome > 0:
        return (
            "EV histórico agregado",
            "Todavía no mide performance de ejecución real; esa lectura vive en Execution Audit.",
        )

    if executed_with_outcome > 0:
        return (
            "EV agregado",
            "Incluye fills reales confirmados; contrastalo con Execution Audit para aislar ejecución real.",
        )

    return (
        "EV agregado",
        "Aún no hay suficiente evidencia para separar histórico y ejecución.",
    )


async def _get_decision_dataset_stats(
    db: PortfolioDatabase,
    lookback_days: int = 90,
) -> list[dict]:
    """
    Resume decision_log por source/status/decision_type/decision.

    Esto no mide performance directamente.
    Mide si estamos juntando datos útiles y de qué tipo.
    """
    pool = await db.get_pool()
    if not pool:
        return []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                COALESCE(source, layers->>'source', 'sin_source') AS source,
                COALESCE(status, 'UNKNOWN') AS status,
                COALESCE(decision_type, 'unknown') AS decision_type,
                decision,
                COUNT(*) AS n,
                COUNT(outcome_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_5d,
                COUNT(outcome_10d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_10d,
                COUNT(outcome_20d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_20d,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'legacy_external'
                ) AS legacy_external
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4
            """,
            lookback_days,
        )

    return [dict(r) for r in rows]


def render_dataset_operativo(stats: dict) -> list[str]:
    dataset_stats = stats.get("dataset_stats", [])
    lines: list[str] = []

    lines.append("<b>DATASET OPERATIVO</b>")

    if not dataset_stats:
        lines.append("   Sin eventos en decision_log para este período.")
        lines.append("")
        return lines

    for row in dataset_stats:
        label = escape(_dataset_label(row))

        n = int(row.get("n") or 0)
        con_5d = int(row.get("con_5d") or 0)
        con_10d = int(row.get("con_10d") or 0)
        con_20d = int(row.get("con_20d") or 0)
        legacy_external = int(row.get("legacy_external") or 0)

        line = (
            f"   • <code>{label}</code>: "
            f"<b>{n}</b> eventos | "
            f"5D {con_5d} | 10D {con_10d} | 20D {con_20d}"
        )
        if legacy_external:
            line += f" | legacy {legacy_external}"
        lines.append(line)

    legacy_total = sum(int(row.get("legacy_external") or 0) for row in dataset_stats)
    if legacy_total:
        lines.append(
            "   ℹ️ "
            f"{legacy_total} eventos legacy_external quedan fuera de métricas canónicas."
        )

    note = _dataset_group_note(dataset_stats)
    if note:
        lines.append(f"   ℹ️ {escape(note)}")

    lines.append("")
    return lines


def render_performance_report(stats: dict) -> str:
    """Genera reporte de performance en HTML para Telegram."""
    total = stats.get("total_trades", 0)
    pending = stats.get("pending", 0)
    days = stats.get("lookback_days", 90)

    header = [
        "📊 <b>PERFORMANCE DEL SISTEMA</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Últimos {days} días | {datetime.now().strftime('%d/%m/%Y %H:%M')} ART",
        "",
    ]

    dataset_lines = render_dataset_operativo(stats)

    if total == 0:
        lines = header + dataset_lines + [
            "⚠️ Sin trades completados en este período.",
            "",
        ]

        if pending > 0:
            lines.append(f"📋 {pending} decisiones pendientes de outcome.")
            lines.append("Corriendo <code>python scripts/run_performance.py</code> se actualizan outcomes elegibles.")
        else:
            lines.append("El sistema aún no tiene decisiones con outcome cerrado.")
            lines.append("Verificá que <b>run_analysis.py</b> esté guardando eventos en decision_log.")

        return "\n".join(lines)

    win_rate = stats.get("win_rate")
    avg_win = stats.get("avg_win_5d")
    avg_loss = stats.get("avg_loss_5d")
    ev = stats.get("ev")
    winners = stats.get("winners", 0)
    losers = stats.get("losers", 0)

    ev_title, ev_note = _ev_scope(stats.get("dataset_stats", []))

    lines = header + dataset_lines + [
        "<b>MÉTRICAS PRINCIPALES (horizonte 5d)</b>",
        f"   Win rate:   <b>{win_rate:.0%}</b>  ({winners}W / {losers}L)"
        if win_rate is not None else "   Win rate:   <b>N/A</b>",
        f"   Avg win:    <b>{_pct(avg_win)}</b>",
        f"   Avg loss:   <b>{_pct(avg_loss)}</b>",
        "",
        f"   <b>{ev_title}: {_pct(ev)}</b>",
        f"   {_ev_label(ev, historical_only=(ev_title == 'EV histórico agregado'))}",
        f"   <i>{escape(ev_note)}</i>",
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

    ticker_stats = stats.get("ticker_stats", [])
    if ticker_stats:
        lines += ["", "<b>POR TICKER</b>"]
        for ts in ticker_stats[:6]:
            t = ts.get("ticker", "?")
            trades = int(ts.get("trades") or 0)
            wins = int(ts.get("wins") or 0)
            avg_ret = ts.get("avg_return")
            wr = wins / trades if trades > 0 else 0
            icon = "🟢" if (avg_ret or 0) > 0 else "🔴"
            lines.append(
                f"   {icon} <b>{escape(str(t))}</b>: "
                f"{trades} trades | WR {wr:.0%} | avg {_pct(avg_ret)}"
            )

    recent = stats.get("recent", [])
    if recent:
        lines += ["", "<b>ÚLTIMAS DECISIONES</b>"]
        for r in recent:
            ticker = r.get("ticker", "?")
            direction = r.get("decision", "?")
            score = float(r.get("final_score") or 0.0)
            outcome = r.get("outcome_5d")
            correct = r.get("was_correct")
            decided = r.get("decided_at")
            date_str = decided.strftime("%d/%m") if decided else "?"

            source = r.get("source") or None
            status = r.get("status") or None

            tag = ""
            if source or status:
                tag_parts = []
                if source:
                    tag_parts.append(str(source))
                if status:
                    tag_parts.append(str(status))
                tag = f" <code>[{'/'.join(tag_parts)}]</code>"

            if correct is True:
                res = f"✅ {_pct(outcome)}"
            elif correct is False:
                res = f"❌ {_pct(outcome)}"
            else:
                res = "⏳ pendiente"

            lines.append(
                f"   {date_str} <b>{escape(str(direction))} {escape(str(ticker))}</b>{tag} "
                f"score <code>{score:+.3f}</code> → {res}"
            )

    curve = stats.get("equity_curve", [])
    if curve and len(curve) >= 2:
        eq_end = stats.get("equity_end", 100.0)
        eq_ret = stats.get("equity_return", 0.0)
        eq_dd = stats.get("equity_max_drawdown", 0.0)
        eq_icon = "📈" if eq_ret >= 0 else "📉"

        lines += [
            "",
            "<b>EQUITY CURVE</b>",
            f"   Inicio: 100 → Actual: <b>{eq_end:.1f}</b>",
            f"   Retorno acumulado: <b>{eq_ret:+.1%}</b> {eq_icon}",
            f"   Max drawdown: <b>{eq_dd:.1%}</b>",
            "",
            "   Últimos movimientos:",
        ]

        for p in curve[-5:]:
            icon = "✅" if p.get("correct") else "❌"
            lines.append(
                f"   {icon} {p['date']} <b>{escape(str(p['ticker']))}</b> "
                f"<code>{p['outcome']:+.1%}</code> → equity {p['equity']:.1f}"
            )

    footer_edge_note = (
        "<i>EV histórico &gt; 0 sugiere edge del modelo en backtest/outcomes acumulados; "
        "no prueba por sí solo edge de ejecución.</i>"
        if ev_title == "EV histórico agregado"
        else "<i>EV &gt; 0 sugiere edge; confirmarlo contra Execution Audit antes de llamarlo edge operativo.</i>"
    )

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>EV = (win_rate × avg_win) − (loss_rate × avg_loss)</i>",
        footer_edge_note,
        "<i>Separar dataset operativo evita mezclar optimizer teórico con ejecución real.</i>",
    ]

    return "\n".join(lines)


async def main(lookback_days: int, no_telegram: bool) -> None:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    notifier = TelegramNotifier(
        cfg.scraper.telegram_bot_token,
        cfg.scraper.telegram_chat_id,
    )

    try:
        await db.connect()

        logger.info("Actualizando outcomes pendientes...")
        updated = await db.update_outcomes(lookback_days=lookback_days)

        if updated:
            logger.info(f"{updated} outcomes actualizados")

        logger.info("Calculando performance...")
        stats = await db.get_performance_stats_v2(lookback_days=lookback_days)

        dataset_stats = await _get_decision_dataset_stats(
            db=db,
            lookback_days=lookback_days,
        )
        stats["dataset_stats"] = dataset_stats

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
    p = argparse.ArgumentParser(
        description="Performance real del sistema de decisiones"
    )
    p.add_argument(
        "--days",
        type=int,
        default=90,
        help="Lookback en días (default: 90)",
    )
    p.add_argument(
        "--no-telegram",
        action="store_true",
        help="No enviar a Telegram",
    )

    args = p.parse_args()

    asyncio.run(
        main(
            lookback_days=args.days,
            no_telegram=args.no_telegram,
        )
    )
