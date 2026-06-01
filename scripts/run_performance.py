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
import json
import os
import sys
from datetime import datetime, time
from html import escape
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.decision_engine import directional_return
from src.core.market_calendar import is_trading_day, market_closed_reason

logger = get_logger(__name__)
ART = ZoneInfo("America/Argentina/Buenos_Aires")


def directional_return_for_report(entry_price: float, exit_price: float, decision: str) -> float:
    """# CONVENTION: SELL returns are positive-up."""
    return directional_return(entry_price, exit_price, decision)


def _pct(x) -> str:
    if x is None:
        return "N/A"
    return f"{float(x):+.1%}"


def _money_ars(x) -> str:
    if x is None:
        return "N/A"
    return f"${float(x):,.0f}".replace(",", ".")


def _json_payload(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _layer_component(layers: dict, name: str) -> float | None:
    payload = layers.get(name)
    if isinstance(payload, dict):
        for key in ("weighted", "raw", "score"):
            if payload.get(key) is not None:
                try:
                    return float(payload[key])
                except Exception:
                    return None
    try:
        return float(payload)
    except Exception:
        return None


def _ev_label(ev: float | None, *, historical_only: bool = False) -> str:
    if ev is None:
        return "SIN DATOS"
    if ev > 0.02:
        if historical_only:
            return "✅ POSITIVO — histórico favorable, ejecución aún por validar"
        return "✅ POSITIVO — muestra operativa favorable, seguir validando"
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

        is_real_execution = (
            (source == "execution_plan" and status in {"EXECUTED", "EXECUTED_MANUAL"})
            or (source == "broker_fill" and status in {"EXECUTED", "EXECUTED_MANUAL"})
            or (source == "broker_movement" and status in {"EXECUTED", "EXECUTED_MANUAL"})
        )

        if is_real_execution:
            executed_with_outcome += con_5d

        if source == "execution_plan" and status == "APPROVED":
            approved_with_outcome += con_5d

        if source == "execution_plan" and status == "BLOCKED":
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
            "Lectura: ya hay outcomes de movimientos reales confirmados. "
            "Execution Audit empieza a medir performance operativa validada."
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

        if (
            (source == "execution_plan" and status in {"EXECUTED", "EXECUTED_MANUAL"})
            or (source == "broker_fill" and status in {"EXECUTED", "EXECUTED_MANUAL"})
            or (source == "broker_movement" and status in {"EXECUTED", "EXECUTED_MANUAL"})
        ):
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
            "EV operativo 5D",
            "Solo usa ejecuciones reales confirmadas; no mezcla BLOCKED, THEORETICAL ni APPROVED sin fill.",
        )

    return (
        "EV agregado",
        "Aún no hay suficiente evidencia para separar histórico y ejecución.",
    )


async def _get_decision_dataset_stats(
    db: PortfolioDatabase,
    lookback_days: int = 90,
    owner_chat_id: int | None = None,
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
                COUNT(COALESCE(executable_outcome_5d, outcome_5d)) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_5d,
                COUNT(COALESCE(executable_outcome_10d, outcome_10d)) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_10d,
                COUNT(COALESCE(executable_outcome_20d, outcome_20d)) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                ) AS con_20d,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'legacy_external'
                ) AS legacy_external
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND ($2::bigint IS NULL OR owner_chat_id = $2)
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4
            """,
            lookback_days,
            owner_chat_id,
        )

    return [dict(r) for r in rows]


async def _get_operational_context(db: PortfolioDatabase) -> dict:
    pool = await db.get_pool()
    if not pool:
        return {}

    async with pool.acquire() as conn:
        latest_candle_day = await conn.fetchval("SELECT MAX(ts::date) FROM market_candles")
        latest_market_ts = await conn.fetchval("SELECT MAX(ts) FROM market_prices")
        latest_portfolio_ts = await conn.fetchval("SELECT MAX(scraped_at) FROM portfolio_snapshots")
        position_signatures_today = await conn.fetchval(
            """
            WITH daily_snapshots AS (
                SELECT
                    ps.snapshot_id,
                    ps.cash_ars,
                    STRING_AGG(
                        p.ticker || ':' || COALESCE(p.quantity::text, '0'),
                        ',' ORDER BY p.ticker
                    ) AS positions_sig
                FROM portfolio_snapshots ps
                LEFT JOIN positions p ON p.snapshot_id = ps.snapshot_id
                WHERE (ps.scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date =
                      (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                GROUP BY ps.snapshot_id, ps.cash_ars
            )
            SELECT COUNT(DISTINCT COALESCE(positions_sig, '') || '|cash:' || COALESCE(cash_ars::text, '0'))
            FROM daily_snapshots
            """
        )
        broker_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'broker_fills'
            )
            """
        )
        broker = None
        if broker_exists:
            broker = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE decision_log_id IS NOT NULL) AS reconciled,
                    COUNT(*) FILTER (WHERE decision_log_id IS NULL) AS unreconciled,
                    MAX(executed_at) AS latest_executed_at
                FROM broker_fills
                """
            )

    return {
        "latest_candle_day": latest_candle_day,
        "latest_market_ts": latest_market_ts,
        "latest_portfolio_ts": latest_portfolio_ts,
        "position_signatures_today": int(position_signatures_today or 0),
        "broker_fills": dict(broker) if broker else None,
    }


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
    owner_chat_id = stats.get("owner_chat_id")

    header = [
        "📊 <b>PERFORMANCE DEL SISTEMA</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Últimos {days} días | {datetime.now(ART).strftime('%d/%m/%Y %H:%M')} ART",
        "",
    ]

    dataset_lines = render_dataset_operativo(stats)

    if total == 0:
        dataset_total = sum(int(row.get("n") or 0) for row in stats.get("dataset_stats", []))
        lines = header + dataset_lines + [
            "⚠️ Sin outcomes canónicos cerrados en este período.",
            "",
        ]

        if pending > 0:
            lines.append(f"📋 {pending} decisiones pendientes de outcome.")
            lines.append(
                "Este comando ya intentó actualizar las elegibles; el resto necesita "
                "5/10/20 ruedas con velas canónicas."
            )
        elif owner_chat_id is not None:
            lines.append("Este usuario todavía no acumuló decisiones propias con outcome cerrado.")
            lines.append("El dataset personal empieza a crecer desde sus próximos análisis.")
        elif dataset_total > 0:
            lines.append("decision_log ya tiene eventos; falta que maduren outcomes canónicos.")
            lines.append("No es un fallo de guardado, es una etapa normal de cold start.")
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
            date_str = decided.astimezone(ART).strftime("%d/%m") if decided else "?"

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


def _fmt_count(value) -> str:
    return f"{int(value or 0):,}".replace(",", ".")


def _dataset_totals(dataset_stats: list[dict]) -> dict:
    totals = {
        "events": 0,
        "closed_5d": 0,
        "closed_10d": 0,
        "closed_20d": 0,
        "legacy": 0,
    }
    for row in dataset_stats:
        totals["events"] += int(row.get("n") or 0)
        totals["closed_5d"] += int(row.get("con_5d") or 0)
        totals["closed_10d"] += int(row.get("con_10d") or 0)
        totals["closed_20d"] += int(row.get("con_20d") or 0)
        totals["legacy"] += int(row.get("legacy_external") or 0)
    return totals


def _friendly_summary(stats: dict, ev_title: str | None = None) -> list[str]:
    total = int(stats.get("total_trades") or 0)
    pending = int(stats.get("pending") or 0)
    pending_all = int(stats.get("pending_all") or pending)
    totals = _dataset_totals(stats.get("dataset_stats", []))
    broker = (stats.get("operational_context") or {}).get("broker_fills") or {}
    broker_total = int(broker.get("total") or 0)
    broker_reconciled = int(broker.get("reconciled") or 0)

    if total == 0:
        if totals["events"] > 0:
            lines = [
                "El sistema ya esta guardando decisiones, pero todavia no hay resultados cerrados para medir edge.",
                f"Eventos registrados: {_fmt_count(totals['events'])}. Pendientes operativos: {_fmt_count(pending)}.",
                "Necesita que pasen 5/10/20 ruedas con velas canonicas para empezar a evaluar.",
            ]
            if pending_all != pending:
                lines.append(f"Pendientes totales de auditoria: {_fmt_count(pending_all)}.")
            if broker_total > 0 and broker_reconciled == 0:
                lines.append(
                    f"Movimientos Cocos detectados: {_fmt_count(broker_total)}; todavia no matchean con planes aprobados."
                )
            return lines
        return [
            "Todavia no hay decisiones medibles en este periodo.",
            "Primero tiene que correr el analisis y luego madurar outcomes de mercado.",
        ]

    win_rate = stats.get("win_rate")
    ev = stats.get("ev")

    if ev is None:
        verdict = "hay trades cerrados, pero falta evidencia para leer edge."
    elif ev > 0.02:
        verdict = "la muestra cerrada viene positiva."
    elif ev > 0:
        verdict = "la muestra cerrada viene apenas positiva; seguir midiendo."
    else:
        verdict = "la muestra cerrada viene negativa; conviene revisar calidad de senales."

    title_key = (ev_title or "").lower()
    is_historical = "hist" in title_key and "agregado" in title_key
    scope = "historico/modelo" if is_historical else "operativo"
    win_txt = f"{win_rate:.0%}" if win_rate is not None else "N/A"
    lines = [
        verdict,
        f"Muestra: {_fmt_count(total)} trades cerrados, acierto {win_txt}, EV {_pct(ev)}.",
        f"Alcance: {scope}. Pendientes operativos por madurar: {_fmt_count(pending)}.",
    ]
    if pending_all != pending:
        lines.append(f"Auditoria total pendiente: {_fmt_count(pending_all)} señales; lo no operativo no entra al EV principal.")
    return lines


def _render_dataset_friendly(stats: dict) -> list[str]:
    dataset_stats = stats.get("dataset_stats", [])
    totals = _dataset_totals(dataset_stats)
    lines = ["<b>Datos usados</b>"]

    if not dataset_stats:
        return lines + ["   Sin eventos en decision_log para este periodo.", ""]

    lines += [
        (
            f"   Eventos: <b>{_fmt_count(totals['events'])}</b> | "
            f"5D cerrados: <b>{_fmt_count(totals['closed_5d'])}</b> | "
            f"10D: {_fmt_count(totals['closed_10d'])} | "
            f"20D: {_fmt_count(totals['closed_20d'])}"
        )
    ]
    if totals["legacy"]:
        lines.append(f"   Legacy externo omitido: {_fmt_count(totals['legacy'])}")

    note = _dataset_group_note(dataset_stats)
    if note:
        lines.append(f"   Nota: {escape(note)}")
    lines.append("   Metrica principal: solo ejecucion real; el resto queda como auditoria.")
    lines.append("   Timing: si la senal fue EOD, el outcome usa la proxima rueda cuando existe.")

    lines += ["", "   Desglose:"]
    for row in dataset_stats[:8]:
        label = escape(_dataset_label(row))
        lines.append(
            f"   - <code>{label}</code>: "
            f"<b>{int(row.get('n') or 0)}</b> eventos | "
            f"5D {int(row.get('con_5d') or 0)} | "
            f"10D {int(row.get('con_10d') or 0)} | "
            f"20D {int(row.get('con_20d') or 0)}"
        )
    if len(dataset_stats) > 8:
        lines.append(f"   - ... {len(dataset_stats) - 8} grupos mas")

    lines.append("")
    return lines


def _render_operational_context(stats: dict) -> list[str]:
    ctx = stats.get("operational_context") or {}
    broker = ctx.get("broker_fills") or {}
    now = datetime.now()
    closed_reason = market_closed_reason(now)
    outside_hours = now.time() < time(10, 30) or now.time() >= time(17, 0)
    if is_trading_day(now) and not outside_hours:
        market_label = "rueda"
    else:
        market_label = "cerrado"
    if closed_reason:
        market_label += f" ({closed_reason})"
    elif outside_hours and is_trading_day(now):
        market_label += " (fuera de horario)"

    lines = ["<b>Contexto operativo</b>"]
    lines.append(f"   Mercado: <b>{escape(market_label)}</b>")
    if ctx.get("latest_candle_day"):
        lines.append(f"   Ultima vela canonica: <b>{ctx['latest_candle_day']}</b>")
    if ctx.get("latest_market_ts"):
        latest_market_ts = ctx["latest_market_ts"]
        if getattr(latest_market_ts, "tzinfo", None):
            latest_market_ts = latest_market_ts.astimezone(ART)
        lines.append(f"   Ultimo precio guardado: <b>{latest_market_ts.strftime('%d/%m %H:%M')}</b>")

    if broker:
        total = int(broker.get("total") or 0)
        reconciled = int(broker.get("reconciled") or 0)
        unreconciled = int(broker.get("unreconciled") or 0)
        lines.append(
            f"   Movimientos/Fills Cocos: <b>{_fmt_count(total)}</b> | "
            f"reconciliados {_fmt_count(reconciled)} | pendientes {_fmt_count(unreconciled)}"
        )
        latest_fill = broker.get("latest_executed_at")
        latest_portfolio = ctx.get("latest_portfolio_ts")
        signatures_today = int(ctx.get("position_signatures_today") or 0)
        if latest_fill and getattr(latest_fill, "tzinfo", None):
            latest_fill_art = latest_fill.astimezone(ART)
            lines.append(f"   Ultimo fill canonico: <b>{latest_fill_art.strftime('%d/%m')}</b>")
        elif latest_fill:
            lines.append(f"   Ultimo fill canonico: <b>{latest_fill.strftime('%d/%m')}</b>")
        if latest_portfolio and latest_fill:
            latest_portfolio_art = (
                latest_portfolio.astimezone(ART)
                if getattr(latest_portfolio, "tzinfo", None)
                else latest_portfolio
            )
            latest_fill_art = (
                latest_fill.astimezone(ART)
                if getattr(latest_fill, "tzinfo", None)
                else latest_fill
            )
            if latest_portfolio_art.date() > latest_fill_art.date() and signatures_today > 1:
                lines.append(
                    "   Aviso: el portfolio cambio hoy, pero Cocos movements todavia "
                    "no expuso fills canonicos de hoy; performance/Bot vs Humano "
                    "los toma cuando aparezcan o se materialicen."
                )
        if total and not reconciled:
            lines.append("   Aclaracion: hay fills reales, pero no estan cruzados con decision_log; performance real sigue en espera.")
    else:
        lines.append("   Movimientos/Fills Cocos: sin tabla o sin datos sincronizados.")
    lines.append("")
    return lines


def _render_main_sample(stats: dict) -> list[str]:
    total = int(stats.get("total_trades") or 0)
    pending = int(stats.get("pending") or 0)
    pending_all = int(stats.get("pending_all") or pending)
    source_stats = stats.get("source_stats") or []

    lines = [
        f"   Base: <b>{_fmt_count(total)}</b> outcomes operativos 5D cerrados.",
        "   Excluye: BLOCKED, THEORETICAL y APPROVED sin fill.",
    ]
    if pending_all != pending:
        non_operational = max(0, pending_all - pending)
        lines.append(
            f"   Pendientes fuera del EV principal: {_fmt_count(non_operational)} "
            "senales no operativas."
        )

    if source_stats:
        parts = []
        for row in source_stats[:3]:
            label = "/".join([
                str(row.get("source") or "?"),
                str(row.get("status") or "?"),
            ])
            parts.append(f"{label}: {_fmt_count(row.get('events'))}")
        lines.append(f"   Fuentes usadas: {escape(' | '.join(parts))}")

    return lines


def _pending_label(row: dict, closed_reason: str | None) -> str:
    source = str(row.get("source") or "")
    status = str(row.get("status") or "")

    if source == "execution_plan" and status == "APPROVED":
        base = "outcome pendiente; plan aprobado, no fill confirmado"
    elif status in {"EXECUTED", "EXECUTED_MANUAL"}:
        base = "outcome pendiente; ejecucion real"
    else:
        base = "outcome pendiente"

    if closed_reason:
        base += "; mercado cerrado/no madura hoy"
    return base


def _recent_decision_notes(row: dict) -> list[str]:
    direction = str(row.get("decision") or "").upper()
    recent_buy_at = row.get("recent_buy_at")
    if direction != "SELL" or not recent_buy_at:
        return []

    layers = _json_payload(row.get("layers"))
    lines: list[str] = []
    date_txt = recent_buy_at.strftime("%d/%m") if hasattr(recent_buy_at, "strftime") else str(recent_buy_at)
    buy_price = _money_ars(row.get("recent_buy_price"))
    buy_amount = _money_ars(row.get("recent_buy_amount"))

    lines.append(
        f"      - Reversion reciente: hubo compra real el {date_txt} "
        f"@ {buy_price} por {buy_amount}; esta linea es senal, no venta confirmada."
    )

    reason = layers.get("reason") or row.get("block_reason")
    if reason:
        lines.append(f"      - Motivo del planner: {escape(str(reason))}.")

    layer_parts = []
    for label, key in (("tecnico", "technical"), ("riesgo", "risk"), ("macro", "macro"), ("sentiment", "sentiment")):
        value = _layer_component(layers, key)
        if value is not None:
            layer_parts.append(f"{label} {value:+.3f}")
    if layer_parts:
        lines.append(f"      - Capas: {escape(' | '.join(layer_parts))}.")

    current_weight = layers.get("current_weight")
    target_weight = layers.get("target_weight")
    if current_weight is not None and target_weight is not None:
        try:
            lines.append(
                "      - Lectura: recorte parcial/rebalanceo "
                f"{float(current_weight):.1%} -> {float(target_weight):.1%}; "
                "no es take-profit ni stop ejecutado."
            )
        except Exception:
            pass

    source_mode = layers.get("technical_data_source_mode")
    source_counts = layers.get("technical_candle_source_counts")
    if source_mode or source_counts:
        if isinstance(source_counts, dict):
            counts_txt = ", ".join(f"{k}:{v}" for k, v in source_counts.items())
        else:
            counts_txt = str(source_counts or "")
        lines.append(
            "      - Historia tecnica: "
            f"{escape(str(source_mode or 'desconocida'))} "
            f"{escape(counts_txt)}."
        )

    return lines


def render_performance_report(stats: dict) -> str:
    """Reporte compacto y legible para Telegram."""
    total = int(stats.get("total_trades") or 0)
    pending = int(stats.get("pending") or 0)
    pending_all = int(stats.get("pending_all") or pending)
    days = int(stats.get("lookback_days") or 90)
    owner_chat_id = stats.get("owner_chat_id")

    ev_title, ev_note = _ev_scope(stats.get("dataset_stats", []))
    lines = [
        "📊 <b>Performance</b>",
        f"Periodo: <b>{days} dias</b> | {datetime.now(ART).strftime('%d/%m/%Y %H:%M')} ART",
        "",
        "<b>Resumen</b>",
        *[f"   {escape(line)}" for line in _friendly_summary(stats, ev_title)],
        "",
    ]

    lines += _render_dataset_friendly(stats)
    lines += _render_operational_context(stats)

    if total == 0:
        dataset_total = _dataset_totals(stats.get("dataset_stats", []))["events"]
        lines.append("<b>Que significa</b>")
        if pending > 0:
            lines.append(f"   Hay <b>{_fmt_count(pending)}</b> ejecuciones reales pendientes de outcome.")
            lines.append("   No es error: aun falta recorrido de mercado para cerrarlas.")
        elif pending_all > 0:
            lines.append(f"   Hay <b>{_fmt_count(pending_all)}</b> senales pendientes de auditoria.")
            lines.append("   No entran al EV operativo hasta ser fills reales o cerrar outcome.")
        elif owner_chat_id is not None:
            lines.append("   Este usuario todavia no acumulo decisiones propias con outcome cerrado.")
        elif dataset_total > 0:
            lines.append("   decision_log ya tiene eventos; falta que maduren outcomes canonicos.")
        else:
            lines.append("   Aun no hay decisiones con outcome cerrado.")
            lines.append("   Verifica que run_analysis.py este guardando eventos en decision_log.")
        return "\n".join(lines)

    win_rate = stats.get("win_rate")
    avg_win = stats.get("avg_win_5d")
    avg_loss = stats.get("avg_loss_5d")
    ev = stats.get("ev")
    winners = int(stats.get("winners") or 0)
    losers = int(stats.get("losers") or 0)

    lines += [
        "<b>Metricas principales (solo ejecucion real)</b>",
        *_render_main_sample(stats),
        (
            f"   Aciertos: <b>{win_rate:.0%}</b> "
            f"({winners} ganadoras / {losers} perdedoras)"
            if win_rate is not None
            else "   Aciertos: <b>N/A</b>"
        ),
        f"   Ganancia promedio al acertar: <b>{_pct(avg_win)}</b>",
        f"   Perdida promedio al fallar: <b>{_pct(avg_loss)}</b>",
        f"   {ev_title}: <b>{_pct(ev)}</b>",
        f"   {_ev_label(ev, historical_only=('hist' in ev_title.lower() and 'agregado' in ev_title.lower()))}",
        f"   <i>{escape(ev_note)}</i>",
        "",
        "<b>Retornos promedio</b>",
        f"   5d:  {_pct(stats.get('avg_return_5d'))}",
        f"   10d: {_pct(stats.get('avg_return_10d'))}",
        f"   20d: {_pct(stats.get('avg_return_20d'))}",
        f"   Mejor trade: {_pct(stats.get('best_trade'))}",
        f"   Peor trade:  {_pct(stats.get('worst_trade'))}",
        f"   Pendientes operativos: <b>{_fmt_count(pending)}</b>",
    ]

    ticker_stats = stats.get("ticker_stats", [])
    if ticker_stats:
        lines += ["", "<b>Por ticker</b>"]
        for ts in ticker_stats[:6]:
            t = escape(str(ts.get("ticker", "?")))
            trades = int(ts.get("trades") or 0)
            wins = int(ts.get("wins") or 0)
            avg_ret = ts.get("avg_return")
            wr = wins / trades if trades > 0 else 0
            lines.append(f"   {t}: {trades} trades | acierto {wr:.0%} | avg {_pct(avg_ret)}")

    recent = stats.get("recent", [])
    if recent:
        lines += ["", "<b>Ultimas decisiones</b>"]
        closed_reason = market_closed_reason(datetime.now(ART))
        for r in recent[:6]:
            ticker = escape(str(r.get("ticker", "?")))
            direction = escape(str(r.get("decision", "?")))
            score = float(r.get("final_score") or 0.0)
            outcome = r.get("outcome_5d")
            correct = r.get("was_correct")
            decided = r.get("decided_at")
            date_str = decided.astimezone(ART).strftime("%d/%m") if decided else "?"
            source = r.get("source")
            status = r.get("status")
            tag_parts = [str(v) for v in (source, status) if v]
            tag = f" <code>[{escape('/'.join(tag_parts))}]</code>" if tag_parts else ""
            if correct is True:
                result = f"OK {_pct(outcome)}"
            elif correct is False:
                result = f"Fallo {_pct(outcome)}"
            else:
                result = _pending_label(r, closed_reason)
            lines.append(
                f"   {date_str} <b>{direction} {ticker}</b>{tag} "
                f"score <code>{score:+.3f}</code> -> {result}"
            )
            lines.extend(_recent_decision_notes(r))

    curve = stats.get("equity_curve", [])
    if curve and len(curve) >= 2:
        lines += [
            "",
            "<b>Equity curve</b>",
            f"   Inicio: 100 -> Actual: <b>{float(stats.get('equity_end', 100.0)):.1f}</b>",
            f"   Retorno acumulado: <b>{float(stats.get('equity_return', 0.0)):+.1%}</b>",
            f"   Max drawdown: <b>{float(stats.get('equity_max_drawdown', 0.0)):.1%}</b>",
        ]

    lines += [
        "",
        "<i>EV = (win_rate x avg_win) - (loss_rate x avg_loss).</i>",
        "<i>Usalo como termometro, no como sentencia: el dataset todavia puede estar chico.</i>",
    ]
    return "\n".join(lines)


async def async_main(args: argparse.Namespace) -> int:
    cfg = get_config()
    owner_chat_id = args.owner_chat_id
    db = PortfolioDatabase(cfg.database.url)

    try:
        await db.connect()
        stats = await db.get_performance_stats_v2(
            lookback_days=args.days,
            owner_chat_id=owner_chat_id,
        )
        stats["dataset_stats"] = await _get_decision_dataset_stats(
            db,
            lookback_days=args.days,
            owner_chat_id=owner_chat_id,
        )
        stats["operational_context"] = await _get_operational_context(db)
        stats["owner_chat_id"] = owner_chat_id

        report = render_performance_report(stats)

        if not args.no_telegram:
            notifier = TelegramNotifier(
                cfg.scraper.telegram_bot_token,
                cfg.scraper.telegram_chat_id,
            )
            notifier.send_raw(report)

        print(report)
        return 0
    except Exception as exc:
        print(f"ERROR run_performance: {exc}", file=sys.stderr)
        logger.error("run_performance fallo: %s", exc, exc_info=True)
        return 1
    finally:
        try:
            await db.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reporte de performance del sistema")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--owner-chat-id", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
