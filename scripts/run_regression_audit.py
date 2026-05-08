"""
scripts/run_regression_audit.py

Auditoría de regresión sobre decision_log.

Usa datos generados por el sistema:
    - final_score
    - layers
    - decision
    - outcome_5d / outcome_10d / outcome_20d
    - source
    - decision_type
    - status
    - block_reason
    - is_executable
    - was_blocked

No usa precios históricos crudos directamente.
Los retornos vienen desde decision_log, previamente calculados por update_outcomes.

Objetivo:
    Calibrar si el score del sistema tiene relación con resultados futuros,
    separando distintos niveles de decisión.

Modos de auditoría:
    signal:
        Evalúa si el score predice retornos, mezclando señales BUY/SELL
        con outcome disponible.

    optimizer:
        Evalúa ideas teóricas del optimizer.
        Sirve para saber si el optimizer propone buenos targets,
        aunque el Execution Planner luego los bloquee.

    execution:
        Evalúa órdenes aprobadas/ejecutables por el Execution Planner.
        Esta es la auditoría más cercana a performance operativa real.

    blocked:
        Evalúa ideas bloqueadas por guards.
        Sirve para saber si los guards protegen bien o bloquean demasiado.

    all:
        Mezcla exploratoria global. Útil para diagnóstico, no para calibración final.

Targets:
    raw:
        Usa outcome_Xd tal como está guardado.

    directional:
        BUY  -> outcome_Xd
        SELL -> -outcome_Xd

Uso:
    python scripts/run_regression_audit.py --mode signal
    python scripts/run_regression_audit.py --mode optimizer
    python scripts/run_regression_audit.py --mode execution
    python scripts/run_regression_audit.py --mode blocked
    python scripts/run_regression_audit.py --mode all

Ejemplos:
    python scripts/run_regression_audit.py --mode optimizer --compact
    python scripts/run_regression_audit.py --mode execution --horizon 5d
    python scripts/run_regression_audit.py --mode blocked --target directional
    python scripts/run_regression_audit.py --mode signal --days 365
    python scripts/run_regression_audit.py --mode optimizer --no-telegram
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.regression_audit import (
    DEFAULT_HORIZONS,
    RegressionAuditConfig,
    render_regression_audit,
    render_regression_audit_compact,
    run_regression_audit,
)
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


AUDIT_MODES = ("signal", "optimizer", "execution", "blocked", "all")


def _normalize_actions(actions: Optional[list[str]]) -> Optional[tuple[str, ...]]:
    if not actions:
        return None

    cleaned = tuple(
        str(a).upper().strip()
        for a in actions
        if str(a).strip()
    )

    return cleaned or None


def _default_actions_for_mode(mode: str, target: str) -> Optional[tuple[str, ...]]:
    """
    Acciones por defecto.

    Para directional normalmente interesa BUY/SELL porque el target se ajusta
    por dirección. Para raw se puede permitir más flexibilidad, pero por defecto
    mantenemos BUY/SELL para evitar mezclar HOLD/WATCH si el usuario no lo pidió.
    """
    mode = (mode or "optimizer").lower()
    target = (target or "directional").lower()

    if target == "directional":
        return ("BUY", "SELL", "SELL_PARTIAL", "SELL_FULL")

    if mode in ("signal", "optimizer", "execution", "blocked", "all"):
        return ("BUY", "SELL", "SELL_PARTIAL", "SELL_FULL")

    return None


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditoría de regresión para calibración de score"
    )

    parser.add_argument(
        "--mode",
        choices=AUDIT_MODES,
        default="optimizer",
        help=(
            "Tipo de auditoría. "
            "signal = score general; "
            "optimizer = ideas teóricas del optimizer; "
            "execution = órdenes aprobadas/ejecutables; "
            "blocked = ideas bloqueadas por guards; "
            "all = mezcla exploratoria. "
            "Default: optimizer"
        ),
    )

    parser.add_argument(
        "--days",
        type=int,
        default=180,
        help="Lookback de decision_log en días. Default: 180",
    )

    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Fecha mínima ISO YYYY-MM-DD. Si se pasa, pisa --days.",
    )

    parser.add_argument(
        "--horizon",
        choices=["5d", "10d", "20d", "all"],
        default="all",
        help="Horizonte a auditar. Default: all",
    )

    parser.add_argument(
        "--target",
        choices=["raw", "directional"],
        default="directional",
        help=(
            "raw = outcome bruto del activo. "
            "directional = BUY usa outcome y SELL invierte outcome. "
            "Default: directional"
        ),
    )

    parser.add_argument(
        "--actions",
        nargs="*",
        default=None,
        help=(
            "Acciones a incluir. Ej: --actions BUY SELL. "
            "Si no se pasa, usa BUY SELL SELL_PARTIAL SELL_FULL."
        ),
    )

    parser.add_argument(
        "--include-non-active",
        action="store_true",
        help="En target raw, incluir HOLD/WATCH/BLOCKED si tienen outcome.",
    )

    parser.add_argument(
        "--compact",
        action="store_true",
        help="Render compacto, recomendado para Telegram.",
    )

    parser.add_argument(
        "--min-n",
        type=int,
        default=12,
        help="Mínimo de observaciones para correr modelo. Default: 12",
    )

    parser.add_argument(
        "--cost-bps",
        type=float,
        default=75.0,
        help="Costo mínimo a cubrir en basis points. 75 = 0.75%%. Default: 75",
    )

    parser.add_argument(
        "--no-telegram",
        action="store_true",
        help="No enviar a Telegram, solo stdout.",
    )

    args = parser.parse_args()

    cfg = get_config()

    horizons = DEFAULT_HORIZONS if args.horizon == "all" else (args.horizon,)

    actions = _normalize_actions(args.actions)
    if actions is None and not args.include_non_active:
        actions = _default_actions_for_mode(args.mode, args.target)

    audit_cfg = RegressionAuditConfig(
        database_url=cfg.database.url,
        days=args.days,
        since=args.since,
        min_n=args.min_n,
        cost_bps=args.cost_bps,
        horizons=horizons,
        target_mode=args.target,
        actions=actions,
        include_non_active=args.include_non_active,
        mode=args.mode,
    )

    report = await run_regression_audit(audit_cfg)

    text = (
        render_regression_audit_compact(report)
        if args.compact
        else render_regression_audit(report)
    )

    print(text)

    if args.no_telegram:
        return

    if not cfg.scraper.telegram_enabled:
        return

    try:
        TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        ).send_raw(text)
    except Exception as e:
        logger.warning("No pude enviar regression audit a Telegram: %s", e)


if __name__ == "__main__":
    asyncio.run(main())