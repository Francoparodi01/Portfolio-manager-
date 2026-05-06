"""
scripts/run_regression_audit.py

Auditoría de regresión sobre decision_log.

Usa datos generados por el sistema:
    - final_score
    - layers
    - decision
    - outcome_5d / outcome_10d / outcome_20d

No usa precios históricos crudos directamente.
Los retornos vienen desde decision_log, previamente calculados por update_outcomes.

Objetivo:
    Calibrar si el score del sistema tiene relación con resultados futuros.

Targets:
    raw:
        Usa outcome_Xd tal como está guardado.

    directional:
        BUY  -> outcome_Xd
        SELL -> -outcome_Xd

Uso:
    python scripts/run_regression_audit.py
    python scripts/run_regression_audit.py --compact
    python scripts/run_regression_audit.py --days 365
    python scripts/run_regression_audit.py --horizon 5d
    python scripts/run_regression_audit.py --target raw
    python scripts/run_regression_audit.py --target directional
    python scripts/run_regression_audit.py --actions BUY SELL
    python scripts/run_regression_audit.py --since 2026-05-04
    python scripts/run_regression_audit.py --cost-bps 75
    python scripts/run_regression_audit.py --no-telegram
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


def _normalize_actions(actions: Optional[list[str]]) -> Optional[tuple[str, ...]]:
    if not actions:
        return None

    cleaned = tuple(
        str(a).upper().strip()
        for a in actions
        if str(a).strip()
    )

    return cleaned or None


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditoría de regresión para calibración de score"
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
            "Default en directional: BUY SELL SELL_PARTIAL SELL_FULL"
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