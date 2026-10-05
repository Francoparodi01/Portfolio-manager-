"""
Run the read-only decision-vs-market audit.

This command compares persisted Quantia decisions with matured market outcomes,
benchmark context, and heuristic calibration gaps. It does not sync, update, or
persist operational database state.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.decision_market_audit import (  # noqa: E402
    DEFAULT_BENCHMARKS,
    DecisionMarketAuditConfig,
    load_decision_market_audit,
    render_decision_market_audit,
    report_to_json,
    write_report_files,
)
from src.core.config import get_config  # noqa: E402


def _configured_owner_chat_id(cfg) -> int | None:
    value = str(cfg.scraper.telegram_chat_id or "").strip()
    return int(value) if value.isdigit() else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoria read-only decision vs market")
    parser.add_argument("--days", type=int, default=180, help="Ventana de decision_log en dias")
    parser.add_argument("--cost-bps", type=float, default=75.0, help="Costo usado para retorno neto")
    parser.add_argument("--owner-chat-id", type=int, default=None, help="Filtrar por owner_chat_id")
    parser.add_argument(
        "--benchmarks",
        nargs="+",
        default=list(DEFAULT_BENCHMARKS),
        help="Benchmarks presentes en market_candles, default SPY QQQ",
    )
    parser.add_argument(
        "--output-dir",
        default="output/decision-market-audit",
        help="Directorio donde guardar JSON y Markdown",
    )
    parser.add_argument("--json", action="store_true", help="Imprimir JSON en stdout")
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    cfg = get_config()
    owner_chat_id = args.owner_chat_id if args.owner_chat_id is not None else _configured_owner_chat_id(cfg)
    if owner_chat_id is None:
        raise SystemExit("Se necesita --owner-chat-id o TELEGRAM_CHAT_ID numérico para esta auditoría.")
    include_legacy_null = bool(owner_chat_id is not None and not cfg.multiuser_enabled)
    report = await load_decision_market_audit(
        DecisionMarketAuditConfig(
            database_url=cfg.database.url,
            days=args.days,
            cost_bps=args.cost_bps,
            owner_chat_id=owner_chat_id,
            include_legacy_null=include_legacy_null,
            benchmarks=tuple(args.benchmarks),
        )
    )
    output_dir = Path(args.output_dir)
    json_path, md_path = write_report_files(report, output_dir)

    if args.json:
        print(report_to_json(report))
    else:
        print(render_decision_market_audit(report))
        print(f"\nArchivos escritos:\n- {json_path}\n- {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
