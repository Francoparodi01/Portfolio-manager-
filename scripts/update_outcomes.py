"""
scripts/update_outcomes.py
──────────────────────────
Rellena outcome_5d / outcome_10d / outcome_20d / outcome_40d / was_correct
para todas las decisiones guardadas donde ya pasaron ≥5 días.

Incluye THEORETICAL del optimizer — son las ideas puras que necesitamos
auditar para saber si el planner bloquea buenas oportunidades.

Uso:
  python scripts/update_outcomes.py
  python scripts/update_outcomes.py --days 180    # lookback extendido
  python scripts/update_outcomes.py --days 180 --include-theoretical
  python scripts/update_outcomes.py --dry-run     # muestra cuántas filas
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.analysis.position_hold_audit import resolve_position_hold_outcomes

logger = get_logger(__name__)


async def _count_pending(db: PortfolioDatabase, lookback_days: int) -> dict:
    """
    Diagnóstico: cuántas filas tienen outcome NULL agrupadas por status.
    Útil para --dry-run y para entender por qué el DCL no tiene datos.
    """
    query = """
        SELECT
            status,
            source,
            decision,
            outcome_basis,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE outcome_5d IS NULL)  AS sin_5d,
            COUNT(*) FILTER (WHERE outcome_10d IS NULL) AS sin_10d,
            COUNT(*) FILTER (WHERE outcome_20d IS NULL) AS sin_20d,
            COUNT(*) FILTER (WHERE outcome_40d IS NULL) AS sin_40d,
            COUNT(*) FILTER (
                WHERE price_at_decision IS NULL OR price_at_decision <= 0
            ) AS sin_price,
            MIN(decided_at) AS mas_antigua,
            MAX(decided_at) AS mas_reciente
        FROM decision_log
        WHERE decided_at >= NOW() - ($1 || ' days')::interval
          AND decided_at < NOW() - INTERVAL '5 days'
        GROUP BY status, source, decision, outcome_basis
        ORDER BY total DESC
    """
    try:
        if not db._pool:
            raise RuntimeError("DB pool no inicializado")
        async with db._pool.acquire() as conn:
            await db._ensure_outcome_horizon_columns(conn)
            rows = await conn.fetch(query, str(lookback_days))
        return {
            "rows": [dict(r) for r in rows],
            "total_pending": sum(r["sin_5d"] for r in rows),
        }
    except Exception as e:
        return {"error": str(e), "total_pending": -1}


async def main(
    lookback_days: int,
    include_theoretical: bool,
    dry_run: bool,
) -> None:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)

    try:
        await db.connect()

        # ── Diagnóstico previo ────────────────────────────────────────────
        if dry_run:
            pending = await _count_pending(db, lookback_days)
            print(f"\n📊 DIAGNÓSTICO DE OUTCOMES PENDIENTES (últimos {lookback_days} días)")
            print(f"   Total sin outcome_5d: {pending.get('total_pending', '?')}")
            if "rows" in pending:
                print(
                    f"\n   {'status':<14} {'source':<16} {'decision':<8} "
                    f"{'basis':<18} {'total':>6} {'sin_5d':>8} {'sin_40d':>8} {'sin_px':>8}"
                )
                print("   " + "─" * 90)
                for r in pending["rows"]:
                    print(
                        f"   {str(r['status']):<14} {str(r['source']):<16} "
                        f"{str(r['decision']):<8} {str(r['outcome_basis']):<18} "
                        f"{r['total']:>6} {r['sin_5d']:>8} {r['sin_40d']:>8} {r['sin_price']:>8}"
                    )
            elif "error" in pending:
                print(f"\n   ⚠️ Error de diagnóstico: {pending['error']}")
            print("\n   ℹ️  Dry-run: no se actualizó nada.")
            return

        # ── Update estándar ───────────────────────────────────────────────
        logger.info(f"Actualizando outcomes (últimos {lookback_days} días)...")

        updated = await db.update_outcomes(
            lookback_days=lookback_days,
        )
        logger.info(f"{updated} decisiones actualizadas")
        hold_updated = 0
        try:
            pool = await db.get_pool()
            async with pool.acquire() as conn:
                hold_updated = await resolve_position_hold_outcomes(
                    conn,
                    lookback_days=lookback_days,
                )
            logger.info("%s outcomes HOLD actualizados", hold_updated)
        except Exception as exc:
            logger.error(
                "Outcomes HOLD fallaron sin afectar decision_log: %s",
                exc,
                exc_info=True,
            )
        if os.getenv("RADAR_DISCOVERY_LEDGER_ENABLED", "false").lower() == "true":
            try:
                from src.analysis.radar_discovery import RadarDiscoveryStore

                pool = await db.get_pool()
                discovery_store = RadarDiscoveryStore(pool)
                discovery_updated = await discovery_store.resolve_pending_outcomes(db)
                setup_events_updated = await discovery_store.resolve_pending_setup_events(db)
                setup_outcomes_updated = await discovery_store.resolve_pending_setup_outcomes(db)
                logger.info("%s outcomes Radar Discovery actualizados", discovery_updated)
                logger.info(
                    "%s eventos y %s outcomes Radar Setup actualizados",
                    setup_events_updated,
                    setup_outcomes_updated,
                )
                print(f"Radar Discovery: {discovery_updated} outcomes por ruedas actualizados")
                print(
                    "Radar Setup: "
                    f"{setup_events_updated} eventos, "
                    f"{setup_outcomes_updated} outcomes desde trigger actualizados"
                )
            except Exception as exc:
                logger.error(
                    "Radar Discovery outcomes fallo sin afectar decision_log: %s",
                    exc,
                    exc_info=True,
                )
        print(
            f"✅ {updated} outcomes actualizados "
            "(THEORETICAL / APPROVED / EXECUTED / BLOCKED elegibles)"
        )
        print(f"HOLD audit: {hold_updated} retornos por ruedas actualizados")

        if include_theoretical:
            print(
                "ℹ️  --include-theoretical queda por compatibilidad: "
                "db.update_outcomes() ya incluye THEORETICAL si la base de precios es canónica."
            )

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        await db.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Actualiza outcomes de decisiones pasadas"
    )
    p.add_argument(
        "--days",
        type=int,
        default=180,
        help="Lookback en días (default: 180 para capturar decisiones antiguas)",
    )
    p.add_argument(
        "--include-theoretical",
        action="store_true",
        help=(
            "Compatibilidad: THEORETICAL ya se incluye automáticamente "
            "si tiene precio de entrada y base canónica."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo muestra cuántas filas tienen outcome pendiente, sin actualizar.",
    )
    args = p.parse_args()

    asyncio.run(
        main(
            lookback_days=args.days,
            include_theoretical=args.include_theoretical,
            dry_run=args.dry_run,
        )
    )
