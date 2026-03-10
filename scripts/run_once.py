"""
scripts/run_once.py
Ejecucion manual de un scrape. Util para testing y depuracion.

Uso:
    python scripts/run_once.py                    # solo portfolio
    python scripts/run_once.py --full             # portfolio + mercado
    python scripts/run_once.py --no-db            # sin guardar en DB
    python scripts/run_once.py --json output.json # guardar snapshot en archivo
"""
import argparse
import asyncio
import json
import sys
import os
import hashlib
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.cocos_scraper import CocosScraper
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.collector.data.models import PortfolioSnapshot, Position, AssetType, Currency



logger = get_logger(__name__)


def _get_cocos_credentials() -> tuple[str, str]:
    email = (
        os.environ.get("COCOS_EMAIL")
        or os.environ.get("COCOS_USERNAME")
        or os.environ.get("COCOS_USER")
        or ""
    ).strip()
    password = (os.environ.get("COCOS_PASSWORD") or os.environ.get("COCOS_PASS") or "").strip()
    return email, password


def _to_snapshot(raw: dict) -> PortfolioSnapshot:
    positions: list[Position] = []
    raw_positions = raw.get("Posiciones", []) if isinstance(raw, dict) else []
    for p in raw_positions:
        ticker = str(p.get("Ticker", "")).upper().strip()
        if not ticker:
            continue
        quantity = float(p.get("Cantidad", 0.0) or 0.0)
        current_price = float(p.get("PrecioActual", 0.0) or 0.0)
        market_value = float(p.get("Valuacion", 0.0) or 0.0)
        unrealized_pnl_pct = float(p.get("GananciaPorcentaje", 0.0) or 0.0) / 100.0
        unrealized_pnl = market_value * unrealized_pnl_pct
        positions.append(
            Position(
                ticker=ticker,
                asset_type=AssetType.CEDEAR,
                currency=Currency.USD,
                quantity=quantity,
                avg_cost=current_price,
                current_price=current_price,
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct,
                weight_in_portfolio=None,
                sector=None,
            )
        )

    total_value = float(raw.get("ValorTotal", 0.0) or 0.0)
    payload = json.dumps(raw, ensure_ascii=False, sort_keys=True)
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return PortfolioSnapshot(
        scraped_at=datetime.now(timezone.utc),
        positions=positions,
        total_value_ars=total_value,
        cash_ars=0.0,
        confidence_score=1.0 if total_value > 0 or positions else 0.0,
        dom_hash=payload_hash,
        raw_html_hash=payload_hash,
    )


async def main(full: bool = False, no_db: bool = False, json_output: str = None):
    cfg = get_config()

    cocos_email, cocos_password = _get_cocos_credentials()
    if not cocos_email or not cocos_password:
        logger.error(
            "Faltan credenciales de Cocos en el .env "
            "(usar COCOS_USERNAME/COCOS_PASSWORD o COCOS_EMAIL/COCOS_PASSWORD)"
        )
        sys.exit(1)

    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url) if not no_db else None

    try:
        if db:
            await db.connect()

        scraper = CocosScraper(
            headless=cfg.scraper.headless,
            telegram_bot_token=cfg.scraper.telegram_bot_token,
            telegram_chat_id=cfg.scraper.telegram_chat_id,
        )
        logger.info("Iniciando scrape manual...")
        ok = scraper.login_with_telegram_mfa(cocos_email, cocos_password)
        if not ok:
            logger.error("Login falló")
            sys.exit(1)

        raw_portfolio = scraper.scrape_portfolio()
        if not raw_portfolio:
            logger.error("No se pudo extraer portfolio")
            sys.exit(1)

        snapshot = _to_snapshot(raw_portfolio)

        logger.info(f"Portfolio scrapeado:")
        logger.info(f"  Total ARS:   ${snapshot.total_value_ars:,.2f}")
        logger.info(f"  Cash ARS:    ${snapshot.cash_ars:,.2f}")
        logger.info(f"  Posiciones:  {len(snapshot.positions)}")
        logger.info(f"  Confianza:   {snapshot.confidence_score:.2%}")

        for p in snapshot.positions:
            logger.info(
                f"  {p.ticker:8s} x{p.quantity:8.2f} "
                f"@ ${p.current_price:>12,.2f} "
                f"= ${p.market_value:>14,.2f}"
            )

        if db:
            sid = await db.save_snapshot(snapshot)
            logger.info(f"Snapshot guardado: {sid}")

        if json_output:
            with open(json_output, "w", encoding="utf-8") as f:
                json.dump(snapshot.to_dict(), f, indent=2, ensure_ascii=False)
            logger.info(f"JSON guardado en: {json_output}")

        if full:
            logger.warning("--full solicitado, pero CocosScraper actual no implementa scrape_market().")

    except Exception as e:
        logger.error(f"Error en run manual: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            scraper.close()
        except Exception:
            pass
        if db:
            await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape manual de Cocos Capital")
    parser.add_argument("--full", action="store_true", help="Incluir scrape de mercado")
    parser.add_argument("--no-db", action="store_true", help="No guardar en base de datos")
    parser.add_argument("--json", dest="json_output", metavar="FILE", help="Guardar snapshot en archivo JSON")
    args = parser.parse_args()

    asyncio.run(main(full=args.full, no_db=args.no_db, json_output=args.json_output))
