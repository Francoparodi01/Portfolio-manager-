from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from scripts.capture_cocos_history import capture_history
from scripts.import_cocos_history import _load_candles
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


def _market_from_asset_type(asset_type: str) -> str:
    normalized = (asset_type or "").upper()
    if normalized == "ACCION":
        return "ACCIONES"
    if normalized == "CEDEAR":
        return "CEDEARS"
    raise ValueError(f"asset_type no soportado: {asset_type}")


def _history_output_path(output_dir: Path, asset: dict) -> Path:
    ticker = str(asset["ticker"]).lower()
    asset_type = str(asset["asset_type"]).lower()
    return output_dir / f"{ticker}_{asset_type}_history.json"


async def _missing_history_assets(
    db: PortfolioDatabase,
    assets: list[dict],
    *,
    min_rows: int,
) -> list[dict]:
    missing = []
    for asset in assets:
        rows = await db.get_market_candles(
            asset["ticker"],
            asset_type=asset["asset_type"],
            limit=min_rows,
        )
        if len(rows) < min_rows:
            missing.append(asset)
    return missing


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill manual/excepcional de velas historicas Cocos"
    )
    parser.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    parser.add_argument("--wait-ms", type=int, default=12000)
    parser.add_argument("--pause-ms", type=int, default=12000)
    parser.add_argument("--min-rows", type=int, default=60)
    parser.add_argument("--output-dir", type=Path, default=Path("logs"))
    parser.add_argument("--all", action="store_true", help="Recapturar aun si ya hay historia")
    parser.add_argument("--import-db", action="store_true", help="Importar cada captura al terminar")
    args = parser.parse_args()

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        assets = await db.get_cocos_universe_assets()
        targets = assets if args.all else await _missing_history_assets(
            db,
            assets,
            min_rows=args.min_rows,
        )

        args.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"targets={len(targets)}")

        for index, asset in enumerate(targets, start=1):
            market = _market_from_asset_type(asset["asset_type"])
            ticker = asset["ticker"]
            path = _history_output_path(args.output_dir, asset)
            try:
                candles = await capture_history(
                    market=market,
                    ticker=ticker,
                    cdp_url=args.cdp_url,
                    wait_ms=args.wait_ms,
                )
            except RuntimeError as exc:
                print(f"{index}/{len(targets)} {asset['asset_type']} {ticker}: ERROR {exc}")
                break
            path.write_text(json.dumps(candles, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"{index}/{len(targets)} {asset['asset_type']} {ticker}: {len(candles)} velas")
            if args.import_db and candles:
                await db.save_market_candles(_load_candles(path))
            if index < len(targets) and args.pause_ms > 0:
                await asyncio.sleep(args.pause_ms / 1000)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())
