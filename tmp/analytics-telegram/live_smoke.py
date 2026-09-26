import asyncio
from pathlib import Path
from types import SimpleNamespace
from scripts.run_analytics_v2 import async_main
from src.core.config import get_config
cfg = get_config()
asyncio.run(async_main(SimpleNamespace(owner_chat_id=int(cfg.scraper.telegram_chat_id), days=180, archive_out=Path('/verification/live-smoke.zip'))))
