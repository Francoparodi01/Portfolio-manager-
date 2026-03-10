"""src/core/config.py — Configuración centralizada desde variables de entorno."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class ScraperConfig:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_enabled: bool = False
    headless: bool = True
    retry_attempts: int = 3
    retry_backoff_s: int = 5
    timeout_ms: int = 60000
    screenshot_on_failure: bool = True
    screenshot_dir: str = "/app/screenshots"
    mfa_timeout: int = 120


@dataclass
class DatabaseConfig:
    url: str = ""


@dataclass
class AppConfig:
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)


_config: AppConfig | None = None


def get_config() -> AppConfig:
    global _config
    if _config is not None:
        return _config

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "")

    _config = AppConfig(
        scraper=ScraperConfig(
            telegram_bot_token=bot_token,
            telegram_chat_id=chat_id,
            telegram_enabled=bool(bot_token and chat_id),
            headless=os.environ.get("HEADLESS", "true").lower() == "true",
            retry_attempts=int(os.environ.get("RETRY_ATTEMPTS", "3")),
            retry_backoff_s=int(os.environ.get("RETRY_BACKOFF_S", "5")),
            timeout_ms=int(os.environ.get("TIMEOUT_MS", "60000")),
            screenshot_on_failure=os.environ.get("SCREENSHOT_ON_FAILURE", "true").lower() == "true",
            screenshot_dir=os.environ.get("SCREENSHOT_DIR", "/app/screenshots"),
            mfa_timeout=int(os.environ.get("TELEGRAM_MFA_TIMEOUT", "120")),
        ),
        database=DatabaseConfig(
            url=os.environ.get(
                "DATABASE_URL",
                "postgresql+asyncpg://portfolio:portfolio_secret@db:5432/portfolio",
            )
        ),
    )
    return _config
