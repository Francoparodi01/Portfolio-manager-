"""
src/core/config.py
Configuración centralizada desde .env
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

def _env_bool(key: str, default: bool = False) -> bool:
    return _env(key, str(default)).lower() in ("1", "true", "yes")

def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default

def _env_float(key: str, default: float = 0.0) -> float:
    try:
        return float(_env(key, str(default)))
    except ValueError:
        return default

@dataclass(frozen=True)
class ScraperConfig:
    username: str = field(default_factory=lambda: _env("COCOS_USERNAME"))
    password: str = field(default_factory=lambda: _env("COCOS_PASSWORD"))
    login_url: str = field(default_factory=lambda: _env("COCOS_LOGIN_URL", "https://app.cocos.capital/login"))
    portfolio_url: str = field(default_factory=lambda: _env("COCOS_PORTFOLIO_URL", "https://app.cocos.capital/capital-portfolio"))
    market_acciones_url: str = field(default_factory=lambda: _env("COCOS_MARKET_ACCIONES_URL", "https://app.cocos.capital/market/ACCIONES"))
    market_cedears_url: str = field(default_factory=lambda: _env("COCOS_MARKET_CEDEARS_URL", "https://app.cocos.capital/market/CEDEARS"))
    headless: bool = field(default_factory=lambda: _env_bool("HEADLESS", True))
    retry_attempts: int = field(default_factory=lambda: _env_int("RETRY_ATTEMPTS", 3))
    retry_backoff_s: int = field(default_factory=lambda: _env_int("RETRY_BACKOFF_S", 5))
    timeout_ms: int = field(default_factory=lambda: _env_int("TIMEOUT_MS", 60000))
    min_confidence_score: float = field(default_factory=lambda: _env_float("MIN_CONFIDENCE_SCORE", 0.6))
    dom_hash_tolerance: float = field(default_factory=lambda: _env_float("DOM_HASH_TOLERANCE", 0.85))
    cache_ttl_seconds: int = field(default_factory=lambda: _env_int("CACHE_TTL_SECONDS", 300))
    screenshot_on_failure: bool = field(default_factory=lambda: _env_bool("SCREENSHOT_ON_FAILURE", True))
    screenshot_dir: str = field(default_factory=lambda: _env("SCREENSHOT_DIR", "./screenshots"))
    telegram_bot_token: str = field(default_factory=lambda: _env("TELEGRAM_BOT_TOKEN"))
    telegram_chat_id: str = field(default_factory=lambda: _env("TELEGRAM_CHAT_ID"))
    telegram_mfa_timeout: int = field(default_factory=lambda: _env_int("TELEGRAM_MFA_TIMEOUT", 120))

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    def validate(self) -> list[str]:
        errors = []
        if not self.username:
            errors.append("COCOS_USERNAME no configurado")
        if not self.password:
            errors.append("COCOS_PASSWORD no configurado")
        return errors

@dataclass(frozen=True)
class DatabaseConfig:
    url: str = field(default_factory=lambda: _env("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/portfolio"))

@dataclass(frozen=True)
class AppConfig:
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return AppConfig()
