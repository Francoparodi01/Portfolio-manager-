"""
collector/cocos_scraper.py
Scraper profesional para Cocos Capital.
Reemplaza implementación Selenium anterior.

MFA Flow (prioridad):
  1. TOTP automático: si COCOS_TOTP_SECRET está en .env → genera el código sin intervención humana
  2. Telegram manual: si no hay secret → pide el código al usuario por Telegram (fallback)
"""

from __future__ import annotations

import asyncio
from email.mime import message
import os
import re
import time
try:
    import pyotp
    HAS_PYOTP = True
except ImportError:
    HAS_PYOTP = False
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional
from pathlib import Path


import requests
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)

from src.core.config import ScraperConfig, get_config
from src.core.logger import get_logger, timed
from src.collector.data.models import (
    AssetType,
    Currency,
    MarketAsset,
    PortfolioSnapshot,
    Position,
    utcnow,
)
from src.collector.data.normalizer import (
    ConfidenceResult,
    DOMFingerprint,
    normalize_ticker,
    parse_decimal,
)


import re as _re

logger = get_logger(__name__)

SELECTOR_VERSION = "v1"

SESSION_FILE = "/app/secrets/cocos_session.json"

SELECTORS = {
    "login": {
        # Selectores verificados contra el DOM real de Cocos Capital (Feb 2026)
        "username": "input[placeholder='Ingresá tu email'], input[type='email'], input[name='email']",
        "password": "input[placeholder='Ingresá tu contraseña'], input[type='password']",
        # Botón exacto del screenshot: 'Iniciar sesión'
        "submit": "button:has-text('Iniciar sesión'), button[type='submit']",
        # MFA: inputs individuales de 1 dígito o campo único
        "mfa_single": "input[type='tel'], input[inputmode='numeric'], input[autocomplete='one-time-code']",
        "mfa_submit": "button:has-text('Confirmar'), button:has-text('Verificar'), button[type='submit']",
    },
    "portfolio": {
        # Verificado contra DOM real de Cocos Capital (Mar 2026)
        "position_row": "[class*='assetWrapper']",   # cada posición es un assetWrapper
        "total_value": "[class*='portfolioDesktop']", # contenedor principal
        "cash": "[class*='dinero'], [class*='cash']",
    },
    "market": {
        "asset_row": "[class*='instrument'], [class*='asset-row'], [class*='market-row'], tr[class*='row'], [data-testid*='instrument']",
        "ticker": "td:nth-child(1), [class*='ticker'], [class*='symbol']",
        "name": "td:nth-child(2), [class*='name'], [class*='nombre']",
        "last_price": "td:nth-child(3), [class*='last'], [class*='price'], [class*='precio']",
        "change_pct": "td:nth-child(4), [class*='change'], [class*='variacion'], [class*='delta']",
        "volume": "td:nth-child(5), [class*='volume'], [class*='volumen']",
    },
}


# ── Telegram MFA Helper ───────────────────────────────────

class TelegramMFA:
    """
    Maneja el flujo MFA via Telegram.
    Manda un mensaje pidiendo el código y hace polling hasta recibirlo.
    """

    def __init__(self, bot_token: str, chat_id: str, timeout: int = 180):
        self._token = bot_token
        self._chat_id = chat_id
        self._timeout = timeout
        self._base = f"https://api.telegram.org/bot{bot_token}"

    def send(self, message: str) -> bool:
        logger.info("Telegram MFA silenciado: %s", message[:80].replace("\n", " "))
        return True

    async def wait_for_code(self) -> Optional[str]:
        """
        Espera el código MFA via Redis BLPOP (event-driven, sin archivos).
        El bot hace LPUSH mfa:<chat_id> cuando el usuario manda los 6 dígitos.
        Usa loop de 10s para ser robusto a drops de conexión con Redis Cloud.
        """
        from src.core.redis_client import client as redis_client

        key = f"mfa:{self._chat_id}"
        await redis_client.delete(key)

        self.send(
            "🔐 <b>CÓDIGO MFA REQUERIDO</b>\n\n"
            "Enviá el código de 6 dígitos acá.\n"
            f"Tenés <b>{self._timeout // 60} minutos</b>."
        )

        logger.info(f"Esperando MFA en Redis key={key} (timeout={self._timeout}s)...")

        import time as _time
        deadline = _time.monotonic() + self._timeout

        while _time.monotonic() < deadline:
            remaining = deadline - _time.monotonic()
            if remaining <= 0:
                break
            try:
                result = await redis_client.blpop(key, timeout=int(min(10, remaining)) or 1)
            except Exception as e:
                logger.warning(f"Redis blpop error (reintentando): {e}")
                await asyncio.sleep(1)
                continue

            if result is not None:
                _, code = result
                if re.fullmatch(r"\d{6}", code):
                    logger.info(f"Código MFA recibido: {code}")
                    return code
                logger.warning(f"Código inválido: {code!r} — ignorado, seguir esperando")

        logger.error("Timeout esperando código MFA")
        self.send("⏱️ Timeout — no se recibió código a tiempo.")
        return None


# ── Cache ─────────────────────────────────────────────────

class ScraperCache:
    def __init__(self, ttl_seconds: int = 300):
        self._store: dict[str, tuple[float, object]] = {}
        self._ttl = ttl_seconds

    def get(self, key: str) -> Optional[object]:
        if key in self._store:
            ts, val = self._store[key]
            if time.monotonic() - ts < self._ttl:
                return val
            del self._store[key]
        return None

    def set(self, key: str, value: object):
        self._store[key] = (time.monotonic(), value)

    def clear(self):
        self._store.clear()


# ── Scraper principal ─────────────────────────────────────

class CocosCapitalScraper:
    
    SESSION_FILE = "/app/secrets/cocos_session.json"
    """
    Scraper async profesional para Cocos Capital.

    Uso:
        async with CocosCapitalScraper() as scraper:
            portfolio = await scraper.scrape_portfolio()
            acciones  = await scraper.scrape_market("ACCIONES")
            cedears   = await scraper.scrape_market("CEDEARS")
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        self._cfg = config or get_config().scraper
        self._cache = ScraperCache(self._cfg.cache_ttl_seconds)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._is_logged_in = False
        self._known_dom_hashes: dict[str, str] = {}

        # Telegram MFA (None si no está configurado)
        self._telegram: Optional[TelegramMFA] = None
        if self._cfg.telegram_enabled:
            self._telegram = TelegramMFA(
                self._cfg.telegram_bot_token,
                self._cfg.telegram_chat_id,
                self._cfg.telegram_mfa_timeout,
            )

    async def __aenter__(self) -> "CocosCapitalScraper":
        await self._init_browser()

        if Path(SESSION_FILE).exists():
            logger.info("Cargando sesión Playwright guardada")
            self.context = await self._browser.new_context(
                storage_state=SESSION_FILE
            )
        else:
            logger.info("No hay sesión guardada, creando contexto nuevo")
            self.context = await self._browser.new_context()

        self.page = await self.context.new_page()

        return self

    async def __aexit__(self, *_):
        await self._teardown()

    # ── Browser ──────────────────────────────────

    async def _init_browser(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._cfg.headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
            ],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            locale="es-AR",
            timezone_id="America/Argentina/Buenos_Aires",
        )
        await self._context.route(
            "**/(analytics|tracking|hotjar|sentry|gtm)/**",
            lambda route: route.abort(),
        )
        self._page = await self._context.new_page()
        logger.info("Browser inicializado", extra={"extra": {"headless": self._cfg.headless}})

    async def _teardown(self):
        for obj in [self._page, self._context, self._browser]:
            if obj:
                try:
                    await obj.close()
                except Exception:
                    pass
        if self._playwright:
            await self._playwright.stop()
        logger.info("Browser cerrado")

    async def _screenshot(self, name: str):
        if not self._cfg.screenshot_on_failure or not self._page:
            return
        os.makedirs(self._cfg.screenshot_dir, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = Path(self._cfg.screenshot_dir) / f"{name}_{ts}.png"
        try:
            await self._page.screenshot(path=str(path), full_page=True)
            logger.warning(f"Screenshot: {path}")
        except Exception as e:
            logger.error(f"Screenshot falló: {e}")

    async def _check_dom_fingerprint(self, page_key: str) -> tuple[str, str]:
        html = await self._page.content()
        current_hash = DOMFingerprint.compute(html)
        raw_hash = DOMFingerprint.raw_hash(html)
        if page_key in self._known_dom_hashes:
            sim = DOMFingerprint.similarity(self._known_dom_hashes[page_key], current_hash)
            if sim < self._cfg.dom_hash_tolerance:
                logger.warning(
                    "DOM cambió — revisar selectores",
                    extra={"extra": {"page": page_key, "similarity": round(sim, 4)}},
                )
        self._known_dom_hashes[page_key] = current_hash
        return current_hash, raw_hash

    # ── Login con Telegram MFA ────────────────────

    @timed("scraper.login")
    async def login(self) -> bool:
        """
        Login en Cocos Capital.
        Si la plataforma pide MFA, usa TOTP automático o fallback manual,
        pero sin enviar notificaciones de Telegram por login/MFA.
        """
        if self._is_logged_in:
            return True

        try:
            logger.info("Navegando a Cocos Capital...")
            await self._page.goto(
                "https://app.cocos.capital/login",
                wait_until="domcontentloaded",
                timeout=60_000,
            )

            await self._page.wait_for_selector(
                "input[type='email']",
                state="attached",
                timeout=60_000,
            )
            await asyncio.sleep(0.3)

            email_input = await self._page.query_selector("input[type='email']")
            password_input = await self._page.query_selector("input[type='password']")

            if not email_input or not password_input:
                raise RuntimeError("No se encontraron los campos email/password")

            await email_input.click()
            await email_input.type(self._cfg.username, delay=30)
            logger.info(f"Email ingresado: {self._cfg.username}")

            await password_input.click()
            await password_input.type(self._cfg.password, delay=30)
            logger.info("Password ingresado")

            await asyncio.sleep(0.2)

            submit_btn = await self._page.query_selector("button:has-text('Iniciar sesión')")
            if not submit_btn:
                submit_btn = await self._page.query_selector("button[type='submit']")
            if not submit_btn:
                raise RuntimeError("No se encontró el botón de login")

            await submit_btn.click()
            logger.info("Click en Iniciar sesión")

            try:
                await self._page.wait_for_selector(
                    "input[type='password']",
                    state="hidden",
                    timeout=15_000,
                )
            except Exception:
                pass

            await self._page.wait_for_load_state("domcontentloaded", timeout=60_000)

            # ── Login directo sin MFA ────────────
            if "capital-portfolio" in self._page.url:
                self._is_logged_in = True
                logger.info("Login exitoso sin MFA")
                return True

            # ── MFA requerido ──────────────────────
            logger.info(f"MFA requerido. URL actual: {self._page.url}")

            totp_secret = getattr(self._cfg, "totp_secret", None) or os.environ.get("COCOS_TOTP_SECRET", "")
            if totp_secret and HAS_PYOTP:
                try:
                    mfa_code = pyotp.TOTP(totp_secret).now()
                    logger.info("TOTP generado automáticamente")
                except Exception as e:
                    logger.warning(f"TOTP falló ({e}), fallback a Telegram manual")
                    mfa_code = None
            elif totp_secret and not HAS_PYOTP:
                logger.warning("COCOS_TOTP_SECRET configurado pero pyotp no instalado. Fallback a Telegram.")
                mfa_code = None
            else:
                mfa_code = None

            # ── Fallback: pedir código manualmente por Telegram/Redis ──────────────
            if not mfa_code:
                if not self._telegram:
                    raise RuntimeError(
                        "Cocos pide MFA pero no hay TOTP secret ni Telegram configurado"
                    )
                mfa_code = await self._telegram.wait_for_code()

            if not mfa_code:
                raise RuntimeError("No se recibió código MFA a tiempo")

            # ── Ingresar el código MFA ──────────────
            await self._page.wait_for_selector(
                "input",
                state="attached",
                timeout=15_000,
            )
            await asyncio.sleep(0.3)

            all_inputs = await self._page.query_selector_all("input")
            logger.info(f"Inputs en pantalla MFA: {len(all_inputs)}")

            JS_FILL = """(el, v) => {
                const setter = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value').set;
                setter.call(el, v);
                el.dispatchEvent(new Event('input',  { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }"""

            if len(all_inputs) >= 6:
                for i, digit in enumerate(mfa_code[:6]):
                    inp = all_inputs[i]
                    await inp.click()
                    await inp.evaluate(JS_FILL, digit)
                    await asyncio.sleep(0.12)
                logger.info("Código MFA ingresado dígito a dígito (React)")

            elif len(all_inputs) > 0:
                await all_inputs[0].click()
                await all_inputs[0].evaluate(JS_FILL, mfa_code)
                logger.info("Código MFA ingresado en input único")

            else:
                await self._screenshot("mfa_no_inputs")
                raise RuntimeError("No se encontraron inputs MFA en el DOM")

            await asyncio.sleep(0.3)
            await self._page.keyboard.press("Enter")
            await asyncio.sleep(2)
            await self._page.keyboard.press("Enter")

            logger.info("Navegando directo al portfolio...")
            await self._page.goto(
                "https://app.cocos.capital/capital-portfolio",
                wait_until="domcontentloaded",
                timeout=self._cfg.timeout_ms,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=60_000)

            final_url = self._page.url
            logger.info(f"URL final post-MFA: {final_url}")

            if "login" in final_url:
                await self._screenshot("login_mfa_failed")
                raise RuntimeError(f"Login fallido post-MFA. URL: {final_url}")

            self._is_logged_in = True
            logger.info("Login con MFA confirmado")
            await self.context.storage_state(path=SESSION_FILE)
            logger.info("Sesion Playwright guardada")
            return True

        except Exception as e:
            await self._screenshot("login_failure")
            logger.error(f"Login fallido: {e}")
            raise

    # ── Portfolio ─────────────────────────────────

    @timed("scraper.portfolio")
    async def scrape_portfolio(self) -> PortfolioSnapshot:
        cache_key = "portfolio"
        cached = self._cache.get(cache_key)
        if cached:
            logger.info("Portfolio desde cache")
            return cached

        await self.login()

        for attempt in range(self._cfg.retry_attempts):
            try:
                await self._page.goto(
                    self._cfg.portfolio_url, timeout=self._cfg.timeout_ms
                )
                await self._page.wait_for_load_state("domcontentloaded", timeout=60_000)
                # Esperar al menos un assetWrapper (posición) — verificado Mar 2026
                await self._page.wait_for_selector(
                    "[class*='assetWrapper']",
                    timeout=self._cfg.timeout_ms,
                    state="visible",
                )

                dom_hash, raw_hash = await self._check_dom_fingerprint("portfolio")
                positions, confidence = await self._extract_positions()
                total_value, cash = await self._extract_totals()

                snapshot = PortfolioSnapshot(
                    scraped_at=utcnow(),
                    positions=tuple(positions),
                    total_value_ars=total_value,
                    cash_ars=cash,
                    confidence_score=confidence.score,
                    dom_hash=dom_hash,
                    raw_html_hash=raw_hash,
                )

                errors = snapshot.validate()
                if errors:
                    await self._screenshot("portfolio_validation_failure")
                    raise ValueError(f"Validación fallida: {errors}")

                if not confidence.is_acceptable(self._cfg.min_confidence_score):
                    await self._screenshot("portfolio_low_confidence")
                    raise ValueError(
                        f"Confidence {confidence.score:.2f} < {self._cfg.min_confidence_score}. "
                        f"{confidence.summary()}"
                    )

                self._cache.set(cache_key, snapshot)
                logger.info(
                    "Portfolio scrapeado",
                    extra={"extra": {
                        "positions": len(positions),
                        "total_ars": str(total_value),
                        "confidence": confidence.score,
                    }},
                )
                return snapshot

            except PlaywrightTimeout as e:
                logger.warning(f"Timeout intento {attempt + 1}/{self._cfg.retry_attempts}: {e}")
                if attempt == self._cfg.retry_attempts - 1:
                    await self._screenshot("portfolio_timeout")
                    raise
                await asyncio.sleep(self._cfg.retry_backoff_s * (attempt + 1))

    async def _extract_positions(self) -> tuple[list[Position], ConfidenceResult]:
        """
        Extrae posiciones usando dos estrategias:
        1. Por elemento [class*='assetWrapper'] — selector verificado Mar 2026
        2. Fallback: regex sobre el texto completo de la página (igual que scraper viejo)
        """
        positions = []
        parse_errors = 0

        # ── Estrategia 1: por elementos assetWrapper ──────
        rows = await self._page.query_selector_all("[class*='assetWrapper']")
        logger.info(f"assetWrapper encontrados: {len(rows)}")

        for row in rows:
            try:
                # El texto de cada assetWrapper viene como:
                # "CVX\nChevron\n34\n$ 17.040,00\n$ 579.360,00\n51,05%\n$ 16.782,64\n-0,52%\n..."
                text = await row.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]

                if len(lines) < 5:
                    continue

                ticker = normalize_ticker(lines[0])
                # lines[1] = nombre (ej: "Chevron")
                # lines[2] = cantidad
                # lines[3] = precio actual
                # lines[4] = importe/valuacion

                quantity    = parse_decimal(lines[2])
                price       = parse_decimal(lines[3].replace("$", "").strip())
                market_val  = parse_decimal(lines[4].replace("$", "").strip())

                # Costo promedio suele estar en lines[6]
                avg_cost = None
                for l in lines[5:10]:
                    v = parse_decimal(l.replace("$", "").strip())
                    if v and v > 0 and v != market_val and v != price:
                        avg_cost = v
                        break

                if not ticker or quantity is None or price is None or market_val is None:
                    parse_errors += 1
                    logger.debug(f"Fila incompleta: {lines[:6]}")
                    continue

                if price <= 0 or market_val <= 0:
                    parse_errors += 1
                    continue

                pnl = (market_val - (quantity * avg_cost)) if avg_cost else Decimal("0")
                pnl_pct = (pnl / (quantity * avg_cost)) if avg_cost and avg_cost > 0 and quantity > 0 else Decimal("0")

                positions.append(Position(
                    ticker=ticker,
                    asset_type=AssetType.CEDEAR,  # Cocos portfolio son mayormente CEDEARs
                    currency=Currency.ARS,
                    quantity=quantity,
                    avg_cost=avg_cost or Decimal("0"),
                    current_price=price,
                    market_value=market_val,
                    unrealized_pnl=pnl,
                    unrealized_pnl_pct=pnl_pct,
                ))
                logger.debug(f"Posición: {ticker} x{quantity} @ ${price} = ${market_val}")

            except Exception as e:
                parse_errors += 1
                logger.warning(f"Error parseando assetWrapper: {e}")

        # ── Estrategia 2: fallback regex sobre texto plano ─
        if not positions:
            logger.info("Fallback: extracción por regex sobre texto de página")
            try:
                full_text = await self._page.inner_text("body")
                # Patrón: TICKER\nNombre\nCantidad\n$Precio\n$Importe
                import re as _re
                pattern = _re.compile(r'([A-Z]{1,6})\n([A-Za-z .&\-]+)\n(\d+)\n\$\s*([\d.,]+)\n\$\s*([\d.,]+)')
                for m in pattern.finditer(full_text):
                    ticker_m, _, qty_m, price_m, val_m = m.groups()
                    quantity   = parse_decimal(qty_m)
                    price      = parse_decimal(price_m)
                    market_val = parse_decimal(val_m)
                    if quantity and price and market_val and price > 0 and market_val > 0:
                        positions.append(Position(
                            ticker=normalize_ticker(ticker_m),
                            asset_type=AssetType.CEDEAR,
                            currency=Currency.ARS,
                            quantity=quantity,
                            avg_cost=Decimal("0"),
                            current_price=price,
                            market_value=market_val,
                            unrealized_pnl=Decimal("0"),
                            unrealized_pnl_pct=Decimal("0"),
                        ))
            except Exception as e:
                logger.warning(f"Fallback regex falló: {e}")

        checks = [
            ("rows_found",      len(rows) > 0 or len(positions) > 0, 2.0),
            ("parse_success",   parse_errors == 0,                   3.0),
            ("min_positions",   len(positions) >= 1,                  2.0),
            ("prices_positive", all(p.current_price > 0 for p in positions), 3.0),
        ]
        return positions, ConfidenceResult.compute(checks)

    async def _extract_totals(self) -> tuple[Decimal, Decimal]:
        """
        Extrae valor total (Tenencia valorizada) y cash.
        
        REGLA:
        - total_ars = valor ANTES de "Tenencia valorizada"
        - cash_ars  = valor bajo "Peso Argentino"
        
        Nota:
        En Cocos, la tenencia valorizada NO incluye el cash.
        """

        total = Decimal("0")
        cash = Decimal("0")

        try:
            import re as _re

            text = await self._page.inner_text("body")

            # =========================================================
            # 🎯 TOTAL — anclado a "Tenencia valorizada" (CRÍTICO)
            # =========================================================
            m_total = _re.search(
                r'\$\s*([\d\.,]+)\s*\n\s*Tenencia valorizada',
                text,
                _re.S,
            )

            # fallback por si Cocos cambia el orden
            if not m_total:
                m_total = _re.search(
                    r'Tenencia valorizada[\s\S]{0,40}?\$\s*([\d\.,]+)',
                    text,
                    _re.S,
                )

            if m_total:
                total = parse_decimal(m_total.group(1)) or Decimal("0")
            else:
                logger.warning("No se pudo extraer Tenencia valorizada")

            # =========================================================
            # 💰 CASH — robusto a saltos de línea de React
            # =========================================================
            m_cash = _re.search(
                r'Peso Argentino[\s\S]{0,60}?AR\$\s*([\d\.,]+)',
                text,
                _re.S,
            )

            if m_cash:
                cash = parse_decimal(m_cash.group(1)) or Decimal("0")
            else:
                logger.warning("No se pudo extraer cash ARS")

        except Exception as e:
            logger.warning(f"Error extrayendo totales: {e}")

        logger.info(f"Total portfolio: ${total}, Cash: ${cash}")
        return total, cash
    # ── Market ────────────────────────────────────

    @timed("scraper.market")
    async def scrape_market(self, market_type: str) -> list[MarketAsset]:
        """
        Scraping del mercado de Cocos Capital.
        La URL /market/ACCIONES y /market/CEDEARS carga la tabla directamente.
        No requiere interacción con dropdown.
        """
        assert market_type in ("ACCIONES", "CEDEARS")

        await self.login()

        url = (
            self._cfg.market_acciones_url
            if market_type == "ACCIONES"
            else self._cfg.market_cedears_url
        )
        asset_type = AssetType.ACCION if market_type == "ACCIONES" else AssetType.CEDEAR

        try:
            logger.info(f"Navegando a mercado {market_type}: {url}")
            await self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await asyncio.sleep(2.0)

            # Esperar que aparezca al menos un ticker conocido
            try:
                await self._page.wait_for_function(
                    """
                    () => {
                        const t = document.body.innerText;
                        return (
                            t.includes("AAPL") || t.includes("NVDA") ||
                            t.includes("CVX")  || t.includes("MSFT") ||
                            t.includes("YPF")  || t.includes("GGAL") ||
                            t.includes("AMZN") || t.includes("Especie")
                        );
                    }
                    """,
                    timeout=20_000,
                )
                logger.info(f"Tabla {market_type} cargada")
            except Exception:
                logger.warning(f"Timeout esperando tabla — intentando parsear igual")

            # Scroll completo para forzar render de filas virtualizadas
            prev_height = 0
            for _ in range(20):
                await self._page.mouse.wheel(0, 3000)
                await asyncio.sleep(0.3)
                height = await self._page.evaluate("document.body.scrollHeight")
                if height == prev_height:
                    break
                prev_height = height

            await self._page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(0.5)

            assets = await self._parse_market_dom(asset_type)

            if not assets:
                await self._screenshot(f"market_{market_type}_empty")
                logger.error(f"0 activos scrapeados para {market_type}")
                return []

            logger.info(f"Market {market_type}: {len(assets)} activos")
            return assets

        except Exception as e:
            await self._screenshot(f"market_{market_type}_error")
            logger.error(f"scrape_market({market_type}) falló: {e}")
            return []

    async def _parse_market_dom(self, asset_type: "AssetType") -> list["MarketAsset"]:
        """
        Parser para la tabla de mercado de Cocos Capital.
        Estructura real (Mar 2026): Especie | Último Precio | Var% | CC | PC | PV | CV
        """
        import re as _re
        assets: list[MarketAsset] = []
        seen: set[str] = set()

        # Scroll adicional para capturar filas virtualizadas
        for _ in range(8):
            await self._page.mouse.wheel(0, 2500)
            await asyncio.sleep(0.35)
        await self._page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.3)

        # Intentar con selectores de fila
        rows = []
        for sel in ["tr", "[class*=\'tableRow\']", "[class*=\'row\']", "[class*=\'instrument\']"]:
            try:
                rows = await self._page.query_selector_all(sel)
                if len(rows) > 5:
                    logger.info(f"Parser: {len(rows)} filas con selector '{sel}'")
                    break
            except Exception:
                continue

        if len(rows) > 5:
            for row in rows:
                try:
                    text = (await row.inner_text()).strip()
                    if not text or len(text) < 3:
                        continue
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if len(lines) < 2:
                        continue

                    ticker = None
                    price = None
                    change = Decimal("0")

                    for line in lines:
                        if ticker is None:
                            word = line.split()[0] if line.split() else ""
                            if _re.fullmatch(r"[A-Z][A-Z0-9\.]{1,5}", word):
                                ticker = normalize_ticker(word)
                        if price is None:
                            m = _re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", line)
                            if m:
                                price = parse_decimal(m.group(1))
                        m_chg = _re.search(r"([+\-]?\d+,\d+)\s*%", line)
                        if m_chg:
                            change = parse_decimal(m_chg.group(1)) or Decimal("0")

                    if not ticker or ticker in seen:
                        continue
                    if ticker in ("ESPECIE", "TICKER", "ULTIMO"):
                        continue
                    if not price or price <= 0:
                        continue

                    seen.add(ticker)
                    assets.append(MarketAsset(
                        ticker=ticker, name=ticker,
                        asset_type=asset_type, currency=Currency.ARS,
                        last_price=price, change_pct_1d=change,
                        volume=None, scraped_at=utcnow(),
                    ))
                except Exception:
                    continue

        # Fallback: parsear innerText completo si el parser de filas falla
        if len(assets) < 3:
            logger.warning(f"Pocas filas ({len(assets)}) — usando fallback de texto")
            assets = await self._parse_market_text_fallback(asset_type)

        logger.info(f"Parser market → {len(assets)} activos")
        return assets

    async def _parse_market_text_fallback(self, asset_type: "AssetType") -> list["MarketAsset"]:
        """Fallback: extrae tickers y precios del innerText completo."""
        import re as _re
        assets: list[MarketAsset] = []
        seen: set[str] = set()
        try:
            text = await self._page.inner_text("body")
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            i = 0
            while i < len(lines):
                line = lines[i]
                if _re.fullmatch(r"[A-Z][A-Z0-9\.]{1,5}", line):
                    ticker = normalize_ticker(line)
                    if ticker not in seen:
                        price = None
                        change = Decimal("0")
                        for j in range(i + 1, min(i + 8, len(lines))):
                            m = _re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", lines[j])
                            if m and price is None:
                                price = parse_decimal(m.group(1))
                            m_chg = _re.search(r"([+\-]?\d+,\d+)\s*%", lines[j])
                            if m_chg:
                                change = parse_decimal(m_chg.group(1)) or Decimal("0")
                        if price and price > 0:
                            seen.add(ticker)
                            assets.append(MarketAsset(
                                ticker=ticker, name=ticker,
                                asset_type=asset_type, currency=Currency.ARS,
                                last_price=price, change_pct_1d=change,
                                volume=None, scraped_at=utcnow(),
                            ))
                i += 1
        except Exception as e:
            logger.error(f"Fallback text parser: {e}")
        logger.info(f"Fallback parser → {len(assets)} activos")
        return assets

    async def save_market_prices(self, assets: list[MarketAsset]) -> int:
        if not assets:
            return 0

        query = """
            INSERT INTO market_prices
                (ts, ticker, asset_type, currency,
                last_price, change_pct_1d, volume)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (ts, ticker) DO NOTHING
        """

        rows = [
            (
                a.scraped_at,
                a.ticker,
                a.asset_type,
                a.currency,
                float(a.last_price),
                float(a.change_pct_1d or 0),
                float(a.volume) if a.volume else None,
            )
            for a in assets
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(query, rows)

        logger.info(f"{len(rows)} precios guardados")
        return len(rows)
    # ── Helpers ───────────────────────────────────

    async def _get_text(self, parent, selector: str) -> Optional[str]:
        for sel in selector.split(", "):
            try:
                el = await parent.query_selector(sel.strip())
                if el:
                    return (await el.inner_text()).strip()
            except Exception:
                continue
        return None

    async def _get_decimal_from_page(self, selector: str) -> Optional[Decimal]:
        for sel in selector.split(", "):
            try:
                el = await self._page.query_selector(sel.strip())
                if el:
                    return parse_decimal((await el.inner_text()).strip())
            except Exception:
                continue
        return None