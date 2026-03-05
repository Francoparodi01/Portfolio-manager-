"""
collector/cocos_scraper.py
Scraper profesional para Cocos Capital.
Reemplaza implementación Selenium anterior.

MFA Flow (Telegram):
  1. Login con email + password
  2. Si Cocos pide MFA → manda mensaje a Telegram al usuario
  3. El usuario reenvía el código de 6 dígitos al bot
  4. El scraper lo lee por polling y lo ingresa automáticamente
"""

from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

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

    def __init__(self, bot_token: str, chat_id: str, timeout: int = 120):
        self._token = bot_token
        self._chat_id = chat_id
        self._timeout = timeout
        self._base = f"https://api.telegram.org/bot{bot_token}"

    def send(self, message: str) -> bool:
        try:
            r = requests.post(
                f"{self._base}/sendMessage",
                data={"chat_id": self._chat_id, "text": message, "parse_mode": "HTML"},
                timeout=10,
            )
            return r.status_code == 200
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

    def wait_for_code(self) -> Optional[str]:
        """
        Pide el código MFA por Telegram y espera que el usuario lo mande.
        Retorna el código de 6 dígitos o None si hay timeout.
        """
        self.send(
            "🔐 <b>CÓDIGO MFA REQUERIDO</b>\n\n"
            "Recibirás un SMS o mail con un código de 6 dígitos.\n"
            f"Tenés <b>{self._timeout // 60} minutos</b> para enviarlo acá."
        )
        logger.info(f"Esperando código MFA via Telegram (timeout: {self._timeout}s)...")

        # Obtener el último update_id para ignorar mensajes viejos
        try:
            r = requests.get(f"{self._base}/getUpdates", timeout=10)
            data = r.json()
            last_id = data["result"][-1]["update_id"] if data.get("ok") and data["result"] else 0
        except Exception:
            last_id = 0

        start = time.time()
        while time.time() - start < self._timeout:
            try:
                r = requests.get(
                    f"{self._base}/getUpdates",
                    params={"offset": last_id + 1, "timeout": 10},
                    timeout=15,
                )
                data = r.json()

                if data.get("ok") and data["result"]:
                    for update in data["result"]:
                        last_id = update["update_id"]
                        msg = update.get("message", {}).get("text", "").strip()
                        match = re.search(r"\b(\d{6})\b", msg)
                        if match:
                            code = match.group(1)
                            logger.info(f"Código MFA recibido: {code}")
                            self.send(f"✅ Código <code>{code}</code> recibido. Intentando login...")
                            return code

                time.sleep(2)
            except Exception as e:
                logger.debug(f"Polling error: {e}")
                time.sleep(2)

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
        Si la plataforma pide MFA, usa Telegram para recibir el código del usuario.
        """
        if self._is_logged_in:
            return True

        try:
            logger.info("Navegando a Cocos Capital...")
            # domcontentloaded evita timeout por websockets de Cocos
            await self._page.goto(
                "https://app.cocos.capital/login",
                wait_until="domcontentloaded",
                timeout=60_000,
            )

            # Esperar que el input exista en el DOM (state=attached, no visible)
            await self._page.wait_for_selector(
                "input[type='email']",
                state="attached",
                timeout=60_000,
            )
            await asyncio.sleep(0.3)  # hydration de React

            email_input = await self._page.query_selector("input[type='email']")
            password_input = await self._page.query_selector("input[type='password']")

            if not email_input or not password_input:
                raise RuntimeError("No se encontraron los campos email/password")

            # click + type simula send_keys de Selenium
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

            # Esperar que desaparezca el campo de password (pantalla cambió)
            try:
                await self._page.wait_for_selector(
                    "input[type='password']",
                    state="hidden",
                    timeout=15_000,
                )
            except Exception:
                pass

            await self._page.wait_for_load_state("domcontentloaded", timeout=60_000)

            # ── ¿Login directo sin MFA? ────────────
            if "capital-portfolio" in self._page.url:
                self._is_logged_in = True
                logger.info("Login exitoso sin MFA")
                if self._telegram:
                    self._telegram.send("✅ Login exitoso (sin MFA)")
                return True

            # ── MFA requerido ──────────────────────
            logger.info(f"MFA requerido. URL actual: {self._page.url}")

            if not self._telegram:
                raise RuntimeError(
                    "Cocos pide MFA pero TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID "
                    "no están configurados en el .env"
                )

            # Pedir código al usuario por Telegram (bloqueante, corre en thread)
            mfa_code = await asyncio.get_event_loop().run_in_executor(
                None, self._telegram.wait_for_code
            )

            if not mfa_code:
                raise RuntimeError("No se recibió código MFA a tiempo")

            # ── Ingresar el código MFA ──────────────
            # Cocos muestra 6 inputs individuales (3 + guión + 3)
            # Verificado en screenshot Mar 2026

            # Esperar que aparezcan los inputs del MFA
            await self._page.wait_for_selector(
                "input",
                state="attached",
                timeout=15_000,
            )
            await asyncio.sleep(0.3)

            # Obtener todos los inputs visibles en pantalla
            all_inputs = await self._page.query_selector_all("input")
            logger.info(f"Inputs en pantalla MFA: {len(all_inputs)}")

            if len(all_inputs) >= 6:
                # 6 inputs individuales — escribir dígito por dígito
                # Igual que el scraper viejo con send_keys
                for i, digit in enumerate(mfa_code[:6]):
                    await all_inputs[i].click()
                    await all_inputs[i].press(digit)
                logger.info(f"Código ingresado dígito a dígito: {mfa_code}")

            elif len(all_inputs) > 0:
                # Un solo input — escribir el código completo
                await all_inputs[0].click()
                await all_inputs[0].type(mfa_code, delay=30)
                logger.info(f"Código ingresado en input único: {mfa_code}")

            else:
                await self._screenshot("mfa_no_inputs")
                raise RuntimeError("No se encontraron inputs MFA en el DOM")

            # Pequeña espera y submit (el form suele auto-submitear al completar los 6 dígitos)
            await asyncio.sleep(0.2)
            submit_btn = await self._page.query_selector(
                "button[type='submit'], button:has-text('Confirmar'), button:has-text('Verificar'), button:has-text('Continuar'), button:has-text('Ingresar')"
            )
            if submit_btn:
                await submit_btn.click()
                logger.info("Submit MFA clickeado")
            else:
                logger.info("Sin botón submit — el form se auto-submiteó")

            logger.info("Código MFA enviado, esperando validación...")

            await asyncio.sleep(0.5)

            # Esperar que desaparezcan los inputs MFA
            try:
                await self._page.wait_for_selector(
                    SELECTORS["login"]["mfa_single"],
                    state="hidden",
                    timeout=10_000,
                )
            except Exception:
                logger.warning("Inputs MFA no desaparecieron, continuando...")

            # Forzar navegación al portfolio
            await self._page.goto(
                "https://app.cocos.capital/capital-portfolio",
                timeout=self._cfg.timeout_ms,
            )
            await self._page.wait_for_load_state("domcontentloaded", timeout=60_000)

            final_url = self._page.url
            logger.info(f"URL final post-MFA: {final_url}")

            if "login" in final_url:
                await self._screenshot("login_mfa_failed")
                raise RuntimeError(f"Login fallido post-MFA. URL: {final_url}")

            if "capital-portfolio" not in final_url:
                await self._screenshot("login_wrong_redirect")
                raise RuntimeError(f"Redirect inesperado: {final_url}")

            self._is_logged_in = True
            self._telegram.send("✅ Login exitoso con MFA")
            logger.info("Login con MFA confirmado")
            return True

        except Exception as e:
            await self._screenshot("login_failure")
            logger.error(f"Login fallido: {e}")
            if self._telegram:
                self._telegram.send(f"❌ Error en login: {e}")
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

        FIX (Mar 2026): La página de mercado carga vacía y requiere que el usuario
        seleccione el tipo desde el dropdown "Seleccione un..." para que aparezca la
        tabla. El scraper ahora simula ese click antes de esperar los datos.
        """
        assert market_type in ("ACCIONES", "CEDEARS")

        await self.login()

        url = (
            self._cfg.market_acciones_url
            if market_type == "ACCIONES"
            else self._cfg.market_cedears_url
        )
        asset_type = AssetType.ACCION if market_type == "ACCIONES" else AssetType.CEDEAR

        # Texto que aparece en el dropdown para cada tipo
        dropdown_label = "Acciones" if market_type == "ACCIONES" else "CEDEARs"

        try:
            await self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # Esperar que el header de mercado esté visible (dropdown "Seleccione un...")
            await self._page.wait_for_selector(
                "button:has-text('Seleccione un'), [class*='dropdown'], [class*='select'], [class*='filter']",
                state="visible",
                timeout=30_000,
            )
            await asyncio.sleep(1.0)  # React hydration

            # ── Interactuar con el dropdown para cargar la tabla ──────────────
            await self._select_market_dropdown(market_type, dropdown_label)

            # Esperar que aparezcan datos reales en la tabla
            await self._page.wait_for_function(
                """
                () => {
                    const t = document.body.innerText;
                    return (
                        t.includes("YPF")  ||
                        t.includes("GGAL") ||
                        t.includes("PAMP") ||
                        t.includes("CVX")  ||
                        t.includes("NVDA") ||
                        t.includes("AAPL") ||
                        t.includes("MSFT") ||
                        t.includes("Último") ||
                        t.includes("Precio")
                    )
                }
                """,
                timeout=30_000,
            )
            logger.info(f"Tabla de {market_type} cargada")

            # Scroll para forzar render de grid virtual
            for _ in range(10):
                await self._page.mouse.wheel(0, 2500)
                await asyncio.sleep(0.35)
            # Volver al inicio para capturar desde la primera fila
            await self._page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(0.5)

            assets = await self._parse_market_dom(asset_type)

            if not assets:
                await self._screenshot(f"market_{market_type}_empty")
                logger.error(f"0 activos scrapeados para {market_type} — se omite")
                return []

            logger.info(f"Market {market_type}: {len(assets)} activos")
            return assets

        except Exception as e:
            await self._screenshot(f"market_{market_type}_error")
            logger.error(f"scrape_market({market_type}) falló: {e} — retornando []")
            return []

    async def _select_market_dropdown(self, market_type: str, label: str):
        """
        Abre el dropdown "Seleccione un..." y elige el tipo de mercado.
        Prueba múltiples estrategias en cascada.
        """
        logger.info(f"Seleccionando mercado: {label}")

        # Estrategia 1: click en el botón del dropdown por texto
        try:
            btn = await self._page.query_selector(
                "button:has-text('Seleccione un'), button:has-text('Seleccioná')"
            )
            if btn:
                await btn.click()
                await asyncio.sleep(0.8)
                # Buscar la opción en el menú desplegado
                option = await self._page.query_selector(
                    f"[role='option']:has-text('{label}'), "
                    f"li:has-text('{label}'), "
                    f"div[class*='option']:has-text('{label}'), "
                    f"span:has-text('{label}')"
                )
                if option:
                    await option.click()
                    await asyncio.sleep(1.5)
                    logger.info(f"Dropdown: opción '{label}' seleccionada (estrategia 1)")
                    return
        except Exception as e:
            logger.debug(f"Estrategia 1 dropdown falló: {e}")

        # Estrategia 2: buscar el select nativo
        try:
            select = await self._page.query_selector("select")
            if select:
                await select.select_option(label=label)
                await asyncio.sleep(1.5)
                logger.info(f"Select nativo: '{label}' seleccionado (estrategia 2)")
                return
        except Exception as e:
            logger.debug(f"Estrategia 2 select nativo falló: {e}")

        # Estrategia 3: click directo en cualquier elemento que contenga el texto
        try:
            # Buscar por texto exacto en cualquier elemento clickeable
            await self._page.click(
                f"text='{label}'",
                timeout=5_000,
            )
            await asyncio.sleep(1.5)
            logger.info(f"Click por texto: '{label}' (estrategia 3)")
            return
        except Exception as e:
            logger.debug(f"Estrategia 3 click por texto falló: {e}")

        # Estrategia 4: simular la URL con query param si Cocos lo soporta
        # Algunos brokers exponen filtros via ?type=acciones o ?category=cedears
        try:
            current_url = self._page.url
            suffix = "ACCIONES" if market_type == "ACCIONES" else "CEDEARS"
            if suffix not in current_url.lower():
                new_url = f"{current_url}?category={suffix}"
                await self._page.goto(new_url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(2)
                logger.info(f"Estrategia 4: navegado a {new_url}")
        except Exception as e:
            logger.debug(f"Estrategia 4 URL param falló: {e}")

        logger.warning(f"Todas las estrategias de dropdown intentadas para {label}")

    async def _parse_market_dom(self, asset_type: "AssetType") -> list["MarketAsset"]:
        """
        Parser robusto para tabla virtualizada de Cocos.
        FIX: no esperar header hardcodeado — puede no existir.
        """
        import re
        assets: list[MarketAsset] = []
        seen: set[str] = set()

        # Header opcional — no bloquear si no existe
        try:
            await self._page.wait_for_selector(
                ".markets-table-header, [class*='tableHeader'], [class*='table-header']",
                timeout=5_000,
            )
        except Exception:
            logger.debug("Header de tabla no encontrado — continuando")

        # Scroll para forzar render del grid virtual
        for _ in range(6):
            await self._page.mouse.wheel(0, 2000)
            await asyncio.sleep(0.4)

        # Buscar filas con múltiples selectores
        rows = await self._page.query_selector_all(
            "[class*='row'], [class*='instrument'], [class*='asset'], [role='row'], tr"
        )

        logger.info(f"Market rows detectadas: {len(rows)}")

        for row in rows:
            try:
                text = (await row.inner_text()).strip()
                if not text:
                    continue

                lines = [l.strip() for l in text.split("\n") if l.strip()]
                if len(lines) < 3:
                    continue

                ticker_raw = lines[0]
                ticker = normalize_ticker(ticker_raw)

                price = None
                change = Decimal("0")

                for l in lines:
                    if price is None:
                        m_price = re.search(r"([\d\.]+,\d{2})", l)
                        if m_price:
                            price = parse_decimal(m_price.group(1))

                    m_change = re.search(r"([+\-]?\d+,\d+)%", l)
                    if m_change:
                        change = parse_decimal(m_change.group(1)) or Decimal("0")

                if not ticker or ticker in seen:
                    continue

                if not price or price <= 0:
                    continue

                seen.add(ticker)

                assets.append(
                    MarketAsset(
                        ticker=ticker,
                        name=ticker,
                        asset_type=asset_type,
                        currency=Currency.ARS,
                        last_price=price,
                        change_pct_1d=change,
                        volume=None,
                        scraped_at=utcnow(),
                    )
                )

            except Exception:
                continue

        logger.info(f"Parser market → {len(assets)} activos")
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