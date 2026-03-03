# ============================================================
# Dockerfile — Cocos Portfolio Scraper
#
# FIX 1: libgdk-pixbuf2.0-0 renombrado en Debian Trixie.
#         Se delega todo a --with-deps que resuelve la distro.
# FIX 2: playwright install debe correr como root y escribir en
#         PLAYWRIGHT_BROWSERS_PATH global ANTES del USER switch.
#         Sin esto el usuario 'scraper' no encuentra el binario.
# ============================================================
FROM python:3.12-slim

# Browsers en path global — legible por cualquier usuario
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# ── Sistema base ─────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Usuario no-root ──────────────────────────────────────────
RUN useradd --create-home --shell /bin/bash scraper
WORKDIR /app

# ── Dependencias Python ──────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Playwright: instalar como root ANTES del USER switch ─────
# --with-deps resuelve los paquetes de sistema correctos
# segun la distro (Trixie, Bookworm, etc.) sin lista manual.
# chmod asegura que 'scraper' pueda leer /ms-playwright.
RUN playwright install chromium --with-deps \
    && chmod -R 755 /ms-playwright

# ── Código fuente ────────────────────────────────────────────
COPY . .

# ── Directorios de runtime ───────────────────────────────────
RUN mkdir -p /app/screenshots /app/logs \
    && chown -R scraper:scraper /app

USER scraper

# ── Variables de entorno ─────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEADLESS=true \
    SCREENSHOT_DIR=/app/screenshots \
    LOG_DIR=/app/logs \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

CMD ["python", "-m", "src.scheduler.runner"]