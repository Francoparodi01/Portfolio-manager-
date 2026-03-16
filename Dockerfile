FROM python:3.12-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash scraper

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxfixes3 \
    libxext6 \
    libx11-6 \
    libasound2 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libpango-1.0-0 \
    libexpat1 \
    fonts-unifont \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

RUN playwright install chromium

COPY . .

# ── directorios de runtime ─────────────────────
RUN mkdir -p /app/screenshots /app/logs /app/secrets \
    && mkdir -p /tmp/cocos_mfa \
    && chmod 777 /tmp/cocos_mfa \
    && chown -R scraper:scraper /app \
    && chmod 700 /app/secrets

USER scraper

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEADLESS=true \
    SCREENSHOT_DIR=/app/screenshots \
    LOG_DIR=/app/logs

CMD ["python", "-m", "src.scheduler.runner"]