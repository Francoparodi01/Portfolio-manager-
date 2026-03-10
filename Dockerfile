FROM python:3.12-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash scraper

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# instalar navegador
RUN playwright install chromium \
    && chmod -R 755 /ms-playwright

COPY . .

RUN mkdir -p /app/screenshots /app/logs \
    && chown -R scraper:scraper /app

USER scraper

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEADLESS=true \
    SCREENSHOT_DIR=/app/screenshots \
    LOG_DIR=/app/logs

CMD ["python", "-m", "src.scheduler.runner"]