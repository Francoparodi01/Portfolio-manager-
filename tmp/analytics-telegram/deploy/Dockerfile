FROM cocos-telegram-before-analytics-20260922:local
USER root
COPY requirements-analytics.txt /app/requirements-analytics.txt
RUN pip install --no-cache-dir -r /app/requirements-analytics.txt
COPY --chown=scraper:scraper src/analysis/analytics_v2 /app/src/analysis/analytics_v2
COPY --chown=scraper:scraper src/analysis/analytics_v2_live.py /app/src/analysis/analytics_v2_live.py
COPY --chown=scraper:scraper scripts/telegram_bot.py scripts/run_analytics_v2.py /app/scripts/
ENV QUANTIA_ANALYTICS_CODE_COMMIT=67d30cf739ff6deb6936d74e2541ae6391d2b22d
USER scraper
