FROM cocos-telegram-before-agentic-20260923:local
USER root
COPY --chown=scraper:scraper src/agentic /app/src/agentic
COPY --chown=scraper:scraper scripts/run_agent.py /app/scripts/run_agent.py
COPY --chown=scraper:scraper scripts/telegram_bot.py /app/scripts/telegram_bot.py
USER scraper
