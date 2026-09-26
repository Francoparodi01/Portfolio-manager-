FROM cocos-telegram-before-diagnostics-v2:local
USER root
COPY --chown=scraper:scraper src/agentic /app/src/agentic
COPY --chown=scraper:scraper scripts/run_agent.py scripts/run_analysis.py /app/scripts/
USER scraper
