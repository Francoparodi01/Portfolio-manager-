FROM quantia-scheduler-before-decision-lab:20260924
USER root
RUN pip install --no-cache-dir pydantic==2.13.5
COPY --chown=scraper:scraper src/ /app/src/
COPY --chown=scraper:scraper scripts/ /app/scripts/
USER scraper
