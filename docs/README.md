# Cocos Copilot / Quantia

Este README documenta el proyecto desde evidencia del repositorio. El nombre
operativo del repo es Cocos Copilot segun [README.md](../README.md); el nombre
de presentacion Quantia aparece en [README_QUANTIA.md](../README_QUANTIA.md).
La normalizacion final de marca queda pendiente.

## Mapa de documentos

- [Negocio](business.md)
- [Requerimientos](requirements.md)
- [Arquitectura](architecture.md)
- [Modelo de datos](data-model.md)
- [Runbook operativo](runbook.md)
- [Testing y calidad](testing-quality.md)
- [Auditoria y trazabilidad](audit-traceability.md)
- [Roadmap](roadmap.md)

## Que problema resuelve

El proyecto organiza el ciclo de decision de una cartera en Cocos Capital:
observa datos de broker y mercado, calcula seniales, propone planes operables,
registra decisiones, reconcilia fills/movimientos reales y mide outcomes. La
evidencia principal esta en [scripts/run_analysis.py](../scripts/run_analysis.py),
[src/analysis/execution_planner.py](../src/analysis/execution_planner.py),
[src/collector/db.py](../src/collector/db.py) e [init.sql](../init.sql).

El objetivo practico no es predecir el mercado de forma autonoma, sino reducir
decisiones manuales poco trazables: que dato se uso, que senial la justifico,
que restricciones bloquearon una operacion, que ejecucion real ocurrio y que
resultado produjo.

## Para quien existe

- Operador/mantenedor de la cartera que necesita disciplina de datos.
- Evaluador tecnico que quiere ver arquitectura, datos, jobs y validaciones.
- Futuro mantenedor que debe entender limites entre scraping, analisis,
  decision, ejecucion observada y auditoria.

No confirmado en el repo: soporte para multiples usuarios en produccion real.
Existe configuracion multiusuario en [docker-compose.multiuser.yml](../docker-compose.multiuser.yml),
[.env.multiuser.example](../.env.multiuser.example) y tabla `bot_users`, pero no
se valido una corrida multiusuario en este relevamiento.

## Que hace

- Scraping autenticado de portfolio, mercado y actividad Cocos con
  `CocosCapitalScraper` en [src/collector/cocos_scraper.py](../src/collector/cocos_scraper.py).
- Persistencia en PostgreSQL/TimescaleDB con 20 tablas declaradas en
  [init.sql](../init.sql).
- Analisis tecnico, macro, riesgo y sentiment contextual en modulos bajo
  [src/analysis](../src/analysis).
- Synthesis de capas con pesos `technical=0.30`, `macro=0.30`, `risk=0.25`,
  `sentiment=0.15` en [src/analysis/synthesis.py](../src/analysis/synthesis.py).
- Optimizer de pesos teoricos en [src/analysis/optimizer.py](../src/analysis/optimizer.py).
- Execution planner que transforma seniales y pesos en `ExecutionPlan`,
  `DecisionIntent` y `OrderIntent` en [src/analysis/execution_planner.py](../src/analysis/execution_planner.py).
- Ledger de decisiones, fills, movements y outcomes en `decision_log`,
  `broker_fills` y `broker_movements`.
- Bot de Telegram en [scripts/telegram_bot.py](../scripts/telegram_bot.py).
- Scheduler APScheduler en [src/scheduler/runner.py](../src/scheduler/runner.py).
- Monitor read-only con API aiohttp en [src/monitor/api.py](../src/monitor/api.py)
  y UI estatica en [src/monitor/static/index.html](../src/monitor/static/index.html).
- Auditorias read-only: performance, regression, confidence, viability,
  override audit, decision ledger y decision timeline.
- Shadow forecasts 5/20/40 en tablas `shadow_thesis_*`, separados del planner.

## Que no hace

- No ejecuta ordenes automaticamente. El codigo tiene planes y estados de orden,
  pero no se verifico un modulo de envio de ordenes al broker.
- No es asesoramiento financiero.
- No garantiza resultados ni edge estadistico.
- No debe mezclar radar/shadow/debug con metricas productivas. Esa frontera esta
  representada por `run_intent`, `decision_stage`, `metric_scope` e
  `is_primary_metric` en `decision_log`, y por
  `classify_decision_audit_scope()` en [src/analysis/audit_scope.py](../src/analysis/audit_scope.py).

## Estado actual

Estado confirmado por archivos:

- Docker Compose define servicios `db`, `scheduler`, `telegram_bot` y
  `monitor_api` en [docker-compose.yml](../docker-compose.yml).
- El scheduler ejecuta jobs de apertura, post-open, pre-close, EOD, candles,
  analisis, shadow, outcomes y sentiment en [src/scheduler/runner.py](../src/scheduler/runner.py).
- El bot registra comandos de cartera, analisis, ticker, radar, performance,
  viability, ledger, policy, override, confidence, calibration, shadow y status
  en [scripts/telegram_bot.py](../scripts/telegram_bot.py).
- En este workspace local hay 80 archivos Python de test bajo [tests](../tests)
  y 314 definiciones `def test_` por inventario estatico con PowerShell. Parte
  de esa suite esta ignorada por `.gitignore`; la validacion final ejecuto 312
  casos con `310 passed` y `2 skipped`; ver [Testing y calidad](testing-quality.md).

Pendiente de validar: estado actual de servicios Docker, conexion a Cocos,
credenciales reales, volumen de datos de produccion y ultima corrida operativa.

## Stack tecnico

- Python 3.12 en [Dockerfile](../Dockerfile).
- PostgreSQL/TimescaleDB en [docker-compose.yml](../docker-compose.yml) e
  [init.sql](../init.sql).
- Playwright para scraping, `python-telegram-bot`, Redis, aiohttp/httpx/requests,
  pandas/numpy/scipy/statsmodels/scikit-learn, matplotlib/Pillow, yfinance,
  PyPortfolioOpt y APScheduler segun [requirements.txt](../requirements.txt).
- Ollama/local LLM como fuente opcional de sentiment/explicacion, configurado por
  `OLLAMA_URL`, `SENTIMENT_OLLAMA_MODEL` y `SHADOW_CAUSAL_OLLAMA_MODEL` en
  [.env.example](../.env.example).

## Como correrlo

PowerShell local:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose --profile localdb up -d db scheduler telegram_bot monitor_api
docker compose ps
```

Sin base local, usando `DATABASE_URL` externo en `.env`:

```powershell
docker compose up -d scheduler telegram_bot monitor_api
```

Monitor:

```text
http://localhost:8010/
```

Requisito: completar `.env` con credenciales Cocos, `DATABASE_URL`,
`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` si se usa Telegram, y
`MONITOR_API_TOKEN` para iniciar `monitor_api`.

## Comandos principales

```powershell
docker compose exec -T scheduler python scripts/run_once.py --full --fills --no-telegram
docker compose exec -T scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
docker compose exec -T scheduler python scripts/run_opportunity.py --no-telegram --no-persist
docker compose exec -T scheduler python scripts/run_performance.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_confidence_audit.py --days 180 --no-telegram
docker compose exec -T scheduler python scripts/run_regression_audit.py --mode execution --no-telegram
docker compose exec -T scheduler python scripts/run_viability_audit.py --days 180 --no-telegram
docker compose exec -T scheduler python scripts/run_decision_ledger.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 90
```

Los scripts existen en [scripts](../scripts). Su ejecucion real depende de DB,
credenciales y servicios activos.

## Limitaciones conocidas

- `decision_log` concentra ledger, plan, estados, outcomes y compatibilidad
  historica; es potente pero sobrecargada.
- No hay `order_id` persistido como entidad formal; `OrderIntent` vive en codigo.
- `market_snapshot_id`, `strategy_version`, `optimizer_run_id`,
  `planner_run_id` y `risk_assessment_id` estan parciales o ausentes; ver
  [docs/architecture/DECISION_LIFECYCLE.md](architecture/DECISION_LIFECYCLE.md).
- Shadow y causal analysis estan mejor separados que algunas explicaciones live.
- No confirmado en el repo: ejecucion automatica de ordenes, backtesting
  completo productivo, soporte multiusuario validado end-to-end y despliegue
  remoto endurecido.
