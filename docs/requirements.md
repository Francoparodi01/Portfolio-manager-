# Documento de requerimientos

## Actores

- Operador: consulta Telegram/monitor, ejecuta manualmente en broker y puede
  cargar catalysts.
- Scheduler: dispara jobs programados en dias habiles ART.
- Bot Telegram: interfaz conversacional y orquestador de scripts.
- Monitor API: expone dashboard read-only y endpoints de diagnostico.
- Base de datos: conserva snapshots, decisiones, fills, outcomes y auditorias.
- Cocos Capital: fuente externa de portfolio, mercado, movimientos y fills.
- Ollama/local LLM: fuente opcional para sentiment y analisis causal shadow.

## Requerimientos funcionales

| ID | Requerimiento | Evidencia |
|---|---|---|
| RF-01 | Scrappear portfolio, mercado, movimientos y fills. | `CocosCapitalScraper` en [src/collector/cocos_scraper.py](../src/collector/cocos_scraper.py), [scripts/run_once.py](../scripts/run_once.py). |
| RF-02 | Persistir snapshots de portfolio y posiciones. | `save_snapshot()` en [src/collector/db.py](../src/collector/db.py), tablas `portfolio_snapshots`, `positions`, `raw_snapshots`. |
| RF-03 | Persistir precios y velas. | `save_market_prices()`, `save_market_candles()`, `build_daily_candles_from_market_prices()` y tablas `market_prices`, `market_candles`. |
| RF-04 | Analizar seniales tecnico/macro/riesgo/sentiment. | [src/analysis/technical.py](../src/analysis/technical.py), [src/analysis/macro.py](../src/analysis/macro.py), [src/analysis/risk.py](../src/analysis/risk.py), [src/analysis/sentiment_fetcher.py](../src/analysis/sentiment_fetcher.py). |
| RF-05 | Combinar capas en score y decision preliminar. | `blend_scores()` y `SynthesisResult` en [src/analysis/synthesis.py](../src/analysis/synthesis.py). |
| RF-06 | Proponer pesos teoricos de cartera. | `run_optimizer()` en [src/analysis/optimizer.py](../src/analysis/optimizer.py). |
| RF-07 | Convertir pesos/seniales en plan operable. | `ExecutionPlan`, `derive_decision_intents()`, `reconcile_funding()` en [src/analysis/execution_planner.py](../src/analysis/execution_planner.py). |
| RF-08 | Validar consistencia del plan antes de reportar. | `validate_execution_plan()` en [src/analysis/validators.py](../src/analysis/validators.py). |
| RF-09 | Registrar decisiones, bloqueos y ejecuciones. | `decision_log` en [init.sql](../init.sql), `_save_execution_plan_events()` en [scripts/run_analysis.py](../scripts/run_analysis.py). |
| RF-10 | Reconciliar fills reales contra decisiones. | `reconcile_broker_fills()` en [src/collector/db.py](../src/collector/db.py), `choose_execution_candidate()` en [src/analysis/fill_reconciliation.py](../src/analysis/fill_reconciliation.py). |
| RF-11 | Materializar fills manuales no matcheados. | `materialize_unmatched_broker_fills()` en [src/collector/db.py](../src/collector/db.py). |
| RF-12 | Calcular outcomes. | `update_outcomes()`/`recompute_outcomes()` en [src/collector/db.py](../src/collector/db.py), [scripts/update_outcomes.py](../scripts/update_outcomes.py). |
| RF-13 | Exponer Telegram. | `build_app()` en [scripts/telegram_bot.py](../scripts/telegram_bot.py). |
| RF-14 | Exponer monitor read-only. | `create_app()` en [src/monitor/api.py](../src/monitor/api.py). |
| RF-15 | Correr jobs programados. | `_scheduler_main()` en [src/scheduler/runner.py](../src/scheduler/runner.py). |
| RF-16 | Auditar performance/regression/viability. | [scripts/run_performance.py](../scripts/run_performance.py), [scripts/run_regression_audit.py](../scripts/run_regression_audit.py), [scripts/run_viability_audit.py](../scripts/run_viability_audit.py). |
| RF-17 | Mantener shadow forecasts separados de decisiones productivas. | Tablas `shadow_thesis_*`, [scripts/run_thesis_shadow.py](../scripts/run_thesis_shadow.py), [tests/test_shadow_causal.py](../tests/test_shadow_causal.py). |

## Requerimientos no funcionales

- Auditabilidad: decisiones con `run_id`, `run_intent`, `decision_stage`,
  `metric_scope` e `is_primary_metric`.
- Seguridad local-first: secretos por `.env`, redaccion de logs en
  [src/core/logger.py](../src/core/logger.py) y token obligatorio del monitor.
- Tolerancia a datos incompletos: bloquear o degradar antes que inventar datos.
- Reproducibilidad parcial: `FeatureSnapshot` y `DecisionRunContext` existen en
  [src/analysis/feature_snapshot.py](../src/analysis/feature_snapshot.py) y
  [src/analysis/decision_context.py](../src/analysis/decision_context.py).
- Separacion de capas experimentales: shadow y causal analysis fuera de
  `decision_log` productivo.
- Operacion Docker: servicios declarados en [docker-compose.yml](../docker-compose.yml).

Pendiente de validar: objetivos de latencia, disponibilidad, RPO/RTO,
retencion de datos y capacidad maxima.

## Reglas de negocio

- El optimizer propone pesos, pero el planner define la accion operable.
- Una compra nueva requiere cash/funding, monto minimo y nominales enteros.
- `MIN_TRADE_ARS = 25_000`, `FEE_PCT = 0.006`,
  `MIN_WEIGHT_DELTA = 0.015`, `MAX_WEIGHT_CONC = 0.25` y
  `MAX_WEIGHT_HARD_CONC = 0.30` estan en
  [src/analysis/execution_planner.py](../src/analysis/execution_planner.py).
- Las decisiones primarias deben venir de fills/movimientos reales o execution
  plan ejecutado; radar/debug no deben mezclarse con EV operativo.
- Off-hours puede pasar a exploratory/no-persist segun
  [tests/test_offhours_sentiment_policy.py](../tests/test_offhours_sentiment_policy.py).
- Eventos manuales con `block_new_buys` pueden bloquear compras nuevas; ver
  [tests/test_manual_market_events.py](../tests/test_manual_market_events.py).
- Sentiment contextual no debe modificar decisiones si la capa se deshabilita;
  el repo redistribuye su peso en `blend_scores()` cuando `skip_sentiment=True`.

## Entradas del sistema

- Credenciales Cocos y URLs: [.env.example](../.env.example).
- Portfolio, posiciones, precios, actividad y fills desde Cocos.
- Velas historicas desde Cocos/TradingView/BYMA por scripts `backfill_*`.
- Noticias RSS/Yahoo/Reuters bridge para sentiment.
- Eventos manuales por [scripts/manual_market_events.py](../scripts/manual_market_events.py).
- Parametros CLI de scripts bajo [scripts](../scripts).

## Salidas del sistema

- Reportes Telegram.
- Monitor web/API.
- Filas en `portfolio_snapshots`, `positions`, `market_prices`,
  `market_candles`, `decision_log`, `broker_fills`, `broker_movements`,
  `sentiment_*`, `shadow_thesis_*`, `manual_market_events`.
- PNGs de ticker/viability cuando se usan `--chart-out`.
- Logs bajo `logs` montados por Docker.

## Flujos principales

### Flujo diario formal

1. Scheduler dispara `run_daily_analysis()` a las 17:12 ART.
2. `scripts/run_analysis.py` carga portfolio, velas, macro, tecnico, riesgo y
   sentiment.
3. `blend_scores()` genera `SynthesisResult`.
4. `run_optimizer()` genera pesos teoricos.
5. `derive_decision_intents()` y `reconcile_funding()` generan `ExecutionPlan`.
6. `validate_execution_plan()` valida consistencia.
7. `_save_execution_plan_events()` persiste eventos en `decision_log` salvo
   `--no-persist`.
8. Telegram/monitor muestran el resultado.

### Flujo de ejecucion observada

1. Scraper detecta fills/movements.
2. `save_broker_fills()` y `save_broker_movements()` hacen upsert.
3. `reconcile_broker_fills()` asocia fills a `decision_log`.
4. Si no hay match, `materialize_unmatched_broker_fills()` puede crear evento
   `EXECUTED_MANUAL`.
5. Outcomes posteriores alimentan performance, ledger y viability.

### Flujo de auditoria

1. Scripts read-only consultan `decision_log`, fills, movements, candles y
   shadow.
2. `decision_ledger.py` y `decision_timeline.py` reconstruyen eventos.
3. Monitor API expone `/api/performance`, `/api/override-audit`,
   `/api/decision-ledger`, `/api/radar-audit`, `/api/shadow` y `/api/fills`.

## Flujos alternativos

- `--no-persist`: corre analisis sin guardar en `decision_log`.
- `--run-intent exploratory`: marca una corrida exploratoria.
- `--no-sentiment`: omite sentiment y redistribuye peso.
- `--no-llm`: evita explicacion LLM.
- `--no-persist` en radar: evita guardar ideas teoricas.
- Off-hours: tests indican que puede forzar exploratory/no persist.
- Shadow causal: persiste solo en `shadow_thesis_causal_analysis`.

## Restricciones

- No hay ejecucion automatica de ordenes confirmada.
- Requiere credenciales Cocos y DB para corridas reales.
- Monitor requiere `MONITOR_API_TOKEN`.
- La fuente Cocos puede cambiar; scraping depende de UI/API observada.
- TimescaleDB es opcional por checks en `init.sql`, pero el compose local usa
  `timescale/timescaledb:latest-pg16`.

## Criterios de aceptacion

- Un plan formal no debe dejar `cash_after` negativo ni ordenes con cantidad
  no entera; cubierto por `validate_execution_plan()` y
  [tests/test_execution_nominals_and_rotation.py](../tests/test_execution_nominals_and_rotation.py).
- Una decision debe tener alcance auditable correcto; cubierto por
  `classify_decision_audit_scope()`.
- Radar/shadow/debug no deben contaminar metricas primarias.
- Fills sinteticos deben ser reemplazados o excluidos cuando aparece fill real;
  cubierto por [tests/test_superseded_broker_fills.py](../tests/test_superseded_broker_fills.py).
- La timeline debe marcar gaps cuando faltan IDs/contexto; cubierto por
  [tests/test_decision_timeline.py](../tests/test_decision_timeline.py).
- Viability debe separar bot-only y manual-only y declarar que no cambia guards;
  cubierto por [tests/test_viability_audit.py](../tests/test_viability_audit.py).
