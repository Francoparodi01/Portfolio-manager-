# Documento de modelo de datos

El schema principal esta en [init.sql](../init.sql). El inventario estatico del
repo muestra 20 tablas `CREATE TABLE IF NOT EXISTS`.

## Tablas principales

| Tabla | Proposito | Campos criticos |
|---|---|---|
| `portfolio_snapshots` | Foto de cartera, cash y calidad del scrape. | `snapshot_id`, `owner_chat_id`, `scraped_at`, `total_value_ars`, `cash_ars`, `confidence_score`, `dom_hash`, `raw_html_hash`. |
| `positions` | Posiciones por snapshot. | `snapshot_id`, `scraped_at`, `ticker`, `quantity`, `current_price`, `market_value`, `weight_in_portfolio`. |
| `raw_snapshots` | Payload crudo asociado a snapshot. | `snapshot_id`, `scraped_at`, `payload`. |
| `market_prices` | Snapshots de precios actuales/universo Cocos. | `ts`, `ticker`, `last_price`, `change_pct_1d`, `volume`. |
| `market_candles` | OHLCV canonico para analisis/outcomes. | `ts`, `ticker`, `long_ticker`, `interval`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `source`. |
| `bot_users` | Usuarios Telegram y credenciales cifradas. | `chat_id`, `telegram_username`, `cocos_user_ciphertext`, `cocos_pass_ciphertext`, `mfa_timeout`, `is_active`. |
| `decision_log` | Ledger central de decisiones, planes, bloqueos, ejecuciones y outcomes. | `id`, `decided_at`, `ticker`, `decision`, `final_score`, `confidence`, `layers`, `price_at_decision`, `status`, `source`, `run_id`, `metric_scope`, `is_primary_metric`. |
| `execution_plans` | Cabecera persistida del plan operativo multiorden. | `id`, `run_id`, `gate`, `feasible`, cash y totales de compra/venta, `summary`, `warnings`. |
| `order_intents` | Ordenes propuestas, bloqueadas o pendientes del plan; no implica envio al broker. | `execution_plan_id`, `decision_log_id`, `sequence_no`, `ticker`, `side`, montos, estado y precio de referencia. |
| `broker_fills` | Fills reales o importados desde broker. | `source`, `external_fill_id`, `executed_at`, `ticker`, `side`, `quantity`, `avg_fill_price`, `fees_ars`, `decision_log_id`. |
| `broker_movements` | Movimientos de Cocos de instrumentos/caja. | `external_movement_id`, `executed_at`, `movement_type`, `amount`, `quantity`, `price`, `ticker`, `raw_payload`. |
| `sentiment_raw` | Noticias crudas deduplicadas por URL. | `source`, `url_hash`, `headline`, `published_at`, `score_status`, `raw_payload`. |
| `sentiment_scored` | Scoring por item de noticia. | `raw_id`, `scorer`, `model`, `ticker`, `score`, `impact`, `confidence`, `raw_response`, `status`. |
| `sentiment_aggregated` | Contexto agregado por ticker/scope. | `bucket_ts`, `ticker`, `asset_scope`, `score`, `confidence`, `event_count`, `sources`. |
| `manual_market_events` | Catalysts declarados manualmente. | `event_date`, `ticker`, `title`, `severity`, `active_from`, `active_until`, `action_policy`, `is_active`. |
| `intraday_preclose_alerts` | Alertas pre-cierre con evidencia. | `business_date`, `slot`, `ticker`, `alert_type`, `severity`, `reason`, `evidence`, `status`. |
| `shadow_thesis_runs` | Corridas shadow independientes. | `run_id`, `owner_chat_id`, `as_of_ts`, `model_version`, `schema_version`, `universe_count`, `metadata`. |
| `shadow_thesis_forecasts` | Forecasts shadow 5/20/40 por ticker. | `run_id`, `ticker`, `horizon_sessions`, `reference_price`, `expected_return`, `probability_up`, `feature_snapshot`. |
| `shadow_thesis_outcomes` | Resultado posterior de forecasts shadow. | `forecast_id`, `target_session_ts`, `outcome_price`, `realized_return`, `direction_correct`, `absolute_error`. |
| `shadow_thesis_causal_analysis` | Auditoria causal LLM sobre shadow. | `forecast_id`, `model`, `prompt_version`, `schema_version`, `input_fingerprint`, `raw_response`. |
| `ml_decision_features` | Feature store experimental. | `decision_log_id`, features tecnicas/macro/riesgo, labels y outcomes. |
| `ml_model_registry` | Registro experimental de modelos ML. | `model_type`, `version`, `trained_at`, metricas, `is_active`, `is_promoted`, `artifact_path`. |

## Relaciones importantes

- `positions.snapshot_id` y `raw_snapshots.snapshot_id` referencian
  `portfolio_snapshots.snapshot_id`.
- `broker_fills.decision_log_id` referencia `decision_log.id`.
- `order_intents.execution_plan_id` referencia `execution_plans.id` y
  `order_intents.decision_log_id` mantiene compatibilidad con `decision_log.id`.
- `decision_log.superseded_by_id` referencia otra fila de `decision_log`.
- `bot_users.chat_id` se vincula con `owner_chat_id` en `decision_log`,
  `portfolio_snapshots`, `broker_fills` y shadow.
- `shadow_thesis_forecasts.run_id` referencia `shadow_thesis_runs.run_id`.
- `shadow_thesis_outcomes.forecast_id` referencia `shadow_thesis_forecasts.id`.
- `sentiment_scored.raw_id` referencia `sentiment_raw.id`.

## Fuente de verdad

| Dato | Fuente de verdad |
|---|---|
| Estado observado de cartera | Ultimo `portfolio_snapshots` + `positions`. |
| Precio/velas para analisis y outcomes | `market_candles` cuando hay cobertura canonica; `market_prices` para snapshot/frescura. |
| Plan operativo formal | `execution_plans` + `order_intents`, con espejo compatible en `decision_log` usando `source='execution_plan'` y `run_intent='formal_plan'`. |
| Ejecucion real | `broker_fills` y `broker_movements`; al reconciliar actualizan `decision_log` a primary/executed. |
| Outcome operativo | Columnas `outcome_*` y `executable_outcome_*` en `decision_log`, con `outcome_basis`. |
| Radar | `decision_log` con `source='radar'` y `metric_scope='radar_audit'`, no primary. |
| Shadow | Tablas `shadow_thesis_*`, no `decision_log`. |
| Sentiment textual | `sentiment_raw` + `sentiment_scored`; agregado en `sentiment_aggregated`. |

## Datos derivados o auditables

- `final_score`, `confidence`, `layers`, `decision_type`, `signal_strength`:
  derivados del pipeline de analisis/planner.
- `outcome_5d`, `outcome_10d`, `outcome_20d`, `outcome_40d`: derivados por
  jobs de outcomes.
- `executable_outcome_*`: derivado cuando la referencia ejecutable difiere del
  precio original.
- `metric_scope`, `decision_stage`, `is_primary_metric`: derivados por
  `classify_decision_audit_scope()`.
- `feature_snapshot_id`: hash parcial de inputs construido por
  [src/analysis/feature_snapshot.py](../src/analysis/feature_snapshot.py).
- `shadow_thesis_outcomes`: derivado de forecasts shadow y precio posterior.

## Calidad del modelo

Fortalezas:

- Usa UUID para snapshots y run IDs en varias capas.
- Usa JSONB para payloads de evidencia y evolucion incremental.
- Separa shadow de decisiones productivas.
- Tiene indices para frescura, ticker, outcomes, fills, sentiment y lookups.

Debilidades:

- `decision_log` sigue sobrecargada para historico, outcomes y compatibilidad;
  plan/orden ya tienen persistencia aditiva propia.
- `market_snapshot_id`, `optimizer_run_id`, `planner_run_id` y
  `risk_assessment_id` no estan normalizados.
- Algunas migraciones aparecen duplicadas en `init.sql`; son idempotentes por
  `ADD COLUMN IF NOT EXISTS`, pero conviene consolidarlas a futuro.

## Incertidumbres

- No confirmado en el repo: cardinalidad real de datos productivos.
- Pendiente de validar: constraints exactos en una DB ya inicializada contra el
  `init.sql` actual.
- Pendiente de validar: retencion, particionado Timescale y politica de backup.
