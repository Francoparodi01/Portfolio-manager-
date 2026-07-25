# Brechas arquitectónicas de Quantia

## Criterio de clasificación

Estados usados:

- **Implementada**: existe con contrato suficiente para operar.
- **Parcialmente implementada**: existe una parte útil, pero falta contrato, versión, entidad o integración.
- **Duplicada**: dos o más módulos calculan o clasifican lo mismo sin contrato común.
- **Inconsistente**: el mismo concepto aparece con semánticas distintas según flujo.
- **Acoplada**: responsabilidad de dominio/application mezclada con CLI, Telegram, monitor o SQL ad hoc.
- **Ausente**: no existe como entidad/flujo.
- **Implementada con otro nombre**: existe una capacidad equivalente bajo otro concepto.

## Mapa funcional

| # | Capacidad | Estado real | Evidencia principal | Tablas | Deuda/riesgo | Cambio mínimo necesario |
|---:|---|---|---|---|---|---|
| 1 | Strategy Registry | Ausente | No hay `Strategy`/registry; estrategia core está embebida en `scripts/run_analysis.py`, `optimizer.py`, `execution_planner.py`. | Ninguna específica. | No se puede comparar políticas sin duplicar scripts. | Crear registry mínimo read-only con `strategy_id`, `version`, `mode`, `can_trade`. |
| 2 | Strategy Lab | Parcialmente implementada | Shadow price-only existe en `thesis_shadow.py`; radar/execution audit existen, pero no corren estrategias alternativas sobre el mismo snapshot. | `shadow_thesis_*`, `decision_log`. | Comparaciones no controlan snapshot, capital y restricciones idénticas. | Introducir `strategy_runs` y `strategy_outputs` shadow sin tocar planner live. |
| 3 | Shadow Strategies | Implementada con otro nombre | `scripts/run_thesis_shadow.py`, `src/analysis/thesis_shadow.py`, `ShadowThesisStore`. | `shadow_thesis_runs`, `shadow_thesis_forecasts`, `shadow_thesis_outcomes`. | Es price forecast, no estrategia de cartera. | Reutilizar el patrón de run/version, pero no mezclarlo con decision_log principal. |
| 4 | Champion-Challenger | Ausente | No hay promoción de challenger ni baseline comparable; viability compara bot/manual, no estrategias. | Ninguna. | Se podría promover una idea por lectura visual sin evidencia controlada. | Definir criterios de promoción: n mínimo, EV neto, drawdown, turnover, IC, no look-ahead. |
| 5 | Decision Timeline | Parcialmente implementada | `decision_ledger.py`, `run_policy_tree.py`, monitor `/api/decision-ledger`; todo reconstruye desde `decision_log` y movimientos. | `decision_log`, `broker_fills`, `broker_movements`. | Timeline no es una entidad; falta snapshot/optimizer/risk/explanation IDs. | Crear vista/servicio `decision_timeline` sobre datos existentes antes de nuevas tablas. |
| 6 | Decision Ledger | Implementada | `src/analysis/decision_ledger.py::fetch_decision_ledger`, `scripts/run_decision_ledger.py`, monitor endpoint. | `decision_log`, `broker_movements`. | Duplicación parcial de override classification con monitor y `run_override_audit.py`. | Extraer clasificación bot-vs-humano a módulo compartido. |
| 7 | Feature Snapshot | Parcialmente implementada | `decision_log.layers`; `ml_decision_features`; `shadow_thesis_forecasts.feature_snapshot`. | `decision_log`, `ml_decision_features`, `shadow_thesis_forecasts`. | No hay `feature_snapshot_id` estable para reproducir una decisión live. | Crear hash/ID de feature snapshot generado desde inputs estructurados. |
| 8 | Market Snapshot | Parcialmente implementada | `portfolio_snapshots.snapshot_id`; `market_prices` por `ts`; `market_candles` por ticker/ts. | `portfolio_snapshots`, `positions`, `market_prices`, `market_candles`. | No hay `market_snapshot_id` que congele universo/precios para Strategy Lab. | Crear tabla o vista materializable `market_snapshots` + `market_snapshot_assets`. |
| 9 | Model and configuration versioning | Parcialmente implementada | `ml_model_registry`, `shadow_thesis_runs.model_version`, constants en módulos. | `ml_model_registry`, `shadow_thesis_*`. | Config live de scoring/planner/risk no versionada por run. | Capturar `config_hash`/`code_version`/versions en `decision_runs`. |
| 10 | Planner versioning | Ausente | `execution_planner.py` usa constantes (`MIN_TRADE_ARS`, `FEE_PCT`, thresholds) sin versión. | `decision_log.layers` parcialmente. | No se puede saber qué umbral produjo un plan histórico. | Agregar `planner_version` constante y persistirla en run/layers. |
| 11 | Optimizer versioning | Parcialmente implementada | `optimizer.py` expone engine/method en `OptimizationResult`, pero no `optimizer_run_id`. | `decision_log.layers`, logs. | Fallback BL vs numpy queda en reporte/log, no como entidad de run. | Crear `optimizer_runs` o persistir `optimizer_version`, engine y input hash. |
| 12 | Risk policy versioning | Ausente | `risk.py`, `risk_levels.py`, `optimizer._get_risk_gate_state`, `execution_planner._buy_guard/_sell_guard`. | `decision_log.layers` parcial. | Risk policy duplicada entre módulos y no versionada. | Definir `risk_policy_version` y `risk_assessment` estructurado. |
| 13 | Prompt versioning | Parcialmente implementada | `shadow_causal.py::PROMPT_VERSION`; `synthesis.py` y `nlp_scorer.py` no tienen prompt_version persistido. | `shadow_thesis_causal_analysis`, `sentiment_scored`. | No se puede auditar cambios de prompt en explicaciones principales/sentiment. | Agregar prompt/model/schema version a todos los outputs LLM. |
| 14 | Evidence-based explanations | Parcialmente implementada | `shadow_causal.py` exige evidence en JSON; `sentiment_raw/scored/aggregated` tiene sources; `synthesis.py` usa texto de capas sin IDs. | `shadow_thesis_causal_analysis`, `sentiment_*`, `decision_log.layers`. | Explicación del plan puede no rastrear cada afirmación a evidence IDs. | Introducir `evidence_id` para noticias/snapshots/features y contract de explainer. |
| 15 | Thesis Engine | Parcialmente implementada | `thesis_shadow.py` y `shadow_causal.py` producen tesis/proyecciones; no hay tesis operativa por decisión. | `shadow_thesis_*`, `shadow_thesis_causal_analysis`. | Thesis existe como shadow forecast, no como lifecycle de decisión. | Agregar `thesis_id` opcional para decisión explicativa, no operativa. |
| 16 | Knowledge Layer | Ausente | Referencia HTML recomienda knowledge layer; repo tiene docs y reportes, no base de conocimiento versionada. | Ninguna. | Repetición de explicaciones y decisiones no se convierten en lecciones consultables. | Crear backlog para `lessons`/`decision_lessons` read-only posterior. |
| 17 | Execution Audit | Implementada | `broker_fills`, `broker_movements`, `reconcile_broker_fills`, `run_override_audit.py`, monitor fills. | `broker_fills`, `broker_movements`, `decision_log`. | Timestamp precision `date_only` limita match intradía; costos incompletos. | Mantener y sumar `execution_plan_id/order_id` antes de atribución fina. |
| 18 | Bot vs Human | Implementada | `run_override_audit.py`, `decision_ledger.py::classify_override`, monitor `/api/override-audit`. | `decision_log`, `broker_movements`. | Clasificación duplicada en tres sitios. | Extraer `override_classification.py` compartido. |
| 19 | Outcome attribution | Parcialmente implementada | `update_outcomes`, `run_performance`, `regression_audit`, `viability_audit`. | `decision_log`, `market_candles`. | Outcomes son direccionales; atribución por factor/causa no existe. | Crear `attribution` posterior a lifecycle IDs. |
| 20 | Financial attribution | Parcialmente implementada | Fees en `broker_fills`; cost estimates en planner; CCL en `macro.py`. | `broker_fills`, `decision_log`, `market_candles`. | No separa subyacente USD, CCL, spread, slippage, timing, sizing. | Diseñar tabla `financial_attribution` y empezar por costos/slippage vs fill. |
| 21 | Benchmark comparison | Parcialmente implementada | `regression_audit` y DCL miden IC/EV; no benchmark formal de cartera. | `decision_log`, `market_candles`. | Sin benchmark, EV absoluto puede engañar en régimen alcista/bajista. | Agregar benchmark read-only por SPY CEDEAR/buy-hold antes de dashboards. |
| 22 | Implementation shortfall | Ausente | Planner estima fees/slippage; fills guardan precio, pero no se compara plan vs fill. | `broker_fills`, `decision_log`. | No se mide costo de ejecución real. | Calcular shortfall: `fill_price` vs `reference_price/next_executable_price`. |
| 23 | Slippage and cost attribution | Parcialmente implementada | `execution_planner.FEE_PCT`, `SLIPPAGE_PCT`; `broker_fills.fees_ars`. | `broker_fills`, `decision_log`. | Costos estimados y reales viven separados; slippage no persistido. | Persistir cost model version y calcular costo real por fill. |
| 24 | USD underlying versus CCL attribution | Ausente | `macro.py` trae CCL/MEP; precios operables son ARS. | `market_candles`, macro no persistido formalmente. | En CEDEARs no se separa si retorno vino de subyacente o FX. | Agregar fuentes USD/CCL y attribution experimental sin entrar a EV primario. |
| 25 | Data lineage | Parcialmente implementada | `market_candles.source`, `technical_candle_source_counts`, `sentiment_raw.raw_payload`, `raw_snapshots`. | Varias. | Lineage queda disperso; no hay grafo por decisión. | `decision_input_refs` con IDs de snapshots/evidence usadas. |
| 26 | Reproducibility | Parcialmente implementada | `run_id`, raw snapshots, layers, shadow run uniqueness. | `decision_log`, `raw_snapshots`, `shadow_thesis_*`. | No se puede rerun exacto sin reconstruir estado actual externo. | Congelar market/portfolio/feature snapshots antes de Strategy Lab. |
| 27 | Idempotency | Parcialmente implementada | Upserts por external ids; radar dedupe por ticker/día; execution plan update por ticker/día/decision. | `broker_*`, `decision_log`, `sentiment_*`, `shadow_*`. | Dedupe de decision_log por ticker/día puede pisar señales múltiples del mismo día. | Reemplazar dedupe operativo por IDs de run/decision/event compatibles. |
| 28 | Metric scopes | Implementada | `audit_scope.py`, `decision_log.metric_scope`, `is_primary_metric`, regression filters. | `decision_log`. | Semántica correcta pero depende de backfills y defaults. | Mantener; documentar contrato y evitar nuevas métricas sin scope. |
| 29 | Primary versus audit metrics | Implementada | `get_performance_stats` filtra `is_primary_metric=TRUE`; monitor distingue radar/blocked/audit. | `decision_log`. | Monitor aún calcula algunos agregados por `outcome_5d` en queries propias. | Unificar capa de métricas primarias compartida. |
| 30 | Operational versus exploratory runs | Parcialmente implementada | `run_intent`, `--no-persist`, radar fuera de rueda como exploratory. | `decision_log`. | `run_intent` vive en decision_log, no en entidad de run. | Crear `decision_runs` con `run_intent`, `trigger`, `code_version`, `config_hash`. |

## Matriz de trazabilidad capacidades-módulos-tablas

| Capacidad | Módulos actuales | Tablas actuales | Debe quedar separado de |
|---|---|---|---|
| Core live decision | `run_analysis.py`, `synthesis.py`, `optimizer.py`, `execution_planner.py`, `validators.py` | `decision_log` | Radar audit, debug, shadow, LLM critic. |
| Execution audit | `db.py`, `broker_fills.py`, `broker_movements.py`, `fill_reconciliation.py` | `broker_fills`, `broker_movements`, `decision_log` | Optimizer theoretical rows. |
| Outcomes/performance | `db.py::update_outcomes`, `run_performance.py`, `regression_audit.py`, `viability_audit.py` | `decision_log`, `market_candles` | Exploratory/debug rows. |
| Radar | `run_opportunity.py`, `opportunity_screener.py` | `decision_log` with `source='radar'` | EV primario hasta fill real. |
| Shadow thesis | `run_thesis_shadow.py`, `thesis_shadow.py`, `thesis_shadow_store.py` | `shadow_thesis_*` | Planner/optimizer/scoring live. |
| Shadow causal LLM | `run_shadow_causal_analysis.py`, `shadow_causal.py`, `shadow_causal_store.py` | `shadow_thesis_causal_analysis`, `sentiment_*` | Decisión financiera y ejecución. |
| Sentiment ingestion | `sentiment_fetcher.py`, `nlp_scorer.py`, `signal_aggregator.py`, `run_sentiment_pipeline.py` | `sentiment_raw`, `sentiment_scored`, `sentiment_aggregated` | Pesos directos y execution plan. |
| Telegram presentation | `telegram_bot.py` | Lee/escribe indirecto vía scripts | Lógica de dominio. |
| Monitor presentation | `monitor/api.py`, `monitor/static/index.html` | Lee DB/logs/Redis | Escritura productiva. |
| Calibration/DCL | `analysis/dcl/*`, `run_calibration.py` | `decision_log` | Threshold changes automáticos. |

## Problemas arquitectónicos respaldados por código

### 1. `decision_log` concentra demasiadas responsabilidades

Evidencia:

- DDL `init.sql::decision_log` contiene campos de decisión (`decision`, `final_score`, `layers`), lifecycle (`source`, `status`, `decision_type`), ejecución (`executed_amount_ars`, `is_executable`), outcomes (`outcome_*`, `executable_outcome_*`), audit (`metric_scope`, `is_primary_metric`) y run (`run_id`, `run_intent`, `decision_stage`).
- `run_analysis.py::_save_execution_plan_events` inserta/actualiza planes.
- `db.py::reconcile_broker_fills` actualiza la misma fila a `EXECUTED`.
- `db.py::update_outcomes` completa outcomes en la misma fila.

Riesgo: cualquier cambio de lifecycle puede romper performance, monitor, regression o ledger.

Cambio mínimo: introducir `decision_runs` y `decision_inputs` primero, sin partir `decision_log` todavía. Después agregar IDs externos compatibles.

### 2. Timeline reconstruida desde inferencias

Evidencia:

- `decision_ledger.py::fetch_decision_ledger` reconstruye planes vs movimientos con CTEs y ventanas temporales.
- `run_override_audit.py::_fetch_audit_rows` repite lógica similar.
- `monitor/api.py::override_audit` repite clasificación y matching para la UI.

Riesgo: distintas vistas pueden contar lo mismo de forma diferente.

Cambio mínimo: crear un servicio compartido `src/analysis/decision_timeline.py` read-only que centralice la consulta y clasificación.

### 3. Clasificación Bot vs Humano duplicada

Evidencia:

- `decision_ledger.py::classify_override`.
- `run_override_audit.py::_classify`.
- `monitor/api.py::_classify_override`.

Riesgo: FOLLOWED/PARTIAL/OPPOSITE puede variar entre Telegram, monitor y ledger.

Cambio mínimo: extraer `OverrideStatus` y clasificación a un módulo común con tests.

### 4. Métricas calculadas en varias capas

Evidencia:

- `run_performance.py` calcula dataset buckets, EV labels y notas.
- `monitor/api.py::performance_view` tiene queries propias para EV/win/loss/path.
- `regression_audit.py` normaliza outcomes y modes por su cuenta.
- `viability_audit.py` calcula EV neto, drawdown e IC con otra preparación.

Riesgo: la misma etiqueta "EV 5D" no siempre representa el mismo universo.

Cambio mínimo: definir `MetricScope` y funciones compartidas para primary/audit/debug datasets antes de sumar nuevas métricas.

### 5. Risk policy dispersa

Evidencia:

- `risk.py` calcula drawdown/VIX/sizing.
- `optimizer.py::_get_risk_gate_state` aplica VIX/drawdown a optimizer.
- `execution_planner.py::_buy_guard` y `_sell_guard` aplican score/cash/concentración/stop.
- `risk_levels.py` calcula stops/targets.

Riesgo: un cambio en riesgo puede afectar pesos, guards o reporting de forma no trazable.

Cambio mínimo: crear un `RiskAssessment` estructurado y versionado que el optimizer/planner lean, sin mover toda la lógica todavía.

### 6. Strategy no es una entidad

Evidencia:

- La estrategia "Quantia Core" vive como combinación de `run_analysis.py`, `synthesis.py`, `optimizer.py`, `execution_planner.py` y constantes.
- Shadow es forecast (`thesis_shadow.py`), no estrategia de cartera.

Riesgo: Strategy Lab terminaría clonando scripts y mezclando live/shadow.

Cambio mínimo: `Strategy` interface fina que envuelva el pipeline actual como `quantia_core_v1`, con challengers shadow que no persisten a EV primario.

### 7. Configuración de decisión no versionada por run

Evidencia:

- Constantes en `execution_planner.py`: `MIN_TRADE_ARS`, `FEE_PCT`, `SLIPPAGE_PCT`, `SCORE_BUY_MIN`, `MAX_WEIGHT_CONC`.
- Constantes en `optimizer.py`: `VIX_CAUTIOUS`, `VIX_BLOCKED`, `DD_CAUTIOUS`, `DD_BLOCKED`.
- Pesos de synthesis en `synthesis.py::LAYER_WEIGHTS`.

Riesgo: una decisión histórica no puede reproducirse si cambian thresholds.

Cambio mínimo: persistir `planner_version`, `optimizer_version`, `risk_policy_version`, `synthesis_version` y `config_hash` en `decision_runs` o `decision_log.layers`.

### 8. LLM governance desigual

Evidencia:

- `shadow_causal.py` tiene `PROMPT_VERSION`, JSON schema, `input_fingerprint`, `raw_response` y `temperature=0.0`.
- `synthesis.py::synthesize_with_llm_local` usa prompt inline, `deepseek-r1:14b`, `temperature=0.2` y solo enriquece texto.
- `nlp_scorer.py::score_with_ollama` usa `format=json`, `temperature=0.0`, pero `sentiment_scored` no persiste prompt_version.

Riesgo: explicaciones/sentiment pueden cambiar por prompt/model sin trazabilidad homogénea.

Cambio mínimo: adoptar el patrón de `shadow_causal.py` para todos los LLM outputs.

### 9. Idempotency de decisiones es por ticker/día

Evidencia:

- `run_analysis.py::_save_execution_plan_events` busca existente por `ticker`, `decision`, `decision_date`, `source='execution_plan'`, owner.
- Si detecta movement posterior puede crear una nueva señal.
- `run_opportunity.py::_save_radar_candidates` dedupe por ticker/día/source/run_intent.

Riesgo: múltiples runs intradía o revalidaciones pueden sobrescribir contexto de decisión.

Cambio mínimo: crear `decision_id` estable por run+ticker+strategy+stage y dejar `superseded_by_id` para compatibilidad.

### 10. Persistencia desde capas de aplicación y scripts

Evidencia:

- `run_analysis.py::_save_execution_plan_events` escribe SQL directo en `decision_log`.
- `run_opportunity.py::_save_radar_candidates` escribe SQL directo en `decision_log`.
- `db.py` también persiste decisions/fills/outcomes.

Riesgo: se dispersan reglas de escritura, dedupe, audit_scope y JSON layers.

Cambio mínimo: crear repositorios estrechos (`DecisionLogRepository`, `StrategyRunRepository`) alrededor del SQL existente; no hace falta ORM.

### 11. Lógica temporal repetida

Evidencia:

- `audit_scope.py` define ART y market session.
- `run_analysis.py::_portfolio_snapshot_stale_reason` evalúa stale.
- `db.py::_next_executable_reference` calcula next executable.
- `decision_ledger.py`, `run_override_audit.py` y SQL del monitor recalculan next open/match windows.
- `fill_reconciliation.py::_age_for_match` trata `date_only` especialmente.

Riesgo: una decisión EOD puede matchear/medirse diferente según vista.

Cambio mínimo: centralizar `DecisionClock`/`ExecutionWindow` read-only.

### 12. Capa monitor calcula negocio

Evidencia:

- `monitor/api.py::performance_view` contiene SQL y lógica para EV, win/loss, source buckets, blocked/radar/human activity.
- `monitor/api.py::_classify_override` duplica dominio.

Riesgo: el monitor read-only se vuelve fuente alternativa de verdad.

Cambio mínimo: mover cálculos a `src/analysis/metrics.py` y dejar monitor como presenter.

### 13. Scripts grandes funcionan como servicios

Evidencia:

- `scripts/run_analysis.py` tiene ~3730 líneas y contiene orquestación, render, persistence y métricas IC.
- `src/scheduler/runner.py` tiene ~2683 líneas y concentra jobs, loops, alerts, scraper coordination.
- `scripts/telegram_bot.py` tiene ~2498 líneas y concentra presentación, comandos y orchestration.

Riesgo: cambios pequeños atraviesan demasiadas responsabilidades.

Cambio mínimo: no partir todo; extraer primero contratos transversales pequeños: timeline, metric scopes, strategy registry.

### 14. Feature store y ML registry no están integrados al live loop

Evidencia:

- `ml_decision_features` y `ml_model_registry` existen en `init.sql`.
- `src/analysis/experimental/feature_builder.py` y `ml_model.py` son experimentales.
- `run_analysis.py` no usa `MLModel.load_active` para decidir.

Riesgo: versionado ML puede confundirse con versionado de estrategia live.

Cambio mínimo: documentar ML como experimental y no usarlo para Strategy Lab inicial.

### 15. Tests existentes quedan parcialmente invisibles por `.gitignore`

Evidencia:

- `.gitignore` contiene `tests/*` y excepciones puntuales.
- `rg --files tests` muestra pocas pruebas; `rg --files -uu tests` muestra muchas más.

Riesgo: tests nuevos pueden no quedar versionados ni correrse en CI/validación normal.

Cambio mínimo: antes de PRs de refactor, decidir qué tests son canónicos y ajustar `.gitignore` con excepciones por suite.

### 16. Imports circulares no aparecen hoy

Evidencia: análisis AST de imports internos no detectó ciclos directos entre módulos `src.*`.

Riesgo: bajo en este punto. El problema no es circularidad, sino acoplamiento por scripts grandes y SQL duplicado.

Cambio mínimo: mantener imports acíclicos como criterio de aceptación de futuras extracciones.

## Prioridad de deuda

| Prioridad | Deuda | Impacto | Esfuerzo | Dependencias |
|---|---|---:|---:|---|
| P0 | IDs/contratos de decision lifecycle | Muy alto | Medio | Ninguna; puede ser aditivo. |
| P0 | Metric scopes centralizados | Muy alto | Bajo | Mantener `is_primary_metric`. |
| P0 | Timeline read-only | Alto | Medio | IDs y clasificación compartida. |
| P1 | Strategy Registry + Core wrapper | Muy alto | Medio | Snapshots/versions mínimos. |
| P1 | Shadow Strategy mínima | Alto | Medio | Strategy Registry. |
| P1 | Risk/optimizer/planner versioning | Alto | Bajo | `decision_runs` o layers. |
| P1 | Override classification compartida | Medio | Bajo | Ninguna. |
| P2 | Financial attribution | Alto | Alto | Execution IDs y market/fx data. |
| P2 | LLM explainer evidence-bound | Alto | Medio | Evidence IDs y prompt governance. |
| P3 | Knowledge Layer | Medio | Medio | Explanation/Lessons persistidos. |

