# Documento de auditoria y trazabilidad

## Que decisiones registra el sistema

La tabla central es `decision_log` en [init.sql](../init.sql). Registra:

- `ticker`, `decision`, `decision_type`, `final_score`, `confidence`.
- `layers` JSONB con contexto de capas y metadata.
- `price_at_decision`, `vix_at_decision`, `regime`.
- `status`, `source`, `block_reason`, `is_executable`, `was_blocked`.
- `theoretical_amount_ars`, `executed_amount_ars`, pesos actuales/objetivo.
- `run_id`, `run_intent`, `decision_stage`, `metric_scope`,
  `is_primary_metric`.
- Outcomes 5/10/20/40 y `outcome_basis`.

Fuentes de decision:

- `execution_plan`: plan formal del planner.
- `radar`: ideas teoricas/auditables.
- `broker_movement` o `broker_fill`: ejecucion real/manual.
- `optimizer`/otros: debug o exploratory segun `audit_scope`.

## Como se reconstruye una decision

1. Buscar filas `decision_log` por `run_id`, ticker o fecha.
2. Leer `layers`, `source`, `status`, `decision_stage` y `metric_scope`.
3. Asociar `broker_fills` por `decision_log_id`.
4. Asociar `broker_movements` por ticker/fecha si aplica.
5. Revisar outcomes en `decision_log`.
6. Usar `decision_timeline.py` para ordenar eventos y marcar gaps.
7. Usar `decision_ledger.py` para resumen economico y bot vs humano.

Evidencia:

- [src/analysis/decision_timeline.py](../src/analysis/decision_timeline.py)
  crea `DecisionTimelineEvent` y une decisiones, movements y fills.
- [src/analysis/decision_ledger.py](../src/analysis/decision_ledger.py) filtra
  metricas primarias y planes formales.
- [scripts/run_decision_timeline.py](../scripts/run_decision_timeline.py) y
  [scripts/run_decision_ledger.py](../scripts/run_decision_ledger.py) son
  entrypoints CLI.

## Que evidencia guarda

- Portfolio observado: `portfolio_snapshots`, `positions`, `raw_snapshots`.
- Mercado/precios: `market_prices`, `market_candles`.
- Decision: `decision_log.layers`, score, confidence, price, regime, source.
- Ejecucion real: `broker_fills`, `broker_movements`, raw payloads y fees.
- Outcomes: columnas `outcome_*`, `executable_outcome_*`, `outcome_basis`.
- Sentiment: `sentiment_raw`, `sentiment_scored`, `sentiment_aggregated`.
- Shadow: `shadow_thesis_*` y `shadow_thesis_causal_analysis`.
- Eventos manuales: `manual_market_events`.
- Alertas preclose: `intraday_preclose_alerts.evidence`.

## Metricas para evaluar performance

- Performance operativa: [scripts/run_performance.py](../scripts/run_performance.py).
- Regression por modo/horizonte/costos: [scripts/run_regression_audit.py](../scripts/run_regression_audit.py).
- Viability bot-only/manual-only, EV neto, IC y drawdown:
  [scripts/run_viability_audit.py](../scripts/run_viability_audit.py).
- Confidence audit: [scripts/run_confidence_audit.py](../scripts/run_confidence_audit.py).
- DCL/calibration: [scripts/run_calibration.py](../scripts/run_calibration.py) y
  [src/analysis/dcl](../src/analysis/dcl).
- Override audit: [scripts/run_override_audit.py](../scripts/run_override_audit.py).

Pendiente de validar: resultado numerico actual de esas metricas contra la DB
viva en este relevamiento.

## Productivo vs shadow/audit-only

| Capa | Estado | Evidencia |
|---|---|---|
| Execution planner | Productivo como fuente de plan operable. | `ExecutionPlan` en [src/analysis/execution_planner.py](../src/analysis/execution_planner.py). |
| Fills/movements | Fuente de verdad de ejecucion real. | `broker_fills`, `broker_movements`, [src/collector/db.py](../src/collector/db.py). |
| Outcomes primary | Productivo para performance si `is_primary_metric=TRUE`. | `classify_decision_audit_scope()` y `decision_ledger.py`. |
| Radar | Audit/contexto, no EV principal. | `source='radar'`, `metric_scope='radar_audit'` en [scripts/run_opportunity.py](../scripts/run_opportunity.py). |
| Shadow thesis | Shadow/audit-only. | Tablas `shadow_thesis_*`, tests de shadow. |
| Shadow causal | Audit-only LLM sobre shadow. | [src/analysis/shadow_causal.py](../src/analysis/shadow_causal.py). |
| Sentiment | Contextual/ponderado segun configuracion, no autoridad final. | `blend_scores()` y `.env.example`. |
| DCL/calibration | Auditoria/calibracion read-only. | [src/analysis/dcl](../src/analysis/dcl). |

## Controles contra mezcla indebida

- `metric_scope`: `primary`, `planner_audit`, `radar_audit`,
  `blocked_audit`, `debug`.
- `is_primary_metric`: marca filas aptas para metricas primarias.
- `run_intent`: `formal_plan`, `scheduled_context`, `broker_sync`,
  `exploratory`.
- Shadow usa tablas propias, no `decision_log`.
- Tests: [tests/test_shadow_causal.py](../tests/test_shadow_causal.py),
  [tests/test_viability_audit.py](../tests/test_viability_audit.py),
  [tests/test_offhours_sentiment_policy.py](../tests/test_offhours_sentiment_policy.py).

## Gaps conocidos

- No hay `order_id` persistido.
- `ExecutionPlan` no tiene tabla propia.
- `strategy_version`, `optimizer_version` y `planner_version` existen como
  idea en `DecisionRunContext`, pero no estan completos en todas las corridas.
- `market_snapshot_id` no esta normalizado.
- `decision_log.layers` contiene evidencia util, pero no todos los contratos
  estan versionados.

## Comandos de auditoria

```powershell
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 90
docker compose exec -T scheduler python scripts/run_decision_ledger.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_override_audit.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_performance.py --days 90 --no-telegram
docker compose exec -T scheduler python scripts/run_viability_audit.py --days 180 --no-telegram
docker compose exec -T scheduler python scripts/run_regression_audit.py --mode execution --no-telegram
```
