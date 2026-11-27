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
- Viability bot-only/manual-only, EV neto, IC (con rango 95%) y drawdown descriptivo:
  [scripts/run_viability_audit.py](../scripts/run_viability_audit.py).
- Confidence audit: [scripts/run_confidence_audit.py](../scripts/run_confidence_audit.py).
- DCL/calibration: [scripts/run_calibration.py](../scripts/run_calibration.py) y
  [src/analysis/dcl](../src/analysis/dcl).
- Override audit: [scripts/run_override_audit.py](../scripts/run_override_audit.py).

Pendiente de validar: resultado numerico actual de esas metricas contra la DB
viva en este relevamiento.

## Modos auditables y reglas de inferencia (2026-11-25)

| Comando Telegram | Script / modulo | Que mide | Como decide |
|---|---|---|---|
| `/cartera_sombra [desde=DD/MM/YYYY]` | `run_shadow_portfolio.py`, `shadow_portfolio.py` | tu cartera real de esa fecha siguiendo los planes reales del bot (todo y solo ventas) vs no operar y SPY | descriptivo, un solo camino; ledger del backtest; ordenes traducidas a peso objetivo |
| `/auditoria` | `run_unified_audit.py`, `unified_audit.py` | vista unica: veredicto, cohortes (bot, seguido, manual, plan no ejecutado, bloqueada, radar), bot vs humano, calibracion, salud y alertas | misma capa que `/viability`; ideas repetidas 1 por ticker+lado+dia |
| `/viability`, `/viabilidad` | `run_viability_audit.py`, `viability_audit.py` | bot-only vs seguido vs manual-only, EV neto, IC, drawdown | gates 5d por rango 95% bootstrap por bloques de fechas |
| `/regression` (`signal`, `optimizer`, `execution`, `blocked`, `all`) | `run_regression_audit.py`, `regression_audit.py` | relacion score -> outcome por modo | OLS con errores agrupados por bloques de fechas |
| `/dcl`, `/calibration` | `run_calibration.py`, `dcl/` | IC por horizonte, win rate, EV | significativo solo si el rango 95% del IC queda sobre 0 |
| `/confianza`, `/confidence` | `run_confidence_audit.py` | salud del pipeline y madurez de outcomes | cuenta outcomes de decisiones ejecutadas y canonicas |
| `/performance` | `run_performance.py` | win rate, EV, retornos por horizonte | etiqueta sobre EV neto de costo estimado; sin test |
| `/neto`, `/resultado_neto` | `net_decision_report.py` | neto por corrida y decision | fees reales solo si son > 0; si no, costo estimado |
| `/ledger`, `/atribucion` | `decision_ledger.py` | resumen economico y bot vs humano | descriptivo |
| `/override`, `/bot_vs_humano` | `run_override_audit.py` | seguimiento humano del plan | descriptivo (intencion = ticker + lado) |
| `/analytics` | `analytics_v2/` | cohortes con inferencia por bloques | intervalos y BH ya incorporados |
| `/qeef`, `/radar_metricas`, `/shadow` | `qeef/`, radar, shadow | evidencia prospectiva y audit-only | fuera del EV operativo |

Reglas comunes (`src/analysis/audit_inference.py`):

- Score alineado por direccion: los outcomes se guardan direccionales (un SELL
  gana si el precio cae) y un SELL nace de un score negativo. Correlacionar el
  score crudo con el outcome direccional mezcla signos; se invierte el score de
  los SELL en `/viability`, `/regression` (modo direccional) y `/dcl`.
- `outcome_basis = legacy_external` nunca entra a metricas.
- Outcome efectivo: `executable_outcome_Nd` si existe, si no `outcome_Nd`.
- Outcomes de decisiones cercanas se solapan: los intervalos remuestrean fechas
  de decision en bloques de ~5/7 del horizonte (5d -> 4 fechas).
- Un gate pasa solo si todo el rango 95% queda sobre 0; si cruza 0 es
  "no concluyente". "IC" es la correlacion score/resultado, no un intervalo de
  confianza; por eso los intervalos se rotulan "rango 95%".
- El drawdown de `/viability` es descriptivo (cronologico, 10% del capital por
  operacion, sin modelar solapamiento) y ya no es gate.
- `/viability` mide el exceso contra un benchmark (por defecto `SPY` CEDEAR en
  ARS, `--benchmark` para cambiarlo): retorno del benchmark entre el dia local
  de la decision y la primera vela >= dia + N, igual que `update_outcomes`. Para
  SELL el retorno del benchmark se suma. Es un gate: sin velas del benchmark el
  veredicto no puede ser favorable. Los demas informes siguen en retorno absoluto.
- Holdout prospectivo: `python scripts/freeze_decision_rules.py --freeze` guarda
  en `config/decision_rules_freeze.json` la fecha y el hash de cada archivo
  alcanzable por import desde `scripts/run_analysis.py` (mas `config/*.json`),
  normalizado a LF. `/viability` reporta aparte las decisiones bot-only
  posteriores a esa fecha (objetivo 40) y avisa si algun archivo de reglas
  cambio (`--check`). No cubre `.env`, estado de la base ni modelos externos.

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
