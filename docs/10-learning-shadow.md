# Learning shadow v2

## Proposito

`learning-shadow-v2` evalua en segundo plano decisiones bloqueadas. Lee evidencia
existente y escribe solo en tablas `learning_shadow_*`. No modifica analisis,
scores, guards, planes, forecasts canonicos ni ejecuciones.

## Poblaciones

- `PLANNER_BLOCKED`: `execution_plan + BLOCKED + blocked_audit`. Es la poblacion principal.
- `RADAR_BLOCKED`: bloqueos del radar. Se informa por separado.
- `RADAR_DEBUG`: casos exploratorios. No se mezcla con planner ni radar auditado.

Los estados `SUPERSEDED` y las decisiones no bloqueadas no entran en la poblacion
principal.

## Metricas

- **Tasa potencial**: casos maduros cuyo retorno direccional posterior es al menos
  75 bps, dividido por todos los casos maduros. No es EV y no prueba un error del guard.
- **MAE/MFE**: peor y mejor recorrido dentro de la ventana del outcome, usando una
  vela diaria por fecha y la prioridad existente `COCOS > TRADINGVIEW_BYMA > internal_snapshot`.
- **Alpha contra SPY**: outcome direccional menos outcome direccional de SPY en la
  misma ventana. Una discontinuidad diaria de 35% o mas invalida el benchmark.
- **Control retrospectivo**: decision `APPROVED/EXECUTED` del mismo lado y horizonte,
  cercana en tiempo, score, delta y regimen. Puede reutilizar controles y no es causal.
- **Tasa limpia**: oportunidad material con recorrido `OK`, benchmark valido y alpha
  positivo contra SPY.

## Etiquetas de revision

| Etiqueta | Lectura |
|---|---|
| `CLEAN_MISSED_OPPORTUNITY` | resultado material, recorrido controlado y alpha positivo |
| `RISKY_COUNTERFACTUAL_WIN` | termino positivo, pero sufrio MAE medio/alto |
| `MARKET_DRIVEN_WIN` | el activo subio, pero no supero al benchmark direccional |
| `UNCONTROLLED_COUNTERFACTUAL_WIN` | resultado material sin benchmark valido |
| `NO_MATERIAL_UPSIDE` | no alcanzo el umbral material |
| `INSUFFICIENT_EVIDENCE` | outcome, recorrido o calidad insuficiente |

## Persistencia y operacion

- `learning_shadow_cases`: evidencia por decision y horizonte.
- `learning_shadow_metric_snapshots_v2`: snapshots por poblacion y horizonte.
- `learning_shadow_cohort_metrics`: backfill por cohorte semanal de decision.
- `learning_shadow_rule_candidates`: propuestas versionadas para revision humana.
- `learning_shadow_policy_versions`: contrato y limites de cada version.

El scheduler ejecuta `scripts/run_learning_shadow.py` a las 21:40 ART, despues de
`update_outcomes`. Las propuestas nacen como `PROPOSED`. El comando
`scripts/review_learning_candidate.py` solo puede aprobarlas para shadow,
rechazarlas o archivarlas; no existe promocion automatica a comportamiento live.

## Consultas

```powershell
docker compose exec -T scheduler python scripts/run_learning_shadow.py --days 365
docker compose exec -T scheduler python scripts/review_learning_candidate.py
```

El panel read-only esta en `http://localhost:5173/auditoria` y el endpoint es
`GET /api/learning-shadow`.
