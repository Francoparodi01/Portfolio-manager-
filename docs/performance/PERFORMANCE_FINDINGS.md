# Performance Findings

Fecha: 2026-07-23.

## Cuellos de botella medidos

| Hallazgo | Evidencia | Impacto | Decision |
|---|---|---|---|
| `Decision Ledger` es el output mas lento medido | 16.3 s promedio, max 21.1 s para 30 dias | Telegram queda en espera larga; no cumple objetivo de 3 s | No se optimizo aun; requiere EXPLAIN y refactor de consulta/render |
| `/api/health` tarda demasiado para 570 B | 1.2 s promedio | Monitor se siente lento aunque solo valida estado | Paralelizar Redis y mantener 1 query DB |
| `GET /` relee HTML desde disco | codigo leia `index.html` por request | I/O innecesario en cada apertura de monitor | Cachear HTML en memoria al crear app |
| `/help` era largo | 2499 B / 41 lineas | lectura pesada en Telegram mobile | Compactar a 896 B / 22 lineas |
| `/status` enviaba acuse previo | `FAST_ACTIONS` no existia | una llamada extra a Telegram en comando rapido | Tratar `status` como accion rapida |
| Muchos comandos Telegram son subprocess | `run_python_script()` envuelve CLI | startup/imports repetidos y salida stdout larga | Mantener por ahora; siguiente fase: servicios/view models |

## Riesgos evitados

- No se tocaron scores, pesos, thresholds, optimizer ni planner.
- No se alteraron outcomes, fills ni decision_log schema.
- No se mezclaron radar, shadow, planner audit y ejecucion real.

## Deuda pendiente priorizada

1. `Decision Ledger`: medir SQL con `EXPLAIN ANALYZE`, revisar joins por ticker/fecha y limitar columnas.
2. `Performance`: bajar de ~3.1 s a menos de 2.5 s con agregaciones y cache por ventana.
3. `Portfolio`: medir cache hit/miss real y evitar fallback DB lento cuando Redis esta caliente.
4. `run_analysis`: descomponer pipeline para no recalcular capas cuando se pide output compacto.
5. Telegram: reemplazar subprocess para reportes read-only por servicios internos reutilizables.
