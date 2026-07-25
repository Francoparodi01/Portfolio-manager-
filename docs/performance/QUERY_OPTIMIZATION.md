# Query Optimization

## Cambios aplicados

| Area | Antes | Despues | Query count |
|---|---|---|---:|
| Monitor `/api/health` | 1 query DB + Redis secuencial | 1 query DB + Redis concurrente | DB sin cambios |
| Telegram `/status` | dos aperturas de `PortfolioDatabase` | una sola conexion/pool para snapshot y market data | SQL sin cambios |
| Monitor `/` | lectura de archivo por request | HTML cargado al crear la app | sin DB |

## No aplicado aun

- No se agregaron indices.
- No se ejecutaron migraciones.
- No se hicieron `EXPLAIN ANALYZE` sobre Ledger/Performance todavia.

## Consultas a perfilar en la siguiente iteracion

1. `src.analysis.decision_ledger.fetch_decision_ledger()`
2. `scripts.run_performance.async_main()` y funciones de stats en `src.collector.db`
3. `src.monitor.api.performance_view()`
4. `src.monitor.api.decision_ledger()`
5. `scripts.run_analysis` para duplicacion de snapshots, sentiment y technical frames

## Regla para indices

Agregar indices solo despues de:

1. `EXPLAIN (ANALYZE, BUFFERS)`
2. medicion de filas leidas
3. comparacion antes/despues
4. rollback documentado
