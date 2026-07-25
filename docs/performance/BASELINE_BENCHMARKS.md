# Baseline Benchmarks

Fecha: 2026-07-23. Medicion previa a optimizaciones de esta iteracion.

## Comandos usados

```powershell
python scripts\benchmark_outputs.py --runs 5 --include-http --json
docker compose exec -T scheduler python scripts/run_decision_timeline.py --days 2 --limit 5 --json
docker compose exec -T scheduler python scripts/run_performance.py --days 30 --no-telegram
docker compose exec -T scheduler python scripts/run_decision_ledger.py --days 30 --no-telegram
docker compose exec -T scheduler python scripts/run_override_audit.py --days 30 --no-telegram
docker compose exec -T scheduler python scripts/run_analysis.py --no-persist --no-telegram --no-llm
```

## Linea base medida

| Target | Runs | Mediana | Promedio | Max | Tamano | Notas |
|---|---:|---:|---:|---:|---:|---|
| `telegram_menu_text` | 100 | 0.0005 ms | 0.0016 ms | 0.0566 ms | 949 B / 17 l | renderer puro |
| `telegram_help_text` | 100 | 0.0001 ms | 0.0002 ms | 0.0011 ms | 2499 B / 41 l | demasiado largo para mobile |
| `monitor_index` | 10 | 4.91 ms | 17.94 ms | 131.71 ms | 156468 B | leia `index.html` por request |
| `monitor_health` | 10 | 1188.98 ms | 1196.97 ms | 1284.30 ms | 570 B | 1 query DB + Redis serial |
| `monitor_override_audit_7d` | 10 | 95.11 ms | 121.34 ms | 339.78 ms | 9526 B | aceptable |
| `timeline_2d_json` | 2 | 1202.89 ms | 1202.89 ms | 1323.49 ms | 22498 B | read-only, salida JSON |
| `performance_30d` | 2 | 3090.48 ms | 3090.48 ms | 3706.72 ms | 7896 B | cerca del objetivo de 3 s |
| `ledger_30d` | 2 | 16344.07 ms | 16344.07 ms | 21057.75 ms | 7514 B | cuello principal medido |
| `override_30d` | 2 | 1393.71 ms | 1393.71 ms | 1572.25 ms | 8256 B | dentro de objetivo de reporte persistido |
| `run_analysis_no_persist_no_llm` | 1 | 70100 ms | 70100 ms | 70100 ms | no medido | incluye pipeline completo; snapshot stale fuerza exploratory |

## Alcance de la medicion

- No se enviaron mensajes reales a Telegram.
- No se persistieron decisiones de prueba.
- Los CLI se ejecutaron en Docker para usar DNS/DB reales del stack.
- Query count exacto queda instrumentado para futuros servicios; en esta iteracion se conto manualmente donde era claro (`health`: 1 DB query).
