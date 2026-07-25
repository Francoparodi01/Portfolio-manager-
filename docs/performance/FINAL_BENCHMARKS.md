# Final Benchmarks

Fecha: 2026-07-23. Iteracion PERF-001/PERF-003 parcial.

Este archivo se actualiza con los benchmarks post-rebuild. Los cambios aplicados en esta iteracion fueron:

- `/help` compacto.
- `/status` sin acuse previo y con una sola conexion DB.
- Monitor `/api/health` con Redis concurrente.
- Monitor `/` con HTML cacheado en memoria.
- Benchmark ejecutable e instrumentacion reutilizable.

## Antes/despues

| Target | Antes | Despues local previo a rebuild | Despues post-rebuild | Cambio |
|---|---:|---:|---:|---:|
| `telegram_help_text` bytes | 2499 B | 896 B | 896 B | -64.1% |
| `telegram_help_text` lineas | 41 | 22 | 22 | -46.3% |
| `monitor_index` avg | 17.94 ms | contenedor viejo | 7.92 ms warm | -55.9% |
| `monitor_index` p95 | 131.71 ms | contenedor viejo | 24.35 ms warm | -81.5% |
| `monitor_health` avg | 1196.97 ms | contenedor viejo | 330.86 ms warm | -72.4% |
| `monitor_health` median | 1188.98 ms | contenedor viejo | 251.78 ms warm | -78.8% |
| `monitor_health` p95 | 1284.30 ms | contenedor viejo | 724.23 ms warm | -43.6% |
| `monitor_override_audit_7d` avg | 121.34 ms | contenedor viejo | 136.08 ms warm | +12.1% control/no optimizado |
| `status` llamadas Telegram | 3 aprox. | 2 aprox. | 2 aprox. | -1 llamada |
| `status` conexiones DB | 2 | 1 | 1 | -50% |

## Controles CLI post-rebuild

| Target | Antes | Post-rebuild | Lectura |
|---|---:|---:|---|
| `timeline_2d_json` | 1202.89 ms avg | 1178.31 ms avg | estable; cambio no apuntaba a esta ruta |
| `performance_30d` | 3090.48 ms avg | 3293.70 ms run unico | sin mejora aplicada; requiere proxima fase |
| `override_30d` | 1393.71 ms avg | 1425.47 ms run unico | sin mejora aplicada; estable |

## Nota

La tabla usa la ultima corrida warm de 20 runs posterior al rebuild final. `monitor_override_audit_7d` no fue optimizado; se mantiene como control y su variacion queda dentro de ruido operativo.
