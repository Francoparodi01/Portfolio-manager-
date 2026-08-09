# Auditoria de performance y practicidad del backend

Fecha del corte: 2026-08-08.

## 1. Alcance, ventana y estado del repo

Incluido: scraper Playwright/Cocos, Postgres/Supabase, APScheduler, pipeline de
analisis y scoring, `monitor_api`, bot de Telegram, Docker y test suite.

Excluido: frontend React. Durante el relevamiento no se modificaron scoring,
thresholds, optimizer, `decision_log`, datos productivos ni servicios en
ejecucion. Luego de la auditoria se aprobo y desplego el lote seguro documentado
en la seccion 9.

Estado confirmado al inicio y nuevamente al cierre:

| Campo | Valor |
|---|---|
| Repo autoritativo | `C:\Users\Franco\OneDrive\Escritorio\backend\cocos_copilot` |
| Branch | `main` |
| Commit | `dac41eccca4027d8ede00f5bf8db7e89bc851d0e` |
| Sync | `HEAD == origin/main` (`0` ahead, `0` behind) |
| Worktree inicial | limpio |
| Medicion activa | 2026-08-08 21:17 a 22:05 ART |
| Logs scheduler | 2026-08-07 01:47:57 a 2026-08-08 21:50:39 ART |
| Logs Telegram | 2026-08-06 14:37:40 a 2026-08-08 21:14:24 ART |
| Estadisticas SQL | acumuladas desde `pg_stat_database.stats_reset = 2026-04-30 21:20:03 UTC` |

La aplicacion activa usa Postgres remoto en Supabase. El contenedor local
`cocos_pg_db` no es la base de estos servicios.

## 2. Resumen ejecutivo

El cuello dominante es I/O serial y consultas que recorren historia completa,
no CPU local. Los casos mas fuertes son:

1. `/api/decision-ledger`: p50 41,758 s y p95 observado 44,830 s. La variante
   exacta desplegada bajo el plan SQL de 27.413 ms a 64,83 ms.
2. Consultas de ultimo precio: la consulta general acumulo 12,46 horas de DB en
   21.676 llamadas desde el reset estadistico. El risk guard acumulo otras
   10,25 horas.
3. Scraper: el ciclo p50 fue 147,553 s. Tres waits deterministas por ciclo
   consumieron 5.892,169 s, o 98,20 minutos, dentro de la ventana observada.
4. `market_candles`: no existe indice cuyo primer campo sea `ticker`. Un query
   real de BMA tardo 1.162 ms y descarto 184.168 filas durante el scan.
5. Sentiment: 134 jobs intentaron guardar 68.887 items; solo 1.355 fueron nuevos.
   El 98,03% de los upserts repitio contenido.
6. El radar operativo completo no procesa hoy 300+ tickers frescos: hay 370
   tickers historicos, 140 quedaron stale y el ciclo real uso 219 tras excluir
   cartera. Dos ejecuciones read-only tardaron 75,013 s y 82,536 s.

No se midio saturacion de CPU o memoria. La suite completa tampoco es un cuello.

## 3. Cronologia y metodos

| Paso | Comando o metodo | Resultado principal |
|---:|---|---|
| 1 | `git status`, `git rev-parse HEAD`, `git rev-list HEAD...origin/main` | repo limpio y sincronizado |
| 2 | `docker ps`, `docker inspect` | servicios activos y DB remota confirmada |
| 3 | parser Python sobre `docker logs cocos_scheduler` | 102 ciclos Playwright y tiempos por etapa |
| 4 | parser de eventos APScheduler | 150 jobs cerrados, sin starts huerfanos |
| 5 | `pg_stat_user_tables`, `pg_total_relation_size` | tamanos, filas y scans acumulados |
| 6 | `extensions.pg_stat_statements` | costo historico de queries y DDL runtime |
| 7 | `EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)` | planes reales de pipeline y API |
| 8 | consultas alternativas read-only + comparacion de IDs/filas | ledger y risk guard equivalentes |
| 9 | 5 requests autenticados por endpoint con `perf_counter` | p50/p95 activo de `monitor_api` |
| 10 | compresion local `gzip` del mismo JSON | ahorro potencial de bytes medido |
| 11 | dos corridas `run_opportunity.py --no-persist --no-telegram --no-sentiment` | ciclo completo read-only |
| 12 | muestra secuencial/concurrente de 30 tickers | costo por ticker y paralelizacion |
| 13 | parser de logs `[BOT] action/callback ... OK en` | latencia Telegram real |
| 14 | `docker stats`, `docker system df -v`, `docker history` | reposo, pico e imagenes |
| 15 | `python -m pytest -q --durations=40` | 398 passed, 2 skipped |
| 16 | `docker build -q .` con cache caliente | build local de 5,991 s |

## 4. Cuellos de botella de velocidad

### 4.1 Scraping Playwright/Cocos

Ventana efectiva: rueda del 2026-08-07.

| Metrica | Medicion |
|---|---:|
| Ciclos Playwright cerrados | 102/102 |
| Login exitoso | 102/102 |
| Login, navegacion a confirmacion MFA p50 / p95 | 15,325 s / 20,419 s |
| Ciclo completo p50 / p95 / max | 147,553 s / 156,868 s / 162,000 s |
| Mercado, primera navegacion a guardado p50 | 119,250 s |
| Inicio a inicio de ciclo p50 | 235,367 s |
| Poll configurado | 90 s |
| Portfolio API-first | 52/52 refreshes, 100% |
| Fallback DOM portfolio | 0/52, 0% |
| Refresh real de portfolio p50 | 464,917 s |
| MFA por TOTP automatico | 102 |
| Esperas MFA manuales | 0 |
| Cloudflare/rate limit | 0 |

El loop duerme 90 s despues del trabajo. Como el trabajo dura 147,6 s p50, la
cadencia real queda en 235,4 s. No es un solapamiento de APScheduler.

Los segmentos ETF, Otros y Nuevos agotaron el wait de tabla en los 101 ciclos
de mercado:

| Segmento | Timeouts | Tiempo acumulado |
|---|---:|---:|
| ETF | 101 | 1.964,463 s |
| Otros | 101 | 1.964,775 s |
| Nuevos | 101 | 1.962,931 s |
| Total | 303 | 5.892,169 s |

El wait busca marcadores fijos como `AAPL`, `NVDA`, `YPF` o `GGAL`; esos
marcadores no estan en los tres segmentos, pero el parser posterior continua y
extrae datos. El costo esta en
[`src/collector/cocos_scraper.py`](../src/collector/cocos_scraper.py), alrededor
de las lineas 1430-1480.

El cooldown efectivo es 1.800 s. Como hubo cero bloqueos Cloudflare o auth en
la ventana, el trabajo util perdido por cooldown observado fue cero.

### 4.2 Postgres/Supabase

#### Tamano y crecimiento por filas

| Tabla | Filas | Tamano total | Filas 7 dias |
|---|---:|---:|---:|
| `market_prices` | 1.608.259 | 297,4 MiB | 119.509 |
| `market_candles` | 184.941 | 33,9 MiB | 1.356 |
| `decision_log` | 741 | 1,92 MiB | 51 |
| `portfolio_snapshots` | 3.270 | 1,06 MiB | 242 |
| `sentiment_raw` | 35.001 | 35,5 MiB | 5.685 |
| `shadow_thesis_forecasts` | 25.590 | 50,9 MiB | 3.264 |

No existe una serie historica de `pg_total_relation_size`; por eso no se informa
crecimiento en bytes. Las filas de siete dias son conteos directos por timestamp.

#### EXPLAIN ANALYZE directo

| Query | Tiempo | Evidencia del plan |
|---|---:|---|
| Ultimo precio de todos los tickers | 23.021 ms | index scan de 1.608.259 filas |
| Ultima rueda fresca | 1.708 ms | seq scan y sort externo; 4.095 bloques temp leidos |
| Velas BMA, 260 filas | 1.162 ms | seq scan; 184.168 filas descartadas |
| API ingestion, asset breakdown | 10.359 ms | 1.608.259 filas del indice recorridas |
| API candles, cobertura | 1.818 ms | dos recorridos de `market_prices` y uno de candles |
| Ledger, pending mark | 27.489 ms | 20.741.410 shared hits |

`market_prices` tiene `(ticker, ts DESC)`. `market_candles` solo tiene el unique
`(ts, long_ticker, interval)`, que no sirve para empezar por `ticker`.

#### Costo acumulado desde el 30 de abril

| Query normalizada | Calls | Total | Media |
|---|---:|---:|---:|
| Ultimo precio, todos los tickers | 21.676 | 12,46 h | 2.070 ms |
| Risk guard, ultimo precio + compras | 9.300 | 10,25 h | 3.967 ms |
| Variante risk guard con `decided_at` | 1.333 | 3,17 h | 8.550 ms |
| Cobertura diaria API | 5.845 | 2,97 h | 1.831 ms |
| Frescura/counts de `market_prices` | 8.175 | 2,43 h | 1.070 ms |
| Pending mark del ledger | 67 | 42,08 min | 37.681 ms |
| Velas canonicas por ticker | 96.387 | 27,17 min | 16,9 ms |

Dos pruebas de reescritura, sin modificar produccion:

| Caso | Original | Alternativa | Validacion |
|---|---:|---:|---|
| Ledger: filtrar decisiones y buscar ultimo precio con `LATERAL` | 27.413 ms | 64,83 ms | mismas 25 filas y semantica |
| Risk guard: limitar precios a 12 tickers activos | 1.070 ms | 7,15 ms | mismos 7 resultados |

#### DDL ejecutado durante runtime

Los procesos llaman DDL idempotente y backfills al iniciar scripts o acceder a
capas lazy. Acumulado en `pg_stat_statements`:

| Tipo | Shapes | Calls | Tiempo total |
|---|---:|---:|---:|
| `ALTER TABLE` | 160 | 490.676 | 377,2 s |
| Backfills `UPDATE decision_log` | 26 | 20.475 | 275,8 s |
| `CREATE INDEX IF NOT EXISTS` | 82 | 162.724 | 230,8 s |
| `CREATE TABLE IF NOT EXISTS` | 48 | 58.739 | 185,7 s |
| Total | 316 | 732.614 | 1.069,5 s |

Ejemplo concreto: `manual_market_events` ejecuto `CREATE TABLE IF NOT EXISTS`
14.347 veces. `scripts/run_sentiment_pipeline.py` llama `init_schema()` en cada
job de 15 minutos.

### 4.3 Scheduler

Se reconstruyeron 150 ejecuciones desde pares `Running job` / `executed
successfully`. No hubo missed jobs ni `maximum running instances`; concurrencia
maxima observada: 2.

| Job | n | p50 | p95 observado | Total |
|---|---:|---:|---:|---:|
| Sentiment context | 134 | 9,195 s | 11,739 s | 1.270,610 s |
| Full 17:02 | 1 | 177,726 s | 177,726 s | 177,726 s |
| Thesis shadow 17:18 | 1 | 171,346 s | 171,346 s | 171,346 s |
| Opening portfolio 10:31 | 1 | 171,111 s | 171,111 s | 171,111 s |
| Daily analysis 17:12 | 1 | 42,523 s | 42,523 s | 42,523 s |
| Issuer events | 4 | 9,332 s | 12,178 s | 38,498 s |

Sentiment corre cada 15 minutos, tambien fuera de rueda. En 134 runs:

- 68.887 items recuperados y reportados como guardados.
- 1.355 filas nuevas y 1.355 scores nuevos.
- 67.532 intentos fueron upsert de contenido existente: 98,03%.
- Tiempo total del job: 21,18 minutos dentro de 43 horas de logs.

### 4.4 Pipeline de analisis/scoring

El universo historico tiene 370 tickers. El filtro de frescura dejo 230 y, luego
de excluir cartera, el radar operativo proceso 219. No se fabrico una prueba de
300+ porque ese no es el universo que hoy deja pasar el codigo productivo.

Dos corridas exactas, read-only, tardaron 75,013 s y 82,536 s; mediana de dos:
78,775 s. En la corrida instrumentada, CPU user+system fue 9,366 s.

| Etapa instrumentada | Tiempo |
|---|---:|
| Portfolio actual | 11.299 ms |
| Scores de cartera | 728 ms |
| Universo fresco | 6.077 ms |
| 219 historias, secuencial | 29.014 ms |
| Contexto sentiment | 239 ms |
| Contexto shadow | 2.955 ms |
| Eventos manuales | 636 ms |
| Scoring puro, 218 evaluados | 4.163 ms |

La carga de historias hace un query por ticker en serie. En una muestra fija de
30 tickers:

| Modo | Wall | p50 por llamada | p95 por llamada | Resultado |
|---|---:|---:|---:|---|
| Secuencial | 5.201 ms | 115,6 ms | 140,3 ms | 6.953 velas |
| Pool concurrente, max 5 | 1.332 ms | 925,1 ms con espera de pool | 1.266,3 ms | mismas 6.953 velas |

La pared bajo 3,9 veces aunque cada task incluyo espera por pool. Aumentar
concurrencia sin indice tambien aumenta presion sobre Supabase; requiere prueba
de carga antes de produccion.

### 4.5 API `monitor_api`

No hay access log con duracion por request. Por eso el p50/p95 historico de
produccion no se puede recuperar. Se ejecuto un benchmark activo con 5 requests
secuenciales por endpoint, desde el mismo contenedor. Con `n=5`, el p95 observado
es el maximo de la muestra, no un percentil de largo plazo.

| Endpoint | p50 | p95 observado | Payload max |
|---|---:|---:|---:|
| `/api/health` | 263 ms | 1.910 ms | 579 B |
| `/api/ingestion` | 28.887 ms | 40.341 ms | 2,2 KiB |
| `/api/candles` | 3.693 ms | 5.227 ms | 2,5 KiB |
| `/api/decisions?days=90` | 302 ms | 409 ms | 12,8 KiB |
| `/api/portfolio?days=90` | 398 ms | 613 ms | 17,2 KiB |
| `/api/performance?days=180` | 873 ms | 1.406 ms | 110,9 KiB |
| `/api/override-audit?days=90` | 247 ms | 314 ms | 50,7 KiB |
| `/api/decision-ledger?days=90` | 41.758 ms | 44.830 ms | 255,9 KiB |
| `/api/audit-timeline?days=90&limit=400` | 646 ms | 748 ms | 885,4 KiB |
| `/api/radar-audit?days=90` | 4.764 ms | 5.106 ms | 272,5 KiB |
| `/api/shadow` | 2.831 ms | 5.572 ms | 300,0 KiB |
| `/api/learning-shadow?days=365` | 754 ms | 1.221 ms | 46,0 KiB |
| `/api/fills?days=90` | 292 ms | 536 ms | 49,9 KiB |

Los endpoints pesados calculan joins, outcomes, cobertura o ultimo precio dentro
del request. Redis estaba disponible, pero estas respuestas no se sirvieron
comprimidas ni desde un snapshot precalculado.

Compresion medida sobre el JSON real:

| Endpoint | Actual | gzip nivel 6 | Reduccion |
|---|---:|---:|---:|
| Audit timeline | 885.449 B | 73.954 B | 91,6% |
| Decision ledger | 255.866 B | 32.768 B | 87,2% |
| Shadow | 300.034 B | 23.878 B | 92,0% |

### 4.6 Telegram

Los tiempos incluyen subprocess, DB y envio de respuesta. Muestra de logs:

| Accion/comando | n | p50 | p95 observado |
|---|---:|---:|---:|
| `analysis_full` | 2 | 251,420 s | 256,748 s |
| `analysis` | 5 | 75,590 s | 94,848 s |
| callback `analysis` | 5 | 79,410 s | 112,468 s |
| `radar_full` | 1 | 62,640 s | 62,640 s |
| callback `radar` | 2 | 58,880 s | 74,207 s |
| `market_context` | 1 | 22,130 s | 22,130 s |
| `ticker_analysis` | 2 | 9,315 s | 10,877 s |
| callback `upcoming_events` | 4 | 1,600 s | 1,644 s |
| `portfolio` | 5 | 0,570 s | 1,036 s |

El bot lanza un proceso Python nuevo por accion pesada y espera el resultado.
Las latencias largas reproducen los costos del pipeline; no son latencia de la
API de Telegram.

Hubo 54 errores de polling entre 00:03:57 y 00:16:34 ART, una ventana de 757 s.
La causa registrada fue DNS: `httpx.ConnectError: Name or service not known`.
No hubo conflicto de dos instancias de `getUpdates`.

### 4.7 Docker e infraestructura

#### Reposo y pico observado

| Contenedor | Memoria reposo | CPU reposo medio | Pico CPU observado | Pico memoria observado |
|---|---:|---:|---:|---:|
| `cocos_scheduler` | 105,5 MiB | 0,007% | 97,5% en radar | 202,2 MiB |
| `cocos_monitor_api` | 45,7 MiB | 0,000% | 7,24% en benchmark API | 59,9 MiB |
| `cocos_telegram_bot` | 212,1 MiB | 0,002% | 0,40% durante auditoria | 212,3 MiB |
| `cocos_pg_db` | 90,6 MiB | 0,119% | 2,02% durante auditoria | 90,9 MiB |

No se capturo un callback pesado de Telegram junto con `docker stats`; su pico
de comando no esta disponible. Los valores de esa fila son el maximo observado
durante la auditoria, no el maximo historico.

#### Imagenes y build

Scheduler, API y Telegram usan el mismo Dockerfile con Chromium:

| Imagen | Tamano virtual | Unique local |
|---|---:|---:|
| scheduler | 2,73 GB | 726,1 MB |
| monitor API | 2,72 GB | 716,8 MB |
| Telegram | 2,72 GB | 723,1 MB |

Capas dominantes reportadas por `docker history`: dependencias Python 855 MB,
Chromium 687 MB y librerias Playwright 309 MB. API y Telegram no ejecutan
Playwright.

Build con cache caliente: 5,991 s. El build frio no se midio. El tiempo de
deploy tampoco se midio porque implicaba recrear servicios productivos; no hay
timestamps de inicio de build conservados para reconstruirlo.

`cocos_pg_db` pertenece a
`cocos_copilot_1-copia\cocos_copilot_pg\docker`, no al Compose autoritativo.
Es el unico contenedor en `docker_default`; scheduler, API y Telegram apuntan a
Supabase. Se mantuvo intacto porque puede contener datos historicos de la copia.

### 4.8 Test suite

| Metrica | Resultado |
|---|---:|
| Resultado | 398 passed, 2 skipped |
| Tiempo pytest | 9,67 s |
| Wall de PowerShell | 11,65 s |
| Modulo mas lento | `test_ticker_technical_report.py`, 3,55 s |
| Test mas lento | render PNG, 2,12 s |
| Segundo test | render de dos PNG, 1,49 s |

No se encontro evidencia de tests redundantes con impacto operativo. Los dos
mas lentos validan artefactos graficos reales y explican la mayor parte del
tiempo no trivial.

## 5. Hallazgos priorizados

| Prioridad | Hallazgo | Impacto medido | Esfuerzo | Tratamiento |
|---|---|---|---|---|
| P0 | Ledger recorre precios completos por cada decision | endpoint p50 41,8 s; SQL exacto 64,83 ms | bajo | seguro de aplicar ya |
| P0 | Risk guard calcula ultimo precio de todo el mercado | 1.070 ms a 7,15 ms con filtro equivalente | bajo | seguro de aplicar ya |
| P0 | JSON grande sin compresion | 87,2% a 92,0% menos bytes con gzip | bajo | seguro de aplicar ya |
| P1 | Falta indice de candles por ticker/intervalo/fecha | BMA 1.162 ms y 184.168 filas descartadas | medio | validar en produccion |
| P1 | Historias cargadas en serie | 5.201 ms a 1.332 ms en 30 tickers | medio | validar limite de conexiones |
| P1 | Tres waits Playwright deterministas | 98,20 min acumulados | medio | validar contra Cocos vivo |
| P1 | Sentiment 24/7 con 98,03% de upserts repetidos | 21,18 min de job en la ventana | medio | validar politica de frescura |
| P1 | Computo pesado dentro de requests | ingestion p50 28,9 s; shadow 2,8 s | medio | precalcular/cachear con TTL |
| P2 | DDL y backfills durante runtime | 732.614 calls; 17,83 min acumulados | medio | migracion explicita primero |
| P2 | Bot crea subprocess por accion pesada | analysis_full p50 251,4 s | medio | job async o resultado cacheado |
| P2 | Una imagen Playwright para los tres servicios | 2,72 GB por imagen | medio | separar imagenes runtime |
| P2 | DB local de la copia sigue encendida | 90,6 MiB y confusion de checkout | bajo | confirmar datos y detener |

## 6. Fricciones operativas separadas de velocidad

1. El contenedor `cocos_pg_db` viene de `_1-copia`, mientras la aplicacion real
   usa Supabase. Esto agrega una fuente falsa de verdad al diagnostico.
2. En 51 refreshes aparecio `No se pudo seleccionar tab Instrumentos en
   movements`; se detectaron cero movimientos en esos intentos. Cuando haya que
   reconciliar una operacion nueva, obliga a revisar manualmente Cocos/fills.
3. `monitor_api` no conserva latencia por ruta. Cada auditoria de p50/p95 exige
   generar carga activa y no permite distinguir regresion historica.
4. Los comandos largos de Telegram mantienen al usuario esperando entre 59 y
   257 s, aunque el callback fue reconocido inmediatamente.
5. Las migraciones lazy esconden el estado de schema: cada proceso intenta
   reparar tablas en lugar de fallar con un diagnostico de version claro.
6. La caida DNS de Telegram genero 54 tracebacks repetidos en 12 min 37 s; el
   retry recupero solo, pero el volumen de log dificulta encontrar otros errores.

No hubo intervencion humana por MFA: las 102 autenticaciones usaron TOTP
automatico y hubo cero esperas por codigo manual.

## 7. Estado final y frontera de cambio

### Optimizaciones seguras para aplicar ya

1. Reescribir pending mark del ledger: filtrar las decisiones primero y buscar
   el ultimo precio por ticker con `LATERAL ... ORDER BY ts DESC LIMIT 1`.
2. Restringir `latest_prices` del risk guard a `active_tickers`, o usar el mismo
   patron lateral. Las pruebas devolvieron las mismas filas.
3. Activar compresion HTTP en `_json()` cuando el cliente acepte gzip.
4. Agregar middleware de latencia por ruta con status, bytes y duracion; no debe
   registrar tokens ni payloads.
5. Corregir la metrica de sentiment para separar `attempted_upserts`, inserts
   nuevos y updates; hoy `raw_saved` equivale a intentos.

Estas cinco acciones no cambian scoring, thresholds, optimizer, outcomes ni
contenido de `decision_log`.

### Requiere validacion o aprobacion antes de produccion

1. Crear concurrentemente un indice de lectura para candles que empiece por
   `ticker` y `interval`, y comparar planes antes/despues.
2. Paralelizar la carga de historias con limite explicito y prueba contra el
   pool/plan de Supabase.
3. Cambiar el criterio de espera de tablas Playwright; debe probarse al menos una
   rueda completa y conservar conteos por segmento.
4. Reducir frecuencia off-hours de sentiment o evitar re-upsert de hashes ya
   vistos; debe definirse primero el SLA de frescura.
5. Precalcular ingestion, ledger, radar y shadow con timestamp/TTL visible.
6. Mover DDL a una migracion de deploy y retirar lazy migrations solo despues
   de validar version de schema al arranque.
7. Separar una imagen Playwright para scheduler de imagenes livianas para API y
   Telegram; validar smoke y deploy.
8. Detener/eliminar `cocos_pg_db` solo despues de inspeccionar o respaldar el
   volumen de la copia.

### No vale la pena tocar con la evidencia actual

1. APScheduler: no hubo missed jobs ni saturacion de instancias.
2. Cooldown Cloudflare: cero eventos y cero trabajo perdido en la ventana.
3. Test suite: menos de 10 s internos; optimizarla no devuelve tiempo relevante.
4. Portfolio API-first: 100% de los refreshes observados; el fallback DOM no fue
   usado.
5. CPU/memoria del API: el pico de CPU fue 7,24%; subir limites no corrige las
   queries remotas.
6. Optimizar primero el login de 15,3 s: los waits de tablas y SQL tienen impacto
   muy superior y ya medido.

## 8. Limitaciones explicitas

- `py-spy` no estaba instalado ni en host ni en scheduler. Se usaron tiempos por
  etapa, `resource`, EXPLAIN y CPU de contenedor; no se presenta flame graph.
- El API no tenia access logs temporizados; el p50/p95 informado es benchmark
  activo `n=5`, no trafico historico.
- El relevamiento inicial no incluyo build ni deploy; el lote seguro aplicado
  despues se reconstruyo y valido como se detalla en la seccion 9.
- No se midio pico de Telegram durante un callback pesado.
- No hubo evento Cloudflare; no se extrapolo costo de cooldown.
- No existe snapshot previo de tamanos de tabla; no se estimo crecimiento en
  bytes.
- Las estadisticas SQL acumuladas se etiquetan desde su reset y no se atribuyen
  solo a los dos dias de logs.

## 9. Aplicacion del lote seguro

El 2026-08-08 se aplicaron solamente las optimizaciones 1 a 4 de la seccion 7.
No se modificaron scoring, thresholds, optimizer, decisiones, outcomes,
scraping ni cadencias del scheduler.

Cambios desplegados:

1. El pending mark del ledger ahora busca el ultimo precio por ticker mediante
   `JOIN LATERAL ... ORDER BY ts DESC LIMIT 1`.
2. El risk guard usa la misma busqueda acotada para los tickers activos.
3. Las respuestas JSON negocian gzip con el cliente.
4. El API registra metodo, ruta sin query string, status, duracion y bytes. No
   registra headers, tokens ni payloads.

Validacion posterior:

| Evidencia | Resultado |
|---|---:|
| EXPLAIN del pending mark exacto | 64,83 ms; 25 filas |
| EXPLAIN del risk guard exacto | 7,15 ms; 7 filas |
| `/api/decision-ledger?days=90`, 3 requests | p50 10.268 ms; max 12.071 ms |
| Ledger antes del cambio | p50 41.800 ms; p95 observado 44.800 ms |
| Payload ledger con gzip | 255.866 a ~32.770 bytes, 87,2% menos |
| Suite completa | 400 passed, 2 skipped, 6,81 s |
| Servicios reconstruidos | `monitor_api` y `scheduler`, ambos `Up` |

El ledger mejoro 75,4% en la mediana observada, pero sus ~10,3 s restantes
confirman que todavia contiene otras etapas costosas. La proxima intervencion
debe medirlas por separado antes de agregar cache o paralelismo.

## 10. Aplicacion a Telegram

El 2026-08-09 se midio el callback real `weekly_analysis -> analysis`. Antes
del cambio tardo 87,93 s: 41,54 s de sincronizacion Cocos/fills y 45,75 s de
calculo.

Se aplicaron tres cambios operativos sin modificar scoring ni decisiones:

1. Una sincronizacion Cocos/fills exitosa se reutiliza durante 300 s para el
   mismo owner. Un fallo nunca se cachea y `analysis_full` sigue forzando una
   sincronizacion completa.
2. Ledger, Performance y Bot vs Humano dejaron de lanzar un scrape antes de
   leer datos historicos persistidos.
3. La consulta de universo fresco limita el scan a registros desde la ultima
   rueda valida. Contra la consulta anterior devolvio las mismas 230 filas. La
   etapa completa paso de 13.466 ms a 6.112 ms en la primera medicion; la
   busqueda interna de precios recientes tardo 416 ms. La deteccion de rueda se
   acoto a 14 dias con fallback automatico al historico completo.

Validacion desplegada:

| Etapa | Antes | Despues |
|---|---:|---:|
| Sincronizacion Cocos ya reciente | 41,54 s | 0,173 s |
| Calculo semanal | 45,75 s | 24,15 s |
| Flujo con sincronizacion reciente | 87,93 s | ~24,3 s |
| Suite completa | 400 passed | 403 passed, 2 skipped |

La primera ejecucion con estado vencido todavia debe entrar a Cocos y validar
portfolio/fills. Esa ruta midio 49,88 s de sincronizacion mas 25,98 s de calculo;
no se presenta como respuesta rapida. El ahorro fuerte aplica cuando el estado
operativo ya fue actualizado dentro de la ventana visible de 5 minutos.

## 11. Segunda aplicacion: cache validado y procesos internos

El 2026-08-09 se desplego el siguiente bloque sin modificar scoring,
thresholds, optimizer, `decision_log` ni ordenes:

1. `/analisis` y `/radar` consultan artefactos por owner, snapshot de cartera,
   bucket de mercado de 15 minutos, watermark de velas y hash del runner. Los
   comandos `_full` conservan la sincronizacion y el recalculo explicitos.
2. Las lecturas de historia por ticker usan concurrencia acotada a cinco.
3. `/api/ingestion` usa cache de proceso por 120 s y queries limitadas a datos
   recientes. Dejo de ejecutarse globalmente en todas las pantallas.
4. La timeline inicial bajo de 400 a 120 filas solicitadas.
5. Se creo `idx_market_candles_ticker_interval_ts` de forma concurrente.
6. Sentiment omite upserts sin cambios, no ejecuta `init.sql` en cada ciclo y
   usa cadencia de 60 minutos fuera de rueda.
7. `monitor_api` usa una imagen sin Chromium ni librerias del navegador.

Validacion desplegada:

| Evidencia | Resultado |
|---|---:|
| Cache Telegram analisis | 0,985 s; 4.345 caracteres |
| Cache Telegram radar | 1,081 s; 6.870 caracteres |
| `/api/ingestion` frio / caliente | 1,42-3,76 s / 0,002-0,004 s |
| Timeline 90d, limit 120 | 0,58-0,70 s; 262.471 bytes |
| `/api/decisions?days=90` | 0,39-0,48 s |
| Navegador: timeline visible | 1,37 s |
| Navegador: consultas asentadas | 2,33 s; sin errores de consola |
| Indice de velas, BMA, warm n=3 | 0,227-0,256 ms; Index Scan confirmado |
| Sentiment, segunda captura | 2 escrituras de 129 items |
| Imagen `monitor_api` | 2,72 GB a 1,31 GB |
| Suite completa | 409 passed, 2 skipped |

No se activo el reemplazo del wait de Playwright por una condicion nueva. La
fecha de despliegue fue fuera de rueda y esa modificacion requiere observar una
vuelta completa de Acciones y todos los segmentos CEDEAR antes de cambiar el
criterio productivo. Mantener el wait actual evita convertir una optimizacion
no validada en perdida silenciosa de cobertura.
