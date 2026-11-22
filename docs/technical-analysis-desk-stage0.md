# Technical Analysis Desk — cobertura y cierre de los puntos abiertos

> Informe histórico de etapa 0. El protocolo de piloto de esta versión quedó reemplazado por [etapa 0b](technical-analysis-desk-stage0b.md): cinco paquetes completos repetidos, USD 2 máximo y debate excluido. La evidencia de cobertura de abajo conserva su corte original.

**Auditoría de solo lectura; no se implementó ni activó el Desk.** Captura principal: 2026-11-14 23:50:59 ART (2026-11-15 02:50:59 UTC). Detalle de identidad y enlaces: 23:52:23 ART. Base local configurada `quantia`, owner del bot, transacciones `REPEATABLE READ` con `transaction_read_only=on` verificado. Ambas capturas seleccionaron el mismo último run Radar.

## Resultado

Hay registros versionados para los **7/7 tickers actuales de cartera**, pero todavía no una fuente lista para producir el Desk diario/semanal con su contrato completo. Las velas cerradas candidatas de esos instrumentos llegan al **4 de noviembre de 2026**; el 5 de noviembre aparece en el archivo bruto como barra no cerrada. Faltan calendario validado, unidades de volumen y ratio depositario. El histórico de precios anterior a octubre no implica que Quantia lo conociera antes de octubre.

El Radar sí conserva historia de **6 fingerprints de scoring**, con **59 corridas y 17.556 snapshots**. `/radar_metricas` selecciona la versión del último run y acumula resultados únicamente de esa versión. Las demás siguen en la base: por ejemplo, hay **4.935 outcomes de 40 ruedas** en versiones anteriores y **ninguno** en la versión que muestra actualmente el comando. Son conteos de registros, no operaciones ni resultados de rentabilidad.

## Cobertura de datos al corte

`market_candle_observations` contiene **39.960 observaciones / 16 tickers**, todas `TRADINGVIEW_BYMA`, ARS. Hay **38.829 observaciones candidatas** según cierre, cronología, lifecycle `COMPLETE` y formato de digest. No son 38.829 ruedas: diferentes capturas/versiones repiten barras. No se recomputaron los hashes de contenido; «candidata» no significa validación criptográfica ni calidad completa aprobada.

Para cartera se deduplicaron etiquetas diarias entre versiones, conservando identidad BYMA/tipo/ticker/ARS. El conteo no certifica ausencia de huecos porque el calendario aún no está validado.

| Ticker | Días distintos candidatos | Primer precio registrado | Último precio cerrado candidato | Primera disponibilidad del registro candidato, UTC |
|---|---:|---|---|---|
| AMD | 279 | 2025-09-17 | 2026-11-04 | 2026-10-07 21:59:24 |
| GDX | 279 | 2025-09-17 | 2026-11-04 | 2026-10-08 02:39:02 |
| GOOGL | 259 | 2025-10-16 | 2026-11-04 | 2026-11-06 00:28:49 |
| MU | 279 | 2025-09-17 | 2026-11-04 | 2026-10-08 02:39:02 |
| NVDA | 279 | 2025-09-17 | 2026-11-04 | 2026-10-08 02:39:02 |
| TQQQ | 271 | 2025-09-29 | 2026-11-04 | 2026-10-08 02:39:02 |
| VIST | 279 | 2025-09-17 | 2026-11-04 | 2026-10-08 02:39:02 |

La última columna es el primer `known_at` candidato observado, calculado con disponibilidad, ingestión/creación y cierre de lifecycle de la captura. No afirma que la serie completa hasta noviembre existiera en octubre. Para cada fecha histórica se deben volver a seleccionar las versiones disponibles a ese corte.

Otros universos:

- **Radar actual:** 417 instrumentos, más 2 controles (`QQQ`, `SPY`). Sólo **15/417** tienen alguna serie candidata BYMA/ARS con tipo de activo coincidente en el archivo versionado consultado. No se asumió que ticker idéntico implica identidad USD equivalente.
- **FOLLOW reciente:** 8 símbolos distintos en los últimos 30 días; ninguno tiene siquiera símbolo coincidente entre las observaciones candidatas: FISV, LLY, SHEL, TSLA, XLB, XLE, XLY y ZM.
- **Subyacentes USD:** ninguna observación USD en esta tabla para el owner. Esto no demuestra inexistencia de datos USD en otras fuentes no consultadas.
- **Snapshots contextuales:** 148 snapshots, 73.240 enlaces a velas; cero huérfanos, cruces de owner, enlaces con disponibilidad/scrape/fin de barra posterior al corte o barras no cerradas en los controles ejecutados. Esos checks no certifican por sí solos todo el contrato PIT.
- **Tabla mutable `market_candles`:** 272.058 filas compartidas; ninguna con `available_at` ni `is_closed=true`. No sustituye al archivo as-of, aunque contenga más histórico.

Metadatos de las **39.960/39.960** observaciones versionadas: `calendar=NULL`, `calendar_validation=NULL`, `volume_unit=NULL`, `depositary_ratio=NULL`. La política registrada de ajustes es `splits` y la unidad de precio `ARS_per_instrument`. La cantidad numérica de volumen existe, pero su unidad no está documentada en ese contrato. La falta de ratio bloquea reconciliar CEDEAR/subyacente; no obliga a negar que exista una serie local identificada.

**Implicación:** se puede inspeccionar evidencia técnica histórica acotada desde la fecha de disponibilidad correspondiente; no está demostrado un replay completo del universo Radar, una lectura actualizada a la última rueda ni un paquete semanal con calendario verificado. Antes del MVP se necesita resolver esas brechas de fuente/contrato y medir frescura, no fabricar metadatos.

## Radar: versiones conservadas y comportamiento del resumen

Las primeras cinco filas corresponden al prefijo `radar-v2+technical-shadow-v2+technical-buy-shadow-v3:`. Se muestra su fingerprint final para legibilidad; `coverage.json` conserva las versiones exactas y los conteos por ticker/fecha. La sexta tiene el nombre `radar-market-v1:4f4591409351bebf:2cd92452bae07abf`.

| Fingerprint | Capturas | Fechas de captura | Snapshots | Con score V2 / V3 | Outcomes 5 / 10 / 20 / 40 ruedas |
|---|---:|---|---:|---:|---|
| `3e1bb4df96712168` | 1 | 20-08 | 260 | 249 / 249 | 251 / 251 / 251 / 251 |
| `774a271b747674d3` | 1 | 21-08 | 258 | 247 / 247 | 249 / 249 / 249 / 249 |
| `1fdd00d2f0b32efe` | 24 | 24-08 a 25-09 | 6.527 | 5.951 / 5.951 | 5.999 / 5.998 / 5.997 / 4.435 |
| `878c69e9bb2f0dd6` | 2 | 28-09 a 30-09 | 578 | 525 / 525 | 529 / 528 / 528 / 0 |
| `b9ac3bfaf000607f` — actual | 30 | 01-10 a 13-11 | 9.534 | 8.534 / 8.534 | 6.799 / 5.457 / 2.830 / 0 |
| `radar-market-v1 … 2cd92452bae07abf` | 1 | 19-10 | 399 | 351 / 351 | 373 / 370 / 0 / 0 |

Todas las fechas son de 2026. Outcomes son filas persistidas de `radar_discovery_outcomes`, antes de los filtros de calidad/cohorte del resumen; no son su denominador final. Cero significa que la consulta no encontró filas de ese horizonte, no un retorno cero. No sumar horizontes como observaciones independientes.

**Qué selecciona el comando.** `scripts/run_radar_metrics.py:40` toma el último run del owner por `captured_at DESC`; `:58` fija su `scoring_version`; `:61` consulta su historia; `:111` mantiene esa versión para todos los horizontes. Ranking/estado inicial pertenecen sólo al último run. No busca la última versión madura por horizonte ni agrega todos los fingerprints.

**Verificación de runtime.** Los hashes SHA-256 normalizados de `run_radar_metrics.py`, `radar_discovery.py`, `query_radar_discovery.py` y `run_opportunity.py` coinciden entre el checkout y `cocos_telegram_bot`. El archivo Telegram completo difiere, pero el handler desplegado ejecuta el lector con owner y sin override de versión. Evidencia y referencias exactas: `output/technical-desk-stage0-20261114/radar_runtime_review.json`.

**Qué se puede recuperar.** Hay **15.857/17.556 snapshots** con score V2 y la misma cantidad con score V3. Se preservan versiones, score/bias/percentil V2 y clasificación/tier/elegibilidad V3. Se pueden construir cohortes por valores originales y enlazarlas a sus outcomes sin recalcular el pasado. El score V3 guardado usa `source_score` V2; no es una probabilidad nueva.

**Qué no demuestra esa persistencia.** No garantiza conservar todos los inputs OHLCV/indicadores necesarios para regenerar cada análisis. Recalcular con código actual es una investigación retrospectiva distinta. Los lectores actuales de comparación seleccionan tier/elegibilidad V3, pero no columnas V2; Telegram muestra discovery top quintile y trigger del universo, no un informe completo de V2/V3. La herramienta `query_radar_discovery.py` admite una versión exacta para consultas históricas; no se ejecutó su bootstrap en esta auditoría porque los lectores pueden crear esquema. Se usaron SELECT directos protegidos por transacción read-only.

## Ruido y presupuesto incorporados al diseño

El [diseño actualizado](technical-analysis-desk-design.md) exige cinco paquetes congelados ejecutados dos veces, sin caché de respuestas ni contaminación entre réplicas. Se miden dirección, conteos/fases, pivotes, barreras, abstenciones y síntesis por agente/timeframe. Un cambio entre repeticiones se registra como variabilidad, no automáticamente como cambio de mercado. Dos réplicas son el mínimo diagnóstico, no una prueba de estabilidad estadística.

El debate se ejecutará completo en toda la muestra preregistrada. El lote repetido necesita 50 llamadas sin debate o 100 con debate antes de reintentos. El tope de diseño propuesto es **USD 2 de API externa por lote**, contabilizando repeticiones, fallos, reintentos y reservas concurrentes. Con tarifa no verificable o presupuesto insuficiente, no se despacha. El costo calculado y la facturación confirmada se registran separados.

**Medición real de ruido/costo: NOT_RUN.** Todavía no existe el Desk que se debe medir. Esta revisión no hizo llamadas de modelos ni atribuye un costo ficticio al primer piloto.

## Línea base reproducible

La baseline fuente local aislada usa la rama `baseline/technical-desk-stage0-20261114` y el worktree `C:\Users\Franco\Documents\Codex\worktrees\quantia-technical-desk-stage0-baseline`, con parent `661236a0bdbe1e4999221ecd498ca5d2efd4e71a`. Preserva las modificaciones locales necesarias y archivos fuente no seguidos/ignorados. Incluye expresamente tests antes ignorados, como `test_technical_candle_sources.py`; una rama apuntando sólo a HEAD habría omitido parte de la base realmente revisada.

La baseline excluye artefactos de `tmp`, `output`, reconstrucciones, credenciales y caches. No se presentan cambios previos como implementación del Desk. El manifiesto externo lista inclusiones/exclusiones, hashes, commit final, branch/worktree y verificación de que el checkout/index originales se conservaron. La baseline requiere estado Git limpio y ejecutar allí las pruebas pertinentes; el resultado de congelación y validación queda en `output/technical-desk-stage0-20261114/baseline_result.json`.

Una baseline de código no congela la base operativa ni garantiza instalación bit a bit: los captures se conservan aparte con sus hashes, y el entorno de Python/paquetes usado se registra como evidencia. Los rangos existentes de `requirements.txt` no se reinterpretan como un lock exacto.

## Evidencia y validación

Artefactos completos en `output/technical-desk-stage0-20261114/`:

- `coverage.json`: 12 grupos de resultados, incluidas 17.556 filas agregadas Radar por versión/ticker/fecha y 6.002 grupos de observaciones por versión/identidad/fecha; conserva SQL exacto y parámetros simbólicos de owner.
- `details.json`: miembros de la última captura, controles y metadatos/integridad de enlaces.
- `summary.json`: agregados reconciliados, hashes de los captures y 17 controles satisfactorios.
- `radar_runtime_review.json`: hashes local/runtime y filtros de los lectores desplegados.
- `baseline_result.json` y manifiestos asociados: procedencia de la baseline aislada.

Comandos de captura y verificación:

```text
python scripts/audit_technical_desk_coverage.py --schema-only
python scripts/audit_technical_desk_coverage.py
python scripts/audit_technical_desk_coverage.py --details-only
python scripts/summarize_technical_desk_coverage.py output/technical-desk-stage0-20261114
python -m py_compile scripts/audit_technical_desk_coverage.py scripts/summarize_technical_desk_coverage.py
```

La captura no sobrescribe archivos existentes: para otra observación usar `--output` con otro directorio. La reducción opera sobre los captures guardados, sin repetir consultas. **17 controles aprobados**: modo read-only/aislamiento, owner, última captura, reconciliación entre granos, universo sin controles y enlaces. Se preservaron los resultados de la revisión inicial (55 pruebas aprobadas); una ejecución en la baseline constituye una validación diferente y se registra allí.

Límite de cobertura: esta auditoría describe sólo la base local configurada y las tablas consultadas. No se consultaron Supabase, backups ni otras bases; no equivale al histórico completo de Quantia. No se cambiaron tablas, modelos, thresholds, órdenes, configuración operativa ni servicios.
