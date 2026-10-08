# Replay histórico del pipeline completo `/analisis`

## Resultado

Se ejecutó el núcleo completo actual de `/analisis` sobre la reconstrucción canónica del portfolio entre 2026-07-01 y 2026-10-07. El replay usa cartera observada, mercado histórico, capas macro/VIX registradas, sentimiento, análisis técnico, riesgo, síntesis, optimizer y planner. Después de congelar el resultado productivo adjunta E2 como contexto `SHADOW_ONLY`.

La corrida es `CURRENT_POLICY_ON_HISTORICAL_DATA`: permite medir qué habría producido la política actual con esos datos, pero no reproduce el binario ni la configuración histórica exacta. Las velas legacy tampoco tienen trazabilidad PIT completa. Por eso los resultados son retrospectivos y descriptivos, no PnL realizado ni evidencia suficiente para cambiar thresholds.

## Identidad y persistencia

- Run: `51ebcbcd-c9f5-4c42-b031-88781b126376`.
- Reconstrucción: `679932e9-6ce8-553b-b096-e6be28377316`.
- Replay contextual anterior: `6075b63f-1a94-4c40-a828-f47be487ef14`.
- Código: `ae75d78de7170607e9d4a7250156e8cfb3fcef5c`.
- Método: `analisis-current-core-recorded-layers-v1`.
- Lifecycle: `STARTED` a `COMPLETE`.
- Hash de contenido: `e397f03e7d5a0888635ad0cab15bda1743bb6292bad09c02545146d8c34d0c0c`.
- Autoridad: `SHADOW_ONLY`, `affects_analysis=false`, `affects_execution=false`, `orders_executable=false`.

La evidencia se guardó en el namespace aditivo e inmutable `historical_analysis_replay_*`. No se escribieron `portfolio_snapshots`, `positions`, `decision_log`, `execution_plans`, `order_intents`, fills, cash ni estado del broker. El `run_id` tiene cero filas en `decision_log` y `execution_plans`, y cero coincidencias en metadata de `order_intents`.

## Cobertura

La fuente canónica conserva 1.088 filas, 93 fechas y 33 tickers. El pipeline completo pudo evaluarse en 57 fechas y produjo 632 decisiones. Generó 90 intenciones con cantidad positiva dentro del replay; todas son simuladas y no ejecutables.

| Estado | Fechas | Motivo |
|---|---:|---|
| COMPLETE | 57 | Evidencia suficiente para todas las capas requeridas |
| UNAVAILABLE | 26 | No existía un vintage de análisis posterior al snapshot observado |
| UNAVAILABLE | 10 | Faltaba al menos una capa macro requerida |

De las 57 fechas completas, 55 usan evidencia legacy sin owner y 2 tienen owner exacto. La diferencia queda explícita; no se promociona evidencia legacy a owner-exact.

## Resultado del planner actual a cinco ruedas

La métrica direccional toma el retorno del activo para compras y el retorno evitado para ventas. Las observaciones comparten ventanas y no representan operaciones independientes.

| Acción del planner | n | Media 5D | Mediana | Positivos |
|---|---:|---:|---:|---:|
| BLOCKED | 30 | -0,298% | -0,084% | 46,7% |
| BUY | 26 | +0,516% | -0,348% | 46,2% |
| HOLD | 338 | +0,243% | +0,046% | 50,3% |
| SELL_PARTIAL | 52 | +1,459% | +2,553% | 57,7% |
| WATCH | 125 | -1,334% | -0,499% | 44,0% |
| **Total** | **571** | **-0,007%** | **-0,081%** | **49,2%** |

El universo completo de 632 decisiones contiene 534 señales HOLD, 62 ACCUMULATE y 36 REDUCE. El planner resolvió 370 HOLD, 139 WATCH, 62 SELL_PARTIAL, 33 BLOCKED y 28 BUY. Las diferencias entre señal y acción son parte normal de la síntesis, el optimizer y el planner actuales.

## Comparación con el replay técnico anterior

El replay anterior volvió a ejecutar sólo la capa técnica y adjuntó E2: cubrió 1.048 filas y 701 outcomes maduros a cinco ruedas, con retorno direccional medio de +0,12%, mediana de -0,19% y 48,07% positivos. El replay completo cubre menos fechas porque exige vintage registrado de macro/VIX y falla cerrado cuando falta una capa.

Los porcentajes no constituyen un A/B directo. El replay anterior clasifica señales técnicas por fila; el nuevo mide acciones finales del planner sobre fechas con todas las capas. La comparación útil es de cobertura y comportamiento por cohorte, no una afirmación de que un análisis sea más rentable que el otro.

## E2 y no regresión

El contexto se calculó para las 632 decisiones: 291 `PASS`, 112 `WARN` y 229 `FAIL`; todas tuvieron confidence contextual `HIGH` dentro de las series seleccionadas. Esos estados no cambiaron el payload productivo.

Se verificó:

- scores iguales antes y después de adjuntar contexto;
- signals iguales;
- decisions iguales;
- órdenes simuladas y cantidades iguales;
- cash igual;
- hashes productivos iguales;
- `context_has_authority=false`.

## Integridad y pruebas

- Hash global reproducido exactamente desde inputs, días y decisiones.
- 632/632 hashes de decisión reproducibles.
- 57/57 hashes de días completos reproducibles.
- Persistencia: 93 días y 632 decisiones, iguales al artefacto.
- Migración aplicada dos veces en PostgreSQL descartable: cinco objetos y once triggers.
- Protección append-only validada para `UPDATE`, `DELETE` y `TRUNCATE`.
- Suite focalizada E1/E2/replays: `59 passed`.
- Suite completa: `824 passed, 27 skipped, 7 failed`.

Las siete fallas completas ya existían en el baseline de esta rama: menú/output de Telegram, radar exploratorio y el challenger opcional sin `pypfopt`. No apareció una falla nueva del replay histórico.

## Artefactos

- `historical_analysis_replay.json`: payload completo de días y decisiones.
- `historical_analysis_replay_decisions.csv`: una fila por fecha/ticker evaluado.
- `historical_analysis_replay_report.md`: resumen operativo.
- `historical_analysis_replay_manifest.json`: hashes y fuentes.

Los artefactos finales están fuera del árbol productivo, en `C:\Users\Franco\OneDrive\Escritorio\quantia_historical_validation\contextual_replay`.

## Límites

- No es un replay PIT estricto de la captura original: gran parte del histórico de velas fue ingerido después de la fecha que describe.
- Ejecuta la política actual, no versiones históricas de código, configuración o modelos.
- Veintiséis fechas no tienen un vintage de análisis compatible y diez tienen capas macro incompletas.
- La mayoría de las fechas completas conserva owner legacy sin alcance exacto.
- El sentimiento agregado histórico podía ser mutable cuando no había una capa registrada en la decisión.
- Cada fecha vuelve a partir del portfolio observado; las compras y ventas hipotéticas no se encadenan.
- No incluye comisiones, impuestos, dividendos, flujos externos ni reconciliación de PnL realizado.

La evidencia sirve para inspeccionar el comportamiento de la política actual sobre tenencias históricas y para formular hipótesis. Todavía no es base suficiente para recalibrar scores, thresholds o autoridad económica.

## Dashboard local

La vista quedó disponible en `http://127.0.0.1:5173/replay-historico`. La API autenticada respondió `200` con el run completo, 93 días y 632 decisiones. Se recrearon solamente `monitor_api` y `frontend`; no se recrearon scheduler, Telegram ni PostgreSQL. El hash de `.env` permaneció sin cambios.
