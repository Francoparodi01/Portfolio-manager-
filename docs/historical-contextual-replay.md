# Replay histórico del portfolio y contexto E2

## Objetivo y alcance

Esta etapa vuelve a ejecutar el análisis técnico determinístico actual sobre las tenencias observadas de la reconstrucción validada entre 2026-07-01 y 2026-10-07. Luego adjunta la capa contextual E2 en modo `SHADOW_ONLY`. La señal, el score, las decisiones, las cantidades y el cash del flujo productivo no reciben datos de E2.

El resultado no es un backtest PIT estricto. La mayor parte de las velas TradingView históricas se persistió después de la fecha que describe. El replay evita barras con fecha posterior a cada corte, pero no puede demostrar que esas barras ya fueran conocidas por Quantia en ese momento. Toda fila queda marcada `RETROSPECTIVE_MARKET_HISTORY_NOT_PIT`.

## Arquitectura

El proceso usa tres capas separadas:

1. La reconstrucción observada conserva fecha, ticker, cantidad, precio, market value, peso, cash, snapshot y confidence sin completar huecos.
2. Un selector determinístico elige una sola serie diaria por instrumento. La prioridad es TradingView BYMA, Cocos, Yahoo BYMA e internal snapshot. No mezcla símbolos o proveedores divergentes.
3. El análisis base genera la señal técnica actual. E2 calcula estructura diaria/semanal, RVOL, volumen, breakout, fuerza relativa contra SPY e invalidadores. La señal y el score publicados como vista nueva son copias verificadas del baseline.

Los resultados se guardan en un namespace separado:

- `historical_portfolio_reconstructions`
- `historical_portfolio_reconstruction_positions`
- `historical_portfolio_reconstruction_events`
- `historical_contextual_replay_runs`
- `historical_contextual_replay_run_events`
- `historical_contextual_replay_results`

Todas las tablas son append-only mediante triggers que bloquean `UPDATE`, `DELETE` y `TRUNCATE`. El lifecycle exige `STARTED` antes de insertar resultados y un único terminal `COMPLETE`, `FAILED` o `ABORTED`.

## Definición de outcomes

Para una observación en la rueda T, el outcome H usa la apertura de la próxima rueda común con SPY como entrada y el cierre de la rueda H como salida. Si falta una rueda exacta, la fila falla de forma cerrada. Una discontinuidad no explicada superior a 30% también excluye el outcome.

- `BUY`: retorno del activo.
- `SELL`: retorno evitado, igual al negativo del retorno del activo.
- `HOLD`: retorno de mantener la exposición ya existente.

No se simulan nuevas cantidades ni una curva de cash. Los outcomes son brutos, sin comisiones, impuestos, dividendos o flujos externos. Las ventanas se superponen y no constituyen operaciones independientes.

## Corrida persistida

- Run: `6075b63f-1a94-4c40-a828-f47be487ef14`.
- Reconstrucción: `679932e9-6ce8-553b-b096-e6be28377316`.
- Código: `af4cfba22f992af1d7942ca04671616a9deb3776`.
- Lifecycle: `STARTED → COMPLETE`.
- Filas observadas/persistidas: 1.088/1.088.
- Eventos persistidos: 134.
- Filas analizadas completamente: 1.048.
- Observaciones únicas ticker/rueda elegibles: 762.
- Outcomes 5D evaluados: 701.
- Tickers preservados/analizados: 33/32.

Los 16 registros `USESPECIE`/`ARSCABLE` se preservan y se excluyen de métricas como etiquetas no negociables. Otras 24 filas no alcanzaron el lookback técnico mínimo. Las 286 observaciones que compartían ticker y última rueda por fines de semana u otros cortes se preservan, pero sólo una entra en las métricas.

YPFD conserva el cambio 5→50 del 2026-08-03 como `CORPORATE_ACTION_SPLIT` con confidence `MEDIUM`; no se cuenta como compra. La reducción 50→37 del día siguiente permanece como SELL confirmado en la reconstrucción fuente.

## Análisis base a cinco ruedas

| Cohorte | n | Media | Mediana | Positivos |
|---|---:|---:|---:|---:|
| BUY/SELL | 152 | +0,21% | +0,18% | 50,66% |
| HOLD como exposición | 549 | +0,09% | -0,38% | 47,36% |
| Total descriptivo | 701 | +0,12% | -0,19% | 48,07% |

Por señal, BUY tuvo media -0,04% en 129 casos; SELL tuvo retorno evitado medio +1,63% en 23 casos; HOLD tuvo media +0,09% en 549 casos. La muestra SELL es pequeña y los casos no son independientes.

## Contexto E2 shadow

| Severidad | n | Retorno medio del activo 5D | Positivos |
|---|---:|---:|---:|
| PASS | 280 | +0,15% | 46,79% |
| WARN | 174 | -1,02% | 41,95% |
| FAIL | 244 | +0,61% | 52,87% |
| UNKNOWN | 3 | -1,39% | 33,33% |

WARN agrupó retornos posteriores más débiles en esta muestra. FAIL no siguió una relación monotónica y rindió mejor que PASS, por lo que la etiqueta contextual todavía no demuestra una política económica utilizable. Como E2 no cambia decisiones, no existe un uplift “nuevo contra viejo” que pueda medirse honestamente en esta corrida.

## No regresión

- Scores iguales: sí.
- Señales iguales: sí.
- Cambios de señal persistidos: 0.
- Decisiones cambiadas: 0.
- Órdenes creadas: 0.
- Cantidades cambiadas: 0.
- Cash cambiado: no.
- `affects_analysis=false` y `affects_execution=false` en el run y en todas las filas completas.

## Dashboard

La API read-only `/api/historical-contextual-replay` devuelve el último run `COMPLETE` del owner configurado. La página `/replay-historico` muestra cobertura, cohortes por señal, cohortes contextuales, exposición estática 5D, filtros y linaje. La vista está separada de Performance para no mezclar replay retrospectivo con ejecución real.

Se desplegaron únicamente `monitor_api` y `frontend`. El archivo `.env` conservó su hash; scheduler y Telegram no se recrearon. No se enviaron mensajes ni órdenes.

## Validación

- 55 pruebas focalizadas de E1/E2/replay: PASS.
- Frontend TypeScript/Vite build: PASS.
- Frontend ESLint: PASS.
- Migración aplicada dos veces en TimescaleDB descartable: PASS.
- Protección append-only: UPDATE, DELETE y TRUNCATE bloqueados.
- Inserción de resultados después de estado terminal: bloqueada.
- API operativa: 200, run correcto, 1.088 resultados y contrato shadow correcto.
- Verificación visual de `/replay-historico`: PASS.

## Límites y siguiente uso

El dataset es suficiente para estudiar cobertura y cohortes descriptivas del análisis técnico/contextual actual sobre las tenencias históricas. No es suficiente para recalcular rentabilidad real de Quantia ni para ajustar thresholds. Antes de evaluar una política económica contextual se necesita evidencia prospectiva PIT, reglas predefinidas, costos completos y muestras no superpuestas o inferencia estadística que controle esa dependencia.

La evidencia estructurada está en `docs/evidence/historical-contextual-replay.json`. Los artefactos completos quedan fuera del repositorio, en `quantia_historical_validation/contextual_replay/`, para no duplicar 18 MB de datos privados dentro de Git.
