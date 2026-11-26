**Quantia — Technical Validation V1 · PR #29 · 2026-10-18**

La corrección del RSI es válida para una ventana completa con subas y ninguna baja: devuelve 100. Una ventana constante devuelve 50 por convención neutral explícita; el cociente 0/0 no determina ese valor matemáticamente. La falta de períodos sigue siendo desconocida. No se cambió el método de promedios simples por Wilder.

El efecto es acotado, pero existe: en **75.840 ventanas retrospectivas**, 44 scores cambian en 2 tickers y **14 señales técnicas pasan de BUY a HOLD, todas en FMCC**. En los últimos extremos disponibles de las series del backup y en las capturas contextuales analizadas no cambian scores ni señales. Estos ensayos aislados no reconstruyen decisiones finales del Radar ni prueban rentabilidad.

**Versiones, fuentes y límites de cobertura.** Se comparó `main` en `1687cdcce50895a9dfacb6dae04188fd8d8d5626` contra la corrección original del PR en `935f7b156fccad5d88a0c4968188c858f003a299`. La única función de motor diferente entre esas versiones es `_rsi`. La ampliación de esta auditoría agrega scripts, pruebas y documentación; conserva ese motor, thresholds, estrategias, optimizer y planner.

| Fuente congelada | Cobertura verificada | Limitación |
| --- | --- | --- |
| Backup Supabase `db_cluster-09-10-2026@17-43-09.backup.gz` | 252.085 filas originales de `market_candles`: TradingView 109.584, Yahoo 84.673, Cocos 33.401, internal_snapshot 24.427 | Se leyó como datos, sin restaurar ni ejecutar SQL. El nombre del archivo no fecha cada observación. No representa todo el histórico de Quantia. |
| Series separadas del backup | 873 combinaciones exactas de ticker, símbolo del proveedor, activo, moneda, mercado, intervalo y fuente; 402 tickers | Se toman hasta 260 velas finales por serie. 638 series / 357 tickers son evaluables; 183 tienen menos de 60 velas y 52 presentan OHLC inválido. |
| Captura privada `live-owner-20261016.json` | 105 snapshots contextuales, 12 tickers; observaciones ASSET recuperadas mediante sus enlaces explícitos | Un solo propietario. Snapshots `SHADOW_ONLY` preexistentes; no son entradas productivas idénticas. No se activó captura ni sistema shadow alguno. |

Los extremos del backup abarcan **2011-03-15 a 2026-10-02**: incluyen instrumentos antiguos y series detenidas, no un universo simultáneo actual. Las velas finales contextuales son del **2026-10-06 al 2026-10-09**, con cutoffs posteriores. Se conserva el material original y se aplica el normalizador existente `candles_to_frame`; sus identidades, selección por día y calidad quedan registradas. No se empalman proveedores para completar huecos.

Los hashes completos de entradas, código y resultados están en [el recibo de evidencia](evidence/technical-validation-v1.json). El backup, las observaciones y los resultados privados permanecen fuera de Git. No se consultaron ni modificaron bases operativas, broker o feeds durante este ensayo.

**Impacto medido con las mismas velas.** Cada celda de cambio expresa **ventanas / tickers distintos**, sin sumar propietarios ni series entre cohortes.

| Cohorte | Denominador evaluable | RSI | Score técnico | BUY/SELL/HOLD | Régimen técnico | Flag de sobrecompra |
| --- | --- | --- | --- | --- | --- | --- |
| Último extremo de cada serie del backup | 638 / 357 | 8 / 7 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| ASSET enlazados a capturas contextuales | 105 / 12 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Prefijos históricos de las 638 series elegibles | 75.840 / 357 | 859 / 8 | 44 / 2 | 14 / 1 | 0 / 0 | 2 / 2 |

En los extremos del backup, los 8 cambios son `None → 50`: AOCA en dos fuentes, BAYN, BRFS, DESP, FMCC, WBA y WBO. El RSI neutral aporta cero al score, igual que la omisión anterior.

En los prefijos de al menos 60 velas hay 815 cambios `None → 50` y **44 `None → 100`**. Estos últimos reducen el score **2,5 puntos**, en FMCC (43 ventanas, Yahoo) e IBB (1, TradingView). Las 14 transiciones BUY → HOLD corresponden a FMCC/YAHOO_BYMA entre **2026-01-20 y 2026-06-19**; ninguna transición involucra SELL. Ejemplos: 20 de enero, score 3,9 → 1,4; 3 de febrero, 5,0 → 2,5. Son ventanas superpuestas de series recuperadas, no 14 operaciones independientes ni un backtest point-in-time.

Se ejecutaron ambos motores de cálculo en cada extremo y en los 859 prefijos donde cambia el RSI. Los demás prefijos tienen RSI idéntico y, dado el cambio exclusivo de `_rsi`, conservan los mismos insumos deterministas de señal. Se comparan `score_raw`, `signal`, `technical_regime` y `overbought_momentum`; no se regeneran registros de decisiones guardadas.

Los 105 payloads productivos conservados contienen RSI finito, pero **0/105 digests productivos coinciden con el frame ASSET enlazado**. Por eso el resultado contextual no prueba el resultado de la decisión productiva. La clasificación y el ranking final Radar quedan **NOT_EVALUABLE_MISSING_FULL_FROZEN_CONTEXT**: faltan entradas exactas de mercado, contexto macro/sentimiento, cartera, caja y universo de la misma ejecución. No se sustituyen por cero ni por datos actuales.

**Validación matemática y acciones.** Una referencia escalar independiente usa sumas, recurrencias y raíces, sin `rolling`, `ewm` ni helpers del motor. Se contrastaron 17 campos × 743 extremos = **12.631 comprobaciones**, incluyendo coincidencia de desconocidos; ninguna supera `rel_tol=1e-10, abs_tol=1e-8`. El mayor error absoluto es `1,077e-9` en bandas. Hay 437 comparaciones numéricas de SMA200 y 469 de volumen/OBV; los restantes faltantes no cuentan como valores numéricos validados.

| Indicador | Estado | Impacto | Acción |
| --- | --- | --- | --- |
| RSI | VERIFIED para aritmética del motor; PARTIAL entre módulos | Corrige pérdidas cero. El screener usa retornos porcentuales y devuelve 100 ante precio constante. | Mantener el arreglo; definir por separado una política común de método, faltantes y constante. |
| MACD | VERIFIED con semilla declarada | EMA12 − EMA26; señal EMA9. La semilla inicial explica diferencias con otros proveedores. | Conservar método y explicitar warmup; no corregir por divergencias de semilla. |
| SMA/EMA | VERIFIED en cálculo; defecto en explicación/señal de igualdad | SMA simple; EMA con primer cierre y `alpha=2/(span+1)`. El consumidor describe igualdad como desigualdad bajista. | Documentar TV1-04; revisar la política de igualdad en otro cambio. |
| ATR | VERIFIED en motor; defecto en nombre/cálculo del screener | Motor: media simple del true range. Screener: High−Low sin gaps, usado en asimetría. | TV1-02: renombrar proxy o implementar true range tras aprobar alcance y medir efecto. |
| Bollinger | VERIFIED para desviación muestral | SMA20 ± 2σ, `ddof=1`. Bandas 2,598% más anchas que con `ddof=0`. | Documentar convención; cambiarla sería metodología, no reparación automática. |
| Volumen | VERIFIED dentro de cobertura | Ratio contra últimas 20 barras incluyendo la actual; volumen inválido permanece desconocido. OBV requiere historia válida. | Conservar faltantes; distinguir este ratio del cálculo contra 20 barras previas. |
| Ranking Radar | VERIFIED fórmula aislada; impacto real NOT_EVALUABLE | Prioridad, score, convicción, asimetría y edge intervienen. RSI no determina por sí solo el orden final. | Obtener una captura completa e inmutable antes del replay final. |

ATR con media simple y ATR suavizado de Wilder son metodologías distintas. El MACD usa `adjust=False`, con EMA iniciada en el primer valor; no se inicializa con SMA. OBV comienza en cero y no acumula volumen si el cierre no varía. Las pruebas cubren series ascendentes, descendentes, constantes, gaps, warmup, OHLC imposible, fechas duplicadas/desordenadas y volumen nulo, cero, negativo o infinito.

**RSI entre módulos y hallazgos pendientes.** `technical.py` promedia diferencias de precio absolutas durante 14 cambios: necesita 15 cierres para el helper y 60 barras para el cálculo público completo. `opportunity_screener.py` promedia los últimos 14 retornos porcentuales tras `pct_change().dropna()`; con menos de 40 filas rechaza el screening pero conserva un **placeholder 50**, que no debe interpretarse como medición. Los 743 extremos evaluables difieren entre ambos módulos; eso no equivale a 743 errores ni al efecto del arreglo.

| ID / prioridad | Hallazgo reproducido | Naturaleza y corrección recomendada |
| --- | --- | --- |
| TV1-03 / alta | Screener emite RSI finito con el último cierre ausente. En pandas 2.3.3, `pct_change` completa faltantes por defecto; `dropna` puede desplazar la ventana. | Calidad matemática del insumo. Validar OHLC, no completar precios, requerir 14 cambios consecutivos y representar dato inválido como desconocido. |
| TV1-02 / alta | Su `atr_pct` omite gaps. Fixture: rango diario 2, true range del gap 16; ATR14 correcto 3 frente al proxy 2, antes de dividir por precio. | Defecto frente al nombre ATR. Puede afectar stop, RR y ranking; los pisos de stop pueden amortiguarlo. Efecto real no cuantificado. |
| TV1-04 / media | EMA12=EMA26 y SMA20=SMA50 entran en ramas bajistas y las razones dicen `<`. Además se llama cruce a una relación estática. | Explicación falsa; el tratamiento de igualdad del score requiere una decisión metodológica. Separar igualdad, relación y cruce temporal antes de cambiar pesos. |
| TV1-01 / media | Precio constante produce RSI100 en screener frente a 50 en motor. | Inconsistencia de convención, no una solución matemática única de 0/0. Declarar política y versionarla antes de unificar. |

Estos cuatro comportamientos quedan expuestos mediante **`xfail(strict=True)`**, como pendientes explícitos, no como correcciones aplicadas. La diferencia entre RSI sobre porcentajes y sobre precios también debe nombrarse; unificarla modificaría resultados normales, por lo que excede este arreglo de frontera.

**Pruebas y CI.** Las pruebas originales del PR y Radar pasan: **27 passed**. El conjunto ampliado técnico/contextual/Radar da **122 passed, 4 xfailed**; las cinco suites estructurales dan **37 passed**. Los 4 xfails son los hallazgos anteriores. Los warnings de raíz de covarianza ya existían; el warning de `pct_change` respalda TV1-03. Localmente se usaron Python 3.13.7 y pandas 2.3.3; las dependencias de optimizer necesarias para ejecutar sus pruebas se aislaron bajo `outputs`, sin cambiar el entorno operativo.

El [CI original de `935f7b1`](https://github.com/Francoparodi01/Portfolio-manager-/actions/runs/39171341381) fallaba: candidato 7 fallos / 946 pases y main 6 / 938, con 27 skips en ambos. La única regresión era una prueba contextual que esperaba RSI desconocido en una serie completamente ascendente. Se actualizó esa expectativa a 100, conservando pruebas separadas que exigen desconocido cuando falta lookback.

El [CI de `63a8c872ba294500be4d607f8628c37e7f528449`](https://github.com/Francoparodi01/Portfolio-manager-/actions/runs/39187543576) terminó **SUCCESS**: 37 pruebas estructurales pasan; la suite completa del candidato registra **1.006 passed, 6 failed, 27 skipped, 4 xfailed**, frente a **938 passed, 6 failed, 27 skipped** en el mismo main `1687cdc`. El control compara identidades de fallos y confirma **0 nuevos**. Esto acredita ausencia de regresiones nuevas, no una suite global sin fallos. Los seis fallos previos quedan fuera del alcance RSI. La incorporación posterior de este informe y del recibo de CI solo cambia documentación; no altera el código validado.

**Reproducción.** Con los objetos Git de ambas versiones, las dependencias del repositorio y los dos archivos privados identificados por hash:

```powershell
python scripts/audit_technical_validation.py extract --backup "C:/Users/Franco/Downloads/db_cluster-09-10-2026@17-43-09.backup.gz" --output outputs/technical-validation/frozen-market-v1
python scripts/audit_technical_validation.py compare --bundle outputs/technical-validation/frozen-market-v1 --context-capture "C:/Users/Franco/Documents/Codex/worktrees/quantia-measurement-baseline-v1/outputs/measurement-baseline-v1/live-owner-20261016.json" --output outputs/technical-validation/comparison-v2
python -m pytest tests/test_technical_validation_v1.py tests/test_technical_measurement_contract.py tests/test_contextual_e1.py tests/test_radar_discovery.py -q --override-ini addopts=''
```

En una repetición, usar un directorio de salida nuevo: el script rechaza sobrescribir resultados. `comparison-v2` y `replay-v2` produjeron tres archivos idénticos byte a byte: resumen, comparación por serie y sensibilidad histórica. La referencia está en `scripts/technical_reference.py`. Sin las entradas privadas, las pruebas sintéticas siguen siendo ejecutables, pero no se puede verificar nuevamente la medición histórica usando solo este recibo público.

**Siguiente paso.** Mantener PR #29 en draft, sin merge ni despliegue. Resolver cada hallazgo adicional en alcance separado, empezando por los faltantes y el proxy ATR. Antes de medir impacto sobre decisiones finales, recuperar un paquete completo por ejecución con versiones, fuentes, owner y digests exactos; conservar la decisión histórica y producir únicamente una comparación externa. La corrección matemática y los cambios de señal observados no demuestran mejora de PnL.

Referencias metodológicas primarias: [RSI y cambios de precio](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/RSI), [true range y ATR de Wilder](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr), [EMA y `adjust=False`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.ewm.html), [reglas de John Bollinger](https://www.bollingerbands.com/bollinger-band-rules). Las convenciones concretas de Quantia se verifican contra su código y las referencias independientes incluidas.
