**Quantia — Correcciones técnicas y Top 10 compartido · 2026-10-19**

Se corrigen los cuatro hallazgos de [Technical Validation V1](TECHNICAL_VALIDATION_V1.md). Las pruebas independientes verifican las fórmulas y los casos límite. **El impacto real del ranking no es verificable**: no están disponibles las entradas productivas exactas de las ejecuciones históricas. El Top 10 entregado conserva una captura real anterior a las correcciones; no es un ranking recalculado ni una cotización actual.

**Versiones y alcance.** Main revisado: `1687cdcce50895a9dfacb6dae04188fd8d8d5626`. Base del ensayo: PR #29, `810836f5b80a0c28b0392a4d8776c039292adb5e`. Se reutilizan en esta rama los commits del PR #25 hasta `661236a0bdbe1e4999221ecd498ca5d2efd4e71a`, sin modificar esos PR. La propuesta contra main incluye ambas dependencias, todavía sin integrar. No hay merge, despliegue, migraciones, modificación de datos operativos, decisiones guardadas, optimizer, planner ni activación de capturas/shadow. Las consultas productivas de esta tarea son SELECT en una transacción `REPEATABLE READ READ ONLY`.

| Corrección | Resultado y decisión metodológica | Impacto medido |
| --- | --- | --- |
| Precios faltantes o inválidos | El screener utiliza el contrato OHLC del motor: rechaza faltantes, infinitos, precios no positivos, OHLC imposible, fechas duplicadas/desordenadas y barras posteriores al cutoff. No completa precios ni reemplaza RSI/ATR faltantes por valores neutrales. | 37/873 extremos del backup pasan de aceptar a rechazar el screening aislado; ninguno cambia en sentido contrario. |
| ATR que omite gaps | Media simple de `max(H−L, abs(H−cierre previo), abs(L−cierre previo))`, compartida con `technical.py`. Se conserva media simple, factor 1,5 y límites de stop 5%–18%; no se introduce Wilder. | 71.764/75.840 ventanas cambian ATR; 31.603 cambian el stop luego de aplicar los límites existentes. |
| RSI inconsistente | Ambos módulos usan cambios absolutos de precio, media simple de 14 cambios, subas sin bajas=100, constante=50 por convención. El screener conserva mínimo 40 barras; el motor completo, 60. Insuficiencia permanece desconocida. | Cambiar porcentajes por diferencias de precio es una unificación metodológica explícita, además del arreglo de frontera: 75.790 ventanas cambian RSI del screener; 2.964 cambian su término RSI de momentum. |
| Medias iguales | Igualdad EMA12/EMA26, SMA20/SMA50 y precio/SMA200 aporta cero. Las desigualdades conservan sus pesos. Se describe la relación de medias sin llamarla cruce temporal. | 788 ventanas / 11 tickers cambian score técnico y fuerza; dos señales SELL→HOLD. Es una política neutral de igualdad, medida antes de proponer integración. |

No cambian thresholds, pesos ni fórmulas de ranking. La igualdad deja de recibir la penalización de la rama bajista: ése es el cambio intencional de comportamiento. MACD, SMA/EMA, Bollinger y volumen conservan las convenciones verificadas en V1 y sus pruebas independientes. Ninguno de estos resultados prueba mejora de rentabilidad.

**Ensayo con entradas congeladas.** Son exactamente las fuentes de V1: 252.085 filas del backup, separadas en 873 series / 402 tickers por fuente e identidad, y 105 frames ASSET enlazados a capturas contextuales de un único propietario. Se mantienen fuentes, fechas, precios y versiones. El cálculo técnico admite 638 series / 357 tickers del backup y los 105 frames / 12 tickers contextuales. Los extremos del backup abarcan 2011–2026-10-02; no forman un universo contemporáneo.

| Cohorte | Extremos técnicos evaluables | RSI motor | Score / fuerza | BUY/SELL/HOLD | Régimen |
| --- | --- | --- | --- | --- | --- |
| Último extremo del backup | 638 / 357 tickers | 0 | 7 / 6 tickers | 0 | 0 |
| ASSET contextual enlazado | 105 / 12 tickers | 0 | 0 | 0 | 0 |
| Prefijos históricos, mínimo 60 barras | 75.840 / 357 tickers | 0 | 788 / 11 tickers | 2 / 2 tickers | 0 |

El RSI del motor no cambia frente al PR #29; cambia el del screener. Las dos transiciones técnicas son **PEP, 2026-08-06 13:30 UTC**, y **PG, 2026-06-10 13:30 UTC**, ambas `TRADINGVIEW_BYMA`, score −3,8→−2,6 por igualdad SMA20=SMA50. Son ventanas retrospectivas superpuestas, no operaciones reales ni decisiones históricas reescritas.

Entre los **696 extremos del backup con RSI/ATR finitos en ambas versiones**, cambian 696 RSI, 676 ATR y 188 stops; en los 105 contextuales, 105 RSI, 105 ATR y 55 stops. Sobre los 873 extremos totales, incluyendo reemplazo de placeholders por desconocidos, el recibo registra 873 cambios RSI y 853 ATR: no confundir esas cifras con pares numéricos válidos. El screening aislado omite benchmarks en ambos brazos; no representa elegibilidad productiva completa. El término RSI de momentum tampoco equivale al score final, que incluye otros componentes y clipping.

Los 105 frames contextuales siguen sin coincidir con los digests productivos (0/105, hallazgo V1). Faltan el universo simultáneo y el contexto exacto de macro, sentimiento, cartera, caja y versiones. Por eso se informa `NOT_VERIFIABLE_MISSING_EXACT_PRODUCTIVE_INPUTS` para el ranking final y no se publica un Top 10 contrafactual con contexto inventado. Los cálculos técnicos y componentes reproducidos sí son verificables dentro de esas cohortes.

**Top 10 real recuperado.** PostgreSQL local, misma cuenta configurada en el monitor; fuente `radar_discovery_snapshots`, última ejecución COMPLETE del **2026-10-16 19:50:08 UTC**. Consulta del **2026-10-19 02:56:02 UTC**; universo 291, elegibles 19. Versión conservada: `radar-v2+technical-shadow-v2+technical-buy-shadow-v3:b9ac3bfaf000607f`. No se refrescó ni persistió ningún run.

| Posición original | Ticker | Último precio registrado* | Tendencia guardada | Score ranking | Motivo guardado | Riesgo principal guardado, resumido |
| --- | --- | ---: | --- | ---: | --- | --- |
| 1 | BBD | 6.440 | STRONG_UPTREND | 30,1934 | VIGILANCIA_A | Convicción bajo umbral; sin caja ejecutable |
| 2 | ITUB | 15.320 | TRANSITIONAL | 30,1655 | VIGILANCIA_A | Convicción bajo umbral; sin caja ejecutable |
| 3 | META | 47.500 | STRONG_UPTREND | 30,1534 | VIGILANCIA_A | Edge marginal frente a cartera; sin caja ejecutable |
| 4 | STNE | 6.080 | TRANSITIONAL | 30,0532 | VIGILANCIA_A | Score bajo umbral; no supera a la cartera |
| 5 | XP | 11.800 | STRONG_UPTREND | 30,0483 | VIGILANCIA_A | Edge marginal; R/R insuficiente en la regla guardada |
| 6 | SHOP | 2.545 | STRONG_UPTREND | 30,0441 | VIGILANCIA_A | R/R 0,9 insuficiente; no supera a la cartera |
| 7 | NOW | 1.330 | TRANSITIONAL | 30,0360 | VIGILANCIA_A | R/R 0,8 insuficiente; no supera a la cartera |
| 10 | VST | 9.485 | TRANSITIONAL | 10,0293 | VIGILANCIA_C | R/R insuficiente y score bajo umbral |
| 12 | XOM | 28.940 | TRANSITIONAL | 0,1128 | NO_OPERABLE | Cerca de máximos; objetivo demasiado cercano |
| 16 | CVX | 22.750 | RANGE | 0,0356 | NO_OPERABLE | Cerca de máximos; objetivo demasiado cercano |

\* Precios de 2026-10-16 entre 19:40:26 y 19:41:44 UTC, aproximadamente 55,2–55,3 horas de antigüedad al consultar; **moneda NO VERIFICADA en esta captura**. No se deduce ARS por ticker ni por proveedor. Todos tienen `INTRADAY_PROVISIONAL`, score técnico **no conservado** y volumen relativo guardado **0**, con definición `setup_features.volume_ratio_latest_20`, sin calidad suficiente para interpretarlo como señal. Fuentes por fila: `TRADINGVIEW_BYMA` y `market_prices_intraday`; algunas incluyen `COCOS`. Se muestran completas en la salida. **Los diez tienen calidad PARTIAL y estado de presentación ESPERAR**. No se convierten scores shadow en scores técnicos. Los saltos de posición se conservan porque se excluyen cartera, controles y candidatos no elegibles sin renumerar el ranking original.

**Telegram y dashboard.** `/top10` y `GET /api/opportunities/top10` llaman al mismo lector `fetch_top_opportunities` y al mismo presentador. Ninguno recalcula scores ni orden. Ambos muestran precio/fecha/antigüedad, moneda, técnico, tendencia, volumen, fuente, calidad, motivo, riesgo y estado. Las lecturas sucesivas pueden ver una captura nueva si el sistema genera otra entre consultas; la igualdad se verifica para el mismo run, identificado por fecha y versión. El dashboard mantiene el panel disponible aunque falle el historial de Radar.

El mapeo conserva las reglas guardadas: COMPRABLE_AHORA→COMPRAR; COMPRA_HABILITADA, SWAP_CANDIDATO y VIGILANCIA_A/B/C→VIGILAR; otros→ESPERAR. Cualquier faltante crítico, evidencia no VALID, precio no canónico o vencimiento retiene ESPERAR. Es una restricción de presentación, no una nueva señal operativa. Se conserva el criterio de vencimiento de PR #25: siete días calendario, sin afirmar que un precio menor a siete días sea una cotización en vivo. La etiqueta COMPRAR, cuando sea posible, describe el estado en la captura y no autoriza órdenes.

Las capturas futuras conservan moneda, score técnico y calidad existentes en `metadata.top10_evidence`, sin nueva tabla ni backfill. La consulta del bot requiere el mismo owner que el monitor, incluso si hay otros chats autorizados; falla explícitamente si falta la fuente. El comando se registra en el bot base que reutiliza `telegram_bot_meta.py`. Las pruebas usan transporte simulado: **no se enviaron mensajes reales ni se desplegó el bot**.

**Validación y reproducción.** 185 pruebas focalizadas pasan; suite local completa: **1.076 passed, 5 failed, 28 skipped**. Los cinco fallos son conocidos de main, vinculados a watchlist y formato Telegram, ajenos a estas correcciones. Se actualizó la prueba del menú para incluir `/top10` y los comandos agente/analytics ya existentes; ésta era el sexto fallo previo de main. Build TypeScript/Vite correcto. Revisión Playwright de escritorio y móvil con el payload real interceptado localmente, sin conexión del navegador a la API operativa. CI del nuevo PR se consulta por SHA antes de cierre.

El [recibo público](evidence/radar-corrections-top10-v1.json) identifica hashes de entradas/código/resultados; no publica propietario ni credenciales. Dos ejecuciones completas (`impact-v3` y `replay-v1`) produjeron comparación, sensibilidad y resumen idénticos byte a byte. El backup, las comparaciones por serie y el payload completo permanecen fuera de Git. El script rechaza sobrescribir salidas. Para ejecutar desde la raíz, usando los archivos privados identificados en el recibo:

```powershell
python scripts/audit_radar_corrections.py --bundle <frozen-market-v1> --context-capture <live-owner-20261016.json> --output outputs/radar-top10/new-comparison
python scripts/capture_top10_readonly.py --env-file <env-del-monitor> --host 127.0.0.1 --port 5432 --output outputs/radar-top10/new-capture
python -m pytest tests/test_radar_indicator_fixes.py tests/test_top10_telegram.py tests/test_opportunities_monitor.py tests/test_technical_validation_v1.py tests/test_technical_measurement_contract.py tests/test_contextual_e1.py tests/test_radar_discovery.py -q --override-ini addopts=''
cd frontend
npm ci
npm run build
```

El override de host se usa sólo después de verificar que el puerto corresponde al PostgreSQL del monitor. Se comprobó el contenedor `cocos_db`/alias `db`, puerto local 5432; no se consultaron la otra instancia local ni el laboratorio Agentic. Repetir la consulta puede obtener un run posterior; la reproducción histórica debe usar el payload congelado y su `evaluated_at` original.

**Pendiente antes de validar oportunidades actuales.** Recuperar o generar por el flujo ya autorizado una captura completa con identidades, moneda, barras cerradas, volumen válido, score técnico y contexto exacto. Repetir el ranking con esa misma entrada y ambos motores, preservando la captura original. Luego validar el flujo desplegado `/top10`–dashboard sobre el mismo run. La corrección de fórmulas está probada; el ranking corregido actual y la mejora económica permanecen sin verificar.
