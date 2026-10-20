# Radar y /top10: captura de mercado del 19/10/2026

Se recalculó y publicó una captura con el motor corregido del PR #31. API y
Telegram leen las mismas filas, en el mismo orden, sin volver a calcular scores.
Los diez resultados tienen estado **ESPERAR / PARTIAL**: son candidatos para
revisión, no compras habilitadas con evidencia completa.

- Captura: **19/10/2026 18:30:44 ART** (`21:30:44.426294 UTC`).
- `run_id`: `626bb30c-6e67-5929-ae9f-9e7331e297f3`.
- SHA-256 de entradas: `2cd92452bae07abfe3a7da908d6ee0da188e0254195f0b8ebde14dd6d7b95830`.
- SHA-256 del motor: `4f4591409351bebf5710bc1cc1a212e82070204d660a4b14cbb903f3bb35adf3`.
- `main` revisado: `1687cdcce50895a9dfacb6dae04188fd8d8d5626`.
- [Recibo público con procedencia por instrumento](evidence/radar-market-20261019.json).

## Fuentes y cobertura

| Fuente | Resultado | Límite |
| --- | --- | --- |
| Collector existente `fetch_tv_candles`, TradingView/BYMA | 373 de 399 instrumentos; 88.637 velas OHLCV; moneda y mercado contrastados con `symbol_resolved` | Feed demorado; para el Top 10 declara 1.200 segundos. No es acceso directo a BYMA Market Data. |
| PostgreSQL local, lecturas por propietario | Universo Cocos, snapshot de cartera del 19/10 18:02:11 ART, contexto de riesgo y sentiment | No se toma el SQL como todo el histórico ni se reutilizan scores de `decision_log`. |
| Collector `fetch_macro` | Nueve series globales con última fecha de barra 19/10, CCL, MEP y riesgo país | Reservas BCRA: HTTP 404; se conserva `None`. Hora de barra diaria no equivale a último negocio. |
| [Programas Comafi](https://www.comafi.com.ar/custodiaglobal/programas.aspx) | Lista fechada 30/09/2026, recuperada el 19/10. Los diez ratios coinciden por ticker **e ISIN** | Es el ratio publicado en esa lista; no acredita la historia de cambios de ratio. No se aplica retroactivamente a las velas. |
| [DolarAPI CCL](https://dolarapi.com/v1/dolares/contadoconliqui) | Compra 1.623,60; venta 1.625,60 ARS/USD; fuente actualizada 19/10 18:00 ART, consulta 18:30:40 ART | Referencia de venta CCL, **no** CCL implícito de cada CEDEAR. No se convierte el precio ARS a un supuesto precio USD. |
| [BYMA, comunicado 18782](https://www.byma.com.ar/comunicados/comunicado-18782-horarios) | Regular hasta 17:00 y precio de cierre 17:05–17:15 ART | El comunicado 19024 publicado en la página de horarios entra en vigencia el **02/11/2026**; no se aplicó anticipadamente. |

Se congelaron las respuestas originales, metadatos del símbolo, horarios de
consulta, errores y hashes. La serialización inicial de macro falló por columnas
MultiIndex; se conservó ese intento y se volvió a ejecutar el collector con
serialización `split`, en otro directorio. El reintento de símbolos recuperó SHOP
y SPCX; este último tiene 88 velas. Ningún error fue convertido a cero.

Los 26 símbolos sin velas, luego del reintento, son **NO VERIFICADOS**:
AOCA, AUY, AXIA, BAS, BK, BRFS, CAJ, CE, DESP, DTEA, EA, ENI, EOAN, FORD,
HHPD, INICIO, IWDA, LINDE, LKOD, NATERA, OGZD, REALTY, SHPW, TEFO, VISA y WBA.
No se sustituyeron por símbolos parecidos ni se infirió que fueran bajas del mercado.

Otros 20 tienen menos de 60 velas. Se conservaron, sin completar el histórico:
BAYN, BBAX, BBCA, BSN, CORN, FMCC, GEV, GSG, IBKR, KLAC, LIN, MS, NTRA,
PLD, SHW, SKHY, SOYB, TLN, WDC y WELL. SPY y QQQ son controles, no candidatos.
Quedan 351 señales técnicas calculadas: 36 BUY, 40 SELL y 275 HOLD; 132 pasan
el screener y 57 son elegibles según el criterio existente.

## Precio reportado y cierre confirmado

La rueda estaba cerrada al recoger esta captura. Las diez velas corresponden al
19/10 y comienzan a las 10:30 ART. Ese timestamp **no es** la hora del último
negocio; el proveedor no la entregó. Se muestran por separado consulta,
demora, fecha de vela y calidad.

Sus precios se etiquetan **«Cierre reportado; sin corroborar»**. Cocos tenía
observaciones alrededor de las 17:02–17:03, anteriores al fin de negociación a
precio de cierre; además hay diferencias de precio, por ejemplo TXN 89.875 frente
a 89.650 ARS de TradingView. No se elevó esa comparación a cierre confirmado.
`confirmed_close` permanece nulo. El cierre previo reportado del 16/10 se conserva
por separado, sin presentarlo como cierre oficialmente certificado.

La política del adaptador distingue intradiario, cierre reportado y cierre
corroborado por dos fuentes. Pasar la hora de cierre no basta para confirmar un
precio. La unidad absoluta de volumen tampoco fue declarada en la respuesta y
queda desconocida; el volumen relativo usa la misma serie del mismo proveedor.

## Top 10 congelado

Precios en ARS, rueda 19/10; fuente TradingView/BYMA. Todos: **ESPERAR / PARTIAL**.
Los saltos de posición conservan el ranking del universo antes de excluir cartera
y controles. El score no es una probabilidad ni un retorno esperado.

| Posición original | Ticker | Precio ARS | Score técnico | Score ranking | Volumen relativo | Ratio publicado |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 1 | TXN | 89.650 | 4,05 | 40,1464 | 1,3722 | 5:1 |
| 3 | MRVL | 27.440 | 2,65 | 40,0870 | 0,5073 | 14:1 |
| 5 | ITUB | 15.380 | 3,45 | 30,4654 | 0,3158 | 1:1 ADR |
| 6 | SHOP | 2.500 | 4,00 | 30,4505 | 0,1875 | 107:1 |
| 7 | META | 47.860 | 3,30 | 30,4351 | 0,6264 | 24:1 |
| 8 | CAAP | 161.450 | 6,70 | 30,4325 | 0,2824 | 1:4 |
| 9 | MA | 28.800 | 6,25 | 30,4226 | 1,8010 | 33:1 |
| 10 | VST | 9.370 | 5,75 | 30,3698 | 0,1637 | 26:1 |
| 11 | STNE | 6.210 | 1,60 | 30,1724 | 0,8702 | 3:1 |
| 13 | PYPL | 11.410 | 2,55 | 30,1450 | 0,2645 | 8:1 |

Volumen relativo = volumen actual / SMA20 inclusiva, metodología existente.
El recibo incluye tendencia, fecha/hora de consulta, fuente y calidad por fila.
Las reglas originales y sus motivos se conservan; la presentación de evidencia
incompleta retiene ESPERAR, incluso si el estado guardado indicaba swap o vigilancia.

## Qué cambió y qué se verificó

`collect_radar_market.py` usa los collectors existentes y conexión PostgreSQL
READ ONLY, sin `PortfolioDatabase.connect()` porque inicializa esquemas. Las
velas nuevas no se mezclan con OHLC sintético construido desde quotes.
`build_radar_market_snapshot.py` usa `analyze_portfolio_from_frames`, riesgo,
`blend_scores`, `run_opportunity_analysis` y el ledger de PR #25.
Los scores de cartera se vuelven a calcular con esos módulos y las mismas
entradas congeladas; faltar un score de cartera detiene la publicación.
Se conserva `min_score=0.10`, `min_rr=0.0` del Radar programado.

No se cargaron scores shadow antiguos. Esto y los metadatos PIT incompletos
impiden afirmar equivalencia con una ejecución productiva histórica completa:
**el impacto real sobre el ranking productivo anterior NO ES VERIFICABLE**.
Sí es reproducible este ranking nuevo bajo las entradas y límites declarados.
La consulta de eventos manuales activos devolvió cero. No se afirma mejora de PnL.

Se añadió **un run y 399 observaciones** en las tablas existentes, mediante una
transacción atómica y sin ejecutar DDL. No se actualizó `decision_log`, el histórico
de precios, portfolios ni decisiones anteriores. Precios, volumen e indicadores
nuevos están en esta captura y sus archivos originales, no sobrescritos en velas
operativas previas. No se ejecutaron optimizer, planner, órdenes o migraciones.

API y bot priorizan el último run con `parameters.top10_publication=v1`, por
propietario. Una captura automática del motor antiguo no desplaza esta publicación.
Cada respuesta muestra ID y hash; el orden viene de las filas guardadas. Las
antigüedades se evalúan a la hora de lectura y pueden variar entre consultas.

La actualización local se limitó a lectores API/bot y frontend. Se verificaron
HTTP 200, registro de `/top10`, inicio del poller y permanencia de `/fundamentals`
y `/meta`. Se invocó el handler desplegado con autorización y PostgreSQL reales,
interceptando únicamente el envío: produjo once tarjetas idénticas al render del
payload de la API. **No se enviaron mensajes de prueba al usuario.**

163 pruebas focalizadas pasan, incluyendo futuros/duplicados, identidad, demora,
ratios, CCL, publicación sin DDL, owner y Telegram. TypeScript/Vite compila.
Playwright validó la aplicación servida con el payload autenticado capturado, en
1440×1050 y 390×844, sin desborde móvil y conservando el Top 10 ante fallo simulado
del panel histórico. La autenticación y consulta HTTP reales se verificaron aparte.
Dos ejecuciones del recálculo dieron el mismo artefacto byte a byte antes de los
ajustes de etiquetas de presentación; los precios, scores, orden, ID e inputs no
cambiaron. Hay fallos previos en pruebas de Radar compacto/watchlist, fuera de
estos cambios; el CI compara la rama contra main para identificar regresiones.

## Reproducción y continuidad

Los outputs privados están en `outputs/radar-live-20261019/`, ignorados por Git.
`collection-close-v4` contiene el bundle final; `published` contiene el recibo de
inserción. No compartir `context-private.json` ni las observaciones de cartera.
Los directorios de salida nuevos deben estar vacíos/no existir.

```powershell
python scripts/collect_radar_market.py --env-file <env-verificado> --host localhost --output outputs/nueva-captura
python scripts/collect_radar_market.py --env-file <env-verificado> --host localhost --retry-inputs outputs/nueva-captura --output outputs/nuevo-reintento
python scripts/build_radar_market_snapshot.py --inputs outputs/nuevo-reintento --ratios <lista-Comafi.xlsx> --output outputs/nuevo-replay
# Sólo con la actualización de captura autorizada; añade filas, no migra:
python scripts/build_radar_market_snapshot.py --inputs outputs/nuevo-reintento --ratios <lista-Comafi.xlsx> --output outputs/nueva-publicacion --publish --env-file <env-verificado> --host localhost
```

Prioridades siguientes: recuperar los 26 instrumentos sin inferir aliases;
contrastar cierres posteriores a 17:15 con fuente independiente; obtener unidad,
calendario y disponibilidad de cada barra; completar historial de los 20 cortos;
versionar cambios de ratio y obtener precios subyacentes sincronizados antes de
calcular CCL implícito. Completar el contexto faltante antes de usar el ranking
como señal operativa.

El scheduler y sus imágenes de cálculo no se reemplazaron ni se habilitó captura
periódica nueva. Los lectores locales se actualizaron en los contenedores existentes,
con backups en `runtime-before/` y parches en `runtime-staged/`; sobreviven al restart
pero deben incorporarse a imágenes reproducibles antes de recrearlos. La captura
permanece en PostgreSQL. El PR #31 sigue **Draft**, sin merge; PR #25 y #29 intactos.
