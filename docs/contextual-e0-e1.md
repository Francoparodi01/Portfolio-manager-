# Quantia E0 / E1 — contratos y autoridad en shadow

Fecha: 2026-10-07. Base: PDF `quantia-preproyecto-analisis-contextual-v1.pdf`, 20 páginas,
leído completo; maqueta de página 12 inspeccionada. Repositorio
`Francoparodi01/Portfolio-manager-`. Baseline congelado:
`f3c6bbeacda4f96226f98c40fb50861068b7abbe`, coincidente con origin/main al iniciar.
Rama: `feature/contextual-e1-contracts`. Checkout habitual limpio y preservado;
implementación en worktree independiente. Sin deploy, merge, órdenes, Telegram,
cambios de configuración, migraciones ni escrituras a la DB.

## Estado de las puertas

- E0: relevamiento, reproducción sintética, inventario y reconstrucción de propuesta
  guardada realizados. **G0 abierto para replay completo**: no existe captura de las
  velas exactas usadas ni semántica de disponibilidad/cierre; no inventar esa evidencia.
- E1: contratos, correcciones y evaluación de autoridad implementados, con pruebas
  aisladas. **G1 no certificado en una corrida productiva nueva**: quedan la captura
  de procedencia completa, la integración PostgreSQL de escritura y el smoke real.
- E2 no implementado por esta entrega. No se declara finalizado el nuevo análisis,
  validada una mejora económica ni activada una política nueva.

## Corrida real reconstruida, sin escritura

`audit_contextual_e0.py` usa conexión directa asyncpg con
`default_transaction_read_only=on`, timeout por sentencia de 8 s y transacción
`repeatable_read, readonly=True`. No inicia `PortfolioDatabase.connect`, migraciones,
scheduler, modelos ni notificador. Owner positivo obtenido del entorno existente,
predicado exacto en planes, decisiones y capturas; órdenes mediante join al plan
del owner. Límite: un plan, cuatro capturas, cien decisiones/órdenes y hasta 520
velas diarias por ticker, treinta tickers como máximo. Las velas son datos públicos
de mercado, no se atribuyen a un owner por inferencia.

Evidencia local privada excluida de git: `output/contextual-e1/recorded-run.json`.
SHA256 de extracción: `de00204c185eb821216182b1fbf8ae9c00eba5f39f936b0c1999c1c794b66d5c`.

| Campo | Observado |
|---|---|
| Plan | `8444cd7f-f2ef-44f5-b834-5b805bc234e1` |
| Run | `18457bc9-b17f-4599-9cd2-d444cb351d5b` |
| Corte del plan | 2026-10-06 16:47:10.419359 UTC |
| Owner | Explícito; coincide entre plan, captura y cartera; ID en artefacto privado |
| Snapshot | `2fe865d5-5761-41e7-a4f5-872e5dab5243`, 16:47:02.778020 UTC |
| Cartera | 11 posiciones, total registrado ARS 3.544.660; cash del snapshot ARS 9.797,277164 |
| Plan financiable | Vende 43 IREN por ARS 239.510; compra 8 MSFT por ARS 229.120 |
| Cash plan | Antes ARS 9.797; después ARS 16.673; costos ARS 1.796 + 1.718 |
| Otros intentos | GDX y VIST WATCH por nominal mínimo/funding; siete HOLD |
| Captura | `decision-lab-plan-capture-v1`, hash `23553ca5e4d429036fdf94c8f8b9bff794ea76e3b575c1bc733e4c3b96f3007a` |
| Features | Scores/capas, régimen, V2, fuentes, hashes de features de las cuatro decisiones auxiliares; señales completas de los 11 activos |
| Versiones | planner v2, synthesis v1, optimizer v1, calibrated optimizer v2 shadow-2; config hash `ce79b627376bc368`; code_version `unknown` |

La captura contiene hashes de siete archivos y macro fechado a 16:47:06.962280 UTC.
Eso no prueba que todo el runtime coincidiera con main. La captura v1 no incluye
run_id propio; su vínculo al run proviene de plan_id. El snapshot encontrado por
fecha coincide con el ID guardado dentro de la captura; no se atribuyó una cartera
por mera proximidad temporal. El `run_context.portfolio_snapshot_id` histórico,
en cambio, contiene un timestamp: corregido hacia adelante para usar el ID real o
null, sin alterar filas históricas.

La extracción obtuvo 4.901 filas de velas para los 11 tickers (máximo 520/ticker).
Son filas disponibles **hoy**, con `ts <= corte`; incluyen fuentes alternativas y
filas capturadas después del corte. No son las 260 barras efectivamente usadas por
cada señal. Por ejemplo GDX tiene 382 filas, 90 con volumen cero y 41 capturadas
después del corte. Esas 41 no pueden probar evidencia disponible entonces.
`scraped_at` se conserva como retrieved_at; no se renombra a available_at.

## AS-IS y contratos E1

| Superficie / campo | Productor → consumidor | Unidad / semántica y cambio |
|---|---|---|
| OHLCV | `db.get_market_candles` → `candles_to_frame` → técnico | Moneda, instrumento, venue e intervalo conservados. Filtros explícitos y chequeo de ambigüedad antes de LIMIT. Timestamp de captura conservado. |
| Identidad | Cocos, TV, snapshots → frame | Aliases de proveedor conservados en ProviderSymbol/provider_symbols. Identidad económica ticker+asset_type+currency+venue; dos símbolos nativos/settlements incompatibles se rechazan. |
| Unidades / ajustes | Metadatos de serie → contrato | volume_unit, calendar, adjustment_policy y depositary_ratio explícitos o desconocidos. Moneda local y subyacente no se fusionan. |
| Tiempo | BarStart/BarEnd/IsClosed/AvailableAt → calidad | Desconocido permanece null. Evidencia conocida posterior al cutoff rechazada; volumen de sesión parcial no confirma. Intradía conserva barras distintas del mismo día. |
| Calidad de precio | `frame_quality` → técnico | Finitud, positividad, OHLC imposible, duplicados y orden; barras inválidas no generan señal. Digest de la serie seleccionada y política de selección registrados. |
| Calidad de volumen | técnico / V3 → síntesis → AssetSignal → planner / snapshots | Cero, faltante, negativo e infinito tienen razones. vol_ratio y OBV no se inventan ni arrastran el último dato válido. Calidad V3 completa y valores de indicadores propagados. |
| Features cortas | `compute_indicators` → régimen / síntesis | SMA200 insuficiente y RSI indefinido se conservan como null, con motivo; no como cero o valor viejo. No se agrega un indicador nuevo. |
| Régimen / V2 / V3 | módulos técnicos existentes → recorrido actual | Se reutilizan; sin calibración ni thresholds nuevos para activos concretos. |
| Macro / sentiment / técnico | `blend_scores` → SynthesisResult | Entradas finitas y rangos [-1,1] para scores, [0,1] para fuerza. Fallo explícito ante contrato numérico inválido. Score no es EV. |
| Convicción | síntesis → bridge | Fracción [0,1], faltante/incorrecta preservada como null; no conversión silenciosa de porcentaje. |
| Optimizer | targets → planner | Peso fraccional [0,1]. Target no otorga autoridad independiente. Pesos/capital no finitos son errores de contrato. |
| Plan / órdenes | guard + reconciliación → validator | No admite score NaN/Inf ni importes no finitos. Costos, mínimos y redondeo del baseline conservados para entradas válidas. |
| Snapshot | layers → `feature_snapshot_v3` | Incluye calidad, V3, signal_action y asset_view; JSON estricto, no finitos convertidos a null con ruta y motivo. Históricos v2 no se reescriben. |
| Linaje | writer transaccional / capture existente | Owner/run y append-only conservados. Nueva evidencia shadow va en payload/metadata/captura; sin tablas o migraciones nuevas. Hashes de los nuevos contratos incluidos. |

El nombre legado `price_usd` sigue existiendo para compatibilidad y no acredita USD:
la identidad de la serie y la moneda de la cartera son las fuentes de unidad.
Un frame local de BYMA ARS queda etiquetado como tal. No se afirma que su volumen
represente demanda del subyacente. Separar esa fuente es trabajo E2.

## Hallazgos comprobados y alcance de las correcciones

1. HOLD con score +0.09 y convicción 0.3333 termina en BUY financiable: reproducido
   con ticker sintético TEST. No se cambia ese comportamiento productivo por una
   regla económica nueva: se registra el conflicto en SHADOW_ONLY.
2. `_buy_guard` aceptaba NaN/+Inf por comparaciones fallidas: ahora los bloquea.
   También se validan rangos, nominales y cash; los guards mantienen umbrales.
3. Volumen cero/faltante devolvía `vol_ratio=1.0` o un último dato anterior:
   ahora null y motivos; se retira confirmación OBV cuando la serie es incompleta.
   Se conserva el denominador legacy inclusivo para datos válidos. El RVOL de 20
   sesiones previas excluyendo la actual es una feature E2 distinta, sin activarla aquí.
4. V3 no cruzaba íntegra ambas ramas de síntesis: helper compartido copia calidad y
   V3 en cartera y universo. Radar también conserva calidad y recibe evaluación
   shadow, sin convertir su elegibilidad técnica en una señal de síntesis inventada.
5. Ranking y conversión de velas podían mezclar moneda/mercado/instrumento:
   ahora se rechaza ambigüedad y se preservan símbolos/procedencia. Overlay de
   volumen exige identidad económica conocida y equivalente además del precio.
   Fixtures antiguos sin identidad se completaron explícitamente para probar el
   camino válido; existe prueba separada de rechazo con identidad ausente/distinta.

## Autoridad, aislamiento y comparación

`evaluate_authority` es una función compartida pura. Mantiene separados asset_view,
signal_action, portfolio_intent, action_reason y execution_status. No deduce una
tesis favorable de un score: asset_view queda UNKNOWN hasta disponer de tesis.
Una reducción por concentración conserva una tesis FAVORABLE recibida.

HOLD + INCREASE exige `RebalanceAuthorization`: ID, owner, run, ticker, motivo,
techo de peso, vencimiento con timezone y autoridad independiente del optimizer.
No se crea autorización desde el target. No hay emisor productivo habilitado en E1.
Falta de calidad crítica bloquea elegibilidad shadow aun con autorización.
ALLOWED en shadow significa que esta política no veta; **no** certifica funding,
riesgo global ni ejecución. Las órdenes siguen dependiendo de los guards existentes.

La evaluación se agrega al plan/captura y metadata de nuevas órdenes. Ninguna rama
de generación de órdenes lee el resultado shadow. Los importes ejecutables del
baseline se adjuntan como observación para distinguir target de operación financiada.

Comparación con la propuesta guardada (11 decisiones, 3 aumentos teóricos):

| Caso | Baseline registrado | Shadow E1 |
|---|---|---|
| MSFT | ACCUMULATE → BUY financiado | BLOCKED: calidad histórica no verificada |
| GDX | ACCUMULATE → WATCH sin mínimo nominal | BLOCKED: calidad histórica no verificada |
| VIST | HOLD → WATCH sin funding | BLOCKED: calidad no verificada y falta autorización independiente |
| 8 restantes | Reducciones / HOLD del baseline | Sin veto de aumento; no constituye autorización de venta |

La comparación no recalcula capital ni simula un PnL. No se reemplazó el plan
registrado. Reproducible con `python scripts/compare_contextual_e1.py
output/contextual-e1/recorded-run.json`; resultado privado en `comparison.json`.
El comparador rechaza mezclas de owner/run y señala el run_id faltante del capture v1.

Entradas válidas ensayadas directamente en baseline y rama: cuatro series OHLCV
sintéticas de 260 barras, con pendientes positivas/negativas, y un plan financiado.
Coinciden score técnico, fuerza, señal, órdenes, cantidades y cash. Prueba separada
compara el plan entero quitando únicamente metadata shadow. Casos inválidos cambian
por correcciones declaradas arriba, no por nuevos thresholds.

## Validación y pendientes

- Pruebas focalizadas: contratos numéricos, volumen cero/NaN/Inf/missing, OHLC
  imposible, futuro/duplicados/sesión parcial, identidad/settlement/intradía,
  transmisión hasta snapshot, owner/run, autorización, cash y neutralidad shadow.
- Baseline limpio: `684 passed, 7 failed, 23 skipped`.
- Suite E1: `721 passed, 7 failed, 23 skipped`; suma 37 pruebas aprobadas
  respecto del baseline por los contratos E1, sin agregar fallos.
- Los siete fallos preexistentes corresponden a menú/help/radar de Telegram,
  watchlist radar y challenger por dependencia local faltante `pypfopt`.
  No se instalaron paquetes ni se modificó producción para ocultarlos.
- Los skips incluyen integración que no se habilitó contra la DB productiva.
- Consulta SQL nueva ejecutada en PostgreSQL real, solo lectura: GDX 260,
  IREN 220, MSFT 260 y VIST 260 barras al corte filtrado, misma identidad BYMA/ARS/CEDEAR.
  Esto valida consulta y contrato de lectura, no disponibilidad PIT ni una nueva corrida.
- Persistencia append-only/atomicidad: se conserva el writer existente y se ejecutan
  sus regresiones aisladas; no se realizaron INSERT ni migraciones como prueba real.

Antes de cerrar G0/G1 y abordar E2:

1. Capturar series seleccionadas inmutables (el digest solo no basta para replay),
   fuente y unidades verificadas, ratio/ajustes versionados, inicio/fin/cierre y
   available_at reales; validar huecos y vencimiento contra calendario versionado.
   E1 los marca desconocidos, no fabrica un calendario ni disponibilidad.
2. Fijar con producto horizonte primario (20 ruedas propuesto por PDF), universo,
   proveedor subyacente frente a BYMA y costos/ejecución del experimento. Se conserva
   la configuración actual: fee 0.006 y slippage 0.0015; no son costos reconciliados.
3. Validar una nueva captura con IDs y versiones completos, escritura transaccional
   en infraestructura de prueba autorizada y smoke real bajo un alcance posterior.
4. E2: reutilizar `trend_regime`, V2/V3, `ticker_technical_report` y challenger
   calibrado. Añadir estructura diaria/semanal cerrada, RVOL comparable, fuerza
   relativa, invalidadores y gráfico sin cambiar autoridad productiva.
5. Congelar comparadores B0/B1/C1/HOLD, capital, costos y protocolo temporal antes
   de evaluar economía. GDX/IREN siguen siendo diagnósticos, nunca parámetros especiales.
