# Technical Analysis Desk — etapa 0b y contrato del piloto

**Resultado validado el 16/11/2026 sobre el corte congelado del 15/11 a las 19:24 ART: 5/5 candidatos con precios hasta el 13/11; 0/5 paquetes aptos para etapa 1.** La nueva evidencia local contiene 1.400 velas D y 292 agregados W con hashes reproducidos. Persisten dos bloqueos: unidad/ajustes del volumen en los cinco instrumentos y clasificación bursátil del 10/10/2025 en cuatro. El contrato ejecutable de identidad, ruido y presupuesto queda en versión 2. Piloto de modelos: `NOT_RUN`; debate fuera del piloto.

Trabajo aislado en `C:\Users\Franco\Documents\Codex\worktrees\quantia-technical-desk-stage0b`, rama `work/technical-desk-stage0b`, derivada de la baseline `93edb79012e49ee4ee97a91ace6c710cbf06f96f`. La baseline queda intacta; estos cambios no tienen commit ni push. No hubo escrituras de base, cambios de servicios/configuración productiva ni llamadas a modelos.

## Remediación y candidatos D/W actuales

El nuevo capturador `scripts/capture_technical_desk_evidence.py` descarga únicamente las cinco series públicas preregistradas y archiva tramas, metadatos, eventos, números decimales y hashes en archivos locales nuevos. La captura del **15/11 a las 17:44 UTC** obtuvo 1.917 barras brutas. `series_completed` acredita descarga completa; no se usa como prueba de cierre. El archivo G2 de producción continúa con su antigüedad original: esta captura no lo actualiza.

El constructor `scripts/build_technical_desk_candidates.py` conserva los inicios de historia y tickers originales. Su corte **2026-11-15T22:24:24.021327Z** es posterior a la captura y al calendario remediado. Descarta semanas parciales, faltantes o con calendario desconocido; calcula OHLC W desde los D y conserva sus hashes componentes. No suma volúmenes cuya unidad está sin acreditar.

| Ticker | D | W completas de precio | Última D cerrada | Fechas esperadas sin verificar | Bloqueos |
|---|---:|---:|---|---:|---|
| AMD | 284 | 59 | 13/11/2026 | 1 | Calendario, unidad y ajustes de volumen |
| GDX | 284 | 59 | 13/11/2026 | 1 | Calendario, unidad y ajustes de volumen |
| GOOGL | 264 | 56 | 13/11/2026 | 0 | Unidad y ajustes de volumen |
| MU | 284 | 59 | 13/11/2026 | 1 | Calendario, unidad y ajustes de volumen |
| NVDA | 284 | 59 | 13/11/2026 | 1 | Calendario, unidad y ajustes de volumen |

No faltan sesiones dentro de las fechas verificadas. Cada instrumento añadió las cinco ruedas 5, 6, 11, 12 y 13/11. Los **1.375 OHLCV anteriores coinciden numéricamente**, con cero diferencias en el solapamiento. El nuevo vintage se conserva separado; no se reescriben timestamps ni digests históricos. Precio fresco al corte y semanas OHLC completas no equivalen a un paquete apto para los cinco roles.

Los metadatos de los cinco símbolos identifican explícitamente sesión `regular`, cierre 18:00 ART y demora de 1.200 segundos. Para la última vela se exige además haber superado la fase BYMA de precio de cierre hasta 18:15: el guard conservador es **13/11 18:35 ART / 21:35 UTC**. La captura del domingo lo supera. La prueba queda identificada como cierre por regla de sesión transcurrida: no existe un flag final explícito del proveedor ni se afirma que la serie incluya postmercado. El código impide reutilizar la frescura de este corte cuando aparecen días posteriores sin revalidar.

Se recuperó el calendario oficial BYMA 2025 archivado el 7/10/2025, con digests WARC comprobados, y los comunicados específicos de noviembre/diciembre. Hay **71 sesiones verificadas de 2025 y 213 de 2026** dentro del período utilizado. Queda **10/10/2025 como `UNKNOWN_TRADING_STATUS`** por clasificación conflictiva entre calendario bursátil archivado y normativa posterior; la marca de cierre del proveedor no sustituye una resolución bursátil. La semana correspondiente se excluye de W y la incertidumbre bloquea los cuatro paquetes que empiezan en septiembre. GOOGL empieza el 16/10. Fuentes y alcance: [revisión del calendario](../output/technical-desk-stage0b-remediation/calendar_review.md).

Se inspeccionaron los metadatos completos de **los cinco símbolos**, con hash vinculado a cada captura: ninguno declara unidad de volumen. `type=dr`, precisión decimal y `adjustment=splits` tampoco acreditan unidad ni política de ajustes del volumen. Quedan `UNKNOWN`; las 292 W llevan `volume=null`. Evidencia: [contratos de volumen](../output/technical-desk-stage0b-remediation/volume/symbol_volume_review-20261115.json). La regla genérica de TradingView no basta para este feed de CEDEARs. [Documentación de volumen](https://www.tradingview.com/support/solutions/43000591617-volume/).

Estos son candidatos de datos. El constructor no dispone de una ruta para promoverlos a `READY` mientras falte ese contrato; no reemplaza el futuro ensamblado de indicadores, anclas causales y contexto de etapa 1.

## Captura inicial SQL y rehash, preservados como antecedente

Selección determinista previa a evaluar calidad/respuestas: primeros cinco tickers distintos ordenados alfabéticamente entre las posiciones positivas CEDEAR/ARS de la última cartera del owner. Resultaron **AMD, GDX, GOOGL, MU y NVDA**. TQQQ y VIST quedan fuera por esa regla, no por sus resultados. Esta muestra no pretende representar todo el Radar.

Captura SQL: **15/11/2026 01:22:11 ART / 04:22:11 UTC**, base `quantia`, owner del bot, `REPEATABLE READ`, `transaction_read_only=on`; límites de tiempo y tamaño de exportación. Se elige la última versión conocida por identidad/barra al corte, con lifecycle `COMPLETE` del mismo owner. Ninguna fila posterior al corte es elegible.

| Ticker | Velas diarias rehasheadas | Primer día de precio | Último cerrado | Fechas de 2026 verificadas | Fechas de 2025 sin calendario completo |
|---|---:|---|---|---:|---:|
| AMD | 279/279 | 17/09/2025 | 04/11/2026 | 208 | 71 |
| GDX | 279/279 | 17/09/2025 | 04/11/2026 | 208 | 71 |
| GOOGL | 259/259 | 16/10/2025 | 04/11/2026 | 208 | 51 |
| MU | 279/279 | 17/09/2025 | 04/11/2026 | 208 | 71 |
| NVDA | 279/279 | 17/09/2025 | 04/11/2026 | 208 | 71 |

Se ejecutó `src.collector.contextual_g2.observation_digest` de la baseline sobre cada fila exportada, con todos sus hechos de mercado, provenance y calidad. **Cero mismatches y cero duplicados de fecha por ticker.** Cada candidato conserva filas completas, IDs de observación, digest almacenado/recalculado, hash del capture, corte y hash propio. No son todavía paquetes D/W habilitados para modelos. Un digest correcto acredita integridad respecto del registro, no exactitud del proveedor ni calidad suficiente.

## Diagnóstico inicial del collector desplegado

Para los cinco tickers, `market_candles` de `TRADINGVIEW_BYMA` tiene velas del **13/11**, obtenidas entre **18:01 y 18:07 ART** de ese día. Es una tabla mutable y no reemplaza el archivo as-of. Esas horas tampoco prueban captura posterior a todas las fases de negociación.

En `market_candle_observations`, la última captura de los cinco ocurrió el **5/11 alrededor de las 21:28 ART**. La última vela recibida, 5/11, sigue abierta/no confirmada; la última cerrada es 4/11. El cierre del collector G2 se confirma mediante el inicio de la siguiente vela: su `bar_end=13:30 UTC` es una confirmación conservadora, **no el cierre horario oficial de BYMA**. No se cambia ese significado ni se reetiquetan las filas archivadas.

El scheduler desplegado tiene el refresh TradingView habilitado. Su job escribe `market_candles`; no tiene un job de captura G2 ni los módulos G2 en esa imagen. La captura contextual reside en el bot y se dispara por el flujo contextual. El refresh general del 13/11 terminó `PARTIAL` —419 targets, 15.010 velas importadas, 37 errores—; eso no demuestra un fallo de los cinco seleccionados, cuyas últimas fechas se verificaron por SQL.

**Remediación local realizada:** captura específica en archivos, sin invocar análisis formal. Integrarla al archivo operativo/scheduler queda fuera de esta revisión. No reutilizar sin cambios `capture_contextual_g2.py`: además de capturar, llama a `run_analysis` con `run_intent=formal_plan` y persistencia habilitada.

## Calendario y volumen en la captura inicial

El calendario oficial de BYMA consultado distingue negociación de liquidación: **6/11 sí negocia; 9 y 10/11 son cierres excepcionales**. La última sesión completa esperada al domingo 15/11 es 13/11. Faltan en los candidatos cerrados **5, 6, 11, 12 y 13/11**. El JSON productivo omite los cierres del 9 y 10; se archivó una versión de auditoría y no se modificó producción. [Calendario BYMA](https://www.byma.com.ar/mercado/calendario-bursatil).

Desde el 2/11, renta variable PPT T+1 tiene negociación regular hasta 18:00 ART y una fase a precio de cierre de 18:05 a 18:15. No está verificado cuáles de esas fases integra la vela diaria de este proveedor. Por eso no se promueve automáticamente una captura de las 18:01 a «sesión completa». [Comunicado BYMA 19031](https://www.byma.com.ar/comunicados/com19031-horarios).

El calendario de auditoría cubre completamente las etiquetas de sesión de **1/1–13/11/2026: 213 sesiones esperadas**, de las cuales cada candidato tiene 208. No aparecen días inesperados dentro del tramo verificado. Hay 43 grupos semanales completos por ticker según ese calendario, pero todavía no son velas W construidas y validadas; las semanas de noviembre están incompletas. **335 de las 1.375 velas pertenecen a 2025** y no tienen cobertura completa de calendario recuperada: se conservan como `UNKNOWN`, sin inventar huecos ni declarar todas las fechas hábiles.

La fuente de calendario se capturó ahora. No acredita disponibilidad en cortes históricos. Para un piloto prospectivo se puede congelar ese calendario junto con un paquete cuyo corte sea posterior a todos sus insumos; no hace falta fingir una versión histórica original. Para reconstruir decisiones de 2025/2026, esa ausencia de vintage sigue siendo una limitación independiente.

**Volumen: `UNKNOWN` en 1.375/1.375 velas.** Todas contienen un valor numérico, pero `volume_unit` es nulo. El capturador pasa explícitamente `volume_unit=None`. La resolución pública actual de `BYMA:AMD` devuelve tipo `dr`, ARS y BYMA, sin unidad de volumen; tampoco prueba la unidad de los otros cuatro ni la de capturas anteriores. La documentación genérica de TradingView distingue volúmenes según instrumento y no alcanza para asignar «acciones» a este feed de CEDEARs. No se equipara precisión decimal con unidad económica. [Volumen en TradingView](https://www.tradingview.com/support/solutions/43000591617-volume/), [información del instrumento](https://www.tradingview.com/pine-script-docs/concepts/chart-information/).

**Acción pendiente:** contrato de fuente específico, fechado y aplicable al instrumento/intervalo, que acredite unidad y ajustes de volumen. Se enlaza como evidencia nueva; no se sobrescriben digests anteriores ni se rellena `volume_unit` por suposición. Mientras falte, Wyckoff no puede tratar este volumen como validado y los cinco paquetes no cumplen el mínimo completo del piloto.

## Identidad semántica: regla ejecutable

`src/technical_desk/pilot_protocol.py` fija `technical-desk-semantic-v2` y `technical-desk-pilot-v2`. No usa parecido textual ni un modelo para emparejar escenarios. La identidad del instrumento exige cuatro componentes canónicos en mayúsculas `MERCADO:TIPO:TICKER:MONEDA`, consistentes con la moneda del paquete; no se admiten alias libres para inflar la cohorte.

La clave de comparación contiene instrumento/moneda, método, D/W, familia de patrón y roles/IDs de anclas causales del paquete. Las familias soportadas y sus roles obligatorios están enumerados en `CAUSAL_ROLES`. Dirección, principal/alternativo, texto, orden de presentación y niveles quedan fuera de esa clave: así un cambio de dirección o de rol puede detectarse sobre el mismo escenario.

El fingerprint semántico sí contiene dirección, rol, estado/anclas estructurales, horizonte en sesiones, predicados de confirmación/invalidation y objetivos. Se admite **exactamente una confirmación y una invalidación**: condiciones compuestas AND/OR requieren otra versión, para evitar tratarlas como cambios sólo de texto. Los predicados incluyen campo, operador y persistencia. Los niveles usan `Decimal` y deben caer exactamente en el tick verificado del paquete; no se redondea una respuesta inválida. El orden de presentación no altera el significado de condiciones/objetivos. No se admiten floats ni números no finitos.

Una clave causal nueva o desaparecida se registra como cambio de estructura. Dos escenarios con la misma clave son ambiguos: no se emparejan por ser principal/alternativo ni se inventa un identificador con texto. Esta versión conservadora puede rechazar conteos alternativos de Elliott con la misma familia y origen; ampliar las reglas exige una nueva versión, no una decisión oportunista del moderador. Las anclas deben existir en el paquete; su confirmación causal se valida al construirlo en etapa 1.

La comparación de ruido exige mismo paquete/hash, modelo resuelto, instrucciones/plantillas de prompts, parámetros y reglas; réplica distinta y grupo agente/timeframe idéntico. La clave semántica no habilita comparar como ruido paquetes con datos distintos. El hash de configuración de prompt excluye las respuestas de especialistas que el moderador recibe en su propia réplica; el ledger de etapa 1 deberá conservar también el hash de cada petición completa.

## Qué se hace con cada resultado de ruido

| Resultado del contrato | Acción obligatoria |
|---|---|
| `STABLE` | Conservar el par y habilitar su lectura como semánticamente estable en el reporte diagnóstico; no implica capacidad predictiva. |
| `TEXT_ONLY` | Conservar ambas redacciones y una sola semántica; no contar cambio de tesis ni de mercado. |
| `NOISE_UNSTABLE`: niveles, dirección, rol, estructura o anclas | Apartar el par de las tesis estables, mostrar ambos originales y las diferencias; no promediar, elegir ganador ni forzar consenso. Un tick de diferencia cuenta. |
| `NOISE_UNSTABLE`: una respuesta se abstiene | Mostrar dependencia de interpretación/abstención y excluir del conjunto estable. |
| `INSUFFICIENT_EVIDENCE`: ambas se abstienen | Registrar los motivos y la evidencia faltante; no computar «100% estable» ni una hipótesis evaluable. |
| `NOT_COMPARABLE`: respuesta inválida, faltante o emparejamiento ambiguo | Registrar fallo/ambigüedad; sólo reparar dentro del único reintento permitido de la llamada. Agotado, lote incompleto; no imputar estabilidad. |
| `INVALID_EXPERIMENT`: cambiaron inputs/configuración o se reutilizó la réplica | Invalidar la comparación y excluirla del denominador de ruido. Repetir sólo bajo un nuevo experimento preregistrado; no ocultar ese costo ni extender automáticamente el lote. |

`ACTION_BY_CHANGE` implementa las acciones por motivo y `compare_replicates` devuelve los cambios concurrentes, añadidos/desaparecidos y fingerprints. `market_change_claim` permanece falso: una comparación de dos ejecuciones sobre los mismos datos no prueba un cambio causado por el mercado. Dos réplicas son un diagnóstico pequeño; no se fija una tasa de éxito predictivo ni un porcentaje mínimo de estabilidad inventado.

En etapa 1 el informe añade distancias en ticks/ATR, dispersión de niveles y tasas con denominadores explícitos. Esas estadísticas todavía no se midieron. La síntesis también se repite; su contexto incluye las cuatro respuestas de su propia réplica. Su variación combina la de los especialistas y la del moderador; no se atribuye toda al azar del moderador. Aislar ese último componente requiere un experimento futuro con entradas del moderador idénticas, fuera de estas 50 llamadas.

## Mínimo completo y tope de USD 2

El piloto contiene **exactamente cinco instrumentos/monedas distintos**, cada uno con paquete D/W completo y dos réplicas de **Elliott, Wyckoff, estructura, momentum y moderador**: 50 llamadas lógicas, máximo 100 intentos contando un reintento por llamada. **Sin escéptico, revisiones ni debate**. Debate queda exclusivamente para etapa 2 con otro experimento/presupuesto.

`plan_pilot_budget` exige tarifas USD explícitas, fuente/fecha/vigencia, modelo resuelto y cobertura de todos los cargos. Reserva el lote entero con todos los reintentos antes de permitir considerarlo factible:

El `PilotManifest` congela los cinco paquetes exactos, los cinco roles, modelo resuelto y digest por rol, tarifas y límites de tokens. La evaluación final reconstruye la reserva y rechaza respuestas de otro modelo o cohorte. Un presupuesto válido para un modelo barato no habilita respuestas de un modelo diferente.

`reserva = 5 paquetes × 2 réplicas × 2 intentos × suma(costo máximo de cada uno de los 5 roles)`.

Cada costo máximo incluye entrada completa, salida facturable —también razonamiento— y cargos adicionales. No se presupone descuento de caché. El límite del moderador debe cubrir las cuatro salidas máximas y sus instrucciones. Si falta evidencia de tarifa, un paquete no está `READY`, el mínimo se reduce, se activa debate o la reserva supera USD 2, el resultado es **`NOT_STARTED`**. No se empieza un subconjunto más barato ni se cambia modelo a mitad de lote.

La prevalidación es pura; no autoriza ni ejecuta peticiones. El orquestador de etapa 1 deberá respetar esos límites y llevar reserva/consumo atómicos por petición, también en fallos/timeouts. Si no conoce el costo final, retiene el máximo reservado. Costo calculado y factura confirmada son campos separados. `assess_pilot_completion` exige los **100 resultados D/W de las 50 llamadas**: dos timeframes por rol/réplica/paquete, sin duplicados y con experimentos compatibles. Si falta uno después de reintentos, el resultado es `INCOMPLETE`, aunque el gasto sea menor a USD 2. Con costo calculado ausente devuelve `COST_UNVERIFIED`; si excede la reserva, `BUDGET_BREACH`. Una abstención con schema válido completa una respuesta, pero se cuenta aparte de las hipótesis evaluables y nunca corrige datos incompletos. El contrato no verifica por sí solo la facturación ni el ledger futuro.

**Factibilidad económica actual: sin tarifa/modelo seleccionado, no calculada. Ruido y costo real del piloto: `NOT_RUN`, costo observado `null`.** Las tarifas de tests son fixtures sintéticos, no precios consultados ni una promesa de que USD 2 alcance.

## Evidencia, validación y salida de etapa 0b

Artefactos locales bajo `output/technical-desk-stage0b/` —excluidos de Git—: `packets/capture.json` conserva SQL y resultados; cinco JSON de candidatos y `packets/rehash.json` conservan pruebas de integridad; `calendar_audit.json` y `calendar_sources/` conservan fuentes oficiales/hash/fecha; `collector_review.json` conserva runtime y contratos; `readiness.json` integra el resultado reproducible.

Comandos de reproducción:

```text
python scripts/audit_technical_desk_stage0b.py --output <directorio-nuevo>
python scripts/audit_technical_desk_stage0b.py --from-capture output/technical-desk-stage0b/packets/capture.json --output <directorio-nuevo>
python scripts/summarize_technical_desk_stage0b.py output/technical-desk-stage0b
python -m pytest -q tests/test_technical_desk_pilot_protocol.py tests/test_contextual_g3.py
python -m py_compile scripts/audit_technical_desk_stage0b.py scripts/summarize_technical_desk_stage0b.py
git diff --check
```

La captura inicial rechaza sobrescribir evidencia y pasó **36 controles de integridad**, independientemente del resultado de aptitud `NOT_READY`. La validación actual agrega reconstrucción exacta de tramas → barras → candidatos, los 1.692 hashes de hechos D/W, OHLC semanal y sesiones comprobados independientemente, y el solapamiento de los 1.375 registros originales. El manifiesto [validation-frozen-candidates.json](../output/technical-desk-stage0b-remediation/validation-frozen-candidates.json) conserva resultados por ticker y hashes del código.

Reproducción de los candidatos actuales, **sin descargar nuevamente** ni atribuir frescura al día de reproducción:

```text
python scripts/build_technical_desk_candidates.py --capture output/technical-desk-stage0b-remediation/capture-20261115 --calendar output/technical-desk-stage0b-remediation/calendar_audit_remediated.json --original-packets output/technical-desk-stage0b/packets --output <directorio-nuevo> --cutoff 2026-11-15T22:24:24.021327+00:00
python output/technical-desk-stage0b-remediation/verify_frozen_candidates.py
python -m pytest -q tests/test_technical_desk_evidence.py tests/test_technical_desk_pilot_protocol.py tests/test_contextual_g3.py
```

Resultado final: **90 passed in 2.30s**; compilación de los siete módulos/scripts revisados y `git diff --check` sin errores. La ejecución del constructor con `--cutoff` reprodujo **6/6 archivos idénticos byte a byte**, incluidos los cinco paquetes y su manifiesto. Los tests cubren además manifests con capturas faltantes/repetidas, ticker interno distinto, fuente bloqueada y calendario que declara cobertura futura pero conserva una última sesión completa obsoleta. Son validaciones de software e integridad, no evidencia predictiva ni de rentabilidad.

Para cerrar la aptitud de etapa 0b faltan un contrato fechado del feed que acredite **unidad y ajustes del volumen**, y evidencia bursátil que resuelva el **10/10/2025**. La próxima captura deberá revalidar frescura/calendario al nuevo corte. No se recorta silenciosamente la historia, se sustituye el feed ni se baja de cinco paquetes. Después comienza etapa 1: indicadores/anclas, dispatcher/ledger, tarifa y modelo fijados, reserva factible de USD 2 y dos corridas reales sin debate. Los tests del contrato no sustituyen esa evidencia ni las corridas.
