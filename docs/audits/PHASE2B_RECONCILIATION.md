# Conciliación de ventanas históricas — avance de Fase 2B

Cierre documental: **2026-10-18**. Corte nuevo de Cocos: **2026-10-17 06:51:31.341–06:52:51.297 UTC**. Este informe amplía [Fase 2A](PHASE2A_RECONSTRUCTION.md) y la [extracción de actividad y comprobantes](COCOS_ACTIVITY_COLLECTION.md), conservando sus cortes originales. Rama `audit/phase2a-reconstruction-20261016`, [PR #28 en draft](https://github.com/Francoparodi01/Portfolio-manager-/pull/28), sin merge. No es una actualización de saldos al 18 de octubre.

**Resultado: PARTIAL.** Se recuperaron ocho resúmenes de cuenta originales y ocho CSV. Las 16 tablas de posiciones suman sus totales reportados; 95 comparaciones de cantidades listadas no presentan residuo; la caja ARS cierra aritméticamente en las ocho ventanas y la caja USD MEP en seis. Dos ventanas no tienen ambos saldos USD explícitos. Se conservan como desconocidos, aunque una hipótesis de saldo cero pudiera hacerlas cerrar.

**PnL real realizado y PnL neto de cuenta siguen NOT_IDENTIFIABLE.** La aritmética documentada permite verificar partes del registro, pero no acredita cobertura completa de flujos, financiación, costos iniciales, identidad de ejecución ni valuación común de las fuentes. No se publica un resultado económico sustituto.

El [recibo reproducible](evidence/phase2b-reconciliation.json) contiene conteos, ventanas, hashes y bloqueos; el [recibo de validación](evidence/phase2b-validation.json) registra pruebas, replay y contrato de moneda de la interfaz. Los originales, saldos e identificadores permanecen fuera de Git.

## Fuentes y límites

| Fuente | Verificación realizada | Cobertura y límite |
|---|---|---|
| PostgreSQL congelado de Fase 2A | Se relee el archivo privado, se verifica hash y contexto del propietario | Corte 16/10 12:50 UTC; 2.337 snapshots, 29.751 posiciones y 109 fills del owner. No se hizo una nueva consulta operativa para esta extensión |
| Backup Supabase y capturas QEEF/Baseline | Se conserva la conciliación por versiones de Fase 2A | No se vuelve a importar ni se redefine como histórico completo; cinco filas derivadas recuperadas siguen separadas |
| Supabase original en vivo | **NO VERIFICADA**; se conserva el fallo de acceso documentado | No se obtuvo nueva evidencia de ese origen |
| Actividad Cocos congelada | Se releen 670 movimientos/detalles, su hash y cuenta explícita/contexto | Corte 16/10 23:04–23:26 UTC; cinco retiros FAILED excluidos; no se vuelve a sumar el canal legacy |
| Resúmenes de cuenta PDF | **VERIFIED** para procedencia, cuenta explícita en cada página, fecha solicitada y aritmética de las tablas | Ocho fechas: 01, 02, 05, 06, 07, 09, 13 y 16 de octubre de 2026. Cada documento tiene apertura al cierre del día calendario anterior y cierre del día solicitado |
| Exportaciones CSV | **PARTIAL** para equivalencia con PDF | 108 filas; 107 tienen correspondencia de nombre/moneda y coinciden en cantidad y total nativo a 0,01. Una especie sin ticker tiene moneda incompatible entre formatos |
| Historia de cartera ARS y USD, período MAX | **VERIFIED** lectura de las dos respuestas; **PARTIAL** para medición patrimonial | 1.020 etiquetas diarias distintas por moneda, 02/01/2024–17/10/2026. Son valores retrospectivos leídos el 17/10, sin prueba de disponibilidad histórica ni criterio de valuación común con PDF |
| Portfolio y balances actuales | Respuestas GET conservadas bajo la misma cuenta de sesión | Observaciones del corte, no apertura histórica; no se usan como costos iniciales ni para reemplazar extremos anteriores |
| Órdenes de la interfaz | GET `/api/orders`: 200, lista vacía | **NO VERIFICADA** la recuperación del histórico de órdenes. Una lista actual vacía no prueba que nunca existieron |
| 39 comprobantes pendientes | Nuevo intento para los mismos IDs de la captura anterior: **39 HTTP 500** | Documentos de 2024 siguen **NO VERIFICADOS**. El detalle del movimiento continúa disponible; no se reemplaza el PDF por un documento inferido |

La captura nueva contiene **61 respuestas GET en ocho lotes**: seis de portfolio/historia/balances/órdenes, 16 exportaciones PDF/CSV y 39 reintentos de comprobantes. Es una secuencia de observaciones, no una transacción atómica. La actividad y los resúmenes proceden de cortes distintos y se conservan como tales.

La descarga corresponde al botón de exportación de Portfolio, con fecha seleccionada y formato PDF/CSV. Se observaron las rutas de la interfaz y se guardaron bytes originales. No se enviaron reportes por correo, no se operaron órdenes ni se inició el runtime de Quantia.

### Identidad y huellas de las entradas

| Archivo privado relativo a la raíz del worktree | SHA-256 de bytes originales |
|---|---|
| `outputs/phase2a/live-capture.json.gz` | `ff0c2b1ae71a1607022f94b7b724585402d3ffbc187ff4b4205fa704a55e2020` |
| `outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz` | `f8576afdf818ee0203a3d637a814231e77987c437c08c6f507c85cae98973dc8` |
| `outputs/phase2b/broker-20261017-v1.json.gz` | `e7ac50ebfc1b222b4b917076b1db6ab392abb824a65c535a813b95575f828aec` |

El analizador exige que captura y actividad tengan la misma cuenta y hash de contexto. Rechaza PDF con otro comitente, otra fecha o una apertura distinta del día anterior; no atribuye documentos por nombre parecido. Los 217 fills sin owner de la base local siguen excluidos del inventario elegible.

## Conciliación diaria obtenida

Se usa la **fecha de liquidación** del movimiento para las diferencias entre posiciones liquidadas. La fecha de ejecución se conserva separada. La compra TQQQ visible en Actividad el 09/10 aparece en las posiciones liquidadas del 13/10; tratar ambos campos como un mismo instante produciría un descuadre artificial.

Las cantidades se comparan como diferencia entre los vectores de instrumentos listados en las tablas completas del PDF, contra compras y ventas observadas. Una especie no listada no aporta al vector; esto no certifica costo de lote cero ni ausencia económica fuera de esa vista. Las posiciones sin ticker quedan fuera de este cruce, explícitamente contadas. No se convierten ratios ni clases de instrumento.

| Apertura → cierre (2026) | Cantidades sin residuo / comparadas | Movimientos por liquidación | Caja ARS | Caja USD MEP |
|---|---:|---:|---|---|
| 30/09 → 01/10 | 11/11 | 4 | VERIFIED_ARITHMETIC | Falta saldo explícito de cierre |
| 01/10 → 02/10 | 11/11 | 5 | VERIFIED_ARITHMETIC | Falta saldo explícito de apertura |
| 04/10 → 05/10 | 13/13 | 6 | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| 05/10 → 06/10 | 13/13 | 3 | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| 06/10 → 07/10 | 11/11 | 1 | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| 08/10 → 09/10 | 12/12 | 2 | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| 12/10 → 13/10 | 12/12 | 1 | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| 15/10 → 16/10 | 12/12 | 0 observados | VERIFIED_ARITHMETIC | VERIFIED_ARITHMETIC |
| **Total** | **95/95** | **22** | **8/8** | **6/8; dos desconocidas** |

Son **ocho ventanas diarias seleccionadas**, no un período continuo conciliado desde el 30/09 hasta el 16/10. Los 95 casos incluyen instrumentos sin movimiento; no equivalen a 95 operaciones ni a 95 resultados independientes. Las tres apariciones de posiciones no monetarias sin ticker no se promocionan a cantidades reconciliadas.

La caja usa `cierre − apertura − suma de importes netos explícitos` por moneda, con Decimal y tolerancia absoluta de **0,01 unidad monetaria**. Si un detalle aporta importe neto, sus comisiones no se restan nuevamente. Si solo hay cargos explícitos, se conserva cada cargo con su moneda y no se inventa un principal de caja. Los cinco movimientos FAILED no entran en la ecuación. Cero movimientos observados en una fecha no prueba cobertura completa de esa fecha.

La interfaz pública identifica **USD como Dólar MEP en caja** (`portfolioCashTitles.web`, módulo 4448); URL y hash del recurso observado están en el recibo de validación. Esa equivalencia se aplica únicamente a caja. No se extiende a `EXT`, a títulos sin ticker ni a dólares especie. Las denominaciones originales quedan preservadas.

## Valuaciones, formatos y evolución patrimonial

Las **16/16 tablas** se reconocen sin líneas numéricas pendientes y sus valores ARS suman el total de posición impreso a la tolerancia declarada. Esto verifica la suma del documento, no que cada cotización sea correcta ni que ese total sea un NAV económico comparable entre fuentes. Los resúmenes presentan por separado **tres campos de posición pendiente de liquidación**, en 02/10, 06/10 y 09/10. Se conservan con su fecha, moneda, importe y texto original, junto con `TOTAL DE LA INVERSION` y `RESULTADO`; este último no se convierte en `economic_pnl_net`.

El CSV tiene un encabezado de ocho columnas para títulos, pero **15 filas de caja tienen cinco columnas**. Un lector que las alineara por posición con el encabezado confundiría moneda, total y fecha de precio. El parser reconoce las dos formas explícitas y rechaza otras. No rellena celdas ausentes.

De las 108 filas, **107/108** tienen correspondencia única con la tabla de cierre del PDF, conservando nombre y moneda y aplicando solo el alias documentado de caja. Las 107 coinciden en cantidad y total nativo. La excepción es **Dolar Especie CV 7000 ()**, el 01/10: el CSV dice USD y el PDF USD MEP; no tiene ticker reconocido y su `precioAsOf` está vacío. En las demás filas de títulos la fecha de precio coincide con el día solicitado. Ninguna fila trae `usedFallbackPrice=1` o `missingPrice=1`; esos flags no certifican que el precio sin fecha sea utilizable.

El gráfico histórico ofrece **1.020 observaciones por moneda sin fechas duplicadas**. Sin embargo, **ninguna de las 16 comparaciones del total de posición PDF contra la historia ARS coincide dentro de 0,01**. Son 13 etiquetas de fecha únicas y tres repeticiones entre documentos; las repeticiones del PDF no se contradicen entre sí. El cruce alinea etiquetas de fecha, no instantes ni bases de valuación. El script excluye del cruce una fecha si la historia devuelve más de una versión, en vez de elegir silenciosamente la última.

Las diferencias entre gráfico y PDF no se atribuyen a una causa probada. Liquidación pendiente, criterio de moneda, precisión y momento de valuación son cuestiones por aclarar con el broker. No se fuerza una corrección ni se interpretan las diferencias de patrimonio como ganancias. Tampoco se sustituyen las 85 discrepancias de valuación detectadas en snapshots locales: las nuevas fuentes no prueban estar observando esos mismos instantes.

## Recuperación de referencias legacy sin mezclar canales

Entre los movimientos legacy del corte local hay **359 filas con cuenta explícita coincidente** y **156 sin esa coincidencia**, excluidas de este cruce. En las 359, **347 referencias de ticket encuentran exactamente un movimiento actual**. Son referencias entre versiones; no son 347 nuevas operaciones elegibles ni resuelven automáticamente las duplicaciones de canal.

Dentro de esas referencias hay **192 filas del canal de caja**. Sus importes se contrastan solo cuando existe un único componente explícito en la misma moneda:

- 186 comparaciones contra el neto explícito del broker coinciden a 0,01.
- Seis comparaciones contra un cargo explícito tienen cinco coincidencias y una discrepancia.
- Total: **191/192 coincidencias numéricas y un componente no conciliado**. La referencia compartida no prueba por sí sola que ambos campos tengan igual semántica; no se declara una comisión reescrita ni una pérdida real.

Las filas de caja legacy referenciadas cubren fechas de abril a agosto de 2026 y no aportan por sí mismas saldos de apertura para las ventanas seleccionadas de octubre. El payload legacy conserva distintas capturas/versiones; ordenar IDs y sumar filas no establece un libro continuo. Ninguna de las 359 filas se agrega al journal económico. Las 156 filas sin cuenta explícita permanecen separadas.

Los **40 pares sospechosos orden–liquidación** de Fase 2A siguen sin puente explícito. La consulta previa de 42 órdenes devolvió error y la ruta actual de órdenes devuelve una lista vacía. Ni la igualdad de importe ni la ausencia en esa lista autorizan a fusionar o borrar registros.

## Clasificación y uso permitido de la evidencia

| Objeto medido | Estado | Uso sustentado / límite |
|---|---|---|
| Procedencia de ocho PDFs, cuenta/fechas y suma de 16 tablas | VERIFIED | Respaldo documental de esos cierres bajo la presentación del broker; no costo histórico ni valuación independiente |
| 95 diferencias de cantidades listadas y 14 ecuaciones de caja con ambos extremos | VERIFIED, aritmética acotada | Control de integridad para las ocho ventanas; no cobertura global ni resultado económico |
| Dos ecuaciones de caja USD MEP sin un extremo explícito | PARTIAL | Registrar flujos y extremo conocido; falta documento que certifique el otro saldo, incluso si fuera cero |
| CSV, posiciones sin ticker, totales del gráfico y precios retrospectivos | PARTIAL | Investigación de formatos/valuaciones; no precios PIT ni retorno ajustado por flujos |
| 347 referencias legacy a un ticket actual | RECOVERABLE | Expedientes candidatos con referencia explícita; requieren reconciliar canal, versión y componente antes de cualquier journal |
| Comprobantes con detalle disponible pero PDF en error | RECOVERABLE para búsqueda; PDF NO VERIFICADO | Hay IDs concretos para pedir exportación alternativa; no se garantiza que el proveedor los pueda recuperar |
| PnL realizado por lote, neto de cuenta y CORE vs HOLD | NOT_IDENTIFIABLE | Faltan precondiciones contables y de atribución. No hay resultado ni ganador declarado |

El preflight registra cinco bloqueos: base de costo de apertura no documentada; eventos con fecha sin hora/zona atestada; cobertura de financiación desconocida; atribución de FX/títulos sin identificar; identidades legacy de órdenes pendientes. **No se llamó al reconciliador canónico `src/analysis/qeef/accounting.py` con datos inventados.** Las pruebas del contrato existente pasan, pero no constituyen una reconciliación real de esta cuenta. Para PnL realizado se necesitan lotes/costos iniciales; para resultado neto de cuenta siguen faltando cobertura de flujos/costos y valuación compatible, además de los requisitos del contrato.

Las 301 observaciones originales de QEEF conservan su cohorte, ventanas y clasificación histórica. Los nuevos documentos amplían la evidencia de cuenta; no convierten esos 301 casos en una conclusión sobre todo Quantia ni prueban rentabilidad de estrategias.

## Bloqueos priorizados y continuación de Fase 2B

| Prioridad | Riesgo de medición | Evidencia concreta que falta | Criterio de cierre |
|---|---|---|---|
| P0 | Doble conteo de órdenes, liquidaciones y canal de caja | Relación oficial orden → ejecución → boleto → liquidación para los 40 expedientes | Un hecho económico por identidad comprobada; conservar las versiones originales |
| P0 | Confundir aportes, financiación o caja desconocida con beneficio | Extracto continuo de una ventana pequeña, cobertura de flujos/costos/financiación y saldos explícitos por moneda | Ambos extremos y todos los conceptos atestados; sin convertir ausencia en cero |
| P0 | PnL realizado sin costo inicial | Lotes de apertura y costos documentados, incluidos gastos y transferencias de títulos | Base de costo y cantidades trazables al mismo owner/cuenta/instrumento |
| P1 | Comparar dos conceptos de patrimonio o precios no disponibles al decidir | Definición de historia/posición liquidada/pendiente, hora de corte, política FX y fecha de precio de especie CV 7000 | Identidad de valuación explicada sin ajustar importes para hacerlos coincidir |
| P1 | Costos o eventos incompletos | 39 PDFs de 2024, costos ausentes y aclaración del componente legacy discrepante | Cada discrepancia queda sustentada o explícitamente no identificable |
| P1 | Mezcla de fecha de ejecución, liquidación y observación | Especificación de reloj/zona del broker y referencias de disponibilidad | Intervalos canónicos construidos con timestamps atestados; recuperación no retrofechada |

El siguiente ensayo contable debe elegir **una sola ventana con ambas monedas documentadas**, recuperar los lotes iniciales y cobertura faltante, y preparar `AccountSnapshot`, `AccountEvent` y `Coverage` para el contrato QEEF en archivos aislados. Los controles ya obtenidos pueden reutilizarse, sin declararlos suficientes. No hace falta repetir toda la extracción para avanzar: los 39 IDs fallidos y los expedientes de identidad ya están inventariados para una exportación oficial alternativa. No se envió una solicitud al broker en esta tarea.

Si esa evidencia no se obtiene, la ventana queda PARTIAL/NOT_IDENTIFIABLE con su faltante preciso. CORE vs HOLD y cualquier cambio de estrategia u operación permanecen fuera de esta etapa.

## Reproducción y validación

Scripts nuevos: `scripts/capture_phase2b.py`, `scripts/capture_phase2b.js` y `scripts/reconcile_phase2b.py`; pruebas `tests/test_phase2b_reconciliation.py` y `tests/test_capture_phase2b.cjs`. El recolector usa únicamente GET sobre rutas observadas, valida cuenta de interfaz y header cuando está disponible, y se detiene ante 401/403/429. Los headers de sesión se usan en memoria y no se guardan en los artefactos. No carga credenciales ni automatiza el ingreso. Los lotes y archivos finales nuevos no sobrescriben rutas existentes.

Requisitos para análisis: Python 3 y **pypdf 6.10.0**, además del repositorio. Para captura: Node y Playwright CLI con sesión autenticada normal. Para tests: pytest y dependencias QEEF existentes. Desde la raíz del worktree, con un intérprete que contenga esos paquetes:

```powershell
# Replay offline del mismo corte; debe usar un directorio de salida nuevo.
python scripts/reconcile_phase2b.py `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --capture outputs/phase2b/broker-20261017-v1.json.gz `
  --activity outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz `
  --output outputs/phase2b/replay-new `
  --public-receipt outputs/phase2b/receipt-new.json

# Una captura nueva produce otro corte. No se requiere para repetir el análisis.
python scripts/capture_phase2b.py `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --retry-source outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz `
  --dates 20261001 20261002 20261005 20261006 20261007 20261009 20261013 20261016 `
  --session quantia-cocos-phase2b `
  --output outputs/phase2b/new-broker-cut.json.gz

python -m pytest tests/test_phase2b_reconciliation.py tests/test_cocos_activity_audit.py tests/test_cocos_tickets.py tests/test_phase2a_reconstruction.py tests/test_measurement_baseline.py tests/test_qeef_phase2.py -q --override-ini addopts=''
node tests/test_capture_phase2b.cjs
node tests/test_capture_cocos_activity.cjs
```

El análisis final está en `outputs/phase2b/reconciled-final/`: 16 PDF/CSV originales, seis respuestas API, ocho textos PDF, `reconciliation-private.json` y `summary.json`. El JSON conserva tablas, campos reportados, filas CSV originales, residuos por fecha, caja por moneda, referencias legacy y bloqueos. Los hashes de cada archivo privado quedan en el resumen; el recibo público contiene el hash de ese manifiesto, sin balances, cuentas ni IDs de tickets.

Validación ejecutada: **144 pruebas Python** (24 nuevas de Fase 2B y las 120 anteriores) y **15 escenarios de navegador** (seis nuevos y nueve anteriores), todos aislados. Cubren propietario ajeno, fechas incorrectas, filas numéricas desconocidas, posición pendiente separada, netos sin doble cargo, caja faltante, monedas no equiparadas, duplicados y referencias ambiguas. El replay desde los tres inputs congelados produjo **32/32 archivos idénticos byte por byte** y el mismo recibo público. Los hashes de código/pruebas usan UTF-8/LF; las fuentes privadas mantienen hashes de bytes originales.

Se revalidó `main` en `1687cdcce50895a9dfacb6dae04188fd8d8d5626`. PR #27 continúa OPEN/DRAFT en `1f73fbbdb2dc326ebfc70181b3c9459b92ce8d0e`; conserva el hallazgo de portabilidad LF/CRLF documentado en Fase 2A. No se modificó PR #25 ni #27, ni archivos operativos, estrategias o migraciones. El checkout principal se preservó; toda esta entrega sigue en el worktree y PR draft de auditoría.
