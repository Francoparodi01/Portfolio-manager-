# Cocos — extracción de actividad y comprobantes

Captura: 2026-10-16; cierre del informe: 2026-10-17. Extensión de [Fase 2A](PHASE2A_RECONSTRUCTION.md), en la misma rama y PR draft. Se consultó la sesión autenticada de Cocos y se conservaron respuestas y PDFs originales fuera de Git. No se modificaron la base operativa, las estrategias, las migraciones ni PR #25.

**La lectura directa del broker ahora está VERIFIED para el corte descargado. La reconstrucción contable de cuenta sigue PARTIAL y el PnL neto sigue NOT_IDENTIFIABLE.** El resultado técnico de captura no certifica apertura de cuenta, cobertura de todas las operaciones ni completitud patrimonial.

## Alcance obtenido

El [recibo agregado](evidence/cocos-activity-collection.json) fija los timestamps, hashes, cobertura, validación y límites de esta entrega. Las fechas de operaciones que devuelve la lista van de **2023-11-21 a 2026-10-09**. No se usa el primer registro como prueba de apertura de cuenta ni el último como cierre contable.

| Fuente / campo | Evidencia observada | Límite |
|---|---|---|
| Actividad / Movimientos | 670 filas, siete páginas, hasta `lastPage=true`; 670 identidades distintas | Fin de esta paginación, sin certificación de histórico completo del broker |
| Detalles de actividad | 670 respuestas; 665 con cuenta explícita coincidente | Cinco `CASH_OUT` dependen del contexto de la sesión y tienen estado `FAILED` |
| Compras y ventas en la lista | 234 BUY y 167 SELL | Filas de actividad por comprobante; una fila puede contener varias ejecuciones |
| Otros conceptos | 91 DEPOSIT, 98 WITHDRAWAL, 28 COCOS_PAY, 15 DIVIDEND, 21 OTHERS, 9 REDEMPTION y 7 SUBSCRIPTION | Son etiquetas del broker; cinco WITHDRAWAL fallidos no constituyen salidas de caja |
| Monedas de la lista | 637 ARS, 13 USD y 20 EXT | EXT se conserva; no se convierte en USD ni ARS sin evidencia de instrumento/conversión |
| Identificadores de comprobantes | 665 `idTicket` positivos distintos | Los cinco retiros fallidos no ofrecen este identificador |
| Comprobantes descargados | 626 PDFs originales sobre 665 descargas intentadas | 39 respuestas HTTP 500; comprobantes NO VERIFICADOS |
| Cargos estructurados | 355 mapas `commissions`; 414 mapas `fees` | Un mapa ausente no significa costo cero; no se confunden comisiones con todos los gastos |
| Fechas de cuenta | La lista no aporta balances de apertura/cierre | No permite certificar caja ni NAV de extremos por sí sola |
| Órdenes históricas | Consulta acotada de los 42 IDs de órdenes del owner: 42 respuestas HTTP 500 con `Order not found` | NO VERIFICADA su recuperación por esta ruta; no prueba que las órdenes nunca existieron |

La captura final abarca **2026-10-16 23:04:49.558–23:26:30.225 UTC** y conserva cada observación con su hora de lectura, en 103 fragmentos. SHA256 del gzip: `f8576afdf818ee0203a3d637a814231e77987c437c08c6f507c85cae98973dc8`. Una segunda lectura de la primera página fue idéntica a la inicial; no convierte las cientos de solicitudes en una transacción atómica.

Los **39 comprobantes no disponibles corresponden a 2024**: 18 BUY, 16 SELL, dos REDEMPTION, dos SUBSCRIPTION y un OTHERS. Se preservaron los detalles de estos movimientos y los errores de descarga. En un caso se reprodujo el HTTP 500 usando el botón normal «Descargar comprobante» de la interfaz; esa comprobación no demuestra la causa ni el estado de los otros 38. Su recuperación queda pendiente, sin afirmar que no existieron.

## Conciliación con los 109 fills de Fase 2A

**67 fills tienen referencia explícita al broker y coinciden en ticker, dirección, moneda y magnitud de cantidad.** Los 67 disponen ahora de mapas de comisiones y otros gastos con moneda. Los 109 `fees_ars` del corte PostgreSQL siguen NULL: esta recuperación es una nueva versión de evidencia, no una reparación de la tabla.

La conciliación mantiene namespaces distintos para números de boleto, órdenes e identidades de movimiento. No vincula registros por igualdad accidental de números, proximidad de fecha o semejanza económica.

- En **65/67**, el `gross_amount_ars` histórico coincide, dentro de 0,01 ARS, con el **neto** informado por Cocos. En los otros **2/67** coincide con el bruto. Este campo local no tiene una semántica económica uniforme.
- Los **42 fills de órdenes** no tienen referencia actual corroborada. Los **40 pares sospechosos orden–liquidación** siguen sin puente explícito común: **cero pares resueltos por ID**. No se eliminó ni fusionó ninguna fila.
- Los detalles nuevos permiten conservar fechas efectivas del broker para los 67 vínculos, junto con los clocks históricos `observed_after`/`date_only`. No se reescriben esos clocks ni se retrofecha la disponibilidad de la evidencia.
- Los 217 fills sin owner de la base original no entran en este cruce. Identificar una cuenta actual no autoriza asignar propietarios a filas legacy.

## Calidad de importes, costos y estados

**355 detalles contienen ambos mapas de costos en la moneda del importe y los 355 cierran `bruto firmado + comisiones firmadas + fees firmadas = neto firmado` a ±0,01.** Las 401 filas BUY/SELL tienen mapa `fees`, pero **46 no tienen mapa `commissions`**. La ausencia se conserva como desconocida, incluso si otros campos permiten comprobar una identidad aritmética.

En 647 filas son comparables el importe de lista y el de detalle: 642 son idénticos y cinco difieren exactamente en 0,01 ARS. Se guardan ambas versiones. Las cinco diferencias textuales de fecha corresponden a los retiros fallidos: fecha simple en lista y timestamp ISO en detalle, con la misma etiqueta de día. No se presentan como cinco errores de fecha económica.

**Los cinco `CASH_OUT` con estado `FAILED` se conservan para auditoría y llevan `EXCLUDED_FAILED_OR_CANCELED`.** Sus importes visibles no entran en una suma de flujos de caja. Para el resto, `REQUIRES_ACCOUNTING_RECONCILIATION` impide confundir una fila descargada con un evento contable ya conciliado.

## Qué agregan los PDFs

El extractor offline conserva el texto completo y el hash de cada PDF. Reconoce cuenta, fechas y número de documento, y separa:

- importe bruto y neto;
- líneas de comisión, débito/crédito de derechos e IVA, con importe original;
- cantidad, precio, importe, registro, número de orden documental, fecha y hora de cada ejecución reconocida.

El número de orden del PDF es un identificador documental; no se equipara al UUID de una orden histórica. La hora impresa conserva `clock_timezone=null`: no se le asigna automáticamente una zona horaria. Los formatos ISO y `dd/mm/yyyy` se normalizan con formato declarado y conservan el texto original.

`VERIFIED_DOCUMENT_ARITHMETIC` verifica solo aritmética del boleto reconocido y su cuenta: suma de importes de ejecuciones frente al bruto; precio × cantidad frente al importe por ejecución; y bruto más/menos cargos documentados frente al neto, con tolerancia fija de **0,01 en la moneda del documento**. La igualdad con la API se informa por separado. No significa rendimiento verificado ni exhaustividad de gastos de cuenta.

Se observaron totales de tres decimales con componentes mostrados a dos: los originales se preservan y los residuos que superan 0,01 quedan PARTIAL. También hay instrumentos con distinta base de cotización: un boleto de bono muestra precio por nominal distinto del precio de la línea de ejecución. **No se aplica una división por 100 ni otra conversión inferida para hacer cerrar el documento.** Los recibos, dividendos y otros formatos permanecen PARTIAL para aritmética aunque se pueda identificar la cuenta y extraer sus campos.

Los **626 PDFs identifican explícitamente la misma cuenta**. Se reconocen 369 boletos, con 448 líneas de ejecución; 309 cumplen las tres pruebas aritméticas y 60 quedan PARTIAL. Los otros 257 documentos también permanecen PARTIAL para aritmética, incluidos 12 de FCI cuya cuenta se reconoce mediante su etiqueta explícita. Los 369 boletos coinciden con el detalle API en bruto, neto y cantidad a la tolerancia declarada; esto no subsana sus inconsistencias internas.

En total, **317/626 PDFs permanecen PARTIAL y 309/626 tienen aritmética documental VERIFIED**. Hay líneas explícitas de costos en 380 documentos. No se publica un total de comisiones de cuenta ni un resultado de PnL a partir de estos conteos. Los formatos sin campos reconocidos conservan texto y valores desconocidos, sin convertirlos en cero.

## Prioridades y siguiente paso

| Prioridad | Hallazgo | Trabajo pendiente |
|---|---|---|
| P0 | Cinco intentos de retiro fallidos visibles junto a movimientos | Toda construcción de caja debe respetar estado y canal; conservar los intentos sin sumarlos |
| P0 | 65 importes históricos llamados brutos representan netos; 40 pares sin identidad canónica | Usar campos y comprobantes originales; cerrar orden → ejecución → boleto sin deduplicar por semejanza |
| P0 | Apertura, cierre y cobertura global no atestados | Obtener estados de cuenta y balances por moneda en extremos de una ventana concreta |
| P1 | Costos recuperados para 67 fills; comisión ausente en 46 filas de la lista completa | Revisar comprobantes por evento; no convertir ausencia en cero ni extrapolar una tarifa |
| P1 | 39 comprobantes de 2024 no descargables | Obtener esos documentos o una exportación oficial alternativa; conservar HTTP 500 y detalle disponible como versiones separadas |
| P1 | Bases de cotización y formatos de documentos heterogéneos | Documentar unidades de instrumentos y ajustar parsers solo con evidencia, manteniendo residuos originales |
| P1 | EXT, dividendos y movimientos en especie | Separar títulos, caja y gastos por moneda; no crear equivalencias de divisas |

Para Fase 2B, el insumo inicial es este conjunto de movimientos y comprobantes con procedencia, más los snapshots congelados. Elegir una ventana con apertura/cierre verificables, resolver los identificadores pendientes y probar las identidades de cantidades/caja antes de alimentar el contrato contable QEEF. CORE vs HOLD, scoring y cambios operativos siguen fuera de alcance. Los 301 casos originales QEEF conservan su cohorte y conclusión histórica.

## Reproducción y archivos privados

Los scripts no cargan la aplicación ni conectan a PostgreSQL. El recolector usa Playwright CLI en una sesión autenticada normal, valida cuenta de interfaz y header cuando está disponible, y permite únicamente GET a las rutas observadas de movimientos, detalles y descarga de boletos. No envía reportes por email ni modifica órdenes. Se detiene ante 401, 403 o 429.

Las solicitudes se guardan en lotes inmutables. Si se interrumpe la ejecución, se reanuda con el mismo directorio `--parts`; el hash de contexto y la configuración deben coincidir. El archivo final y los directorios de análisis deben ser nuevos. Reanudar amplía el intervalo de captura y no prueba atomicidad.

```powershell
# Abrir Playwright CLI y completar el ingreso normal antes de capturar.
npx --yes --package @playwright/cli playwright-cli -s=quantia-cocos-access open https://app.cocos.capital/activity --headed

python scripts/cocos_activity_audit.py capture `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --output outputs/cocos-activity-audit/new-capture.json.gz `
  --parts outputs/cocos-activity-audit/new-checkpoints

python scripts/cocos_activity_audit.py capture --historical-orders-only `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --output outputs/cocos-activity-audit/new-order-probe.json.gz

python scripts/cocos_activity_audit.py consolidate `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --capture outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz `
  --output outputs/cocos-activity-audit/replay-new

# Este paso requiere pypdf 6.10.0; el runtime local de Codex lo incluye.
python scripts/extract_cocos_tickets.py `
  --context-capture outputs/phase2a/live-capture.json.gz `
  --capture outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz `
  --consolidated outputs/cocos-activity-audit/replay-new `
  --output outputs/cocos-activity-audit/pdf-replay-new

python scripts/cocos_activity_audit.py receipt `
  --capture outputs/cocos-activity-audit/live-activity-20261016-v6.json.gz `
  --consolidated outputs/cocos-activity-audit/replay-new `
  --pdf-output outputs/cocos-activity-audit/pdf-replay-new `
  --orders-capture outputs/cocos-activity-audit/historical-orders-20261016.json.gz `
  --output outputs/cocos-activity-audit/replayed-public-receipt.json

python -m pytest tests/test_cocos_activity_audit.py tests/test_cocos_tickets.py tests/test_phase2a_reconstruction.py tests/test_measurement_baseline.py tests/test_qeef_phase2.py -q --override-ini addopts=''
node tests/test_capture_cocos_activity.cjs
```

Las salidas privadas están en `outputs/cocos-activity-audit/consolidated-v1/` y `outputs/cocos-activity-audit/pdf-analysis-v2/`. Contienen `movements-private.csv/json`, `historical-comparison-private.json`, `identity-review-private.json`, los PDFs originales, `tickets-private.json`, `ticket-executions-private.csv`, `ticket-charges-private.csv` y textos por PDF. Los resúmenes registran hashes de entrada/salida. El PR solo contiene código, pruebas, documentación y agregados sin nombres, cuentas, identificadores bancarios ni importes personales.

## Validación ejecutada

- **120 pruebas Python**: 32 de captura/consolidación/PDF, 26 de Fase 2A, 20 de Baseline y 42 de QEEF. Todas pasaron con fixtures aislados; no consultan ni modifican la base operativa.
- **9 escenarios del protocolo de navegador** pasaron con respuestas simuladas: cubren GET, control de cuenta, bloqueo de acceso y etapas de captura. La evidencia de acceso real es el corte privado y su recibo, no estas simulaciones.
- La consolidación se repitió desde el gzip congelado: **631 archivos idénticos byte por byte**, incluidos 626 PDFs. La extracción PDF se repitió con pypdf 6.10.0: **630 archivos idénticos**, incluidos texto, JSON, dos CSV y resumen. También se reprodujo el recibo agregado con el mismo SHA256.
- Los hashes de los seis archivos de código/pruebas del recibo se calculan sobre UTF-8 con saltos LF, evitando el falso cambio CRLF/LF identificado en PR #27. El PR #27 sigue draft en `1f73fbbdb2dc326ebfc70181b3c9459b92ce8d0e` al cierre; no se modificó.
