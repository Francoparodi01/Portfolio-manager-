# Fase 2A — Reconstrucción histórica y conciliación

Fecha: 2026-10-16. Base revisada: `main` **1687cdcce50895a9dfacb6dae04188fd8d8d5626**, posterior al merge de PR #26. Rama: `audit/phase2a-reconstruction-20261016`. Captura PostgreSQL: **2026-10-16 12:50:27.619735 UTC**, transacción `REPEATABLE READ / READ ONLY` confirmada. Este informe constituye una reconstrucción de evidencia, sin cambios operativos ni declaración de rentabilidad.

**Resultado: PARTIAL.** Se reconstruyeron 2.337 estados de cartera del propietario explícito, 29.751 posiciones, 2.336 intervalos, 57 cierres diarios observados y un inventario de 109 registros de fills. Se recuperaron cinco registros derivados ausentes del SQL local, conservando su versión del backup. **PnL realizado bruto, PnL neto, comisiones totales y retorno ajustado por flujos siguen NOT_IDENTIFIABLE.** No se reemplazan por cero.

El hallazgo de mayor impacto nuevo es la identidad de ejecución: **80 de los 109 fills forman 40 pares sospechosos entre órdenes y movimientos**. La igualdad de clave primaria no detectaba este problema. No es válido sumar todos los registros como operaciones independientes, ni deduplicarlos por semejanza sin comprobantes. Además, **160 tickets tienen una fila de caja y otra de títulos**; son canales que necesitan conciliación, no dos operaciones que puedan sumarse indistintamente.

El [recibo agregado reproducible](evidence/phase2a-reconstruction.json) contiene cortes, hashes, denominadores, comparaciones y límites. El [recibo de revisión](evidence/phase2a-source-review.json) registra acceso a fuentes, hashes de #27, pruebas y replay. Los importes de cuenta, identificadores y payloads permanecen en artefactos privados ignorados por Git.

## 1. Revisión de main, Baseline, QEEF y PR #27

Se revisaron [Measurement Baseline V1](MEASUREMENT_BASELINE_V1.md), `init.sql`, `src/collector/schema_migrations.py`, los escritores de `src/collector/db.py`, parsers de fills/movimientos, el scraper, reconstrucción histórica existente y los contratos contables de `src/analysis/qeef/accounting.py`. No se ejecutaron migraciones ni el bootstrap de la aplicación.

La reconstrucción histórica existente (`scripts/historical_reconstruction`) clasifica **señales**, no demuestra PnL de cuenta. Su posible inferencia legacy de propietario no se utiliza en esta fase. Se respeta el contrato contable de QEEF: cobertura de costos, flujos, acciones corporativas, bases de costo, financiación, moneda e identidad son requisitos; coincidencia de cantidades o saldo por sí sola no certifica rendimiento. Esta fase entrega observaciones y bloqueos para ese contrato, sin crear un motor alternativo de PnL.

### Revisión de PR #27

[PR #27](https://github.com/Francoparodi01/Portfolio-manager-/pull/27), HEAD **1f73fbbdb2dc326ebfc70181b3c9459b92ce8d0e**, estaba **OPEN / DRAFT**, sin merge, al revisarlo. Agrega una sección documental, un verificador del recibo público y pruebas. Su alcance declarado es correcto: no pretende haber consultado fuentes privadas ni certificar PnL. No incorpora evidencia histórica nueva.

**Hallazgo P2 — el gate `--check-code` no es portable y falla con el checkout estándar.** En `scripts/verify_measurement_receipt.py:76–85`, la comparación usa bytes del archivo de trabajo. El recibo original contiene una mezcla de hashes de archivos LF y CRLF. Con `core.autocrlf=true`, fallan seis de ocho archivos; sobre los blobs Git LF fallan los otros dos (`capture_qeef.py` y `audit_qeef_phase2.py`). Por tanto, cambiar simplemente a hashes de blobs tampoco reproduce todos los hashes originales.

- `python -m pytest tests/test_verify_measurement_receipt.py -q --override-ini addopts=''`: **8 passed**.
- `python scripts/verify_measurement_receipt.py`: **PASS**, invariantes internos del recibo.
- `python scripts/verify_measurement_receipt.py --check-code`: **FAILED**, primer archivo `scripts/audit_measurement_offline.py`.
- Los tests prueban archivos sintéticos, pero no el contrato de finales de línea del checkout real. El fallo no demuestra modificación semántica del código ni invalida los hashes de los datos privados.

Corrección propuesta para un trabajo separado sobre #27: conservar los hashes históricos de bytes como evidencia del entorno original y añadir un manifiesto explícito de contenido UTF-8/LF o identidad Git, con pruebas de checkout LF/CRLF. Esta entrega registra el hallazgo y no modifica #27. Los hashes de código nuevos de Fase 2A declaran explícitamente normalización LF; los hashes de archivos de datos siguen siendo de bytes originales.

## 2. Fuentes, cobertura y procedencia

| Fuente | Acceso y evidencia en esta fase | Límite |
|---|---|---|
| PostgreSQL local operativo, `cocos_db` | **VERIFICADA** lectura de 13 relaciones seleccionadas; captura privada nueva de texto PostgreSQL que preserva numeric/JSON, sin escrituras | Holdings limitados al owner explícito. Tablas de identidad legacy/movimientos se mantienen separadas; no se considera todo el histórico de Quantia |
| Backup Supabase suministrado, `db_cluster-09-10-2026@17-43-09.backup.gz` | **VERIFICADA** integridad SHA-256 `901df029a557ba9147be00e4317e6e334d7579592525e5f7e44176e8e3d41579`; lectura completa gzip/COPY, sin ejecutar SQL; 13 relaciones extraídas en SQLite aislado | Fecha de extracción, zona horaria y completitud del proyecto siguen desconocidas; el nombre del archivo no prueba el corte |
| Supabase original en vivo | **NO VERIFICADA**; nuevo intento de lectura falló antes de obtener tablas (`InternalServerError`, SQLSTATE `XX000`) | No se atribuye una causa específica al fallo ni se declara que el backup cubra todo el origen |
| QEEF original, captura 2026-10-14 04:53:59 UTC | **VERIFICADA** lectura y owner token; 2.233 snapshots, 28.503 posiciones, 109 fills; claves/campos financieros comparados con captura actual | Captura parcial de una cohorte, no extracto de cuenta ni todas las versiones de decisiones |
| Captura Baseline, 2026-10-16 04:05:47 UTC | **VERIFICADA** lectura; 2.335 snapshots, 29.727 posiciones, 109 fills | El nuevo corte agrega 2 snapshots y 24 posiciones; no mezclar denominadores |
| Seis CSV de `quantia_reconstruction/` | **VERIFICADA** lectura y hashes; sin claves duplicadas | Origen/corte no atestados; comparación de claves, no igualdad de payloads ni equivalencia vacío/NULL |
| PostgreSQL antiguo `cocos_pg_db`, puerto 5433 | **VERIFICADA** consulta de catálogo: cinco tablas, un snapshot y dos posiciones; no owner explícito | No aporta una apertura atribuible al propietario actual; no se incorpora a su cartera |
| Broker | **PARTIAL**: payloads persistidos en PostgreSQL/backup, con órdenes, actividad, caja e identificadores | Extractos originales, boletos, costos desglosados, cobertura completa de cuenta y lectura directa actual: **NO VERIFICADOS** |

La extracción consulta las tablas necesarias con SELECTs acotados, `default_transaction_read_only=on`, timeout y guardia del host local. El dump se trata como datos, nunca como instrucciones. No se ejecutan sus roles, funciones, DDL ni comandos. Las salidas se crean con nombres nuevos; se rechazan rutas existentes.

### Conciliación de versiones

El archivo privado `evidence.sqlite` conserva `(source, table, key_hash, content_hash, scope, row_json)`, con una versión por fuente y clave. El manifiesto registra el hash de cada fuente. No existe una tabla que elija silenciosamente la versión “más reciente”. Se comparan las columnas del esquema del backup, sin equiparar columnas posteriores ausentes a NULL.

| Relación | Filas comparables del backup | Ausentes en local | Contenido cambiado |
|---|---:|---:|---:|
| Snapshots del owner | 1.868 | 0 | 0 |
| Posiciones del owner por padre explícito | 24.296 | 0 | 0 |
| Raw snapshots del owner por padre explícito | 1.868 | 0 | 0 |
| Broker fills, todos los scopes separados | 310 | 0 | 0 |
| Broker movements sin owner | 499 | 0 | 0 |
| Decision log, scopes separados | 1.206 | 0 | 317 |
| Execution plans | 130 | 0 | 0 |
| HOLD observations | 528 | 0 | 0 |
| Plan execution attributions | 43 | 1 | 42 |
| Attribution plans | 80 | 1 | 0 |
| Attribution movements | 62 | 3 | 0 |
| Corporate events | 2 | 0 | 0 |
| Corporate event applications | 1 | 0 | 1 |

Se recuperaron **1 atribución + 1 vínculo de plan + 3 vínculos de movimientos** como filas originales privadas del backup. La atribución tiene owner NULL; sus hijos no tienen columna owner. **RECOVERABLE** para trazabilidad de esa versión; no se atribuyen al owner actual. Los patrones de recomputación/borrado en `plan_follow_attribution.py` explican una vía posible, pero no prueban el motivo de cada ausencia.

Las capturas QEEF y Baseline no pierden claves de snapshots, posiciones o fills ni cambian los campos financieros seleccionados. Esto no afirma igualdad de todas sus columnas. Las revisiones de decisiones y outcomes siguen versionadas: no se usan para volver a fechar conocimiento ni calcular rentabilidad.

Los CSV contienen 320 fills, 509 movimientos, 5.834 snapshots, 58.154 posiciones, 551 HOLD y 5.834 raw snapshots. En la extracción acotada actual aparecen las 320/509/551 claves de sus tablas completas, pero solo 2.012 snapshots/raw y 25.921 posiciones: el resto pertenece al perímetro fuera de la captura owner-scoped. **No son pérdidas de filas demostradas**; Baseline ya había comparado las claves contra el catálogo global.

## 3. Reconstrucción de cartera, caja y evolución patrimonial

**Ventana observada:** 2026-08-21 22:22:42.064086 UTC a 2026-10-16 12:32:42.389976 UTC. Son 57 fechas locales con observaciones; el último día está incompleto. El archivo diario elige la última observación disponible de cada fecha UTC−03, sin rellenar días, sin interpolar precios y sin declarar que sea el cierre de mercado.

Cada estado conserva cantidades, precio observado, costo medio reportado, valor invertido, caja ARS y `NAV reportado = valor invertido reportado + caja reportada`. La serie permite describir evolución de saldos. **Variación de NAV no equivale a ganancia** sin aportes/retiros y demás flujos completos. `avg_cost` reportado tampoco prueba una base fiscal o contable reconstruida.

| Verificación | Resultado |
|---|---|
| Cantidades/precios/valor de posiciones disponibles en ARS | 29.751 posiciones en 2.337 snapshots; sin componentes ausentes en esta extracción |
| Suma cantidad × precio frente a total invertido | 2.252 snapshots cierran a ±0,01 ARS; 85 no cierran |
| Snapshot normalizado frente a raw envelope | 2.337/2.337 coinciden en owner, UUID, timestamp y claves de posiciones; precios, total y caja coinciden a ±0,01 ARS |
| Cantidades exactas entre raw y numeric | 2.008/2.337 snapshots coinciden exactamente; 329 diferencias de representación en posiciones, máximo absoluto `2.66453525910037569701671600341796875e-17` |
| Cambios entre estados consecutivos | 42 intervalos con cambios de cantidad; 2.294 sin cambios visibles. Ninguno certifica por sí solo cobertura de movimientos/caja |

Las diferencias numéricas minúsculas aparecen en `USESPECIE` y `ARSCABLE`, compatibles con serialización de float a numeric; no se redondean ni sustituyen los originales. No explican pérdidas económicas. Los **85 residuos de valuación** persisten aun con coherencia entre tablas y raw: `raw_snapshots` guarda una serialización normalizada del mismo objeto de la aplicación, no un comprobante independiente del broker. El scraper puede normalizar total invertido cuando la diferencia supera `max(1000 ARS, 2%)`; por eso un objeto internamente aceptado no garantiza concordancia de sus componentes a un centavo. No se afirma que esa regla haya originado cada residual histórico.

### Compras, ventas e identidad de ejecución

Existen **65 registros BUY y 44 SELL** con owner explícito. Son conteos de filas persistidas, **no de ejecuciones únicas confirmadas**. Los otros 217 fills sin owner quedan fuera. La reconstrucción retiene cada fila y su versión, sin sumar una supuesta negociación total.

- **40 pares / 80 filas** coinciden en fuente, owner, reloj observado, ticker, dirección y cantidad. Cada par contiene una fila del canal órdenes y otra del canal movimientos. **38 pares tienen igual importe; dos difieren. Los 40 tienen precios diferentes.** No comparten un identificador directo de ejecución corroborado en los payloads inspeccionados. Se generan candidatos revisables, sin eliminarlos ni elegir uno.
- El backup ya contiene **31 pares / 62 filas** entre sus 93 fills del owner. Es un problema recuperado del histórico, no una duplicación introducida por esta auditoría.
- **26 fills preceden al primer snapshot del owner**; 83 caen dentro de la ventana. En 81 filas, el reloj `observed_after` coincide con un snapshot y cada cantidad individual coincide con el cambio observado. **80 de esas filas son los 40 pares:** la suma del par duplicaría ese cambio. Esta contradicción impide utilizar el ledger completo sin resolver identidad.
- **106 timestamps `observed_after` y 3 `date_only`; cero `exact`.** El escritor infiere tiempos desde deltas de posiciones; su consulta histórica `_portfolio_observation_times` no filtra por owner. Se constata el riesgo estructural, no una contaminación efectiva probada. Una coincidencia de timestamp no es confirmación independiente: el tiempo fue derivado precisamente de esas posiciones.
- Los 109 vínculos a movimientos por `(source, external_id)`, ticker, dirección y moneda son únicos y sus cantidades absolutas coinciden. Las 28 diferencias de signo responden a representación de dirección; no son diferencias de magnitud ni justifican invertir importes de caja automáticamente.

### Precios y comisiones

En **42/109** fills, `gross_amount_ars` no coincide con cantidad × precio dentro de 0,01 ARS. En los **109/109**, el importe guardado coincide con el valor absoluto del `amount` de su payload; en 44 hay precio explícito coincidente, y **65** llevan `_price_derived_from_amount=true`. Un precio derivado del importe no es una segunda observación independiente de ejecución.

Los **109 `fees_ars` son NULL**. El censo de claves de payload no recuperó campos explícitos de comisión/fee/tax. Esto no prueba ausencia de costos: podrían estar embebidos o faltar. La diferencia entre importe y cantidad × precio **no se clasifica como comisión**; faltan desgloses de derechos, impuestos, liquidación y eventuales otras partidas. El parser admite tanto claves de importe bruto como neto, por lo que el nombre `gross_amount_ars` no certifica semántica bruta del dato original.

### Caja y movimientos

Los **515 movimientos no tienen owner en el esquema**. Dos fills con owner explícito conservan `id_account`; la misma cuenta aparece en **359 movimientos**. Es un puente de identidad **RECOVERABLE**, con alcance provisional, no una reasignación retroactiva ni certificación del período completo. Otros **156** movimientos carecen de ese puente directo. Se preservan todos en cuarentena, sin entrar al saldo del owner.

Se identificó un canal específico de **192 registros de caja** con `id_cash_movement`, sin IDs duplicados: 97 BUY, 62 SELL, 11 DEPOSIT, 16 WITHDRAWAL y 6 DIVIDENDOS EN ESPECIE, todos etiquetados ARS. La etiqueta de movimiento en especie no autoriza tratar su importe como cash económico.

Hay **160 tickets válidos repetidos / 320 filas**, todos pares caja–títulos; 159 comparten moneda y uno difiere. No se consideran duplicados borrables ni se agregan como eventos independientes. Se excluyen identificadores vacíos y cero de la comparación; igual vacío no demuestra identidad. Los saldos de caja y sus fechas se conservan, pero no se fuerza un roll-forward usando canales mezclados, signos de títulos como signos de caja o coberturas no atestadas.

El inventario global incluye 15 depósitos ARS, 17 retiros ARS y uno USD, siete dividendos EXT y uno ARS sin importe, seis movimientos en especie y otros EXT. Es un inventario sin atribución de propietario; no son flujos externos elegibles para ajustar el NAV del owner.

## 4. Propietarios, relaciones y recuperabilidad

**36 fills apuntan a decisiones con owner NULL**, y 73 a decisiones del mismo owner. Se conserva el enlace observado, pero no se atribuye la decisión NULL al propietario del fill ni a una estrategia. La coincidencia estable de backup/local no resuelve una identidad faltante.

**14 referencias HOLD** contienen dos timestamps (siete registros por timestamp) en `portfolio_snapshot_id`. Los dos tiempos identifican de forma única snapshots del mismo owner. La historia Git del productor en `a48ca44:scripts/run_analysis.py` muestra que `_portfolio_snapshot_id_from_snapshot` devolvía `scraped_at.isoformat()`; en `0466f0d` ya devuelve `snapshot_id`. Este respaldo permite conservar **14 candidatos RECOVERABLE** y nueve links por UUID **VERIFIED** en cuanto a identidad del snapshot. No demuestra qué binario produjo cada HOLD ni una tesis completa e inmutable; tampoco se actualiza la referencia operativa.

| Evidencia / afirmación | Clase | Justificación y uso permitido |
|---|---|---|
| Bytes del backup y captura local; claves/contenidos comparados | VERIFIED | Hashes, lectura completa, transacción RO y comparación reproducible; verificación técnica, no rendimiento |
| Estados y NAV reportados del owner | PARTIAL | Owner/linaje observados, pero 85 residuos y cobertura de flujos/costos no cerrada |
| 109 filas de fills | PARTIAL | Cantidades/importe observables; 40 pares sin identidad canónica, relojes derivados, costos desconocidos |
| Pares de ticket caja–títulos | RECOVERABLE | Identidad compartida explícita; falta resolver papel contable, moneda y liquidación sin contar dos veces |
| 359 movimientos con account ID anclado | RECOVERABLE | Puente explícito a dos fills del owner; falta atestar cuenta/período y canal, sin promoción automática |
| 14 HOLD con timestamp legacy | RECOVERABLE | Coincidencia única del mismo owner y código histórico compatible; vínculo derivado auditado, no reparación |
| Cinco filas ausentes del SQL local | RECOVERABLE | Contenido original recuperado del backup, preservado como versión legacy |
| Decisiones owner NULL y movimientos sin identidad de cuenta suficiente | NOT_IDENTIFIABLE para atribución al owner | Un vínculo externo/proximidad temporal no reemplaza evidencia de propiedad |
| PnL bruto realizado, PnL neto, costos totales y retorno de cuenta | NOT_IDENTIFIABLE | Ejecuciones, apertura, base de costo, flujos/costos y acciones corporativas incompletos; los outputs quedan NULL |

## 5. Contraste con QEEF y Baseline

Los **301 casos** de QEEF original siguen siendo 278 decisiones + 23 HOLD de su captura original. No son 301 operaciones ni toda la base. Su resultado “cero casos económicamente VERIFIED” expresa insuficiencia de evidencia en esa cohorte, no ganancia cero ni una conclusión de todas las estrategias. Baseline posterior tenía otra cohorte de 313 casos y otra captura; no se modifica ni se vuelve a etiquetar el resultado congelado.

Fase 2A confirma estabilidad de campos financieros seleccionados de los 109 fills originales, pero añade un diagnóstico que QEEF/Baseline no cerraban: **la ausencia de claves duplicadas no descarta representaciones múltiples de un mismo hecho económico**. Tampoco convierte los 328 gaps del replay contable previo en 42: son métricas distintas. Los 328 eran residuos bajo los eventos y reglas de aquel replay; los 42 de este informe cuentan intervalos con cambio de cantidades observado, sin atribuir eventos. Los 40 pares ayudan a investigar esa discrepancia, pero no prueban la causa de cada gap anterior.

Las 317 decisiones revisadas, las 42 atribuciones modificadas y la aplicación corporativa modificada se preservan como versiones. Los defectos de mercado/PIT documentados por Baseline —incluidos 236 OHLC inválidos y velas sin disponibilidad/ajustes suficientes— siguen abiertos; esta extracción no repite el censo de 59/73 tablas ni revalida todo el mercado. No se usan precios revisados para construir un backtest o completar retornos.

## 6. Prioridades para la medición

| Prioridad | Hallazgo | Riesgo financiero de medición | Próxima acción |
|---|---|---|---|
| P0 | 40 pares orden–movimiento sin identidad canónica | Duplicar cantidades, negociación, exposición, costos o atribución de resultados | Obtener vínculo broker orden → ejecución → boleto → liquidación; resolver por comprobante, no por fecha/importe |
| P0 | Caja/fees sin cobertura por owner y período | Confundir aportes con ganancias, calcular neto con costos cero o contar dos canales | Extracto identificado, costos e impuestos desglosados y balances de ambos extremos por moneda |
| P1 | 85 discrepancias de valor; raw no independiente | Valuación/base de costo incorrectas o circularmente “verificadas” | Confrontar con respuesta original/estado de cuenta del mismo instante y especie |
| P1 | Relojes derivados y consulta de observación sin owner | Asignar operación al intervalo o propietario equivocado | Recuperar hora/fecha efectiva y liquidación del broker; mantener límites temporales explícitos |
| P1 | 36 vínculos a owner NULL, 14 IDs HOLD legacy | Atribución falsa a una decisión/estrategia | Expediente de identidad separado y evidencia explícita de cada resolución |
| P1 | Instrumento/ratio/acciones y versiones PIT incompletos | Cantidad o retorno sobre bases no comparables | Maestro versionado y eventos corporativos documentados por fecha y moneda |
| P2 | Cinco filas derivadas ausentes, versiones revisadas | Perder explicación histórica | Conservar staging y registro de versiones sin reescribir operación |
| P2 | Gate de hashes #27 no portable | Falso bloqueo o falsa interpretación de drift | Separar hashes de bytes del origen y contrato portable de código |

Estas prioridades no cuantifican pérdidas reales ni implican cambios de estrategias.

## 7. Fase 2B propuesta y criterio de salida

1. **Obtener el perímetro contable del broker.** Extracto de cuenta con titular/cuenta verificable, rango de cobertura, posiciones y caja de apertura/cierre por moneda; órdenes, ejecuciones, boletos, comisiones, derechos, impuestos, dividendos y acciones corporativas. Los campos desconocidos conservan NULL. Documentar qué falta y qué sistema puede exportarlo.
2. **Resolver los 40 expedientes de identidad.** Vincular cada orden y movimiento a ejecución/boleta mediante IDs explícitos. El expediente registra fuentes, versiones, resolución y quién/qué la respalda. Los dos pares de importe distinto requieren investigación propia. No fijar por anticipado que habrá 69 operaciones únicas.
3. **Separar canal de títulos y canal de caja.** Resolver los 160 tickets compartidos, los 359 candidatos de cuenta y la discrepancia de moneda. Solo después, preparar un journal por owner/cuenta/moneda con signos y fechas efectivos documentados. No promover las 217 filas de fills legacy por la existencia de un único owner actual.
4. **Cerrar una ventana pequeña con ambos extremos verificables.** Mostrar apertura + compras − ventas + acciones = cierre en cantidades; apertura de caja + flujos + liquidaciones − cargos = cierre por moneda. Verificar 85 residuos y base de costo inicial, sin reemplazar saldos reportados para que las identidades cierren.
5. **Pasar el contrato contable QEEF existente.** Con cobertura atestada, identidad única, costos observados, precios/FX/instrumentos y clocks adecuados, alimentar `AccountSnapshot`, `AccountEvent` y `Coverage` en un entorno aislado. Solo si supera las identidades canónicas se podrá publicar PnL para esa ventana. Lo demás permanece PARTIAL/NOT_IDENTIFIABLE.
6. **Conservar trazabilidad de recuperación.** Todo vínculo nuevo tendrá `recovered_at`, fuente de soporte y versión; no se retrofecha como disponible al decidir. La comparación CORE vs HOLD y cualquier cambio operativo siguen fuera de esta etapa.

Criterio de salida: cero mezcla de owners en el journal elegible; cada hecho económico contado una vez con identidad comprobada; cantidades y caja conciliadas bajo tolerancias explícitas; costos/cobertura/monedas conocidos; mismo input congelado reproduce el resultado; ningún resultado neto aparece si falta una precondición. Si el broker no aporta la evidencia, entregar el período como no identificable con el faltante concreto.

## 8. Artefactos y reproducción

Archivos nuevos: `scripts/capture_phase2a.py`, `scripts/reconstruct_phase2a.py`, `tests/test_phase2a_reconstruction.py`, este informe y el recibo público. Único ajuste de configuración de repositorio: permitir el nuevo test en `.gitignore`. Sin cambios de `src`, migraciones, scoring, optimizer, planner, shadow, configuración runtime o PR #25; sin merge.

Requisitos: Python 3, `asyncpg` en el contenedor existente para la captura; el análisis offline usa biblioteca estándar. Pytest y dependencias existentes de QEEF para las pruebas. Desde la raíz del nuevo worktree, en PowerShell:

```powershell
python scripts/capture_phase2a.py --output outputs/phase2a/live-capture.json.gz

$baseline = 'C:\Users\Franco\Documents\Codex\worktrees\quantia-measurement-baseline-v1\outputs\measurement-baseline-v1'
$principal = 'C:\Users\Franco\OneDrive\Escritorio\backend\cocos_copilot'
$qeef = 'C:\Users\Franco\OneDrive\Escritorio\backend\.worktrees\quantia-qeef-v1\outputs\qeef\capture-final-20261014.json'
python scripts/reconstruct_phase2a.py `
  --capture outputs/phase2a/live-capture.json.gz `
  --backup 'C:\Users\Franco\Downloads\db_cluster-09-10-2026@17-43-09.backup.gz' `
  --manifest "$baseline\backup\manifest.json" `
  --old-capture $qeef --baseline-capture "$baseline\live-owner-20261016.json" `
  --csv-dir "$principal\quantia_reconstruction" `
  --output outputs/phase2a/replay-new

python -m pytest tests/test_phase2a_reconstruction.py tests/test_measurement_baseline.py tests/test_qeef_phase2.py -q --override-ini addopts=''
```

Para repetir el corte de este informe se reutiliza **la captura congelada**, SHA-256 `ff0c2b1ae71a1607022f94b7b724585402d3ffbc187ff4b4205fa704a55e2020`. Una nueva captura produce otro corte; debe usar otro nombre y no se espera que tenga los mismos conteos. El directorio de output debe ser nuevo. Los SQL operativos y el dump nunca reciben escrituras.

Salidas privadas en `outputs/phase2a/final/`:

- `evidence.sqlite`: 13 relaciones, versiones de backup/local separadas, claves/hashes/scope y filas originales.
- `reconstruction-private.json`: estados, cantidades/precios reportados, intervalos, fills, 40 candidatos, movimientos en cuarentena y vínculos HOLD.
- `daily-observed-nav-private.csv`: 57 últimas observaciones diarias con caja, invertido, NAV y residuo; PnL vacío explícitamente no identificado.
- `missing-attributions-private.json`: cinco filas originales recuperadas, sin restauración operativa.
- `summary.json`: agregados publicables, hashes de entradas/salidas y código, sin IDs de cuenta ni balances personales.

Validación ejecutada: **88 passed** (26 pruebas nuevas, 20 Baseline y 42 QEEF Phase 2), más **8 passed** de #27 en su worktree separado. Se cubren owner cruzado, owner NULL, doble identidad, costos desconocidos, signos, monedas, precisión Decimal, duplicados ambiguos, conservación de versiones y rechazo de rutas existentes. Una segunda ejecución completa con los mismos inputs produjo los **cinco archivos de salida idénticos byte por byte**, incluido SQLite. La captura real y la reconstrucción completa se ejecutaron; los tests no demuestran rentabilidad. El gate de hashes original #27 conserva el fallo explicado arriba.
