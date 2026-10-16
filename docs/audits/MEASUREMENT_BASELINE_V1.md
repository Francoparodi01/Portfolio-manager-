# Quantia — Measurement Baseline V1

**Fase 1: validación y conciliación de datos. Fecha: 16 de octubre de 2026.**

Quantia conserva una parte extensa y comprobable del histórico aportado en el backup de Supabase. Se compararon las **59 tablas públicas**, incluidas cuatro vacías, con PostgreSQL local: de **2.232.721 claves de registros**, **2.232.716 están presentes** y cinco están ausentes en tablas de atribución. Encontrar las claves no implica conservar la misma versión: **27.026 filas compartidas cambian en algún campo**. Estos totales cuentan filas heterogéneas; no son operaciones, oportunidades ni un denominador de rentabilidad.

Hay datos aprovechables para censos, trazabilidad, investigación de versiones y comprobaciones aritméticas. **Todavía no se puede certificar PnL neto de cuenta ni valor incremental de Quantia** con el conjunto revisado: faltan costos observados, cobertura de flujos por propietario, identidad temporal suficiente y conciliación completa de estados. No se concluye que Quantia gane o pierda dinero.

El resultado económico anterior de QEEF se reproduce exactamente sobre **su captura de 301 casos**, sin extrapolarlo al histórico. Una captura nueva contiene **313 casos con propietario exacto: 290 decisiones y 23 HOLD**. Se mantienen separados los cortes, las versiones y las cohortes legacy.

## 1. Alcance, código y evidencia

- Base revisada: `origin/main` **`071ac22b52b8e9c7955f76cdf549421c50a6addd`**, obtenida con fetch; incluye la revisión de fuentes y conciliación QEEF del PR #24. El `main` local anterior estaba atrasado y no se usó como referencia actual.
- Rama de esta entrega: `audit/measurement-baseline-v1-20261016`, creada desde esa base en un worktree independiente. El checkout principal y la rama del PR #25 se preservaron.
- [Evidencia agregada completa](evidence/measurement-baseline-v1.json): inventario de 73 tablas locales, 59 comparaciones, columnas, claves, nulos, rangos temporales, diferencias, consultas y hashes. Secciones principales: `catalog`, `backup_vs_local`, `quality_checks`, `changed_fields`, `offline`, `qeef_original`, `qeef_current`.
- [Cuaderno de revisión](MEASUREMENT_BASELINE_V1.ipynb): lectura offline de esos agregados. No contiene conexiones ni datos de cuenta.
- El diagrama aportado se usa como contexto de las etapas posteriores. No autoriza avanzar a comparación de estrategias, cambios de capital o activaciones.

Las lecturas operativas usan conexión directa `asyncpg`, `default_transaction_read_only=on`, transacción `REPEATABLE READ READ ONLY`, límites de tiempo y UTC. No se invoca el bootstrap de la aplicación. El SQL del dump se interpreta como datos y nunca se ejecuta. SQLite se usa exclusivamente en archivos nuevos de diagnóstico bajo `outputs/`, ignorados por Git. No se borran, restauran ni sobrescriben fuentes históricas.

Los archivos públicos contienen agregados, esquemas y hashes; no incluyen credenciales, propietarios identificables, balances, filas de operaciones ni payloads privados. La clasificación de calidad es específica de cada afirmación, no un sello global de la base.

## 2. Fuentes: qué se consultó realmente

| Fuente | Verificación de acceso / alcance | Calidad y límites |
|---|---|---|
| PostgreSQL operativo `cocos_db`, base `quantia` | **VERIFICADA**. PostgreSQL 17.10; 73 tablas públicas visibles. Scheduler, Telegram y monitor apuntan a `db:5432/quantia` según sus configuraciones inspeccionadas | Censo y consultas reales de solo lectura. No se supone que sea todo el histórico remoto |
| Supabase original, configuración histórica candidata | **NO VERIFICADA**. Nuevo intento falla antes de leer tablas: `InternalServerError`, SQLSTATE `XX000` | No se atribuye causa al fallo ni se afirma que el proyecto esté vacío. El proyecto remoto y su estado actual siguen sin verificarse |
| `db_cluster-09-10-2026@17-43-09.backup.gz`, aportado por el usuario | **VERIFICADA como archivo**. 55.277.777 bytes; lectura completa y CRC gzip válido; 59 bloques COPY públicos; 1.038 tipos de columnas contrastados con el catálogo local, sin diferencias | Procedencia aportada como backup de Supabase. El nombre no prueba fecha/zona de extracción ni autentica el identificador del proyecto; corte de extracción **UNKNOWN** |
| Captura QEEF original `capture-final-20261014.json` | **VERIFICADA**. Hash y resultado determinista coinciden con lo publicado | Corte y owner token congelados; es una extracción acotada, con payloads/columnas omitidos por diseño |
| Captura nueva `live-owner-20261016.json` | **VERIFICADA**. Captura real por owner exacto, sin truncamientos declarados | 23 tablas extraídas o relacionadas; no equivale al censo completo de las 73 tablas |
| Seis CSV en `quantia_reconstruction/` del principal | **VERIFICADA su lectura** y cobertura de claves; hashes registrados | Origen de extracción y corte no atestados. Vacío CSV puede significar NULL o cadena vacía; comparación limitada a claves, sin equiparar payloads |
| PostgreSQL antiguo `cocos_pg_db`, puerto 5433 | **VERIFICADA**. PostgreSQL 15.17, cinco tablas: un snapshot, dos posiciones, cero filas en las otras tres | Sin columna owner en esas tablas. No se atribuye a la cuenta ni se suma al histórico operativo |
| Movimientos/fills persistidos del broker | **VERIFICADA la evidencia almacenada** en local, backup y CSV | No es verificación independiente contra el broker. El extracto original completo, sus comisiones y su ledger de caja quedan **NO VERIFICADOS** |
| Portal/API actual del broker y otros backups no aportados/localizados | **NO VERIFICADA** | No se abrió sesión directa ni se obtuvo un extracto independiente. No se afirma imposibilidad técnica de acceso ni ausencia de otros archivos |

El inventario de archivos se limitó a los checkouts identificados de Quantia, sus capturas y exportaciones, y los archivos relevantes de Downloads. No se inspeccionó ni conectó el laboratorio Agentic Portfolio Manager.

### Cortes e integridad

| Artefacto / lectura | Corte UTC | SHA256 / referencia |
|---|---|---|
| Backup Supabase | **UNKNOWN** | `901df029a557ba9147be00e4317e6e334d7579592525e5f7e44176e8e3d41579` |
| QEEF original | 2026-10-14 04:53:59.810706 | `814fac6d86339bce6bf31ec2c2053c6fcafb51354b31f01e9866c18170e3c935` |
| Nueva captura owner | 2026-10-16 04:05:47.349668 | `bc28c4a7e3bd3671e6ea9037fb1a57cc00e1955f7903bddcca6746d7ff92ab91` |
| Catálogo operativo | 2026-10-16 04:09:14.320890 | `private_artifact_sha256.catalog-verified.json` |
| Comparación local de 59 tablas | 2026-10-16 04:11:51.351079 | `4b0570811d0d5f803782bc0502eeb40d9713d654f8321d26505ab605de9ffe85` — stream de fingerprints |
| Confirmación de campos cambiados | 2026-10-16 04:16:12.400541 | `changed_fields`; cero conflictos con las versiones del corte anterior en las filas revisadas |
| Diagnósticos finales de calidad | 2026-10-16 04:19:08.744942 | `quality_checks` |

Son snapshots distintos: no se presenta una transacción común entre archivos. Los procesos normales siguieron funcionando. Los rangos de fechas de filas no sustituyen el corte de extracción. Por ejemplo, las decisiones del backup van de 2026-04-09 a 2026-10-05; sus snapshots, de 2026-04-15 a 2026-10-05. Las velas desde 2000 son cotizaciones históricas, no veinte años de decisiones de Quantia.

## 3. Persistencia y migraciones revisadas

| Capa | Persistencia / identidad | Implicación para medir |
|---|---|---|
| Estado de cartera | `portfolio_snapshots` → `positions` y `raw_snapshots`; ID de snapshot y relojes | `total_value_ars` es valor invertido; la identidad de NAV añade cash. La coincidencia interna no prueba flujos completos |
| Broker | `broker_movements`, clave `(source, external_movement_id)`; `broker_fills`, clave `(source, external_fill_id)` | Movimientos sin owner directo. Fills con owner opcional, metadatos de precisión temporal y payloads revisables |
| Decisiones operativas | `execution_plans`, `order_intents`, `decision_log`, `position_hold_observations` | ExecutionPlan conserva la autoridad operativa; `primary`, `planner_audit`, `blocked_audit`, radar y broker_sync son cohortes distintas |
| Atribución derivada | `plan_execution_attributions` y sus tablas puente | El proceso existente recalcula, actualiza y elimina relaciones derivadas; no constituye un archivo inmutable de versiones |
| Mercado | `market_prices` y `market_candles` con upsert | Las claves actuales no incluyen todos los ejes de proveedor/moneda/mercado. La misma clave no garantiza el mismo contenido o disponibilidad histórica |
| Evidencia contextual | `market_candle_observations`, snapshots y enlaces contextual G2/G3 | Registros con identidad/digest y controles adicionales; tener hashes no completa ratios, calendarios ni costos |
| Laboratorios / replay | Decision Lab, learning/shadow, reconstrucciones y replay histórico | Persistencia de investigación; no es ledger de ejecución real. Owner cero y resultados retrospectivos permanecen separados |
| QEEF | Código para `qeef_artifacts`, `qeef_events`, `qeef_jobs` y spool SQLite | Las tres tablas QEEF no aparecen en el catálogo operativo consultado. El código/documentación en main no demuestra despliegue de esas migraciones |

Se revisaron `init.sql`, `src/collector/schema_migrations.py` y las siete migraciones SQL fechadas de `migrations/`: G2, G3, Telegram contextual, replay contextual, replay de análisis, QEEF v1 y QEEF prospectivo. Algunas rutinas de esquema contienen DDL y actualizaciones de metadatos; por eso el diagnóstico usa conexiones directas sin `_ensure_*` ni `init_schema`.

Referencias de implementación: [persistencia](../../src/collector/db.py), [migraciones embebidas](../../src/collector/schema_migrations.py), [atribución derivada](../../src/analysis/plan_follow_attribution.py), [arquitectura QEEF](../qeef/QEEF_ARCHITECTURE.md), [contrato contable](../qeef/QEEF_ACCOUNTING_RECONCILIATION.md). En `db.py`, `_portfolio_observation_times` deriva un reloj a partir de deltas de posiciones sin recibir owner; esto es un riesgo de mezcla en el código, no una prueba de contaminación monetaria de cada fill observado.

El compose de main declara una imagen Timescale PG16 y el contenedor operativo inspeccionado usa PostgreSQL 17. Se documenta esta diferencia entre configuración declarada y runtime; no se modificó ni se dedujo de ella una pérdida de datos. No existe una certificación en esta auditoría de que cada migración histórica se haya ejecutado exactamente una vez; se verificó el esquema visible.

## 4. Cobertura y conciliación ampliada

### Poblaciones principales

| Tabla | Backup | Local comparado | Owner exacto local | Legacy / alcance |
|---|---:|---:|---:|---|
| decision_log | 1.206 | 1.285 | 290 | 995 owner NULL |
| execution_plans | 130 | 143 | 13 | 130 owner NULL |
| order_intents | 762 | 787 | 25 vía plan | Resto se conserva separado |
| portfolio_snapshots | 5.690 | 6.157 | 2.335 | 3.822 owner NULL |
| positions | 56.529 | 61.960 | 29.727 vía snapshot | No tiene owner directo |
| raw_snapshots | 5.690 | 6.157 | 2.335 vía snapshot | No tiene owner directo |
| broker_fills | 310 | 326 | 109 | 217 owner NULL |
| broker_movements | 499 | 515 | No certificado | Sin owner; solo 109 IDs enlazan fills del owner |
| position_hold_observations | 528 | 551 | 23 | 528 owner NULL |
| plan_execution_attributions | 43 | 43 | 0 | 43 owner NULL |
| market_prices | 1.647.830 | 1.657.067 | No aplica | Mercado global |
| market_candles | 252.085 | 257.428 | No aplica | Mercado global |
| radar_discovery_outcomes | 14.393 | 19.748 | Vía run/snapshot | Outcomes de investigación, no trades |
| shadow_thesis_forecasts | 51.654 | 57.768 | 0 | Owner cero |
| learning_shadow_cases | 1.980 | 1.992 | 0 | Owner cero |

Las 59 tablas del backup están en local. Cuatro están vacías en el backup (`ml_decision_features`, `ml_model_registry`, `radar_setup_outcomes`, `shadow_thesis_causal_analysis`); su igualdad vacía no aporta evidencia económica. Las 14 tablas adicionales locales son evidencia contextual y reconstrucción/replay; están inventariadas, pero no tienen un antecedente en este backup con el que compararlas. La auditoría de owner y mercado cubre parte de ellas, no todos sus payloads.

### Claves y versiones

Se usó por tabla una clave primaria o UNIQUE declarada en el catálogo actual y compatible con las columnas del dump. Se comprobaron duplicados sobre esa clave en ambas fuentes, sin sobrescribir colisiones. Se normalizaron números por tipo con Decimal sin redondear su precisión, timestamps conscientes de zona a UTC, escapes COPY y estructura JSON. Texto numérico sigue siendo texto; NULL no se convierte en cero. Arrays y tipos no especializados conservan representación textual. La igualdad es semántica bajo estas reglas, no igualdad del archivo completo.

- **Cero claves duplicadas** en las 59 comparaciones; **cero cambios de clase de propietario** sobre claves compartidas.
- **Cinco claves del backup ausentes**: una en `plan_execution_attributions`, una en `plan_execution_attribution_plans` y tres en `plan_execution_attribution_movements`. No se inventa equivalencia con claves nuevas. La pérdida de identidad derivada no demuestra pérdida de cinco operaciones del broker.
- **48.464 claves solo locales** dentro de esas 59 tablas. Pueden incluir actividad posterior y backfills; la diferencia entre cortes no certifica por sí sola su causa.
- **27.026 versiones de fila diferentes**, en 15 tablas. La mayor parte corresponde a campos derivados o relojes, no a cambios de precios.
- Los campos del backup en `broker_fills`, `broker_movements`, `portfolio_snapshots`, `positions`, `raw_snapshots` y `market_prices` coinciden en todas sus claves compartidas. Esto amplía la revisión previa, que para movimientos/raw snapshots comparaba solo unas pocas columnas contra QEEF.

| Cambio con relevancia económica | Denominador / resultado | Interpretación |
|---|---|---|
| Decisiones, backup → local | 317/1.206 cambiadas: 163/211 exact-owner y 154/995 NULL | Cambios en outcomes y disponibilidad; en dos decisiones exact-owner cambian `price_at_decision` y `layers` |
| Velas | 12.379/252.085 cambiadas | Todas cambian `scraped_at`; siete también `close_price`. Las otras 12.372 solo cambian el reloj de scraping |
| Outcomes Radar | 9.920/14.393 cambiados | Cambia el tamaño del universo en 9.920; benchmark de universo/exceso en 8.964; benchmarks QQQ/SPY/cartera en tres |
| Atribuciones | 42/42 claves compartidas cambian | Todas cambian `updated_at`; seis metadata, cuatro outcome 20D y dos 40D, entre otros campos relacionados |
| Aplicación corporativa | 1/1 cambia | Solo `applied_at`; owner NULL. No prueba una segunda aplicación económica |

La confirmación de campos se hizo en otro corte de solo lectura y verificó primero el hash de cada fila contra el corte comparado: **cero conflictos de versión posterior** en esas seis tablas investigadas. La causa de cada cambio no está certificada mediante un log de escrituras. Los upserts/recomputaciones existentes explican mecanismos posibles; no sustituyen ese log.

### Capturas y CSV recuperados

Entre QEEF original y la captura actual no faltan claves de las tablas comparadas. Hay 12 decisiones, 102 snapshots y 1.224 posiciones adicionales; las filas compartidas de snapshots/posiciones no cambiaron. Sí cambiaron 37 de las 278 decisiones compartidas y 7.248 de los 18.722 outcomes Radar compartidos. La versión antigua sigue disponible en la captura congelada.

Los CSV contienen 320 fills, 509 movimientos, 5.834 snapshots, 58.154 posiciones, 551 HOLD y 5.834 raw snapshots. **Todas sus claves están en el corte local** y ninguna está duplicada dentro de su archivo. Algunas claves no existen en el backup, por lo que son una fuente intermedia útil; no se certificó la fecha de exportación ni igualdad integral de contenido. Los propietarios NULL/cero/exactos no se reasignaron.

## 5. Calidad, relaciones y temporalidad

Los siguientes conteos de cuenta usan exclusivamente el propietario configurado o un padre exacto. Los censos globales de mercado y movimientos se identifican expresamente.

| Prueba | Resultado observado | Límite / consecuencia |
|---|---|---|
| FKs declaradas | 73 constraints comprobadas; cero hijos huérfanos | Una FK no asegura mismo propietario ni significado económico |
| Fill → decisión | 109 enlaces almacenados; 36 apuntan a decisiones owner NULL y 73 a decisiones del mismo owner | No combinar los 36 con evidencia de cuenta exacta. Son enlaces de registros, no 109 decisiones independientes |
| Intents → plan/decisión | 25/25 con padre, owner y run consistentes | Esto no prueba ejecución del broker |
| HOLD → snapshot | 14/23 referencias no encuentran snapshot; esas 14 ni siquiera tienen formato UUID | Concentradas el 06/10 entre 16:39 y 16:47 UTC. Recuperación requiere fuente exacta; no usar snapshot cercano |
| Duplicados de negocio de cartera | Cero relojes de snapshot repetidos y cero duplicados por snapshot/ticker normalizado/moneda | No demuestra que no existan duplicados semánticos en otras tablas |
| Decisiones repetidas | Cuatro grupos con una fila adicional por `(source, run, ticker, decisión, stage)` | Candidatos a revisión, no deduplicados automáticamente: podrían ser versiones/eventos distintos |
| Precios × cantidades de posiciones | 29.727/29.727 con entradas presentes, precios positivos y `market_value` coincidente a 0,01 ARS | Consistencia de cada posición; todavía no valida el total de cartera |
| Total invertido vs suma de posiciones | 2.250/2.335 coinciden a 0,01 ARS; 85 difieren | 20 diferencias absolutas >1 y ≤100 ARS; 65 >100 y ≤10.000 ARS. Son residuales de valuación, no pérdidas |
| Cantidades entre snapshots, replay QEEF | 1.974 pares coinciden; 328 con gaps; 32 con precisión temporal insuficiente, sobre 2.334 pares | Prueba aritmética del inventario visible. No incorpora un ledger exhaustivo certificado |
| Costos observados | 109/109 fills exact-owner con `fees_ars=NULL`; también NULL en los 217 legacy | Los ceros del planner o costos hipotéticos no pueden sustituirlos |
| Fills: reloj y aritmética | 106/109 usan `observed_after` derivado de deltas de portfolio; tres `date_only`; 42/109 no cumplen cantidad × precio = importe a 0,01 ARS | Ninguno tiene reloj `exact`. 65/109 están marcados con precio derivado de importe; requiere contrastar campos originales del broker |
| Movimientos ↔ fills exact-owner | 109 coincidencias de ID, fuente, ticker y moneda ARS; cero diferencias de cantidad absoluta | Hay 28 diferencias de signo, compatibles con convenciones de compra/venta. No se declaran errores de magnitud |
| Ledger de movimientos global | 515 filas sin owner; 212 no enlazan ningún fill por ID externo | Incluye 15 depósitos, 18 retiros, dividendos, especie y otras monedas. Un dividendo carece de importe. No atribuir estos flujos al owner por mera presencia en la misma DB |
| Reloj de snapshot | En 1.707/2.335 `scraped_at > created_at`; diferencia máxima 2,641921 segundos, ninguna >1 minuto | Revisar relojes/transacción; `created_at` no puede adoptarse sin más como disponibilidad exacta. No se infiere lookahead de años ni fraude |
| Mercado global | 257.428/257.428 velas sin `available_at`, `bar_end`, calendario, política de ajuste ni ratio en esas columnas | No habilita replay PIT estricto; no confundir con la tabla inmutable de observaciones |
| Validez OHLC global | 236/257.428 violan orden OHLC: 208 COCOS y 28 YAHOO_BYMA | Candidatas a cuarentena analítica, sin corregir valores originales |
| Precios globales | 1.657.067 precios, sin NULL/no positivos ni timestamps futuros en el chequeo | Positividad no prueba calidad de proveedor, unidad ni ejecutabilidad |
| Observaciones inmutables | 28.615 PARTIAL en QEEF, 120 barras no confirmadas cerradas; cero OHLC inválidos observados | Faltan versión de calendario/acciones, base de precio, sesión y unidad de volumen; ratio desconocido en 26.275 CEDEARs |

Los snapshots exact-owner abarcan **21/08–16/10**, mientras hay fills del mismo owner desde **04/08**. Sin saldo/posiciones de apertura atestados no se reconstruye por inferencia el período anterior. Las últimas filas de fills/movimientos tienen fecha efectiva 09/10: no se interpreta la falta de filas posteriores como confirmación de que no hubo actividad.

La nueva captura mantiene 37 decisiones `broker_movement`, 25 `execution_plan` (13 bloqueadas, ocho aprobadas y cuatro pendientes) y 228 radar. Ninguna de las 25 de plan tiene `outcome_20d` conocido en este corte. El campo `metric_scope=primary` de una decisión manual no la convierte en una decisión de CORE.

## 6. Contraste con QEEF y clasificación de evidencia

Se revisaron [hallazgos económicos](../qeef/ECONOMIC_AUDIT_FINDINGS.md), [validación empírica](../qeef/QEEF_EMPIRICAL_VALIDATION.md), [informe Phase 2](../qeef/QEEF_PHASE2_FINAL_REPORT.md), [revisión de fuentes](../qeef/QEEF_SOURCE_COVERAGE_REVIEW.md) y [conciliación del backup](../qeef/QEEF_SUPABASE_BACKUP_RECONCILIATION.md).

La reejecución de `audit_qeef_phase2.py` sobre el archivo original coincide **en el objeto JSON completo**, incluido audit ID `88d3efe971fced5e16d9de1a68e5ac2c03516f5c251b827e8234b250add71e7d`. Conserva 301 casos, 109 fills sin fees, 67 gaps de total en 2.233 snapshots y cero casos económicos certificados bajo ese contrato. Esos hechos no se extienden a cohortes no capturadas.

El nuevo replay produce 313 casos y cero certificados bajo el mismo contrato; siguen sin tesis congeladas ni bundle económico completo, con 48 enlaces exactos item/HOLD→plan, 22 casos vinculados a FULL_CONTEXT y cero atribuciones formales exact-owner. Los 37 vínculos decisión→fill de QEEF y los 109 enlaces fill→decisión de SQL usan granos/direcciones diferentes y no se suman.

La revisión del 15/10 cubría 20 tablas y una clave faltante; esta entrega amplía a 59 tablas y detecta cuatro claves faltantes adicionales en tablas puente. Las 157 decisiones exact-owner y 9.439 outcomes Radar distintos en aquella revisión pasan a 163 y 9.920 frente al corte actual. No es una contradicción: cambiaron los cortes y se conserva el anterior. No se repitieron los probes de permutación/bootstrap/episodios de QEEF F02–F04; sus hallazgos de software siguen como antecedentes, no como una nueva certificación de todos sus usos operativos.

| Evidencia / uso | Estado | Justificación |
|---|---|---|
| Lectura del backup, CRC/hash, censo y comparación declarada | **VERIFIED** | Archivo aportado leído completamente; claves, columnas y versiones comprobables con scripts |
| Coincidencia de claves y contenido en snapshots/posiciones/fills compartidos | **VERIFIED** para la comparación | Igualdad de campos del backup bajo normalización; no implica exactitud económica de origen |
| Conteos y relaciones exact-owner, por corte | **VERIFIED** | Predicados explícitos y consultas reproducibles; los defectos detectados también son hechos verificados |
| Estados de cartera y aritmética visible | **PARTIAL** | Bases útiles para medir composición/valuación con banderas, pero 85 gaps y ledger incompleto |
| Historial direccional, radar, sombras y legacy | **PARTIAL** | Se puede describir por cohorte/versión; sin convertir scores/outcomes en PnL ni filas en trades independientes |
| Versiones antiguas alteradas en local | **RECOVERABLE** | Backup y capturas conservan versiones anteriores identificadas por archivo/tabla/clave/hash. Recuperar en staging, sin sobreescribir local |
| Cinco identidades de atribución ausentes | **RECOVERABLE** como registros del backup | Se conserva la evidencia antigua. Su equivalencia causal/económica con filas actuales no está identificada |
| CSV intermedios | **PARTIAL** | Cobertura de claves verificada; origen/corte y contenido integral no certificados |
| Owner de legacy NULL/cero, movimientos sin owner y DB antigua | **NOT_IDENTIFIABLE** para la cuenta con lo revisado | No hay autorización factual para asignarlo por ticker, fechas, parecido o unicidad aparente de usuario |
| PnL neto, TWR, drawdown de cuenta y alpha atribuible | **NOT_IDENTIFIABLE** con estas fuentes | Costos/flujo/estado/ejecución no reconciliados; no se inventa un valor ni se afirma imposibilidad futura |
| Estado actual del Supabase original y extracto directo del broker | **NO VERIFICADA** la fuente; conclusión económica **NOT_IDENTIFIABLE** | No se obtuvo lectura independiente suficiente |

`RECOVERABLE` indica que existe un registro conservado que puede recuperarse, no que cualquier comisión o relación faltante pueda deducirse. Los cuatro estados de evidencia no reemplazan el estado de acceso de una fuente.

## 7. Problemas priorizados por impacto financiero

Prioridad sobre la **medición**, sin afirmar un incidente operativo ni cuantificar pérdidas no observadas.

| Prioridad / ID | Problema y evidencia | Impacto posible | Acción propuesta |
|---|---|---|---|
| P0 / MB01 | Fees desconocidas en 109/109 fills exact-owner; movimientos sin owner; saldo inicial anterior al 21/08 no atestado | PnL neto, aportes y retorno de cuenta incorrectos | Obtener extracto completo identificado por cuenta/moneda/período; conciliar costos, flujos y balances antes de publicar desempeño |
| P1 / MB02 | 36 enlaces fill→decisión NULL y 14 HOLD→snapshot no resolubles | Atribuir a Quantia una operación o estado de otra cohorte; baseline equivocado | Cuarentena en la vista analítica; recuperar IDs originales y owner desde evidencia explícita, nunca proximidad |
| P1 / MB03 | Dos precios de decisión, siete cierres y benchmarks de 8.964 outcomes revisados | Lookahead/revisión retrospectiva al evaluar con la última versión | Staging multiversión con corte de observación real, hashes y vista as-of; conservar versiones previas |
| P1 / MB04 | 85 totales de cartera no concilian; 328 gaps de cantidad y 42 importes de fills no cierran | Valuación, exposición y bases de costo incorrectas | Comparar snapshot/raw payload/extracto del mismo instante; explicar cada residual sin reemplazar valores |
| P1 / MB05 | 257.428 velas sin contrato PIT completo; 236 OHLC inválidos; timestamps de fills no exactos | Retornos/entradas/salidas evaluados con datos o bases incompatibles | Segmentar por proveedor/mercado/moneda/instrumento/calendario, cuarentena analítica y recuperación de unidades/acciones |
| P2 / MB06 | Cinco claves derivadas ausentes; escritor de atribuciones usa recomputación y DELETE | Perder trazabilidad de la explicación histórica aun conservando operaciones | Conservar las versiones recuperadas e investigar el cambio; diseñar un registro de revisiones sin ejecutar migración ahora |
| P2 / MB07 | Diferencias de relojes de hasta 2,64 s; cuatro grupos de decisión repetida | Disponibilidad o independencia mal definidas | Definir clocks y grain por productor; revisar recurrencias antes de deduplicar |
| P2 / MB08 | Supabase live y corte de migración no verificados; diferencias compose/runtime | Sobrestimar la cobertura histórica o reproducir contra otra base | Restablecer lectura autorizada o recibir export con proyecto/corte documentados; comparar fuera de operación |

## 8. Fase 2 propuesta: conciliación en entorno aislado

Esta es la siguiente fase de **validación de datos** solicitada, distinta del nombre histórico “QEEF Phase 2” de la implementación existente. No se ejecuta en esta entrega.

1. **Sellar las fuentes y crear staging separado.** Registrar proyecto, origen, owner explícito, alcance de extracción, reloj UTC, esquema y hash. Importar solo datos a una base de laboratorio distinta, sin ejecutar funciones/roles del dump ni conectarla al scheduler. Identidad de registro: `(source_id, source_snapshot_hash, table, original_key, content_hash)`. No unificar versiones por timestamp parecido.
2. **Cerrar el perímetro de cuenta.** Obtener del broker movimientos, operaciones, notas de liquidación, comisiones/impuestos, aportes/retiros, dividendos, acciones corporativas y balances de apertura/cierre por moneda. Empezar por un período con ambos extremos verificables; ampliar hacia atrás solo con nueva evidencia. Resolver owners legacy únicamente con documentación de origen; lo demás permanece separado.
3. **Conciliar posiciones y caja.** Para cada intervalo y moneda, demostrar apertura + movimientos + acciones = cierre en cantidades/cash; valuar con instrumento, precio, FX y disponibilidad documentados. Explicar MB04 por registro. Reusar los contratos Decimal existentes, sin añadir costos supuestos ni remplazar el valor reportado por el calculado.
4. **Recuperar identidad y versiones.** Revisar los 36 enlaces de owner, 14 referencias HOLD, cinco claves de atribución y los cambios de precios/benchmarks. Definir resoluciones explícitas con evidencia de soporte o estado no resoluble. Mantener acceso a ambas versiones y documentar cuándo se conoció cada dato; no retrofechar un conocimiento recuperado hoy.
5. **Calificar mercado y horizontes.** Separar CEDEAR/acción, moneda, proveedor, sesión, ajustes y ratio; revisar las 236 barras inválidas y los siete cierres cambiados. Declarar días calendario vs sesiones para cada outcome. No convertir retrospectivamente `scraped_at` en `available_at`.
6. **Entregar baseline contable y de datos, todavía sin modificar estrategias.** Por cuenta/período: registros elegibles/excluidos y motivos, cobertura de costos y flujos, conciliación de extremos, versiones de mercado y límites de inferencia. Solo un período que cumpla el contrato puede mostrar PnL neto; el resto conserva PARTIAL/NOT_IDENTIFIABLE. Comparar CORE vs HOLD requiere otra validación de contrafactuales y no está autorizado por esta fase.

**Criterios de salida verificables:** cero enlaces entre propietarios en el conjunto elegible; cero discrepancias de identidad sin estado explícito; cada importe/fuente/moneda tiene linaje; balances y movimientos cierran dentro de tolerancias documentadas (cantidades exactas, 0,01 ARS para la prueba monetaria actual); costos desconocidos nunca son cero; toda revisión tiene versión y disponibilidad; mismos inputs congelados reproducen el resultado. Si la cobertura completa no puede atestarse, se publica la limitación en vez de aprobar el período. Los tests de software no son evidencia de rentabilidad.

## 9. Reproducción y validación de esta entrega

Herramientas nuevas: [censo/conciliación](../../scripts/measurement_baseline.py), [consultas de calidad](../../scripts/measurement_checks.sql), [acceso a fuentes](../../scripts/probe_measurement_sources.py), [auditoría offline](../../scripts/audit_measurement_offline.py), [empaquetado de agregados](../../scripts/package_measurement_evidence.py) y [pruebas aisladas](../../tests/test_measurement_baseline.py).

Usar Python con `pytest` para los tests, `python-dotenv` y `asyncpg` para el probe de acceso. La captura operativa reutiliza Python/asyncpg del contenedor existente, sin instalar nada allí. Los demás análisis usan biblioteca estándar. Los comandos rechazan destinos ya existentes: elegir un directorio nuevo por ejecución.

```powershell
# Ejecutar desde el worktree de auditoría. No iniciar servicios ni aplicar migraciones.
$AuditDir = 'outputs/measurement-baseline-reproduction'
$BackupPath = 'C:/Users/Franco/Downloads/db_cluster-09-10-2026@17-43-09.backup.gz'
python scripts/measurement_baseline.py catalog --output "$AuditDir/catalog-verified.json"
python scripts/measurement_baseline.py backup --input "$BackupPath" --catalog "$AuditDir/catalog-verified.json" --output-dir "$AuditDir/backup"
python scripts/measurement_baseline.py live --manifest "$AuditDir/backup/manifest.json" --output "$AuditDir/live-fingerprints.jsonl.gz"
python scripts/measurement_baseline.py compare --backup-dir "$AuditDir/backup" --live "$AuditDir/live-fingerprints.jsonl.gz" --output-dir "$AuditDir/comparison"
python scripts/measurement_baseline.py checks --output "$AuditDir/quality-checks-v2.json"
python scripts/measurement_baseline.py details --manifest "$AuditDir/backup/manifest.json" --comparison-db "$AuditDir/comparison/comparison.sqlite" --output "$AuditDir/changed-field-digests.json"
python scripts/measurement_baseline.py explain --backup "$BackupPath" --manifest "$AuditDir/backup/manifest.json" --details "$AuditDir/changed-field-digests.json" --output "$AuditDir/changed-fields-summary.json"
python scripts/capture_qeef.py --output "$AuditDir/live-owner-20261016.json"
python scripts/audit_qeef_phase2.py --capture "$AuditDir/live-owner-20261016.json" --output "$AuditDir/qeef-current-replay.json"
python -m pytest tests/test_measurement_baseline.py -q --override-ini addopts=''
```

Para reproducir la comparación offline se pasa `--old-capture` con la captura privada original, `--new-capture` con la nueva, `--csv-dir` con `quantia_reconstruction`, además de `--manifest`, `--catalog`, `--backup`, `--comparison-db` y un `--output` nuevo. Para repetir Supabase se pasa su archivo de configuración a `probe_measurement_sources.py --remote-env`, el archivo del owner a `--owner-env` y opcionalmente `--legacy-container cocos_pg_db`; no se pegan DSNs ni secretos en argumentos. Cada CLI expone `--help`.

El empaquetador reproduce **esta publicación fechada** usando los nombres de artefacto de la sección de hashes y verifica sus invariantes; una auditoría posterior debe publicar otro corte, no actualizar este baseline. La repetición contra una DB viva puede producir nuevos conteos. La reproducción exacta utiliza los archivos congelados y sus hashes; el repositorio público solo incluye agregados por privacidad.

Validación: **20 tests sobre fixtures sintéticos y archivos temporales aislados**, sin conexiones ni migraciones. Cubren precisión decimal, tipos JSON, UTC, NULL/escapes COPY, CRC/truncamiento, rechazos de sintaxis no soportada, separación de propietarios, colisiones de claves, cambios de contenido, outputs inmutables, integridad de fingerprints, valuación con faltantes/FX y schema con identificadores entre comillas. Además se ejecutaron las lecturas reales y se reprodujo exactamente QEEF original. No se declara la suite completa del repositorio verde.

Se publican solo diagnóstico, pruebas y documentación. **Sin cambios a scoring, optimizer, planner, activaciones shadow, configuración operativa ni PR #25; sin migraciones operativas, órdenes, merge o deploy.**
