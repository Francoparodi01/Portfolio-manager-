# QEEF: conciliación del backup histórico de Supabase

Revisión del 15 de octubre de 2026. Estado: cobertura histórica parcialmente conciliada; sin certificación económica global.

## Conclusión corregida

El backup aportado permite contrastar la historia de Supabase con la base local y con la captura congelada de QEEF. **No corresponde afirmar que el histórico de Supabase faltaba por completo en local.** En las 20 tablas revisadas por clave, se encontraron 149.784 de las 149.785 claves del backup en la base local; una atribución legacy requiere revisión de linaje.

La limitación principal del informe anterior era de cobertura y presentación: los 301 casos son una población con propietario explícito, no toda la historia del sistema. Existen cohortes antiguas con propietario NULL o cero, tanto en el backup como en local, que quedaron fuera de ese denominador. No se reasignaron a una cuenta por inferencia.

Tampoco basta con encontrar las mismas claves para certificar integridad histórica. Se observaron diferencias de contenido entre snapshots, incluidas dos decisiones manuales con cambios en el precio de referencia y actualizaciones de benchmarks de radar. Ambas versiones deben conservarse con su procedencia temporal.

## Fuentes y cortes

| Fuente | Identidad y alcance |
|---|---|
| Backup aportado | `db_cluster-09-10-2026@17-43-09.backup.gz`; 55.277.777 bytes comprimidos, 411.066.664 bytes descomprimidos |
| Integridad | Lectura completa del gzip con CRC válido; SQL del dump no ejecutado |
| Formato | Dump de clúster PostgreSQL 17.6; esquemas de Supabase; 59 bloques COPY de tablas `public` inventariados |
| Fecha del backup | El nombre sugiere una fecha, pero no establece por sí solo el instante ni la zona horaria del snapshot. Corte exacto: UNKNOWN |
| Captura QEEF | 14 de octubre de 2026, 04:53:59.810706 UTC; captura privada original e inmutable |
| Inventario local actual | 15 de octubre de 2026, 22:32:07.435851 UTC; transacción repeatable-read de solo lectura; metadatos de claves y clases de propietario |

SHA256 del backup: `901df029a557ba9147be00e4317e6e334d7579592525e5f7e44176e8e3d41579`.

SHA256 de la captura QEEF: `814fac6d86339bce6bf31ec2c2053c6fcafb51354b31f01e9866c18170e3c935`.

La coincidencia del esquema y de las claves vincula el archivo con los datos de Quantia. No equivale a una lectura actual de Supabase ni verifica por sí sola el identificador del proyecto remoto. Las decisiones del backup abarcan del 9 de abril al 5 de octubre de 2026; sus snapshots de portfolio abarcan del 15 de abril al 5 de octubre. Son rangos de esas tablas, no la fecha de extracción del archivo.

## Conciliación de claves contra local

Se verificaron claves primarias y clases de propietario en 20 tablas. Los conteos son registros por tabla, no operaciones independientes; no deben sumarse para calcular rendimiento.

| Tabla | Backup | Local al 15/10 | Claves del backup ausentes en local |
|---|---:|---:|---:|
| decision_log | 1.206 | 1.285 | 0 |
| execution_plans | 130 | 143 | 0 |
| order_intents | 762 | 787 | 0 |
| broker_fills | 310 | 326 | 0 |
| broker_movements | 499 | 515 | 0 |
| portfolio_snapshots | 5.690 | 6.153 | 0 |
| positions | 56.529 | 61.912 | 0 |
| raw_snapshots | 5.690 | 6.153 | 0 |
| position_hold_observations | 528 | 551 | 0 |
| plan_execution_attributions | 43 | 43 | 1 |
| decision_lab_objects | 1.499 | 1.499 | 0 |
| decision_lab_plan_captures | 17 | 32 | 0 |
| decision_lab_runs | 1 | 1 | 0 |
| radar_discovery_runs | 30 | 38 | 0 |
| radar_discovery_snapshots | 8.164 | 10.434 | 0 |
| radar_discovery_outcomes | 14.393 | 19.040 | 0 |
| learning_shadow_cases | 1.980 | 1.992 | 0 |
| learning_shadow_metric_snapshots | 4 | 4 | 0 |
| learning_shadow_metric_snapshots_v2 | 656 | 784 | 0 |
| shadow_thesis_forecasts | 51.654 | 57.768 | 0 |

No se detectaron claves duplicadas en las poblaciones comparadas ni cambios de clase de propietario sobre claves compartidas. Las diferencias de totales entre snapshots de fechas distintas no se interpretan automáticamente como pérdida o como actividad nueva validada.

La atribución discrepante tiene propietario NULL. No coincide su ID ni su `attribution_key`; sí existe una fila local con la misma decisión representativa, ticker y lado. Esto indica una discrepancia de identidad que requiere explicación, no prueba por sí solo pérdida de la operación ni equivalencia económica de las filas.

Las otras 39 tablas públicas se inventariaron, pero no se conciliaron íntegramente por claves o contenido. Entre ellas hay 1.647.830 precios, 252.085 velas y 32.264 outcomes de tesis en el backup. El volumen de esas tablas no constituye evidencia de rentabilidad ni de completitud PIT.

## Qué quedó fuera de los 301 casos

| Cohorte del backup | Registros | Tratamiento |
|---|---:|---|
| Decisiones con propietario exacto | 211 | Todas sus claves están entre las 278 decisiones de la captura QEEF |
| Decisiones con propietario NULL | 995 | Presentes en local; excluidas del denominador de cuenta |
| Planes con propietario NULL | 130 | Cohorte legacy separada |
| HOLD con propietario NULL | 528 | Cohorte legacy separada |
| Fills con propietario NULL | 217 | Sin atribución automática a la cuenta |
| Snapshots con propietario NULL | 3.822 | Sin reconstrucción de cuenta basada en propietario inferido |
| Forecasts con propietario cero | 51.654 | Historial shadow separado; no equivale a 51.654 operaciones |
| Casos learning shadow con propietario cero | 1.980 | Población de investigación separada |

Los 301 casos originales siguen siendo 278 decisiones más 23 HOLD con propietario explícito en la captura del 14/10. Las 211 decisiones del backup con ese mismo propietario ya estaban dentro de la captura. No se agregaron casos duplicados ni se mezclaron cohortes para ampliar artificialmente la muestra.

## Comparación de contenido con la captura QEEF

Se compararon los campos compartidos de filas con propietario exacto o enlace a un padre de ese propietario. Se normalizaron escapes COPY, fechas UTC, representación decimal, booleanos y JSON; la igualdad resultante es semántica bajo esa normalización, no igualdad de bytes. Los campos ausentes en una de las fuentes no se completaron.

- Coinciden los campos compartidos de los 93 fills del propietario, 1.868 snapshots, 24.296 posiciones, 1.499 objetos de Decision Lab y 8.164 snapshots de radar presentes en ambas fuentes. Para movimientos de broker se compararon únicamente los cinco campos de identidad/fecha conservados por la captura; para raw snapshots, únicamente ID y fecha.
- En decisiones, 54 de las 211 filas compartidas coinciden; 157 difieren. Las 157 tienen diferencias en `outcome_filled_at`, y hay cambios de outcomes o referencias de ejecución en parte de ellas. Estos datos deben leerse con sus fechas; no se confirmó aquí la causa de cada actualización.
- Dos decisiones de origen `broker_movement`, registradas el 5 de octubre, difieren también en `price_at_decision` y en partes de `layers`. No corresponde tratar su contenido actual como una copia inmutable del registro anterior.
- En radar, 9.439 de los 14.393 outcomes compartidos difieren. Cambia `universe_sample_count` en esas 9.439 filas, y hay diferencias en benchmark de universo/exceso en 8.710. Los benchmarks QQQ, SPY y cartera propia difieren en tres filas. La presencia del mismo ID no demuestra invariancia del benchmark histórico.

Estas diferencias justifican conservar el backup, la captura original y sus hashes por separado. No se sustituyeron valores de una fuente por los de la otra ni se retrodató disponibilidad.

## Reevaluación y conclusión económica

Se reejecutó `scripts/audit_qeef_phase2.py` sobre la captura original cuyo hash se volvió a comprobar. El resultado completo coincidió con `docs/qeef/evidence/phase2-readiness.json`, incluido el audit ID `88d3efe971fced5e16d9de1a68e5ac2c03516f5c251b827e8234b250add71e7d`.

**Se mantienen 301 casos y cero casos económicos VERIFIED dentro del contrato y de esa captura.** Esto no demuestra que el sistema nunca haya ganado ni que no pueda existir evidencia útil en las cohortes legacy o en tablas todavía no conciliadas.

La validación técnica de Phase 2 conserva su alcance. La evaluación histórica global sigue pendiente: requiere tratamiento explícito de cohortes legacy, explicación de cambios de identidad/contenido y cobertura de mercado, costos, flujos y NAV. Esta revisión no ejecuta una nueva política, no certifica alpha y no autoriza promoción.

## Evidencia y límites prácticos

Los agregados, las columnas comparadas, los rangos por tabla y los hashes están en [supabase-backup-20261015.json](evidence/supabase-backup-20261015.json). Los scripts de lectura/reconciliación y los resultados privados quedan en `outputs/qeef/`, ignorados por Git. El backup, sus credenciales internas y sus filas no se publican.

El SQL del dump no se restauró ni ejecutó. La base operativa se consultó en modo de solo lectura; no se modificaron registros, configuración o servicios. Los resultados sobre claves actuales no certifican igualdad integral de payloads ni el estado remoto actual.

El PDF revisado incorpora esta conciliación antes del informe original, que se conserva como anexo histórico. El encabezado de revisión prevalece sobre cualquier lectura global del denominador de 301 casos del anexo.
