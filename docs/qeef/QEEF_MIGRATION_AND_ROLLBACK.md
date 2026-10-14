# Migración, reproducción y rollback

## Procedimiento Phase 2 — preparado, no ejecutado operativamente

QEEF v1 fue publicado en [PR #22](https://github.com/Francoparodi01/Portfolio-manager-/pull/22), draft hacia main y sin merge. Phase 2 está en la rama `feature/qeef-prospective-evidence-pipeline`, worktree hermano `quantia-qeef-phase2`, basada exactamente en `08623cc9b418d7aef4da89b020ec26737a20d455`. Las secciones v1 siguientes describen el estado de aquella entrega; las frases "sin push" y "colector pendiente" corresponden al cierre v1.

| Configuración | Default / requisito |
|---|---|
| QEEF_CAPTURE_ENABLED | false; sólo el valor true habilita hooks/worker |
| QEEF_TELEGRAM_ENABLED | false; lectura independiente de captura |
| TELEGRAM_CHAT_ID | Owner exacto; no se acepta seleccionar otro desde HTTP/Telegram |
| QEEF_CAPTURE_START_AT | ISO-8601 con zona explícita, preregistrado al activar |
| QEEF_SPOOL_PATH | Ruta absoluta en almacenamiento persistente, con permisos del proceso |
| DATABASE_URL | Para CLI de status/work; no se muestra en logs/argumentos |
| Autoridad | RESEARCH_ONLY; affects_analysis/execution/promotion_allowed false, no configurables |

Para una **futura activación expresamente autorizada**:

1. Revisar y aprobar los PRs; verificar árbol, backup, esquema y rol de aplicación sin superuser/BYPASSRLS. La prueba local no sustituye staging.
2. Aplicar primero `migrations/20261014_qeef_v1.sql` y luego `migrations/20261014_qeef_prospective.sql` en el entorno autorizado. Ambas se probaron dos veces en PostgreSQL descartable. No hay auto-migración en hook/CLI/monitor.
3. Otorgar sólo SELECT/INSERT sobre evidencia QEEF y SELECT/INSERT/UPDATE sobre qeef_jobs al rol de servicio; permitir las lecturas owner-scoped de fuentes. RLS usa `qeef.owner` transaccional. No otorgar DDL/TRUNCATE a la aplicación.
4. Preparar spool persistente y clocks/owner/inicio explícitos. Desplegar con flags false y consultar `python scripts/run_qeef_pipeline.py status --output <directorio>`.
5. Con la autorización de captura, activar únicamente QEEF_CAPTURE_ENABLED. Observar el siguiente flujo normal de Quantia: un plan existente debe producir un CASE sin alterar su contenido; comparar esperados/ausentes, jobs y lag. No emitir una operación para generar evidencia de prueba.
6. La captura básica será PARTIAL mientras el productor no aporte tesis/bundle ex ante y las fuentes no aporten calendario, ratios, costos y ledger completos. Habilitar captura no certifica rentabilidad. Telegram puede permanecer apagado.

Rollback de uso: poner QEEF_CAPTURE_ENABLED=false y QEEF_TELEGRAM_ENABLED=false; detener el consumidor/CLI y retirar su despliegue de forma controlada. Conservar spool, qeef_events, qeef_jobs y qeef_artifacts. Revocar permisos del servicio si corresponde; no DELETE, DROP ni TRUNCATE. Cambiar payloads inmutables no es recuperación: corregir el productor y agregar otra versión/evidencia, o reintentar metadata de entrega después de investigar la causa.

Reproducción del diagnóstico histórico (sólo archivos, sin DB):

```powershell
python scripts/audit_qeef_phase2.py --capture <ruta-privada-capture-final-20261014.json> --output <nuevo-phase2-readiness.json>
```

La ruta histórica original sigue en `quantia-qeef-v1/outputs/qeef/`; no se incluyeron datos privados en Git. El audit verifica que el archivo fuente conserve su SHA y rechaza sobrescribir resultados distintos. La lectura HTTP/Telegram no inicia captura ni migraciones. Los límites de recuperación de movimientos sin owner y extensiones sólo en memoria están declarados en [QEEF_PROSPECTIVE_PIPELINE.md](QEEF_PROSPECTIVE_PIPELINE.md).

## Estado entregado

Rama local `feature/economic-audit-reengineering-v1`, worktree:

`C:\Users\Franco\OneDrive\Escritorio\backend\.worktrees\quantia-qeef-v1`

Base remota verificada `f2e08096a2a0b6254598243600f522b8608276d5`; implementación `d3cca84c06f631773810b4aec5077c35f157aafb`. No push, merge, despliegue ni cambios de configuración de producción. Principal `cocos_copilot` y cambios ajenos preservados. No se usa `_1-copia` como checkout.

La migración **no se aplicó a la base operativa**. El endpoint está implementado pero no desplegado. La captura operativa sólo hizo lecturas. No se cambió política económica ni se enviaron mensajes u órdenes.

## Compatibilidad y tabla adicional

[20261014_qeef_v1.sql](../../migrations/20261014_qeef_v1.sql) agrega sólo tabla, índice, función/triggers y política RLS propios de QEEF. No altera tablas existentes. Puede aplicarse dos veces en el esquema esperado. Una instalación previa con estructura divergente requiere revisión; `IF NOT EXISTS` no es un motor de reparación de drift.

Tipos de artefacto: THESIS, THESIS_EVENT, EVALUATION. IDs estables scoped por owner, contenido canónico y hash. El store verifica contrato/hash/procedencia y conflictos antes de aceptar idempotencia. La referencia de evento a tesis se valida en el servicio dentro de la transacción con advisory lock; JSONB no tiene FK interna que sustituya este control. Acceso SQL directo debe restringirse al servicio, no utilizarse para eludir contratos.

RLS exige `qeef.owner` a nivel transacción; el servicio configura ese valor y también filtra SQL por owner. No usar superuser/BYPASSRLS como rol de aplicación. RLS con GUC es una defensa de aislamiento de la aplicación, no autenticación independiente: un rol con SQL arbitrario podría establecer su GUC. La ruta HTTP usa owner configurado e impide override.

Los triggers bloquean UPDATE/DELETE/TRUNCATE; roles administradores con DDL podrían desmontarlos. Inmutabilidad de aplicación no sustituye backup, controles de administración o evidencia externa. No se declara resistencia ante administrador malicioso.

## Verificación realizada en base descartable

[test_qeef_postgres.py](../../tests/test_qeef_postgres.py) crea contenedor único `qeef-test-<uuid>`, imagen `postgres:17-alpine`, puerto aleatorio limitado a 127.0.0.1 y sin volumen persistente. No reutiliza credenciales ni DB de Quantia. Espera servidor TCP accesible antes de probar. El cleanup comprueba nombre y label de descartable antes de remover exclusivamente ese contenedor.

Se verificaron: registro histórico preexistente intacto; migración doble; inserción/idempotencia de tesis, evento y evaluación; conflicto de identidad; cierre incompatible; lectura exact-owner; UPDATE/DELETE/TRUNCATE rechazados; RLS bajo rol no superuser; inserción cruzada rechazada; revocación de permisos conserva registros. Esta prueba no certifica todos los esquemas PostgreSQL históricos posibles ni una instalación operativa.

## Reproducción offline de la auditoría

Desde el worktree indicado, con la captura privada conservada y dependencias del proyecto:

```powershell
python scripts/run_qeef.py audit --capture outputs/qeef/capture-final-20261014.json --output outputs/qeef/release-v1 --code-version d3cca84c06f631773810b4aec5077c35f157aafb
python scripts/validate_qeef_empirical.py --capture outputs/qeef/capture-final-20261014.json --output outputs/qeef/reproduced-probes.json
```

La primera orden puede repetirse: el writer acepta el resultado idéntico y rechaza sobreescrituras diferentes. `--code-version` fija el SHA de implementación porque el commit posterior agrega documentación; el CLI también registra el hash efectivo de los archivos de implementación. Si cambia código, dependencias o bytes de fuente, usar otro directorio y comparar hashes; no falsear la versión para lograr igualdad. En checkouts con normalización de saltos de línea, el hash de bytes puede cambiar aunque la semántica sea igual: el manifest refleja los bytes realmente usados.

Para evaluación de un episodio congelado compatible con Decision Lab:

```powershell
python scripts/run_qeef.py evaluate --episode <episode.json> --dataset <dataset.json> --benchmarks <benchmarks.json> --as-of <ISO-8601-con-zona> --horizons 5 10 20 --output <directorio-nuevo>
```

Los placeholders requieren inputs reales tipados; no son archivos incluidos ni una demostración de backtest. Benchmarks es opcional: los no suministrados quedan no identificables. Este CLI no conecta a broker ni DB. APIs de lifecycle/detección/atribución se consumen como librería; no hay job prospectivo activado.

Para obtener **otra** captura, con contenedor operativo ya existente y disponible:

```powershell
python scripts/capture_qeef.py --container cocos_scheduler --output outputs/qeef/capture-nueva-fecha.json
```

La herramienta ejecuta su código de lectura por stdin, usa credenciales dentro del contenedor y verifica `transaction_read_only=on`; no copia código al runtime ni inicia la aplicación. Un archivo existente se rechaza. Una captura nueva no reproducirá automáticamente los conteos de este informe: cambia el corte y se debe emitir otra evaluación. Timeout por sentencia30s y límite100.000 filas por tabla; una limitación se registra, no se ignora. `raw_snapshots` se reduce a metadata. El esquema/listado de fuentes y los payloads privados permanecen fuera de Git.

## Suite verificada

```powershell
python -m pytest tests/test_qeef.py tests/test_qeef_postgres.py tests/test_analytics_v2.py tests/test_viability_audit.py tests/test_execution_nominals_and_rotation.py tests/test_contextual_e1.py tests/test_historical_analysis_replay.py -q --disable-warnings --junitxml=outputs/qeef/tests-release.xml
```

Resultado real documentado: 140 passed, sin skips. Docker y la imagen PostgreSQL son necesarios para la integración; no omitirla silenciosamente. La auditoría offline no requiere Docker.

## Rollout futuro y reversión sin destruir evidencia

No se ejecutó rollout operativo. Antes de usar la persistencia en un entorno autorizado: verificar snapshot/backup y rol mínimo, revisar estructura esperada, aplicar migración aditiva en staging, ejecutar los checks de owner/idempotencia con artefactos research, y configurar consumidor explícito. El monitor no instala esquema; si falta, devuelve `QEEF_SCHEMA_NOT_INSTALLED`. La activación de un colector prospectivo es trabajo separado pendiente.

Rollback de uso: retirar consumidor/ruta mediante despliegue controlado y revocar permisos de aplicación sobre `qeef_artifacts`; **mantener tabla y evidencia**. Ejemplo orientativo para un rol real previamente identificado: `REVOKE ALL ON qeef_artifacts FROM <rol_aplicacion>;` No se ejecuta con placeholder ni contra roles desconocidos. No DROP, DELETE, TRUNCATE ni reescritura de decisiones como rollback. El rol de auditoría, si existe y está autorizado, puede conservar SELECT.

La reversión del código no requiere migración destructiva porque las tablas operativas no dependen de QEEF. La prueba descartable validó revocación y conservación de filas. No se realizó un ensayo de rollback sobre producción ni se afirma que haya sido necesario.
