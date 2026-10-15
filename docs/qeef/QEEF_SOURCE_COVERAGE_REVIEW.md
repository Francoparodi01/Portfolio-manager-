# QEEF: revisión de cobertura de fuentes

Fecha de revisión: 15 de octubre de 2026.

**Actualización tras recibir el backup:** se completó una [conciliación parcial del histórico aportado](QEEF_SUPABASE_BACKUP_RECONCILIATION.md): inventario de 59 tablas públicas y comparación de claves en 20 tablas. La mayor parte de la historia revisada ya estaba en local; siguen pendientes cohortes excluidas, diferencias de contenido y cobertura integral. El informe QEEF no debe interpretarse como una auditoría completa de toda la historia.

## Evidencia comprobada

- La [auditoría del 8 de agosto](../11-backend-performance-audit-2026-08-08.md) documenta que los servicios utilizaban PostgreSQL remoto en Supabase y que `cocos_pg_db` no era su base operativa en ese momento.
- El commit `0aef3d46accf8ce005371c38b55b8a53958343ee`, del 4 de octubre, cambió la configuración para hacer autoritativo PostgreSQL local. Ese cambio de configuración, por sí solo, no prueba que se migrara y conciliara toda la historia.
- El 15 de octubre se inspeccionaron únicamente los destinos de conexión, sin publicar credenciales: scheduler, Telegram, monitor y el `.env` principal apuntaban a `db:5432`.
- [`capture_qeef.py`](../../scripts/capture_qeef.py) usa `DATABASE_URL` del contenedor `cocos_scheduler`. No abre una segunda conexión a Supabase ni compara ambas bases.
- La [validación empírica](QEEF_EMPIRICAL_VALIDATION.md) registra una captura del 14 de octubre a las 04:53:59.810706 UTC, con 301 casos y SHA256 `814fac6d86339bce6bf31ec2c2053c6fcafb51354b31f01e9866c18170e3c935`. Phase 2 reutilizó esa captura; no realizó una captura nueva.
- Se encontró una conexión histórica de Supabase en otro checkout de Quantia. Dos intentos de inventario configurados como solo lectura fallaron antes de consultar tablas, con `InternalServerError` y SQLSTATE `XX000`. No se obtuvo evidencia del contenido remoto. No se determinó la causa del error.

## Qué cambia en la interpretación

Los 301 casos son la población de la captura, no un denominador demostrado de toda la historia de Quantia. Los resultados `NOT_IDENTIFIABLE` y los cero casos económicos certificados se limitan a esa captura y al contrato aplicado. No prueban que Supabase carezca de información adicional.

La validación de software conserva su alcance técnico. No sustituye la validación de cobertura histórica ni la conciliación entre bases. Tampoco hay evidencia suficiente en esta revisión para afirmar que los datos locales sean incorrectos o que Supabase contenga casos económicamente verificables.

## Trabajo pendiente tras la conciliación del backup

1. Ampliar la conciliación de las 20 tablas revisadas a las otras 39 tablas públicas y a los payloads que corresponda.
2. Mantener separadas las cohortes con owner exacto, NULL y cero; definir un tratamiento histórico explícito para las excluidas.
3. Explicar la discrepancia de una clave de atribución y los cambios de contenido observados en decisiones y benchmarks.
4. Verificar el corte exacto de migración y la cobertura posterior del origen remoto cuando exista acceso. El backup no es una lectura actual de Supabase.

No se cambió la conexión operativa ni se ejecutaron migraciones, escrituras de datos o despliegues durante esta revisión. Se repitió la evaluación de la captura original con resultado idéntico. El PDF del 14 de octubre se conserva como anexo del PDF revisado y debe leerse junto con la conciliación.
