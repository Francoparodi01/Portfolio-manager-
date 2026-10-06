# High-Confidence Evidence v2

## Objetivo

A partir de esta versión, una decisión formal nueva de Quantia no debe poder
persistirse sin evidencia suficiente para que `Historical Reconstruction v1` la
clasifique como `HIGH`.

Esto no cambia scores, thresholds, optimizer, señales ni sizing. Sólo endurece
persistencia y linaje.

## Flujo nuevo

```text
analysis_run_id único
        ↓
owner_chat_id explícito
        ↓
BEGIN (una conexión asyncpg)
        ↓
INSERT execution_plans
        ↓
DB valida owner + run_id + source
        ↓
payload_version = execution-plan-v2-immutable
        ↓
misma sentencia crea PERSISTENCE_IDENTITY capture owner-scoped
        ↓
decision_log auxiliar nuevo + order_intents inmutables + HOLD audit
        ↓
FULL_CONTEXT capture agrega portfolio/signals/macro/events
        ↓
COMMIT (sólo si todas las escrituras y capturas tuvieron éxito)
        ↓
Historical Reconstruction => HIGH para intents ejecutables elegibles
```

## Garantías

### Owner

Un `execution_plan` formal nuevo con `owner_chat_id = NULL` es rechazado por
PostgreSQL. El pipeline también exige owner antes de una corrida persistida.

### Run lineage

Un `execution_plan` formal nuevo sin `run_id` es rechazado. La captura de identidad
guarda el mismo `run_id` de la fila persistida y la captura rica vuelve a leer el
plan para comprobar owner y run antes de guardar contexto.

### Captura obligatoria

La captura mínima `PERSISTENCE_IDENTITY` se crea con un trigger `AFTER INSERT` en
la misma sentencia/transacción que crea el plan. Si esa captura falla, el INSERT
del plan también falla. Por lo tanto no puede existir un plan v2 persistido sin
una captura owner-scoped.

La captura `FULL_CONTEXT` es obligatoria en el escritor formal del pipeline:
conserva plan serializado, snapshot de portfolio, señales, macro, eventos y hashes
de código. Una excepción o un resultado `INSUFFICIENT` aborta la transacción
completa. Un snapshot con owner explícito distinto del plan también se rechaza.

### Inmutabilidad

Los planes con `payload_version = execution-plan-v2-immutable` rechazan `UPDATE`
y `DELETE`. Sus `order_intents` también rechazan `UPDATE` y `DELETE`.

Las filas legacy no se reescriben ni se convierten a v2. La migración sólo cambia
el comportamiento de nuevas inserciones.

`decision_log` sigue siendo compatibilidad/auditoría auxiliar. La señal formal se
define desde `execution_plans + order_intents`.

### Alcance de atomicidad

`_save_execution_plan_events` usa una sola conexión y una sola transacción para:

- `execution_plans` y la captura automática `PERSISTENCE_IDENTITY`;
- filas nuevas de `decision_log`, siempre auxiliares;
- todos los `order_intents`: ventas, compras, bloqueados y pendientes representables;
- observaciones HOLD;
- captura `FULL_CONTEXT`.

Un fallo de SQL, serialización, captura insuficiente, cancelación o COMMIT hace
rollback del conjunto. Los IDs se devuelven sólo después del COMMIT. También se
rechazan INSERTs que no escriben una fila. Un plan vacío válido conserva ambas
capturas y cero intents.

Cada plan recibe un UUID nuevo y filas auxiliares nuevas: no hay UPSERT de planes
ni intents, ni reutilización o actualización de `decision_log` de planes previos.
Un reintento con el mismo run no es idempotente: crea otro plan completo, sin
alterar el anterior. Los runs normales conservan el identificador único del
pipeline. La deduplicación existente de HOLD por `(run_id, ticker)` se conserva.

La garantía all-or-nothing corresponde al escritor formal del pipeline. Las
migraciones de esquema se preparan antes de la transacción; no se hace backfill
nuevo. Un escritor SQL externo debe usar el mismo límite transaccional: los
triggers por sí solos sólo garantizan identidad e inmutabilidad, no conocen el
número esperado de intents ni exigen `FULL_CONTEXT` al COMMIT.

## Qué significa HIGH

`HIGH` significa que podemos demostrar de forma inmutable:

- qué plan formal se guardó;
- a qué owner pertenecía;
- qué run lo originó;
- qué intents formales quedaron efectivamente persistidos bajo ese plan.

No significa que exista un backtest point-in-time perfecto de toda la política.
Las vintages históricas completas de configuración, universo, macro y fuentes
externas siguen siendo un problema separado.

## Validación antes de merge

```powershell
python -m pytest tests/test_high_confidence_evidence_v2.py -q
python -m pytest tests/test_execution_nominals_and_rotation.py -q
python -m pytest scripts/historical_reconstruction -q
python -m pytest scripts/bot_stats_dashboard -q
git diff --check origin/main...HEAD
```

### Pruebas transaccionales con PostgreSQL aislado

Los tests de integración usan `QUANTIA_EVIDENCE_TEST_DATABASE_URL` y sólo aceptan
un servidor loopback con base de control `quantia_pr19_test`. Crean y eliminan una
base descartable propia por caso; no leen `.env` ni conectan a la base operativa.
Sin esa variable, esos casos se reportan como SKIP, no como una validación real.
El workflow `High Confidence Evidence V2` siempre la configura con PostgreSQL 17.

Se verifica rollback ante fallos del plan, captura de identidad, `decision_log`,
primer intent e intents posteriores, bloqueados, pendientes, HOLD, captura rica,
serialización, owner incorrecto, contexto insuficiente, cancelación y COMMIT.
También se comprueba invisibilidad desde otra conexión antes del COMMIT,
inmutabilidad, reintento append-only y clasificación HIGH de BUY/SELL elegibles
con el clasificador sin cambios de Historical Reconstruction v1.

## Smoke test posterior al deploy

Después de la primera corrida formal nueva, verificar sólo lectura:

```sql
SELECT
  p.id,
  p.owner_chat_id,
  p.run_id,
  p.payload_version,
  p.created_at,
  count(DISTINCT i.id) AS intents,
  count(DISTINCT c.capture_hash) AS captures
FROM execution_plans p
LEFT JOIN order_intents i ON i.execution_plan_id = p.id
LEFT JOIN decision_lab_plan_captures c ON c.plan_id = p.id::text
WHERE p.payload_version = 'execution-plan-v2-immutable'
GROUP BY p.id, p.owner_chat_id, p.run_id, p.payload_version, p.created_at
ORDER BY p.created_at DESC
LIMIT 5;
```

Esperado para cada plan nuevo:

- `owner_chat_id` no NULL;
- `run_id` no NULL;
- `payload_version = execution-plan-v2-immutable`;
- ambas capturas: `PERSISTENCE_IDENTITY` y `FULL_CONTEXT`;
- intents presentes si el plan generó órdenes formales.

Luego volver a correr:

```powershell
python scripts/historical_reconstruction/run.py `
  --env-file ..\cocos_copilot\.env `
  --days 180 `
  --cost-bps 75
```

Los intents elegibles de ese plan deben aparecer como `HIGH`.
