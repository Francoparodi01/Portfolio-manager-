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
INSERT execution_plans
        ↓
DB valida owner + run_id + source
        ↓
payload_version = execution-plan-v2-immutable
        ↓
misma sentencia crea PERSISTENCE_IDENTITY capture owner-scoped
        ↓
order_intents del plan quedan append-only
        ↓
FULL_CONTEXT capture agrega portfolio/signals/macro/events
        ↓
Historical Reconstruction => HIGH
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

La captura `FULL_CONTEXT` es adicional: conserva plan serializado, snapshot de
portfolio, señales, macro, eventos y hashes de código. Si esta segunda captura no
está disponible, la identidad formal ya quedó congelada por la captura obligatoria.

### Inmutabilidad

Los planes con `payload_version = execution-plan-v2-immutable` rechazan `UPDATE`
y `DELETE`. Sus `order_intents` también rechazan `UPDATE` y `DELETE`.

Las filas legacy no se reescriben ni se convierten a v2. La migración sólo cambia
el comportamiento de nuevas inserciones.

`decision_log` sigue siendo compatibilidad/auditoría auxiliar. La señal formal se
define desde `execution_plans + order_intents`.

## Qué significa HIGH

`HIGH` significa que podemos demostrar de forma inmutable:

- qué plan formal se guardó;
- a qué owner pertenecía;
- qué run lo originó;
- qué intents formales pertenecían a ese plan.

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
- al menos una captura (`PERSISTENCE_IDENTITY`), normalmente también una
  `FULL_CONTEXT`;
- intents presentes si el plan generó órdenes formales.

Luego volver a correr:

```powershell
python scripts/historical_reconstruction/run.py `
  --env-file ..\cocos_copilot\.env `
  --days 180 `
  --cost-bps 75
```

Los intents elegibles de ese plan deben aparecer como `HIGH`.
