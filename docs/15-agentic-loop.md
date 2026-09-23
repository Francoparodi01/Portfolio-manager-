# 15 — Loop agéntico de Quantia

## Objetivo

Esta capa agrega un agente real y acotado sobre Quantia sin convertir al LLM en
autoridad financiera. El ciclo es:

```text
goal
  ↓
LLM controller
  ↓ elige una tool
tool read-only
  ↓
observation
  ↓
LLM controller
  ├─ necesita más evidencia → otra tool
  └─ evidencia suficiente → final
```

El loop termina por respuesta final del modelo o por `max_steps`.

## Frontera de autoridad

El agente **puede** decidir qué evidencia consultar y en qué orden. Puede invocar
análisis de cartera, análisis puntual, macro, radar y performance.

El agente **no puede**:

- ejecutar órdenes;
- escribir `decision_log`;
- cambiar pesos;
- reemplazar optimizer o planner;
- cambiar thresholds;
- saltear risk guards;
- llamar tools no registradas.

`ToolRegistry` rechaza directamente cualquier tool marcada `read_only=False`.

Los análisis financieros reutilizan pipelines existentes:

- `run_analysis.py` siempre se llama con `--no-persist --no-telegram --no-llm`;
- `run_opportunity.py` siempre se llama con `--no-persist --no-telegram`;
- el modelo controlador recibe las observaciones y decide el siguiente paso.

Esto conserva la regla arquitectónica existente: el LLM orquesta y explica; el
motor cuantitativo y los guards siguen siendo deterministas.

## Archivos

- `src/agentic/contracts.py`: contratos de decisiones, tools, observaciones y resultado.
- `src/agentic/model.py`: controlador Ollama JSON-only.
- `src/agentic/tools.py`: registry + tools read-only.
- `src/agentic/orchestrator.py`: loop, límites, cache y anti-loop.
- `src/agentic/persistence.py`: auditoría en `agent_runs` y `agent_steps`.
- `scripts/run_agent.py`: CLI de producción.
- `src/agentic/read_only.py` y `read_only_runner.py`: protección de conexiones y procesos de evidencia.
- `tests/test_agentic_loop.py`: contrato del loop y guards.

## Tools iniciales

| Tool | Fuente | Side effect financiero |
|---|---|---|
| `get_portfolio_snapshot` | `PortfolioDatabase.get_latest_snapshot()` | ninguno |
| `get_macro_context` | `fetch_macro()` | ninguno |
| `analyze_portfolio` | `run_analysis.py --no-persist` | ninguno |
| `analyze_ticker` | `run_analysis.py --tickers ... --no-persist` | ninguno |
| `scan_opportunities` | `run_opportunity.py --no-persist` | ninguno |
| `get_performance` | SELECT de outcomes brutos guardados, por cuenta y cohorte | ninguno |

No existe tool de `BUY`, `SELL`, broker order, optimizer override o planner override.

## Auditoría

En el primer run se crean idempotentemente:

- `agent_runs`
- `agent_steps`

Cada paso guarda:

- tool y argumentos;
- rationale breve del controller;
- observación;
- SHA-256 de la observación;
- duración;
- error;
- cache/repetición;
- modelo;
- objetivo y stop reason.

Por defecto `QUANTIA_AGENT_REQUIRE_AUDIT=true`: si no puede persistir el audit,
el agente falla cerrado.

## Anti-loop

Controles activos:

1. `QUANTIA_AGENT_MAX_STEPS` (default 8; hard cap 20).
2. Una misma llamada exacta no puede ejecutarse dos veces.
3. Tool timeout.
4. Output truncado por tool.
5. Finalización forzada al agotar el budget.
6. Tools desconocidas o argumentos fuera de schema se convierten en observaciones
   de error; nunca se ejecutan.

## Prompt-injection / tool-output safety

El system prompt indica que una observación es **dato no confiable**, no
instrucción. El modelo no recibe herramientas de escritura, por lo que aunque
una noticia o reporte incluyera una instrucción maliciosa, no existe una
capacidad registrada para ejecutar un trade o modificar políticas.

## Variables

Copiar los valores relevantes de `.env.agentic.example`.

Producción recomendada:

```env
QUANTIA_AGENT_ENABLED=true
QUANTIA_AGENT_MODEL=qwen2.5:3b
QUANTIA_AGENT_MAX_STEPS=8
QUANTIA_AGENT_REQUIRE_AUDIT=true
```

## Smoke test antes de producción

Primero, tests:

```bash
python -m pytest tests/test_agentic_loop.py tests/test_agentic_guards.py -q
python -m py_compile \
  src/agentic/contracts.py \
  src/agentic/model.py \
  src/agentic/tools.py \
  src/agentic/persistence.py \
  src/agentic/orchestrator.py \
  scripts/run_agent.py
```

Luego verificar Ollama:

```bash
curl http://localhost:11434/api/tags
```

En Docker, conservar el host que ya usa Quantia para Ollama.

Run manual read-only:

```bash
python scripts/run_agent.py \
  --force \
  --goal "Evaluá si hoy existe una acción material que deba revisar en mi cartera."
```

Para inspeccionar toda la traza:

```bash
python scripts/run_agent.py \
  --force \
  --json \
  --goal "Revisá cartera, contexto macro y oportunidades externas sólo si hacen falta."
```

Verificación SQL:

```sql
SELECT id, goal, model, status, stop_reason, started_at, finished_at
FROM agent_runs
ORDER BY started_at DESC
LIMIT 5;

SELECT run_id, step_no, decision_kind, tool_name, observation_ok, elapsed_ms, error
FROM agent_steps
ORDER BY id DESC
LIMIT 30;
```

## Activación

El feature flag queda `false` por defecto. Para producción:

1. ejecutar los tests;
2. verificar Ollama;
3. ejecutar un run con `--force`;
4. revisar `agent_runs` / `agent_steps`;
5. recién entonces poner `QUANTIA_AGENT_ENABLED=true`.

No hace falta cambiar el scheduler ni el planner para habilitar el loop. El
rollout es **on-demand**, mediante CLI o Telegram. No se agregan ejecuciones
automáticas al scheduler.

## Correcciones de revisión para el despliegue

`run_performance.py` llama a `get_performance_stats_v2`, que cierra trades vencidos
y modifica `decision_log`. Por eso no se invoca desde este agente. La herramienta
usa SELECTs de outcomes existentes, separados por source/status/scope: no presenta
esos promedios sin deduplicar como EV neto, PnL económico ni prueba de edge.

Todas las conexiones de evidencia aplican `SET default_transaction_read_only=on`
y comprueban `SHOW transaction_read_only`. No alcanza con parámetros en la URL:
el servidor desplegado los sobreescribe durante la conexión. Los pools repiten la
protección en cada adquisición. Los scripts legacy se ejecutan en un proceso
aislado mediante `src.agentic.read_only_runner`, que sólo admite los dos scripts
registrados y exige los flags de no persistencia/no envío. La conexión de auditoría
queda separada y escribe únicamente `agent_runs` / `agent_steps`.

El owner es obligatorio. En multiusuario debe pasarse `--owner-chat-id`; en modo
single-user se resuelve desde el chat configurado. Nunca se usa owner null como
comodín global. Las tres herramientas de pipelines legacy requieren comprobar
que la DB contiene un único propietario, porque algunos de sus lectores auxiliares
todavía no están aislados por cuenta. En multiusuario se rechazan esas tres rutas;
snapshot, macro y outcomes permanecen disponibles con sus contratos propios.

Las llamadas se normalizan antes del anti-loop (ticker mayúscula y defaults),
los argumentos rechazan tipos incorrectos y la cancelación termina/recolecta el
subproceso. El hash corresponde al texto realmente observado, después del límite
de salida. Si falla algún paso de auditoría, `audit_persisted` nunca queda true.
Los pasos de auditoría no se sobrescriben mediante upsert. Una respuesta final
sin ninguna observación exitosa produce `FAILED`, no `COMPLETE`.

Desde el checkout operativo, una vez habilitado el flag en `.env` y recreado el
servicio que ejecuta la CLI:

```powershell
docker compose exec -T telegram_bot python scripts/run_agent.py --goal "Revisá mi cartera y explicá qué evidencia falta para decidir."
```

También está disponible bajo demanda en Telegram:

```text
/agente Revisá mi cartera y explicá qué evidencia falta para decidir.
```

Alias `/agent`, botón Auditoría → Agente. Exige un chat autorizado, fija el owner
al chat y entrega la respuesta más un JSON con la traza completa. Usa cuatro pasos
de herramientas como máximo, 240 segundos de presupuesto total y una consulta
simultánea por chat. El comando no usa `--force`; respeta el feature flag. La CLI
mantiene sus defaults (8 pasos, 600 segundos). `--output-json` evita truncar la
traza al transportarla por stdout. El timeout cancela el loop y sus herramientas.

No se agregan jobs automáticos. Ollama debe estar disponible con el modelo
configurado. Su disponibilidad no se considera una autorización financiera.

## Criterio de aceptación agéntico

Este módulo sí cumple la definición estricta:

```text
percepción → razonamiento → acción(tool) → observación → razonamiento → ...
```

porque el siguiente tool call no está preprogramado: lo elige el controller en
función del objetivo y de las observaciones anteriores, con límites explícitos.

## Validación del rollout — 2026-09-23

- Rama revisada: `feature/agentic-loop-orchestrator`; PR #1 contra `main`.
- Suite de integración local: **118 passed**. Incluye agente, guards, Analytics v2,
  viability, auditoría decision/market y menú de tickers. Es validación focalizada;
  no representa una certificación de toda la suite histórica.
- PostgreSQL real: una escritura sin filas (`UPDATE ... WHERE FALSE`) fue
  rechazada con `ReadOnlySQLTransactionError` en la conexión protegida.
- Ollama local `qwen2.5:3b` + DB: loop completo con snapshot, outcomes y macro,
  auditado con estado `COMPLETE` (run `efe268e5-f973-41b6-a4de-ddfb0582eefd`).
- Wrapper legacy: `analyze_ticker` completó sin persistencia ni envío
  (run `b1d3a934-1a11-4c54-946d-a633026e6c77`).
- Handler Telegram real + CLI/modelo/DB reales, con receptor simulado:
  `COMPLETE`, dos pasos, respuesta y documento JSON, archivos temporales limpios,
  owner e integridad de hashes contrastados contra `agent_runs`/`agent_steps`
  (run `8949f9b5-e74a-4bbd-aa3f-af6623d66567`). No se envió un mensaje de prueba al usuario.
- Despliegue: sólo se recreó `telegram_bot`, con feature flag y auditoría activados.
  `get_my_commands` confirmó `/agente` y la conservación de `/analytics`.
  Scheduler mantuvo el mismo container ID y hora de inicio.
- Imagen desplegada:
  `sha256:a2a0cbbd5b192c4dd234b6c87268fe2752e7ec49e8e433a2eb0fbb8d7c0fd037`.
  Se preservaron los cambios previos del checkout operativo; la rama del PR
  contiene únicamente el agente y su integración incremental con Telegram.

Comando de regresión utilizado en el checkout operativo:

```powershell
python -m pytest tests/test_agentic_loop.py tests/test_agentic_guards.py tests/test_analytics_v2.py tests/test_analytics_v2_telegram.py tests/test_viability_audit.py tests/test_decision_market_audit.py tests/test_ticker_telegram_menu.py -q
```

Los artefactos completos de smoke quedan locales bajo `tmp/agentic-review/` y
contienen evidencia privada de la cuenta; no se incorporan al repositorio.
El modelo puede fallar o llegar al límite de tiempo. En esos casos el comando
informa el estado y no transforma una respuesta incompleta en evidencia económica.
