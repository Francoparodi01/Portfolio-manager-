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
- `tests/test_agentic_loop.py`: contrato del loop y guards.

## Tools iniciales

| Tool | Fuente | Side effect financiero |
|---|---|---|
| `get_portfolio_snapshot` | `PortfolioDatabase.get_latest_snapshot()` | ninguno |
| `get_macro_context` | `fetch_macro()` | ninguno |
| `analyze_portfolio` | `run_analysis.py --no-persist` | ninguno |
| `analyze_ticker` | `run_analysis.py --tickers ... --no-persist` | ninguno |
| `scan_opportunities` | `run_opportunity.py --no-persist` | ninguno |
| `get_performance` | `run_performance.py` | ninguno |

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
python -m pytest tests/test_agentic_loop.py -q
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
primer rollout recomendado es **on-demand**. Después de observar trazas reales
puede añadirse una invocación desde Telegram o un job scheduler sin ampliar
permisos.

## Criterio de aceptación agéntico

Este módulo sí cumple la definición estricta:

```text
percepción → razonamiento → acción(tool) → observación → razonamiento → ...
```

porque el siguiente tool call no está preprogramado: lo elige el controller en
función del objetivo y de las observaciones anteriores, con límites explícitos.
