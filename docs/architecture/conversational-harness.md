# Quantia Conversational Harness

## Objetivo

Quantia deja de exponer módulos como navegación de Telegram y pasa a exponer una única superficie: lenguaje natural. El cambio no mueve la lógica económica al LLM. Portfolio, análisis determinístico, optimizer/planner, Decision Lab, Ledger, macro y Economic Meta Policy siguen siendo las fuentes de verdad existentes.

```text
USER
  -> conversational gateway
  -> task + conversation references
  -> context selector + capability gate
  -> bounded execution loop
      -> Quantia tools/services
      -> EvidenceObject artifacts
  -> deterministic verification
  -> grounded synthesis
  -> USER
```

No existe capability de ejecución de órdenes en este harness.

---

## CURRENT

### Telegram

`scripts/telegram_bot.py` concentra la UX histórica: comandos, callbacks, inline keyboards y adapters que llaman scripts/servicios. Es útil como capa de compatibilidad interna, pero mezcla navegación con acceso a business logic.

### Agentic layer existente

`src/agentic/` ya resolvía una parte importante del problema:

- `ToolRegistry` read-only;
- DSN/PG guards de solo lectura;
- loop acotado con anti-loop;
- auditoría en `agent_runs` / `agent_steps`;
- herramientas de portfolio, macro, análisis, radar y performance;
- Decision Lab account-scoped;
- Ollama local como planner;
- fallback determinístico para respuestas sensibles.

La migración reutiliza esto en vez de crear un segundo motor económico.

### Deuda que motivó esta capa

- `/agente` era una función dentro de un bot orientado a menú;
- follow-up = historial corto, sin `ConversationState` explícito;
- no había un contrato común de `Task`, `ContextPlan`, `EvidenceObject` y `VerificationReport`;
- el tool set se presentaba de manera demasiado amplia para algunas tareas;
- faltaban capabilities explícitas más allá de `read_only`;
- faltaba compaction/handoff formal entre evidencia y síntesis;
- Ledger, Meta Policy y health seguían atados a UX/commands distintos;
- Telegram publicaba catálogo de comandos y keyboards.

---

## TARGET

### 1. Task specification

`src/agentic/harness/task.py` convierte cada mensaje en `TaskSpec`:

- intent conocido cuando existe;
- entities/symbols;
- objetivo;
- evidencia esperada;
- ambigüedad;
- subject heredado de conversación.

El parser no es un router cerrado. Un pedido no reconocido queda como `general` y pasa al planner dinámico dentro del registry permitido.

### 2. Context engine

`ContextSelector` limita el tool surface por tarea. Ejemplos:

- portfolio review: snapshot + current decision evidence;
- PnL: Ledger + performance evidence;
- market: macro only;
- Meta Policy: shadow meta + supporting decision evidence;
- general: safe registry dinámico.

Principio: **relevancia > tokens**.

Budgets configurables:

- `QUANTIA_HARNESS_MAX_STEPS`
- `QUANTIA_HARNESS_MAX_TOOL_CALLS`
- `QUANTIA_HARNESS_MAX_RETRIES`
- `QUANTIA_HARNESS_MAX_SECONDS`
- `QUANTIA_HARNESS_CONTEXT_CHARS`
- `QUANTIA_HARNESS_OBSERVATION_CHARS`

### 3. Context compaction

Cada tool call se convierte en `EvidenceObject` con:

- source;
- tool;
- timestamp;
- payload compacto;
- quality;
- mode;
- warnings;
- hash;
- latency.

El sintetizador recibe estos artifacts, no el contexto interno que cada módulo utilizó para producirlos. El bundle final tiene un límite adicional (`QUANTIA_HARNESS_SYNTHESIS_EVIDENCE_CHARS`).

### 4. Context vs memory vs state

Separación explícita:

- **Context**: `ContextPlan` + artifacts del run actual.
- **State**: `ConversationState` en Redis, TTL corto. Guarda subject, símbolos activos, último intent y referencias de evidencia. Máximo seis mensajes de usuario; no guarda conclusiones del asistente como evidencia.
- **Memory**: `MemoryStore`, namespace aparte, sólo acepta `methodology`, `configuration` y `structural_preference`. No es un transcript store.

Redis no reemplaza la DB financiera.

### 5. State machine

`HarnessState` mantiene:

- run id;
- status;
- steps completos/pendientes/fallidos;
- tool call count;
- retries/errors;
- evidence refs;
- start timestamp.

Estados finales: `COMPLETE`, `PARTIAL`, `FAILED`.

### 6. Tool registry

Se conserva `src/agentic/tools.py` y se agregan adapters read-only en `harness/tools_ext.py`:

| Tool | Fuente | Capability | Mode |
|---|---|---|---|
| `get_portfolio_snapshot` | DB portfolio | READ | PRODUCTION observation |
| `get_decision_evidence` | motor actual no-persist | READ | OBSERVATION |
| `analyze_portfolio` | analysis/optimizer/planner no-persist | COMPUTE | OBSERVATION |
| `analyze_ticker` | analysis no-persist | COMPUTE | OBSERVATION |
| `scan_opportunities` | radar no-persist | COMPUTE | OBSERVATION |
| `get_macro_context` / exposure | macro | READ | OBSERVATION |
| Decision Lab tools | stored PIT evidence | READ | RESEARCH |
| `get_decision_ledger` | Decision Ledger | READ | PRODUCTION evidence |
| `get_meta_policy` | Economic Meta store | READ | SHADOW |
| `get_system_status` | DB/Redis | READ | OBSERVATION |

No se agregó tool de broker/order execution.

### 7. Permissions

`PermissionPolicy` clasifica tools como:

- READ;
- COMPUTE;
- WRITE;
- FORBIDDEN.

El runtime sólo permite READ/COMPUTE. Nombres asociados a orders, broker execution, overrides, secretos o mutaciones quedan fail-closed aunque un modelo los solicite.

### 8. Execution loop

`ConversationalHarness.run()`:

```text
message
  -> load conversation state
  -> parse task
  -> build safe registry
  -> select context + budgets
  -> parallel prefetch of independent required sources
  -> dynamic planner loop
  -> tool validation/capability check
  -> execute + retry policy
  -> EvidenceObject handoff
  -> safe deterministic fallback
  -> grounded synthesis
  -> deterministic verifier
  -> persist trace + conversation state
```

Los parallel groups se usan únicamente para fuentes independientes. Tool calls duplicadas quedan bloqueadas por key `(tool, canonical args)`.

### 9. LLM strategy

Configuración central en `ModelRoles`:

- `QUANTIA_LLM_ROUTER`
- `QUANTIA_LLM_REASONING`
- `QUANTIA_LLM_SYNTHESIS`
- `QUANTIA_LLM_VERIFIER`

Pueden apuntar todos al mismo Ollama local. En v1:

- task routing conocido: determinístico, más barato y verificable;
- planificación abierta: reasoning model existente;
- síntesis natural: synthesis role con temperature 0;
- verificación financiera: determinística; no se delega a otro LLM cuando una regla programática alcanza.

### 10. Grounding y verification

`HarnessVerifier` revisa al menos:

- existe evidencia exitosa;
- se obtuvieron required tools;
- SHADOW no se presenta como producción;
- timestamps/warnings;
- números de la respuesta tienen trazabilidad aproximada en artifacts.

Para intents económicos/decisionales, una síntesis con inconsistencia numérica cae al renderer determinístico. Si aun así falta evidencia requerida, la respuesta es explícitamente insuficiente.

### 11. Economic safety

Modes se conservan en el artifact boundary:

- `PRODUCTION`: datos reales/persistidos de cuenta o Ledger;
- `OBSERVATION`: cálculo/read actual sin persistir decisión;
- `RESEARCH`: Decision Lab/counterfactuals;
- `SHADOW`: Economic Meta Policy.

`get_meta_policy` fija `capital_effect=NO`; el synthesizer recibe esa marca y el verifier impide promocionarla silenciosamente.

### 12. Prompt injection y secrets

- tool output se serializa como `evidence`, no como instrucciones;
- system prompt de síntesis dice explícitamente que evidence es contenido no confiable;
- no se pasan API keys, Telegram tokens ni DB credentials al LLM;
- commands/subprocesses del backend no son construidos desde shell text del modelo;
- no existe tool arbitraria de SQL/shell.

### 13. Observability

Se reutilizan `agent_runs` y `agent_steps`. Metadata del nuevo run incluye:

- conversation id;
- normalized task;
- context plan/budgets;
- model roles;
- tool calls;
- LLM calls;
- evidence refs;
- verification report.

Cada `ToolObservation` conserva elapsed time, status, error y sha256. El objetivo es poder reconstruir “por qué Quantia respondió esto” sin guardar secretos.

### 14. Cost/performance controls

- simple requests usan un tool surface pequeño;
- required independent reads se hacen concurrentes;
- no se cargan universos/radar completos salvo intención relevante;
- synthesis bundle cap;
- session history cap;
- hard tool/step/time/retry budgets;
- model roles centralizados para poder cambiar router/reasoning/synthesis por costo/latencia sin editar múltiples archivos.

---

## Telegram

Nuevo entrypoint: `scripts/telegram_conversational.py`.

Propiedades:

- sin `CallbackQueryHandler`;
- sin inline/reply keyboard;
- sin catálogo visible de comandos;
- `/start` sólo existe como bootstrap técnico de Telegram;
- mensajes normales van al mismo conversational gateway;
- slash input legacy se traduce internamente a lenguaje natural por retrocompatibilidad, pero no se publicita;
- configuración multiusuario puede reutilizar el flujo de credenciales existente, neutralizando el retorno al menú viejo;
- portfolio-sensitive questions pueden solicitar el refresh operativo existente antes del análisis;
- heartbeat y Meta watcher continúan.

`docker-compose.yml` apunta el servicio `telegram_bot` al nuevo entrypoint. Rollback: volver el command a `scripts/telegram_bot.py`.

---

## MIGRATION

Estado de las etapas solicitadas:

1. **Inventario command → capability**: realizado a partir de `scripts/telegram_bot.py`, agent tools y módulos existentes.
2. **Separar business logic de UX**: la nueva UX no llama command handlers para análisis; llama registry/services. `telegram_bot.py` permanece legacy.
3. **Tool Registry**: reutilizado y extendido aditivamente.
4. **Task/context/state**: implementado en `src/agentic/harness/`.
5. **Orchestrator/execution loop**: `ConversationalHarness` con planning dinámico y budgets.
6. **Conversation state**: Redis + fallback process-local, explicit refs.
7. **Verifier**: determinístico con fallback seguro.
8. **Telegram sólo al gateway**: nuevo entrypoint desplegable.
9. **Eliminar botones/menús de UX**: nuevo entrypoint no registra callbacks/keyboards/command catalog.
10. **Tests/evals/observability**: tests/evals agregados; observability reutiliza audit tables.

No se migraron schemas financieros persistidos ni se modificó ledger/outcomes/optimizer/radar/Decision Lab.

---

## Conversaciones objetivo

### Portfolio + follow-up

```text
Usuario: ¿Cómo está mi cartera?
Quantia: <síntesis grounded de snapshot + decisiones vigentes>

Usuario: ¿Qué te preocupa más?
Quantia: <retoma el mismo contexto estructural; consulta evidencia adicional si hace falta>

Usuario: ¿Por qué?
Quantia: <explica la decisión usando evidence refs; no trata la respuesta anterior como dato financiero>
```

### Símbolo + comparación

```text
Usuario: ¿Qué hago con GDX?
Quantia: <posición + decisión + evidencia>

Usuario: ¿por qué?
Quantia: <hereda GDX>

Usuario: comparalo con NVDA
Quantia: <hereda GDX y agrega NVDA; workflow de comparación>
```

### Meta Policy

```text
Usuario: ¿Por qué A y B dejan pasar GDX pero C no?
Quantia: <lee get_meta_policy + evidencia pertinente>
         <declara SHADOW_ONLY y Capital effect: NO>
```

### Resultados

```text
Usuario: ¿Cuánto ganó Quantia?
Quantia: <Decision Ledger account-scoped>
         <si no hay reconciliación suficiente, distingue PnL observado de EV/retornos teóricos>
```

---

## Validación

Validación mínima antes de merge/deploy:

```bash
python -m compileall -q src/agentic/harness src/agentic/conversation scripts/telegram_conversational.py
pytest -q tests/test_conversational_harness.py
pytest -q tests/test_conversational_telegram_surface.py
pytest -q
```

Con infraestructura disponible:

```bash
QUANTIA_AGENT_ENABLED=true python scripts/run_agent.py --goal "¿Cómo está mi cartera?" --continue-conversation
python scripts/telegram_conversational.py
```

Smoke conversacional obligatorio: portfolio → “¿por qué?” → comparación de ticker → opportunities → PnL → Meta Policy → status.

---

## Known limitations

- El router determinístico cubre intents frecuentes; consultas nuevas caen al planner general, por diseño.
- El verifier numérico es conservador y puede forzar fallback determinístico ante transformaciones derivadas que no aparecen textualmente en evidence.
- `MemoryStore` está implementado pero no se autoalimenta: una política futura debe decidir qué hechos metodológicos merecen persistencia; se evita “recordar todo” por defecto.
- La calidad/timestamp depende de lo que cada fuente real exponga; ausencia de timestamp se conserva como warning, no se inventa frescura.
- La validación end-to-end necesita DB, Redis, Ollama y Telegram reales. Los unit/eval tests no sustituyen ese smoke.
- `scripts/telegram_bot.py` sigue existiendo como compatibilidad/rollback. El criterio UX se cumple en el entrypoint conversacional desplegado, no borrando código legacy útil.
