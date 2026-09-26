# Quantia Conversational Harness

## Purpose

Quantia's user-facing interface becomes a natural-language conversation. The user does not need to know Telegram commands, menus, callback buttons, internal module names, or the sequence in which portfolio, risk, market, Decision Lab, optimizer, ledger, and analytics evidence must be consulted.

The migration is deliberately additive. Existing economic engines remain authoritative. The harness orchestrates them; it does not reimplement their economics inside an LLM.

Core invariant:

```text
LLM / harness = understand, select, orchestrate, explain
Quantia backend = facts, calculations, policies, decisions, economic evidence
```

No component added by this migration can execute production trades.

---

# CURRENT

## Telegram

`scripts/telegram_bot.py` exposes a command- and button-oriented UX. It contains command handlers for portfolio, analysis, radar, performance, ledger, analytics, system health and the `/agente` entrypoint. Inline keyboards are the primary navigation mechanism.

`src/agentic/telegram.py` adapts `/agente <goal>` to `scripts/run_agent.py`, sends progress messages and returns an audit artifact. It is safe but still feels like a command attached to a traditional Telegram bot.

## Existing agentic layer

The repository already has a useful first agentic foundation under `src/agentic/`:

- `contracts.py`: tool, observation, decision, trace and result contracts.
- `orchestrator.py`: bounded model -> tool -> observation loop, duplicate-call guard, in-run result cache and audit persistence.
- `model.py`: Ollama controller, bounded context, JSON control output and prompt-injection warnings for tool output.
- `tools.py`: read-only registry backed by the real Portfolio DB, macro module, deterministic analysis, radar, performance and Decision Lab.
- `persistence.py`: `agent_runs` and `agent_steps` audit trail plus a minimal 24-hour conversation context.
- `diagnostics.py`: deterministic evidence policies for several known question families.
- `answer.py`: evidence-only fallback synthesis.

The current implementation already has valuable safety properties and they MUST be retained:

1. registered tools are read-only;
2. analysis subprocesses run with read-only DB settings and no-persist flags;
3. the controller cannot place orders;
4. the loop is bounded;
5. identical calls are blocked;
6. every run can be audited;
7. tool output is treated as untrusted data;
8. Decision Lab remains evidence, not production policy.

## Current gaps

The existing agent is a bounded evidence loop, not yet the complete conversational harness:

- task specification is implicit in `question_plan` rather than a durable typed task object;
- conversation memory stores recent goals but not structured subject/reference state;
- context selection and context budgets are spread across model history truncation rather than represented explicitly;
- evidence lacks a common source/mode/quality/freshness contract at the harness boundary;
- run state is reconstructed from trace steps rather than exposed as an explicit state machine;
- verification is mostly embedded in deterministic answer builders and tool behavior rather than a named final gate;
- permissions are effectively read-only but not modeled as capabilities;
- telemetry is incomplete for context size, tool failures, retries and verification;
- several legacy business capabilities are not available through the agent registry;
- Telegram still exposes menus, buttons and commands.

---

# TARGET

```text
Telegram / future UI
        |
        v
Conversational Gateway
        |
        v
Task Parser ---- Conversation State
        |                 |
        +------ Context Engine
                    |
                    v
             Harness Runtime
          /        |         \
      Planner   Run State   Budgets
          |        |         |
          +------ Tool Registry
                     |
      +--------------+----------------+
      |              |                |
  Portfolio       Market          Research
  Risk            Macro            Decision Lab
  Ledger          Analytics        Optimizer / plan
      +--------------+----------------+
                     |
               Evidence Objects
                     |
               Verification Gate
                     |
               Response Synthesis
                     |
                     v
                   User
```

## Task specification

Every message is normalized into a typed `TaskSpec` containing at minimum:

- intent/family;
- objective;
- entities/tickers;
- requested horizons;
- required and optional evidence capabilities;
- references inherited from conversation state;
- ambiguity flags;
- economic importance / verification requirement.

Known task families are useful accelerators, not an exhaustive intent whitelist. Unknown or compound questions remain valid and can be planned dynamically.

## Context

Context is selected for the current task instead of replaying the entire product state or entire chat.

The Context Engine produces a `ContextPack` with:

- current structured conversation references;
- small recent-user-goal summary;
- evidence artifacts selected for the current task;
- explicit character/token budget metadata;
- pruning decisions.

Tool observations are compacted into evidence artifacts. Large raw outputs remain in the audit trace and only relevant, bounded excerpts are rehydrated for planning/synthesis.

## Memory vs context vs state

These concepts are kept separate:

- **Context**: ephemeral information required for the current answer.
- **Memory**: durable product/methodology preferences deliberately persisted elsewhere by Quantia configuration; the harness does not indiscriminately save chat history.
- **State**: current conversation references and current run execution state.

Conversation state stores subjects such as active tickers, last intent/task, last run/decision reference and current conversation ID. It does not promote prior assistant prose into financial evidence.

## Tool registry and capabilities

All user-visible analytical capabilities are exposed as typed tools over existing business logic. Tools declare:

- capability (`READ`, `COMPUTE`, `WRITE_INTERNAL`, `FORBIDDEN`);
- input schema;
- timeout;
- freshness/quality metadata where applicable;
- economic mode (`PRODUCTION`, `SHADOW`, `RESEARCH`, `OBSERVATION`);
- failure semantics.

The production conversational profile permits financial reads/computes and internal conversation/audit writes only. Trade execution and unsafe DB mutation are forbidden.

## Evidence object

Every successful observation is normalized at the harness boundary into an `Evidence` object:

```json
{
  "source": "decision_lab",
  "tool": "compare_plan_vs_hold",
  "timestamp": "...",
  "mode": "RESEARCH",
  "quality": "LOW|MEDIUM|HIGH|UNKNOWN",
  "data": {},
  "warnings": [],
  "freshness_seconds": 0,
  "sha256": "..."
}
```

Mode and quality are never inferred upward. Research/shadow evidence cannot be relabeled as production.

## Orchestration

The runtime uses a bounded loop:

```text
parse task
-> select context
-> choose next required/useful action
-> execute allowed tool(s)
-> normalize/compact evidence
-> update run state
-> enough evidence?
   yes -> verify -> synthesize
   no  -> continue within budget
```

Independent evidence calls may run concurrently when the plan marks them safe to parallelize.

Budgets are explicit for steps, tool calls, identical calls, retries, total elapsed time, observation size and model context.

## Verification

A deterministic verification gate runs before the response is accepted. It checks at least:

- at least one successful source for substantive financial claims;
- every material number in a structured synthesis payload has an evidence origin;
- failed/missing sources are surfaced when material;
- current decisions include timestamps/source mode;
- SHADOW/RESEARCH evidence is not described as production policy;
- low/insufficient Decision Lab quality cannot become a claim of edge;
- the run did not request a forbidden capability;
- budget exhaustion is disclosed.

An optional verifier model may be configured for complex cases, but deterministic checks remain authoritative for invariants.

## LLM strategy

Model names/roles are centralized in harness settings:

- `QUANTIA_LLM_ROUTER`
- `QUANTIA_LLM_REASONING`
- `QUANTIA_LLM_SYNTHESIS`
- `QUANTIA_LLM_VERIFIER`

They may all point to the same local model. Separate calls are used only when they add measurable value. Deterministic routing/verification is preferred for cheap obvious cases.

## Telegram target UX

Production Telegram runs a conversational gateway only:

```text
User: ¿Cómo está mi cartera hoy?
Quantia: <answer grounded in portfolio + relevant plan/risk evidence>

User: ¿Qué te preocupa más?
Quantia: <follow-up using structured subject/reference state>

User: ¿Por qué?
Quantia: <rehydrates relevant decision/evidence, not the whole chat>
```

No inline keyboard, reply keyboard, command menu, or command list is needed for normal operation. Telegram `/start` may remain as a transport-level bootstrap only if required by Telegram, but it does not expose navigation. Administrative break-glass operations stay outside the conversational UX.

---

# MIGRATION

## Stage 1 - inventory and invariants

Complete in this document and in code review:

- preserve existing deterministic portfolio/optimizer/risk/Decision Lab modules;
- preserve read-only agent guards and audit trail;
- reuse `ToolRegistry`, existing tool handlers and `AgentRunStore`;
- do not add trade execution to the agent.

## Stage 2 - typed harness contracts

Add typed task, context, evidence, conversation, budget, run-state and verification contracts. Pydantic is already a project dependency and is used at harness boundaries.

## Stage 3 - conversation/context layer

Add explicit structured conversation state with Redis short TTL for hot state and PostgreSQL/audit metadata as fallback/source of trace continuity. Only references and user goals are persisted; previous assistant conclusions are never reused as market facts.

## Stage 4 - registry adapter

Wrap the existing registry with capability/mode metadata and add missing read-only capabilities by calling the real services/scripts. Do not duplicate economic formulas in the harness.

## Stage 5 - runtime and verification

Introduce a `ConversationalHarness` that composes task parsing, context selection, bounded orchestration, evidence normalization, state updates, verification and response rendering. Keep legacy `AgentOrchestrator` available for CLI compatibility while the conversational runtime becomes the production entrypoint.

## Stage 6 - Telegram cutover

Add a minimal conversational Telegram process. Every authorized text message becomes a harness request. Remove visible command menus and keyboards from the production entrypoint. Keep the legacy bot file temporarily for rollback/internal maintenance but do not run it as the default container command.

## Stage 7 - tests and evals

Add deterministic unit tests for:

- task parsing and reference inheritance;
- context pruning/budgets;
- capability enforcement;
- evidence mode/quality preservation;
- state transitions and stop conditions;
- verification failures;
- conversational follow-ups.

Add integration/eval fixtures for portfolio review, ticker explanation, follow-up `¿por qué?`, comparison, opportunities, performance, 20D outcomes, Decision Lab/meta-policy explanation and degraded-source behavior.

## Stage 8 - observability and production validation

Every run records:

- run/conversation IDs;
- normalized task;
- selected context metadata;
- tools/capabilities/modes;
- tool latency and failure count;
- model role/name and token estimates when available;
- state transitions;
- verification result;
- response status.

Deployment remains fail-closed for production trading.

---

# Non-goals

- Replacing Quantia's optimizer, risk, macro, ledger or Decision Lab calculations with LLM arithmetic.
- Teaching the model secrets or DB credentials.
- Enabling autonomous broker execution.
- Treating research/shadow results as production policy.
- Saving the complete Telegram chat as semantic memory.
- Running the heaviest workflow for every question.

# Definition of done

The migration is done when the production Telegram entrypoint is conversational-only, existing economic capabilities remain accessible through registered tools, follow-ups preserve structured references, all runs are bounded and traceable, responses pass evidence verification, research/shadow modes stay isolated, and the new harness tests/evals pass together with the existing suite.