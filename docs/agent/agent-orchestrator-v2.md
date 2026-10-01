# Quantia Agent Orchestrator v2

## Scope

This change fixes orchestration only. It does **not** change financial logic, optimizer behavior, BUY/SELL thresholds, risk scoring, execution semantics, Decision Lab semantics, database ownership, or read-only permissions.

The problem addressed is control-flow drift: the controller LLM could continue requesting tools after the canonical evidence needed for a known question was already available, eventually consuming `max_steps` and falling back to a less specific renderer or older persisted evidence.

## Previous architecture

```text
User /agente goal
  -> GroundedQuantiaAgentModel controller
  -> choose tool
  -> observation
  -> controller again
  -> choose tool / final
  -> max_steps forced final when necessary
  -> generic evidence renderer fallback
```

The model was bounded and read-only, but it still owned the decision to continue gathering evidence. For a canonical portfolio review this could lead to redundant SQL or unrelated tools after the current snapshot and decision evidence already contained the answer.

## New architecture

```text
User
  -> task / intent classification
  -> AgentOrchestrator
  -> deterministic evidence contract for known intents
  -> required canonical tools
  -> EvidenceCompletenessGate
       -> complete: synthesis
       -> incomplete: current-run safe renderer / fail closed
  -> QUANTIA_LLM_SYNTHESIS
  -> QUANTIA_LLM_VERIFIER
  -> final
```

Open-ended questions retain the bounded controller. Deterministic intents no longer return to the controller after their evidence contract is fulfilled.

## Deterministic intents

The first v2 contract is `portfolio_review`.

Required tools, in deterministic order:

1. `get_portfolio_snapshot`
2. `get_decision_evidence`

Required normalized fields for a decision row:

- `current_weight`
- `theoretical_target_weight`
- `executable_target_weight`
- `signal_class`
- `portfolio_intent`
- `risk`
- `technical_regime`
- `trend_score`

`risk` is normalized from the current decision evidence, including the existing risk layer shape (`name=risk`, `raw_score`). Regime and trend come from `technical_regime` and `trend_score`.

The gate intentionally normalizes the existing production payload instead of requiring every field to exist at one literal JSON path.

## Early completion

After every required observation:

```text
tool executed
  -> observation persisted
  -> EvidenceCompletenessGate
  -> if satisfied: composing
  -> no controller round-trip
```

For a complete `portfolio_review`, no third analytical tool is permitted or needed. `query_quantia_sql`, `analyze_portfolio`, `get_decision_ledger`, and `get_persisted_decision_evidence` are not part of the canonical plan.

A separate guard rejects redundant exploratory SQL when current `get_decision_evidence` already supplies the canonical target/signal/risk/regime/trend fields.

## Model roles

Configuration is environment-driven rather than hard-coded in orchestration logic.

Recommended local roles:

```env
QUANTIA_AGENT_MODEL=qwen3.5:9b
QUANTIA_LLM_ROUTER=qwen3.5:9b
QUANTIA_LLM_REASONING=qwen3.6:27b
QUANTIA_LLM_SYNTHESIS=qwen3.6:27b
QUANTIA_LLM_VERIFIER=qwen3.6:27b
```

Responsibilities:

- Router/controller: intent classification and bounded open-ended control.
- Python orchestrator: required tools, evidence contracts, budgets, loop guards, read-only/fail-closed behavior.
- Synthesis model: explanation over evidence already obtained; it receives no tool-selection authority.
- Verifier model: checks grounding and semantic consistency; it cannot alter a financial decision.

## Synthesis evidence

The composer receives compact evidence from the current run including:

- original goal and intent
- successful canonical observations
- current/executable/theoretical target fields
- risk/regime/trend
- timestamps and stale marker
- current portfolio snapshot and cash
- scope and completion-gate result
- detected non-evaluable positions

Controller chain-of-thought is never forwarded.

## Verifier contract

The verifier checks at minimum:

- user-visible numbers are grounded in supplied evidence
- theoretical target is not described as executable target
- `WATCH` / `BLOCKED` are not described as orders
- a plan is not described as a fill
- a frozen/non-evaluable position is not assigned an invented zero target
- stale evidence is disclosed
- absent fields are not invented

Allowed verifier outcomes:

- `APPROVE`
- `RESYNTHESIZE` (one grounded retry)
- `SAFE_RENDER`

The verifier never changes financial decisions.

## Frozen / non-evaluable assets

The current portfolio is interpreted as:

```text
optimizable positions
+ frozen/non-evaluable positions
+ cash
```

A ticker present in the portfolio snapshot but absent from evaluable decision evidence is reported as residual/frozen/non-evaluable. It is **not** assigned target 0. If the optimizer output does not expose an explicit `frozen_weight`, the response states that limitation rather than deriving one.

## Fallback behavior

For a deterministic portfolio review the safe renderer consumes the normalized evidence from the **current run**. This preserves the distinction between current weight, theoretical optimizer target, and executable planner target even if synthesis or verification fails.

Current-run evidence is never discarded merely because the step budget was reached. A complete gate at the budget boundary returns `COMPLETE`, not `max_steps`.

Older persisted evidence is not consulted when fresh current evidence is already sufficient.

## Observability and audit

The run records these progress events without exposing chain-of-thought:

```text
✓ Consulta interpretada
✓ Cartera cargada
✓ Evidencia de decisión obtenida
✓ Evidencia suficiente
⏳ Elaborando respuesta
✓ Respuesta verificada
```

Audit metadata includes:

- `intent`
- `required_tools`
- `executed_tools`
- `completion_gate`
- `completion_reason`
- `synthesis_model`
- `verifier_model`
- `verifier_action`
- `fallback_used`
- `progress_events`

`AgentRunStore`, existing step persistence, conversation context, tool registry, SQL protections, Telegram integration, and Decision Lab routes remain intact.

## Tests

`tests/test_agent_orchestrator_v2.py` covers:

- early completion after exactly two canonical tools
- no redundant SQL or stale persisted evidence after completion
- missing required field prevents premature completeness
- GDX `WATCH`: current 12.56%, theoretical 34.95%, executable 12.56%
- NVDA integer/planner rounding: current 17.5%, theoretical 2.0%, executable 2.19%
- 11 snapshot positions vs 10 evaluable signals with `USESPECIE` residual
- no implicit target 0 for non-evaluable positions
- fresh evidence at `max_steps=2` completes from the current run
- environment-driven controller/synthesis/verifier roles
- verifier guards for order/fill semantic drift

Existing agent loop and grounded-agent tests continue to cover generic controller behavior and read-only protections.

## Real validation target

Validation query:

> Para la decisión más relevante de mi cartera actual, explicame la diferencia entre el peso actual, el target teórico del optimizer y el target ejecutable. Mostrame también SignalClass, PortfolioIntent, Risk, Regime y Trend, y decime si alguna posición quedó frozen/no evaluable y cómo afecta eso al presupuesto total.

Expected flow:

```text
get_portfolio_snapshot
  -> get_decision_evidence
  -> evidence complete
  -> QUANTIA_LLM_SYNTHESIS
  -> QUANTIA_LLM_VERIFIER
  -> final
```

For this query the run must not invoke `query_quantia_sql`, `get_decision_ledger`, `get_persisted_decision_evidence`, or `analyze_portfolio`.

Expected terminal state:

- `status = COMPLETE`
- `stop_reason != max_steps`
- `objective_status = EXPLAINED`
- evidence belongs to the current run

## Limitations

- v2 initially registers a deterministic evidence contract only for `portfolio_review`; other intents retain their existing bounded behavior until an equally explicit evidence contract is defined.
- A missing canonical source or missing required field is reported as incomplete rather than reconstructed with exploratory SQL.
- The large synthesis/verifier models require the configured Ollama models to be locally available; deterministic rendering remains the fail-safe path.
- This branch does not deploy or merge itself. Production rollout remains gated on tests, CI, and an explicit deployment action.
