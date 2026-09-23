# Jev router for the Quantia agent loop

## Purpose

Jev is integrated only as a **System One control router** in front of the existing
Ollama controller. It does not calculate indicators, returns, volatility, sizing,
weights, risk limits, optimizer output, execution plans, or orders. Those remain
owned by deterministic Quantia code and the existing read-only tool boundary.

The production control path is:

`goal/state -> Jev route -> (safe read-only tool | Ollama controller) -> observation -> repeat`

Jev never produces final prose and never supplies dynamic trading parameters.

## Modes

`QUANTIA_AGENT_ROUTER=off`
: Baseline behavior. Jev is not called and no API key is required.

`QUANTIA_AGENT_ROUTER=shadow`
: Jev runs on each non-forced step and its choice/probabilities/confidence are
  audited, but Ollama remains authoritative. Use this first in production.

`QUANTIA_AGENT_ROUTER=active`
: A Jev decision is applied only when confidence meets
  `QUANTIA_JEV_CONFIDENCE_THRESHOLD`. Jev may directly select only a registered
  `read_only=True` tool whose JSON schema has no required arguments. Tools that
  need dynamic arguments are delegated to Ollama. A Jev `final` choice only
  forces Ollama to synthesize the final text.

## Fail-safe behavior

The existing bounded Ollama path is the fallback for:

- TypeSafe timeout, network error, 429 or 5xx;
- malformed or schema-incompatible response;
- unknown tool choice;
- tool that is not read-only;
- tool requiring dynamic arguments;
- confidence below the configured threshold.

HTTP 4xx responses other than 429 are treated as non-retryable. Retries are
bounded by `QUANTIA_JEV_MAX_RETRIES`, with exponential backoff. If Ollama then
fails, the existing orchestrator stops safely. Jev cannot bypass max-steps,
anti-loop, tool validation, read-only enforcement, or audit requirements.

## Configuration

Required only in `shadow` or `active`:

```text
QUANTIA_JEV_API_KEY=<secret>
```

Operational settings are documented in `.env.agentic.example`. The API key must
be supplied through the deployment secret mechanism and must never be committed.
Invalid mode, missing key, unsafe timeout/threshold/retry values, or invalid state
budget fail during model construction before an agent run starts.

## Audit and observability

`agent_steps.routing` stores a JSON audit record for each routed step, including:

- mode and whether the Jev choice was applied;
- provider/model;
- selected choice;
- confidence and probability distribution;
- provider latency and token usage;
- attempt count;
- fallback reason or sanitized error.

The JSON trace returned by `scripts/run_agent.py --json` includes the same routing
metadata. Logs record applied routes and provider fallbacks without logging API
keys or the complete state payload.

`AgentRunStore.ensure_schema()` performs the additive migration using
`ADD COLUMN IF NOT EXISTS`, so existing audit tables remain compatible.

## Production rollout

1. Deploy the code with `QUANTIA_AGENT_ROUTER=off` and keep
   `QUANTIA_AGENT_ENABLED=false` unless the existing agent rollout is already
   approved.
2. Run the existing Postgres/Ollama/market-data smoke test from
   `docs/15-agentic-loop.md` with the router off.
3. Provision `QUANTIA_JEV_API_KEY` as a secret. Confirm the account exposes the
   configured model with TypeSafe `GET /v1/models`.
4. Set `QUANTIA_AGENT_ROUTER=shadow` and run representative on-demand goals with
   `scripts/run_agent.py --force --json`. Confirm every step has a routing audit
   record and that agent behavior matches the baseline controller.
5. Review shadow disagreement rate, confidence distribution, TypeSafe latency,
   fallbacks and errors. Do not activate based on a single run.
6. Set `QUANTIA_AGENT_ROUTER=active` only after shadow evidence is acceptable.
   Keep the initial confidence threshold at `0.85` or higher.
7. Continue monitoring `routing.fallback_reason`, provider latency, and the
   existing Decision Ledger/outcome evidence before expanding autonomy.

## Smoke cases before ACTIVE

At minimum exercise:

- high-confidence zero-argument tool route;
- low-confidence route -> Ollama fallback;
- dynamic-argument tool -> Ollama;
- Jev final -> forced Ollama synthesis;
- Jev timeout/provider outage -> Ollama fallback;
- malformed Jev response -> Ollama fallback;
- audit database unavailable while `QUANTIA_AGENT_REQUIRE_AUDIT=true` -> fail closed.

The unit suite covers these control paths without external credentials. A live
TypeSafe smoke requires a real API key and cannot be replaced by mocks.

## Rollback

Fast rollback is configuration-only:

```text
QUANTIA_AGENT_ROUTER=off
```

Restart/redeploy the agent process. This restores the pre-Jev controller path
without reverting database changes; the additive `routing` JSONB column is safe
to leave in place. If the whole agentic layer must be disabled, also set:

```text
QUANTIA_AGENT_ENABLED=false
```

No rollback path changes optimizer, planner, risk guards, portfolio state, or
execution state because Jev has no authority over those components.
