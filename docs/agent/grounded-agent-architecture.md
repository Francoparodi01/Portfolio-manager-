# Quantia Grounded Agent v1

## Objective
Move `/agente` from intent-specific routing toward a grounded analytical agent that can investigate novel questions while preserving deterministic financial semantics and read-only safety.

The Telegram classic bot, buttons, `/analisis`, `/meta`, execution planner and capital behavior are not changed by this architecture.

## Runtime flow

```text
User /agente question
        |
        v
GroundedQuantiaAgentModel
        |
        +-- AGENTS.md
        +-- quantia-semantics.md
        +-- quantia-data-model.md
        |
        v
Dynamic evidence planning
        |
        +-- canonical read-only tools
        |     Decision Lab / portfolio / analytics / Historical Edge
        |
        +-- inspect_quantia_schema
        +-- query_quantia_sql
        +-- search_quantia_docs
        |
        v
Tool observations + hashes
        |
        v
LLM synthesis
        |
        v
Deterministic semantic verifier
        |
        v
Answer + audited tool provenance
```

## Separation of responsibilities

### LLM owns
- understanding open-ended analytical intent;
- selecting relevant read-only evidence sources;
- forming exploratory SQL within the permitted grammar;
- deciding whether more evidence is needed;
- synthesizing observed evidence into a concise explanation.

### Deterministic software owns
- database read-only enforcement;
- owner scoping;
- SQL allowlist and validation;
- tool schemas and timeouts;
- canonical Decision Lab semantics;
- canonical Historical Edge methodology;
- critical semantic guards for DVA, realized PnL and causal claims;
- trace persistence and evidence hashes.

## SQL Explorer security model
`query_quantia_sql` is not a raw PostgreSQL shell.

It accepts one SELECT and rejects:
- write/DDL/transaction statements;
- comments and multiple statements;
- schema-qualified relation access;
- relations outside the allowlist;
- nested SELECT/CTE/UNION-style escape paths;
- dangerous PostgreSQL functions;
- implicit comma cross joins.

The runtime rewrites referenced allowed relations as owner-scoped CTEs before execution and connects with `default_transaction_read_only=on` and a statement timeout.

Current generic analytical surface:
- `decision_log`
- `portfolio_snapshots`
- `broker_fills`
- `position_hold_observations`

The allowlist should grow deliberately, not by giving the model arbitrary DB access.

## Canonical vs exploratory analysis
Use canonical tools when a project-defined metric must not drift:
- real/account performance or reconciled PnL;
- PLAN vs HOLD / DVA;
- canonical episode construction;
- current portfolio state where a dedicated source exists.

Use SQL Explorer for novel exploratory questions such as:
- score bucket comparisons;
- regime interactions;
- decision counts and descriptive distributions;
- exploratory associations across allowed relations.

Exploratory evidence does not become a production rule automatically.

## Project-document RAG
`search_quantia_docs` performs bounded retrieval over checked-in Markdown documentation. It returns snippets with file path and SHA-256 so architecture/metric explanations are grounded in the repository rather than model memory.

AGENTS.md and the two core semantic/catalog documents are loaded into every agent run. The wider documentation set is retrieved on demand.

## Synthesis and verification
For open-ended questions, Qwen may synthesize the final answer from successful observations. The runtime then applies deterministic guards:
- DVA claims require Decision Lab evidence;
- realized/real PnL claims require canonical performance evidence;
- causal/guarantee claims are rejected when based only on exploratory SQL;
- at least one successful evidence observation is required.

Every accepted grounded synthesis appends the successful tool names. SQL observations also expose a query hash.

## Traceability
Agent runs persist:
- goal;
- model;
- source hashes;
- prompt-context hashes;
- selected tools and arguments;
- observations;
- observation hashes;
- final status and answer origin.

`agent_version=quantia-grounded-agent-v1` identifies the new architecture.

## Evals
Static architecture/security tests:

```bash
pytest -q tests/test_grounded_agent_architecture.py
```

Real local evals against Ollama + Quantia DB:

```bash
python scripts/run_grounded_agent_evals.py --owner-chat-id <CHAT_ID>
```

Single cases can be targeted with repeated `--case-id`.

The eval corpus lives at:

```text
evals/agent/grounded_queries_v1.json
```

The real eval runner records selected tools, status, model, answer and run ID. It never enables write/trading capabilities.

## Deployment verification
After rebuilding the Telegram container, a new agent trace should include:

```text
agent_version: quantia-grounded-agent-v1
dynamic_planning: true
sql_explorer: owner-scoped-read-only-v1
docs_retriever: checked-in-markdown-v1
```

The Dockerfile copies the whole repository (`COPY . .`), so AGENTS.md and the agent documentation are present in the runtime image.

## Explicit non-goals in v1
- no passive listening to ordinary Telegram chat;
- no automatic trading;
- no database writes;
- no threshold/config mutation;
- no unrestricted SQL;
- no multi-agent topology solely for architectural complexity.
