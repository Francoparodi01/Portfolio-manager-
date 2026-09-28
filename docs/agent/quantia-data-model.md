# Quantia agent data catalog

This is the compact logical catalog exposed to the analytical agent. Runtime schema inspection remains the source for exact live column availability.

## Scoped analytical relations

### decision_log
Primary historical decision/audit relation.

Common fields used by the agent:
- `id`
- `owner_chat_id`
- `run_id`
- `decided_at`
- `ticker`
- `decision`
- `final_score`
- `regime`
- `status`
- `metric_scope`
- `source`
- `layers`
- `outcome_5d`
- `outcome_10d`
- `outcome_20d`
- `outcome_40d`
- `outcome_basis`
- `outcome_filled_at`

Use for exploratory decision history. Respect `source`, `metric_scope`, maturity and episode semantics before treating rows as independent recommendations.

### portfolio_snapshots
Account state snapshots.

Typical concepts:
- owner
- snapshot/scrape timestamp
- total account value / NAV-like fields
- cash
- position state stored by the snapshot schema

Use the canonical portfolio tools when exact current account state is requested. SQL exploration is useful for changes over time, but must not infer PnL from two valuations without handling flows/costs.

### broker_fills
Owner-scoped broker fill evidence when present.

Use for execution/fill investigation. Do not assume fills alone reconcile full account PnL.

### position_hold_observations
Chronology of formal HOLD observations used by historical methodologies when available.

Useful for sequence/episode reconstruction. The Historical Edge implementation remains canonical for its exact episode rules.

## Agent metadata relations
Agent audit tables may exist for traces and runs, but they are not financial truth. They can answer questions about what the agent consulted or why a run failed.

## Runtime catalog
The agent has a read-only schema inspection tool that exposes allowlisted relation columns from `information_schema`. If this document and the live schema differ, the live schema determines what SQL can execute, while this document determines semantic expectations.

## Access boundary
The generic SQL explorer does not expose arbitrary PostgreSQL relations. It presents only an allowlisted, owner-scoped analytical surface. The model cannot request a different owner or bypass the scope with schema-qualified table names.