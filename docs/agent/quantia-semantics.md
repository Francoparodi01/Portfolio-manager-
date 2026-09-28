# Quantia semantic layer

This document gives the agent stable meanings for financial and analytical terms. It is context, not a substitute for canonical code.

## Decisions and scores
- `decision`: proposed action recorded by Quantia, commonly BUY, SELL, REDUCE or HOLD depending on the producing layer.
- `final_score`: bounded internal decision score assembled from Quantia layers. It is not a return forecast, probability of profit, PnL or position size.
- A change in score between runs is not itself a realized gain/loss.

## Outcomes
- `outcome_5d`, `outcome_10d`, `outcome_20d`, `outcome_40d`: directional outcomes attached to a recorded decision under `outcome_basis`.
- `outcome_filled_at`: when the outcome became available in the database. Point-in-time analysis must not use an outcome before this timestamp.
- Canonical Cocos outcomes are observational directional returns. They are not automatically realized portfolio PnL.

## Episodes
When measuring whether a decision pattern historically worked, repeated same-ticker/same-direction recommendations across consecutive formal runs can represent one continuing episode rather than independent bets.

The canonical Historical Edge implementation owns the exact episode definition. The generic SQL explorer may inspect rows, but should not claim independent episode statistics unless it reproduces or invokes that canonical methodology.

## PnL and return concepts
- Realized/account PnL: must come from reconciled account/NAV/fill evidence when available.
- Directional return: outcome of the signal direction over a horizon.
- Gross return: before the explicitly modeled research/trading cost.
- Net return: gross return minus the cost definition used by that analysis.
- `PnL direccional` in historical reports is not necessarily identical to reconciled broker/account PnL.
- Missing NAV reconciliation means real net PnL may remain N/D even if directional evidence exists.

## PLAN vs HOLD / DVA
Decision Value Added (DVA) is a counterfactual comparison between a recorded PLAN and a valid HOLD alternative under Decision Lab methodology.

A ticker's directional `outcome_20d` is not DVA. Do not infer HOLD performance by negating or zeroing a directional return.

## Point-in-time discipline
For a question evaluated as-of time T:
- decision evidence must have existed by T;
- historical outcomes used as evidence must have `outcome_filled_at <= T` when that field applies;
- later corrections/data fills must not be presented as contemporaneously known evidence.

## Sample quality
Always expose sample size when making historical comparisons. Prefer reporting:
- n observations/episodes;
- number of distinct dates when relevant;
- win rate;
- mean/EV;
- median;
- profit factor or downside metric when available;
- evidence quality/limitations.

Small exploratory subgroups are hypotheses, not production rules.

## Owners
Account-scoped financial data must never be mixed across owners. Legacy `owner_chat_id IS NULL` rows may only be used through an explicitly verified single-owner compatibility path.