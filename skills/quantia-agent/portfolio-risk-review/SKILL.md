---
name: portfolio-risk-review
description: Review the current account snapshot and latest formal decision evidence without recomputing the portfolio.
version: v1
---

# Portfolio Risk Review

## Workflow

1. Read the latest account snapshot.
2. Pair it with the latest persisted formal decision evidence.
3. Identify concentration, cash and positions that matter most.
4. Keep stale snapshots and non-evaluable positions explicit.
5. Distinguish current holdings from proposed targets.

## Verification

Do not infer account PnL from valuation differences. Do not treat missing current signals as target zero.
