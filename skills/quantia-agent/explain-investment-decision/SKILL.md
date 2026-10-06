---
name: explain-investment-decision
description: Explain why Quantia produced a BUY, SELL, REDUCE or HOLD by tracing evidence layers and planner output.
version: v1
---

# Explain Investment Decision

## Workflow

1. Identify the requested ticker.
2. Read structured decision evidence.
3. State signal decision and score separately.
4. State current weight, target weight and planner action separately when available.
5. Explain the material score layers/reasons.
6. Surface guards or blocked orders.
7. Keep historical edge separate from the current mechanism.

## Verification

Do not collapse signal, optimizer target and planner action into one decision. If structured signals are missing, fail closed.
