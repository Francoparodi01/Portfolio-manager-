---
name: audit-decision-consistency
description: Audit whether signal, score, portfolio target, planner action and guards are internally coherent.
version: v1
---

# Audit Decision Consistency

## Workflow

For each relevant ticker, compare in order:

1. signal decision and final_score;
2. technical regime and material layers when present;
3. current_weight;
4. target_weight and delta_weight;
5. planner action;
6. guards, restrictions or blocked orders.

Flag a contradiction only from observed fields. Example: signal HOLD plus planner BUY is a cross-layer disagreement; explain which layer produced each value instead of silently choosing one as truth.

## Verification

Both structured signals and planner decisions are required. If either side is absent, return insufficient evidence. Do not repair the economic engine from this skill.
