---
name: analyze-ticker
description: Evaluate one portfolio ticker using current Quantia evidence without turning a score into a return forecast.
version: v1
---

# Analyze Ticker

## Workflow

1. Establish current holding and weight from the portfolio snapshot.
2. Read the current signal and planner evidence.
3. Use targeted ticker analysis only as supporting evidence.
4. Add macro or Decision Lab evidence only when it changes the explanation.
5. Separate signal, optimizer/planner target and executable action.

## Verification

Do not answer as fully grounded unless current decision signals are present. Never claim that a score is expected return, probability of profit or realized PnL.
