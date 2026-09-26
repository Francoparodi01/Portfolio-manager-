# Decision vs Market Audit

Generado: 2026-09-02T22:17:17.757512+00:00
Ventana: 180 dias; costo usado para neto: 75 bps.

## Lectura ejecutiva

- Veredicto: Plan ejecutado positivo neto a 5d; requiere estabilidad y calibracion de score.
- Plan ejecutado 5d: n=33, hit=+63.6%, neto=+1.5%.
- Plan ejecutado 20d: n=28, hit=+64.3%, neto=+4.7%.
- Los bloqueos capturaron una cohorte con retorno neto negativo; no aflojar guards.
- final_score se reporta como diagnostico, no como autorizacion para cambiar thresholds.

## Cobertura de datos

- decision_log: 926 filas (2026-04-09T18:00:00+00:00 a 2026-09-02T19:50:26.976345+00:00).
- broker_fills: 245 filas (2026-04-09T03:00:00+00:00 a 2026-08-27T17:12:14.935742+00:00).
- plan_execution_attributions: 36 filas (2026-05-27T03:00:00+00:00 a 2026-08-14T15:19:05.128900+00:00).

## Cohortes principales

| Cohorte | 5d n | Hit 5d | Ret. neto 5d | 20d n | Hit 20d | Ret. neto 20d | Score corr 5d |
|---|---:|---:|---:|---:|---:|---:|---:|
| Plan ejecutado | 33 | +63.6% | +1.5% | 28 | +64.3% | +4.7% | -0.310 |
| Plan aprobado sin fill | 117 | +47.0% | -1.0% | 101 | +53.5% | +1.4% | 0.051 |
| Plan bloqueado | 317 | +52.4% | -1.1% | 279 | +54.5% | -1.2% | 0.035 |
| Manual/broker real | 145 | +44.1% | -1.4% | 139 | +42.4% | -3.1% | N/A |
| Radar idea | 93 | +54.8% | +1.7% | 63 | +34.9% | -2.7% | -0.015 |

## BUY vs benchmarks

| Cohorte BUY | Horizonte | Benchmark | n | Ret. decision | Benchmark | Alpha BUY |
|---|---|---|---:|---:|---:|---:|
| execution_plan_approved | 5d | QQQ | 42 | +1.5% | +0.1% | +1.3% |
| execution_plan_approved | 5d | SPY | 42 | +1.5% | +0.6% | +0.9% |
| execution_plan_approved | 20d | QQQ | 35 | +2.3% | +2.3% | -0.0% |
| execution_plan_approved | 20d | SPY | 35 | +2.3% | +3.7% | -1.4% |
| execution_plan_blocked | 5d | QQQ | 291 | -0.4% | +0.6% | -0.9% |
| execution_plan_blocked | 5d | SPY | 291 | -0.4% | +0.8% | -1.2% |
| execution_plan_blocked | 20d | QQQ | 239 | -0.8% | +1.9% | -2.8% |
| execution_plan_blocked | 20d | SPY | 239 | -0.8% | +3.3% | -4.2% |
| execution_plan_executed | 5d | QQQ | 9 | +0.2% | +0.1% | +0.1% |
| execution_plan_executed | 5d | SPY | 9 | +0.2% | +0.6% | -0.3% |
| execution_plan_executed | 20d | QQQ | 7 | -6.0% | +1.5% | -7.5% |
| execution_plan_executed | 20d | SPY | 7 | -6.0% | +3.1% | -9.1% |
| radar_idea | 5d | QQQ | 93 | +2.5% | +1.1% | +1.4% |
| radar_idea | 5d | SPY | 93 | +2.5% | +0.8% | +1.7% |
| radar_idea | 20d | QQQ | 63 | -2.0% | +2.3% | -4.3% |
| radar_idea | 20d | SPY | 63 | -2.0% | +2.4% | -4.4% |

## Mejores planes 5d

| ID | Fecha | Ticker | Decision | Estado | Score | Outcome 5d |
|---:|---|---|---|---|---:|---:|
| 741 | 2026-08-04 | TEAM | BUY | APPROVED | 0.080 | +47.6% |
| 753 | 2026-08-04 | TEAM | BUY | BLOCKED | 0.050 | +47.3% |
| 649 | 2026-07-23 | MU | SELL | EXECUTED | -0.119 | +22.8% |
| 67 | 2026-05-21 | AMD | BUY | BLOCKED | 0.063 | +21.7% |
| 52 | 2026-05-18 | AMD | BUY | BLOCKED | 0.107 | +19.3% |
| 305 | 2026-06-06 | ASTS | BUY | BLOCKED | 0.010 | +18.5% |
| 582 | 2026-07-13 | ALAB | SELL | EXECUTED | -0.159 | +17.9% |
| 490 | 2026-06-25 | SNOW | BUY | BLOCKED | 0.000 | +17.1% |

## Peores planes 5d

| ID | Fecha | Ticker | Decision | Estado | Score | Outcome 5d |
|---:|---|---|---|---|---:|---:|
| 398 | 2026-06-11 | ASTS | BUY | BLOCKED | -0.066 | -39.4% |
| 577 | 2026-07-10 | IBM | BUY | BLOCKED | 0.079 | -30.2% |
| 565 | 2026-07-08 | IBM | BUY | BLOCKED | -0.063 | -28.8% |
| 572 | 2026-07-09 | IBM | BUY | BLOCKED | 0.076 | -28.8% |
| 309 | 2026-06-07 | ASTS | BUY | BLOCKED | -0.012 | -28.2% |
| 293 | 2026-06-05 | ASTS | BUY | BLOCKED | 0.014 | -21.6% |
| 275 | 2026-06-05 | QCOM | BUY | APPROVED | 0.104 | -21.2% |
| 658 | 2026-07-24 | MU | BUY | BLOCKED | -0.041 | -21.0% |

## TimesFM-3 T-1 shadow

Agregar timesfm3_tminus1_shadow como consumidor de snapshots point-in-time; sus deltas solo comparan contra la heuristica y no escriben decisiones reales.

Regla de seguridad: el shadow puede proponer deltas de heuristica, pero no modifica score, optimizer, planner, orders ni fills.

## Caveats

- En planes ejecutados, final_score no ordena bien el outcome; requiere calibracion antes de tocar thresholds.
- TimesFM-3 queda fuera de ejecucion: solo shadow T-1 por licencia/evidencia pendiente.
