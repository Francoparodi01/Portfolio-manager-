# Quantia semantic layer

This document gives the agent stable meanings for financial and analytical terms. It is context, not a substitute for canonical code.

Revisión: 2026-10-22. Alcance: definiciones, no resultados actuales ni autorización de ejecución.

## Estados y autoridad
- BUY, SELL, REDUCE, INCREASE, HOLD, WAIT y WATCH describen la acción de la capa que los emitió. No conviertas WAIT/WATCH/HOLD en una compra o venta implícita.
- Una propuesta bloqueada conserva su acción y motivo, pero no demuestra ejecución.
- Un estado `EXECUTED` en un log analítico debe interpretarse según su productor; para acreditar una operación real se necesita evidencia de ejecución del broker vinculada.
- `ExecutionPlan` es la referencia operativa del plan; pesos del optimizer, ideas del radar, overlays técnicos y respuestas del agente tienen alcances diferentes.
- `SHADOW_ONLY` significa evaluación sin efecto operativo; no implica que todos los módulos compartan horizonte, métrica o criterio de promoción.
- El LLM interpreta consultas, elige herramientas y sintetiza evidencia. El motor cuantitativo calcula señales y restricciones. Cambiar de modelo LLM no recalcula ni mejora automáticamente los resultados de los shadow.

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

## Unidades, horizontes y denominadores
- `5d/20d` no garantiza sesiones bursátiles. Identificar si el productor usa días calendario, sesiones, o primera vela disponible desde una fecha objetivo.
- La medición histórica Telegram con entrada al cierre de la siguiente sesión tiene su propio protocolo. No reemplazarlo por el precio de publicación ni mezclarlo con outcomes nativos.
- 75 bps equivalen a 0,75%; una diferencia entre retornos se expresa en puntos porcentuales. Un retorno unitario de `0.05` se presenta como 5%.
- Tasa de acierto = movimientos favorables / casos evaluables de la cohorte definida. Informar pendientes, faltantes y exclusiones; no contarlos como pérdidas o ceros.
- Retorno medio de señales no es rentabilidad acumulada de cartera. No sumar retornos solapados como si fueran operaciones independientes.
- Diferenciar una señal diaria por ticker, un episodio y una ventana sin solapamiento. Declarar cuál se utilizó antes de comparar porcentajes.

## Cuatro shadow: métricas distintas
Consultar `docs/agent/shadow-modules.md` para sus fuentes y límites.
- Calibración Shadow V3 (`price_trend_calibrated_shadow_v3`): error absoluto medio de retorno y Brier de probabilidades; menor es mejor. Cobertura de un intervalo de 80% no es 80% de acierto direccional. Separar fit y walk-forward.
- Learning Shadow: evalúa bloqueos y oportunidades perdidas. `clean_miss_rate` no es win rate de una estrategia ni autorización para eliminar guards.
- Technical Shadow V2: utilizar `bias` POSITIVE/NEGATIVE/NEUTRAL guardado; no asignar dirección por el signo de un score dentro de la zona neutral.
- Technical Buy Shadow V3: candidatos BUY A/B, C por confirmar y REJECTED; no decide ventas. Su horizonte nativo de 20 días calendario no equivale a 20 ruedas.
- `get_calibration_audit` consulta Decision Calibration Layer (DCL); no sustituye la evaluación de Calibración Shadow V3.

## Fuentes, moneda y cobertura
Una vela de CEDEAR local en ARS no es intercambiable con su subyacente en USD.
No inferir ratios, ajustes corporativos ni conversiones ausentes. En `/ticker`,
la alternativa yfinance cambia la fuente y se etiqueta USD: verificar la leyenda.
La última fecha disponible no demuestra precio en vivo.

## Documentación frente a evidencia de resultados
Las guías describen implementación y contratos. Las tablas de un replay o auditoría
con fecha son resultados de ese corte. Una actualización de documentación no
actualiza sus datos. Para responder «hoy», usar una fuente vigente y citar su corte;
si solo existe un corte histórico accesible, dar ese resultado con su fecha.

## Owners
Account-scoped financial data must never be mixed across owners. Legacy `owner_chat_id IS NULL` rows may only be used through an explicitly verified single-owner compatibility path.
Además, separar `source` y `metric_scope`: primary, planner_audit, blocked_audit,
radar_audit y debug no forman una única muestra. Un registro global de un módulo
con owner 0 no se debe reasignar a una cuenta. Falta de filas en la superficie
owner-scoped no demuestra ausencia de evidencia global del módulo.
