# Auditoría del sistema de medición económica

Fecha de inspección: 2026-10-14. Base: `f2e08096a2a0b6254598243600f522b8608276d5`.
Implementación QEEF: `d3cca84c06f631773810b4aec5077c35f157aafb`.
La captura, población y pruebas están identificadas en [evidencia verificable](evidence/validation.json) y [validación empírica](QEEF_EMPIRICAL_VALIDATION.md).

## Diagnóstico

Quantia no tiene una única definición de desempeño: conviven diagnóstico direccional, pronóstico, simulaciones de decisiones y contabilidad de cuenta. Algunas piezas están razonablemente concebidas, otras contienen defectos específicos. La capacidad de demostrar valor incremental del portfolio está **insuficientemente validada con los datos disponibles**. No está demostrado que las estrategias destruyan valor ni que generen alpha.

El principal error de interpretación sería tratar win rate o retorno direccional como respuesta a la pregunta de asignación de capital. Un SELL con retorno direccional positivo mide una caída posterior del instrumento; no representa por sí solo una ganancia monetaria, una venta en corto ni el rendimiento de la reinversión. Un HOLD necesita el estado completo del portfolio y alternativas factibles.

## Hallazgos por impacto

| ID | Clasificación | Evidencia comprobable | Implicación y tratamiento |
|---|---|---|---|
| F01 | METHODOLOGICAL_RISK | Los 109 fills exact-owner tienen `fees_ars=NULL`; no hay un ledger NAV/flujos/costos reconciliado en esta captura | No se puede certificar PnL neto ni alpha. QEEF conserva `null`; no asume comisión cero |
| F02 | VERIFIED_DEFECT | `viability_audit._compute_horizon_metrics` compone retornos direccionales por observación con `_max_drawdown`, sin capital ni exposición temporal común | La estadística no es drawdown del portfolio. Siete de nueve cohortes reales 5D presentan variación bajo permutación; existen ventanas solapadas. QEEF sólo admite riesgo de cuenta con curva NAV válida |
| F03 | VERIFIED_DEFECT | `dcl.statistical_auditor._bootstrap_mean_ci([x])` devuelve `[x,x]`; reproducido con una observación real | Un intervalo degenerado no demuestra certeza. QEEF devuelve intervalo ausente con tamaño insuficiente |
| F04 | VERIFIED_DEFECT | `analytics_v2.episodes.build_episodes` particiona por cuenta/módulo/instrumento/original, sin `cohort`; dos cohortes distintas pueden formar un episodio | Test mínimo reproducible demuestra la colisión semántica. No se ha demostrado su frecuencia operativa. QEEF impide mezclar identidades de cohorte; no reescribe episodios legacy |
| F05 | METHODOLOGICAL_RISK | `decision_log` usa días calendario y primera vela posterior; HOLD y Decision Lab usan sesiones | Las etiquetas 5D/10D/20D no son intercambiables. QEEF preserva definición original y explicita unidad |
| F06 | METHODOLOGICAL_RISK | 301 registros, sin `thesis_id` canónico; recurrencias observadas y ventanas superpuestas | No son 301 oportunidades independientes. El agrupador exploratorio produce 143 proxies, nunca oportunidades certificadas |
| F07 | METHODOLOGICAL_RISK | 28.615 observaciones inmutables tienen reloj de disponibilidad, pero todas declaran ratio CEDEAR, versión de calendario y unidad de volumen desconocidos; 120 no confirman cierre | Timestamp y hash no certifican base accionaria, precio ejecutable ni cobertura de acciones corporativas |
| F08 | METHODOLOGICAL_RISK | 48 enlaces exactos decisión/HOLD→plan; 253 ausentes/ambiguos; 0 atribuciones plan-ejecución exact-owner | Ausencia de ejecución enlazada no demuestra que la orden no se ejecutó. Los 253 incluyen manual/radar, que no necesariamente debían tener plan |
| F09 | METHODOLOGICAL_RISK | 30 episodios Decision Lab almacenados, LOW y no primarios; no contienen las barras de mercado necesarias para revalorizar todos sus outcomes desde esta captura | Se preservan sus deltas registrados, sin elevarlos a evidencia QEEF verificada |
| F10 | ACCEPTABLE_DESIGN | Decision Lab congela estado, plan, costos y referencias; evalúa PLAN/HOLD sobre capital común y sesiones explícitas | Reutilizado como núcleo contrafactual. QEEF añade contratos de tesis, base de precios y alternativas registradas |
| F11 | ACCEPTABLE_DESIGN | Analytics v2 separa resultados por oportunidad del PnL y exige reconciliación NAV/flujos/componentes para PnL económico | Reutilizado. El defecto de partición F04 no invalida todos sus mecanismos |
| F12 | ACCEPTABLE_DESIGN | Persistencia formal atómica, `ExecutionPlan`, owner/run, capturas FULL_CONTEXT y evidencia append-only | Mejoran trazabilidad prospectiva, sin corregir retroactivamente evidencia legacy |
| F13 | UNVERIFIED_HYPOTHESIS | Selección sobreviviente, revisiones fundamentales, acciones corporativas omitidas, selección de políticas tras ver outcomes | Riesgos a investigar con universos y versiones históricos completos. No se afirma contaminación general demostrada |
| F14 | UNVERIFIED_HYPOTHESIS | No hay ablaciones controladas prospectivas de los once componentes | No es identificable qué componente causa mejora/deterioro real ni un porcentaje de responsabilidad |

La dependencia del drawdown respecto del orden es normal en una curva temporal auténtica. F02 no se basa sólo en permutar: el defecto es capitalizar observaciones de señales con posiciones/ventanas superpuestas como si fueran retornos sucesivos de un mismo capital. El diagnóstico de permutación hace visible la fragilidad; no estima pérdidas reales.

## Inventario de mecanismos

Los paths de esta tabla son relativos al repositorio. Cada fila distingue fuente, unidad, horizonte, fórmula, supuestos, riesgo/costos y dependencias. No se encontró un archivo `price_trend_shadow.py`: esa denominación se corresponde con el forecast de `thesis_shadow.py` y el registro de estrategia en `docs/architecture/STRATEGY_LAB_DESIGN.md`.

| Mecanismo / archivo y función | Fuente, unidad y horizonte | Cálculo y supuestos | Riesgo, costos, dependencias y límites |
|---|---|---|---|
| Core: `src/analysis/synthesis.py::blend_scores` | Capas técnica/fundamental/macro; ticker por análisis | Mezcla ponderada de scores configurados; score no es probabilidad calibrada ni retorno | Riesgo y restricciones de otras capas; sin costo de ejecución en score. Depende de versiones/disponibilidad de inputs |
| Técnico: `technical.py::compute_indicators`, `generate_signals`, `analyze_ticker_from_frame` | OHLCV por ticker, ventanas de indicadores | Indicadores y reglas de señal; no estimación de valor del portfolio | Historia corta, ajustes y volumen afectan comparabilidad; no costo/ejecución incorporado |
| Shadow V2/V3: `technical_shadow_v2.py::build_technical_shadow_v2`, `technical_buy_shadow_v3.py::build_technical_buy_shadow_v3` | Features/ticker; V3 BUY-only | Clasificaciones/score de investigación, separados del score operativo | No autoridad de sizing/ejecución. No confundir etiquetas del replay con evidencia fuera de muestra |
| Radar: `radar_discovery.py::build_discovery_observations`, `measure_discovery_outcome`, `summarize_comparisons` | Universo de exploración, snapshot/ticker/horizonte | Retorno posterior y comparaciones del universo registrado | 9.864 snapshots y 18.722 outcomes no son operaciones ni oportunidades independientes. Selección/benchmark deben preservarse |
| Price Trend / tesis shadow: `thesis_shadow.py::build_shadow_thesis`, `_forecast_horizon`, `mature_forecast` | Velas y contexto; ticker/forecast; N sesiones posteriores | Tendencia/volatilidad y forecast; retorno `P_h/P_ref-1`, error absoluto/cuadrático y dirección | Pronóstico price-only con overlay; no PnL, costos ni alternativas de capital. Cero forecasts exact-owner capturados |
| Learning: `learning_shadow.py::classify_population`, `classify_counterfactual`, `build_cohort_metric_rows` | Casos por población/cohorte/outcome | Clasificación y agregados condicionales | Sin promoción automática; no mezcla audit/radar/manual. Cero casos y snapshots en la captura |
| Optimizer: `optimizer.py::run_optimizer`, `_attach_sharpe_contract` | Retornos/covarianza/señales y restricciones; pesos de portfolio | Optimización de asignación y métricas esperadas bajo contrato | Riesgo modelado no es riesgo realizado; sensible a covarianza, historia y expectativa. Costos/restricciones sólo según configuración explícita |
| Calibración optimizer: `optimizer_calibration_v2.py::fit_walk_forward_binning`, `_dedupe_primary`, `walk_forward_predictions` | Observaciones primarias y outcomes maduros, corte por entrenamiento | Binning y predicción walk-forward; expectativa condicionada | Deduplicación y mínimo de muestra; research/shadow. No extrapolar resultado diario a anual sin contrato |
| Planner: `execution_planner.py::derive_decision_intents`, `reconcile_funding`, `build_signals_from_synthesis` | Portfolio, cash, pesos, lotes y señales; plan y órdenes | Traduce intención a cantidades factibles y financiación | Incluye restricciones operativas; plan aprobado no equivale a ejecución. `ExecutionPlan` es la verdad operacional |
| Persistencia: `scripts/run_analysis.py::_save_execution_plan_events`; `src/collector/schema_migrations.py` | Owner/run/plan/order_intent/decision/HOLD | Escritura formal atómica y enlaces estables | Integridad de linaje aceptable; no demuestra calidad PIT de cada input ni costo final |
| Outcomes: `src/collector/db.py::_compute_directional_outcomes`, `_compute_executable_outcomes`, `_next_executable_reference` | Decision log + velas; decisión por 5/10/20/40 días calendario | BUY `P_h/P_0-1`; SELL invierte signo; primera barra disponible tras objetivo calendario | Postcierre busca apertura posterior, con fallback a close si falta open: no debe llamarse ejecución observada. Sin dividendos/ratio/costos verificados por ese número |
| HOLD: `position_hold_audit.py::resolve_position_hold_outcomes` | Observación de posición, `position-hold-sessions-v1` | Retornos de tenencia por sesiones | No es decisión BUY adicional. 23 observaciones actuales sin outcomes; preservadas, no convertidas en pérdidas |
| DCL: `dcl/statistical_auditor.py::StatisticalAuditor.run`, `_bootstrap_mean_ci` | Loader de decisiones/outcomes, modo/calidad de muestra | Media, proporción positiva, correlación Spearman y bootstrap | Población y horizontes condicionan interpretación; bootstrap IID/singleton no prueba independencia. F03 |
| Viability: `viability_audit.py::_compute_horizon_metrics`, `_max_drawdown` | Decisiones por cohortes/ventanas | Retorno direccional menos supuesto fijo de costo; secuencia compuesta | 75 bps por defecto es escenario, no fees observadas. F02 impide usar el drawdown como pérdida de cuenta |
| Analytics v2: `analytics_v2/episodes.py::build_episodes`, `outcomes.py::build_outcomes`, `metrics.py::cohort_metrics` | Recomendación→episodio; sesiones y escenarios de costo | Retorno direccional/neto unitario, diagnóstico de dependencia | No publica Sharpe/drawdown de episodios sin curva comparable; F04 afecta partición de episodios |
| PnL: `analytics_v2/pnl.py::economic_pnl`, `execution_facts`; `metrics.py::equity_metrics` | Fills, NAV, flujos y componentes de cuenta; período/curva | NAV final−inicial−flujos; reconciliación; TWR y riesgo de curva | Comisiones/impuestos/otros costos observados requeridos; moneda explícita; distingue shortfall de PnL |
| Decision Lab: `decision_lab/state.py::build_state`; `counterfactuals.py::freeze_episode`, `evaluate_episode`; `statistics.py::summarize` | Evidencia bitemporal y portfolio; episodio con capital ARS común | PLAN−HOLD; ejecución simulada próxima sesión, costos, lotes, acciones corporativas | Núcleo reutilizado. HIGH/PRIMARY no se infiere de legacy. Requiere cobertura de precios/corporate actions |
| Meta A/B/C/D: `economic_meta_policy.py::evaluate_candidate`, `assert_shadow_only`; `economic_meta_lab.py::load_decision_lab_context` | Candidato y evidencia de estrategia/cohorte | Reglas preregistradas de decisión shadow; lectura contextual PLAN/HOLD | Filtros de riesgo/calidad no son beneficio económico causal. Authority flags permanecen false |
| Contextual E/G: `contextual_market.py::build_contextual_snapshot`, `relative_strength`, `rvol_context`; `collector/contextual_g2.py::observations_from_provider_sequence`, `read_market_evidence_as_of` | OHLCV inmutable, disponibilidad y snapshots | Estructura, volumen relativo, fuerza relativa e invalidadores | Descriptivo salvo challenger congelado. Nuevos relojes mejoran PIT; faltantes de ratio/calendario siguen explícitos |
| Replay: `historical_analysis_replay.py::replay_complete_analysis` | Inputs históricos y política elegida para replay | Reejecución de análisis completo | Política actual aplicada atrás cambia estimando; no confundir con política efectivamente disponible entonces |
| Agentic/Telegram: `agentic/orchestrator.py::AgentOrchestrator`; `agentic/tools.py::build_default_registry`, `read_only_dsn` | Consultas, herramientas y evidencia recuperada | Explicación/orquestación read-only; no unidad de retorno propia | Respuesta convincente no es evidencia financiera. No se atribuye PnL al agente sin variante controlada; Telegram no modificado |

## Matriz de trazabilidad real

| Enlace | Evidencia accesible | Resultado |
|---|---|---|
| source→signal | 28.615 candle observations; 105 contextos; 51.841 enlaces contexto-vela | Clocks presentes, base CEDEAR/calendario incompletos; no promoción automática a HIGH |
| signal→decision | `layers`, versiones y referencias en 278 decisiones | Snapshot de features parcial; 37 manuales sin run. No se reconstruye un input faltante con la vela actual |
| decision→plan | 13 planes, 25 intents, 23 HOLD | 48 enlaces exactos. 253 casos manual/radar sin enlace exacto. Cero formal-run huérfano en la consulta ejecutada |
| plan→execution | 109 fills enlazados a decision; 0 attributions exact-owner | Enlace a decisión no demuestra ejecución de plan. No distribuir fills por proximidad temporal |
| execution→outcome | Fills reales, fees desconocidas; outcomes de precio separados | No ledger reconciliado. Estado `OBSERVED_EXECUTION` no equivale a `REALIZED_ACCOUNT` |
| outcome→metric | DCL/viability direccional, Analytics, Decision Lab | Mantener unidad, fuente, horizonte, costo, calidad y versión en todas las agregaciones |
| decision→thesis/opportunity | No identificadores canónicos en fuente actual | Proxies de recurrencia sólo exploratorios. Contrato prospectivo QEEF implementado y sin activación |

## Historial y PRs

La carpeta inicial `_1-copia` no era checkout Git. Se usó el principal `cocos_copilot`, preservando su carpeta ajena no versionada `quantia_reconstruction/`, y se creó un worktree desde el main remoto verificado. El main local permaneció en `f3c6bbe`; no se actualizó ni mezcló con el trabajo.

La inspección de PRs encontró [PR #13](https://github.com/Francoparodi01/Portfolio-manager-/pull/13) abierto/draft, con correcciones relacionadas con interpretación de viability; no se hizo merge. [PR #19](https://github.com/Francoparodi01/Portfolio-manager-/pull/19) estaba cerrado/merged y su persistencia formal atómica está presente en la base inspeccionada. El estado histórico de un PR no sustituye leer el código del commit evaluado. QEEF mantiene las fórmulas históricas y evita usar sus estimandos como PnL nuevo; no presenta los defectos legacy como reparados en sus consumidores originales.

Los resultados históricos 874/626, 50,3%, −0,75% y +5,2% quedan **NO_REVALIDADOS**: no se identificaron sus manifiestos originales como comparables a esta población. No se suman ni se recalibran silenciosamente.
