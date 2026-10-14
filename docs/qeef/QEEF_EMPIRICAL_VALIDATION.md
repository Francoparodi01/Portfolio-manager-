# Validación empírica QEEF

**Veredicto: EXPERIMENTAL.** Software y contratos verificados con pruebas; capacidad de demostrar valor económico incremental todavía bloqueada por evidencia. No hay PnL/alpha nuevo certificado.

## Corte y procedencia

Captura: **2026-10-14 04:53:59.810706 UTC** (01:53:59.810706 Buenos Aires). Se consultó PostgreSQL mediante el contenedor existente, con `default_transaction_read_only=on`, transacción repeatable-read y owner exacto configurado. No se ejecutaron escrituras operativas ni migraciones en esa base. Se verificó el esquema real; nombres/columnas y conteos están en [validation.json](evidence/validation.json).

| Artefacto | Identidad |
|---|---|
| Base Git | `f2e08096a2a0b6254598243600f522b8608276d5` |
| Commit de implementación evaluado | `d3cca84c06f631773810b4aec5077c35f157aafb` |
| Captura privada | `outputs/qeef/capture-final-20261014.json` |
| SHA256 captura | `814fac6d86339bce6bf31ec2c2053c6fcafb51354b31f01e9866c18170e3c935` |
| Evaluación | `5c3aa7a82b546986768cdc7418c057b84dcb67f964c1ef82609e8a08244507d8` |
| SHA256 report.json | `6b1571149cea1e3894a6a19830503ed6e6c858abb352ef9f0ced324ddb9b8cb8` |
| Método | `qeef-v1.0.0`, `RESEARCH_ONLY` |
| Entorno | Python 3.13.7; numpy 2.1.2; pandas 2.3.3; pydantic 2.13.5; asyncpg 0.31.0 |

El JSON/Markdown completos están en [reporte local](../../outputs/qeef/release-v1/report.md) y [casos y comparaciones](../../outputs/qeef/release-v1/report.json). Contienen información privada y permanecen ignorados por Git. El agregado versionado excluye owner, balances, posiciones, fills y payloads individuales. Los hashes permiten verificar la captura privada conservada; los agregados públicos por sí solos no permiten reconstruir los 301 casos.

Se ejecutó el mismo comando offline dos veces, mismo directorio: ambos devolvieron el mismo evaluation_id y el writer no modificó archivos existentes. Código/hash de implementación y dependencias forman parte del resultado. Las instrucciones para repetir después del commit documental fijan el SHA de implementación explícitamente.

## Censo y denominadores

Se usaron **todos** los registros exact-owner de decisión/HOLD disponibles, sin seleccionar ganadores/perdedores: 301 casos, 72 tickers, 2026-08-20 19:50:17 UTC a 2026-10-13 19:50:07 UTC. BUY 254, SELL 24, HOLD 23. Esos conteos incluyen radar/manual/formal y sólo describen composición; no se agrupan sus retornos.

| Fuente / scope / estado | Acción | n | Outcomes registrados 5D / 10D / 20D / 40D |
|---|---|---:|---|
| broker_movement / primary / EXECUTED_MANUAL | BUY | 23 | 21 / 19 / 14 / 3 |
| broker_movement / primary / EXECUTED_MANUAL | SELL | 14 | 13 / 10 / 9 / 1 |
| execution_plan / planner_audit / APPROVED | BUY | 2 | 2 / 0 / 0 / 0 |
| execution_plan / planner_audit / APPROVED | SELL | 10 | 10 / 0 / 0 / 0 |
| execution_plan / blocked_audit / BLOCKED | BUY | 13 | 13 / 0 / 0 / 0 |
| radar / radar_audit / BLOCKED | BUY | 107 | 107 / 104 / 90 / 46 |
| radar / radar_audit / THEORETICAL | BUY | 109 | 97 / 76 / 54 / 18 |
| position_hold_observations | HOLD | 23 | 0 / 0 / 0 / 0 |

La tabla resume conteos de cobertura. El JSON mantiene particiones adicionales por calidad, régimen y versión: no combina sus medias. Los outcomes decision_log son calendario/primera barra; HOLD declara su versión de origen por sesiones. Valores registrados no se presentan como retornos de cuenta verificados.

Hay 37 casos sin régimen y 264 con el mismo vector registrado de régimen (market neutral, oil bull, rates/dollar neutral, argentina crítico). No existe aquí diversidad de regímenes suficiente para una comparación robusta; no se inventaron estados retrospectivos.

| Tabla real | Conteo capturado / alcance | Observación |
|---|---:|---|
| decision_log | 278 exact-owner | 995 adicionales owner NULL, excluidos |
| position_hold_observations | 23 | 528 owner NULL, excluidos |
| execution_plans / order_intents | 13 / 25 | 130 planes owner NULL, excluidos |
| portfolio_snapshots / positions | 2.233 / 28.503 | Posiciones vía snapshot del owner; 3.822 snapshots owner NULL excluidos |
| raw_snapshots | 2.233 | Sólo ID/fecha vía portfolio; no HTML/payload de cuenta |
| broker_fills | 109 | 217 owner NULL excluidos; 0/109 comisiones conocidas |
| broker_movements | 109 | Sin columna owner; sólo movimientos vinculados al external ID de fills exact-owner; no ledger completo de flujos |
| plan_execution_attributions | 0 exact-owner | 43 owner NULL, no apropiados por inferencia |
| contextual_market_snapshots / contextual_snapshot_candles | 105 / 51.841 | Enlaces no equivalen a barras únicas ni a operaciones |
| market_candle_observations | 28.615 | 28.495 cierre confirmado; 120 sin confirmar |
| decision_lab_plan_captures | 32 | 19 v1 + 11 PERSISTENCE_IDENTITY + 2 FULL_CONTEXT |
| decision_lab_runs / objects | 1 / 1.499 | 30 episodios, 1.080 outcomes, 225 evidencias y 164 métricas |
| radar_discovery_runs / snapshots / outcomes | 36 / 9.864 / 18.722 | Población de exploración separada |
| learning_shadow_cases / metric_snapshots / metric_snapshots_v2 | 0 / 0 / 0 | Tablas existen, sin evidencia en este owner/corte |
| shadow_thesis_forecasts | 0 | No se presupone inexistencia global ni se generan forecasts históricos falsos |

No hubo truncamiento por el límite protector de 100.000 filas por tabla. Las tablas con owner NULL se perfilaron con conteos agregados, sin sumar esas filas a la población. `market_candles` legacy se perfiló globalmente como datos de mercado, sin tomarlo como fuente PIT certificada.

## Enlaces y suficiencia económica

- 48 casos tienen enlace exacto a item/plan/HOLD; 253 quedan sin enlace exacto (37 manuales y 216 radar). No son 253 planes formales huérfanos: la consulta de formal-run huérfano dio **0**.
- 37 decisiones no tienen run, coinciden con población manual. Ningún outcome20 observado careció de `outcome_filled_at` en la consulta ejecutada. Tener ese reloj no prueba disponibilidad ex ante del precio o de la política.
- El adaptador infiere **143 proxies de recurrencia** por ticker/fuente/scope/estado/versión/calidad y brecha ≤7 días. No equivalen a oportunidades canónicas ni a n independiente.
- No hay identificadores de tesis canónicas congeladas ni corpus de precios/corporate actions enlazado que satisfaga el contrato estricto completo. **0 oportunidades canónicas identificadas y 0 pares económicos certificados por esta captura/adaptador.** Los 301 casos quedan `NOT_IDENTIFIABLE` para valor neto incremental.
- Las 28.615 barras nuevas tienen `available_at`, pero todas declaran `DEPOSITARY_RATIO_UNKNOWN`, `EXCHANGE_CALENDAR_VERSION_UNKNOWN`, `VOLUME_UNIT_UNKNOWN`; 120 añaden `BAR_END_UNCONFIRMED_NO_LATER_PROVIDER_BAR`. La procedencia inspeccionada declara ajuste splits, no base RAW_AS_TRADED certificada para el contrato QEEF.
- Las tablas legacy de mercado no aportan los clocks/base de ajuste/ratio necesarios en los perfiles inspeccionados. No se convirtió retrospectivamente su timestamp de barra en fecha de primera disponibilidad.

Estos ceros describen admisión de evidencia, **no una estimación de retorno cero ni una demostración de que Quantia carezca de valor**. El mínimo de 30 oportunidades defendibles no está cumplido. Se inspeccionaron todos los candidatos y se entregan sus razones; no se fabrica el cupo.

## Treinta episodios legacy, conservados por separado

Los objetos de Decision Lab contienen 30 episodios LOW, no elegibles para PRIMARY. Se recuperaron los valores PLAN y HOLD almacenados, conservando costo original. No se recalcularon precios desde un corpus íntegro: las 225 evidencias guardadas son 165 FEATURES, 30 PLAN y 30 PORTFOLIO; no incluyen el conjunto de barras requerido para una revaloración independiente completa desde esta captura.

| Horizonte sesiones | Pares con retorno registrado / candidatos | Media PLAN−HOLD registrada | Pares verificados QEEF |
|---|---:|---:|---:|
| 5 | 24/30 | −0,2760466 puntos porcentuales | 0 |
| 10 | 21/30 | −0,3277887 puntos porcentuales | 0 |
| 20 | 11/30 | −0,6530625 puntos porcentuales | 0 |

Son números históricos exploratorios, no PnL realizado ni evidencia nueva de degradación. Los denominadores cambian por disponibilidad; no se comparan medias como efecto del horizonte. Sus modelos de costo no son fees observadas. QEEF no elimina esos resultados ni los promociona por aportar más funciones.

## Pruebas empíricas de defectos

[validate_qeef_empirical.py](../../scripts/validate_qeef_empirical.py) reproduce sobre la misma captura:

1. **Intervalo singleton:** primera fila cronológica elegible `decision_log:855`, retorno registrado 0,14705882352941177. La rutina legacy devuelve `[0,14705882352941177; 0,14705882352941177]`. QEEF informa intervalo `null`, razón N1_NO_INFERENCE. Es una prueba de rutina con una observación real, no un intervalo reportado de estrategia.
2. **Secuencia de señales como drawdown:** nueve cohortes con observaciones 5D; semilla42, 100 permutaciones y costo diagnóstico fijo75bps. Siete cambian de drawdown. Manual BUY tiene n21, 18 solapamientos nominales de cinco días calendario; resultado original −39,4983%, rango de permutaciones −51,1383% a −38,2838%. **Ninguno es drawdown de cuenta.** Se conserva como prueba de la fórmula, sin declararlo pérdida monetaria.
3. **Partición sin cohorte:** test con dos recomendaciones del mismo instrumento/módulo/cuenta y distinta cohorte produce un episodio legacy; QEEF mantiene scopes separados. Prueba sintética de código, sin afirmación de prevalencia en los datos operativos.

Los escenarios sintéticos de tests se identifican como tales y nunca se insertaron en la captura real.

## LEGACY_EVALUATION vs QEEF sobre observaciones comunes

| Dimensión | Legacy inspeccionado | QEEF v1 | Diferencia verificable |
|---|---|---|---|
| Unidad | Decisión/forecast; Analytics y Lab ya tienen episodios | Decisión, tesis, oportunidad, episodio y fill explícitos | 301 registros conservados; 143 proxies no elevados a oportunidades |
| Independencia | DCL IID; viability secuencia; Analytics tiene bloques | Conflictos por oportunidad rechazados; ventanas desde decisión; clusters separados | Sin CI para evidencia pequeña/LOW; sin n efectivo inventado |
| Horizonte | Calendario y sesiones según módulo | Unidad preservada y sesiones explícitas para nuevo CF; lifecycle | 5/10/20 siguen disponibles; ninguna conversión silenciosa |
| HOLD | Tabla separada; Lab sí usa benchmark HOLD | HOLD_EXISTING/NO_TRADE y tesis persistente | 23 HOLD conservados sin imputar outcomes; 9HOLD+BUY =1tesis en test |
| Contrafactuales | Lab PLAN/HOLD y otras variantes existentes | Reutiliza kernel; cash total y alternativas ex ante tipadas | Benchmark futuro rechazado; no identifica pares reales donde falta evidencia |
| Costos | Supuestos fijos/research y fees de ejecución separadas | Observados y modelos nombrados, nunca intercambiables | 109/109 fees desconocidas permanecen desconocidas |
| PnL real/simulado | Distintos módulos con distintas semánticas | result_kind y campos separados | PnL real de la auditoría =null, no suma de outcomes |
| Riesgo | Viability secuencia; Analytics ya exige curva | Reutiliza curva reconciliada; guardas de riesgo | No publica drawdown/Sharpe de señales solapadas |
| PIT | Clocks y capturas con calidad desigual | Guardas de inputs, política, calendario, precios/ratio y fuente | 28.615 clocks no bastan para declarar simulación verificada |
| Intervalos | Singleton degenerado en DCL; bloques en módulos nuevos | Mínimo de evidencia y CI por bloques/ticker, sólo research | N1→null; no claim de certeza ni promoción |
| Trazabilidad | Persistencia formal mejoró prospectivamente | Hash evidencia/código, fuente original, contrato y razones | Dos ejecuciones idénticas, reporte inmutable y owner-scoped |

La mejora demostrada es consistencia de contratos, interpretación y trazabilidad. **No se demostró superioridad de retorno, alpha, reducción de drawdown real ni beneficio monetario de QEEF.**

## Pruebas ejecutadas

Suite final: **140 passed, 0 failed, 0 errors, 0 skipped, 23,58 s**. JUnit local `outputs/qeef/tests-release.xml`; agregado sin hostname en validation.json. Distribución: QEEF45, PostgreSQL1, Analytics v2 35, viability8, ejecución/rotación11, contextual E1 36, replay histórico4.

Una ejecución anterior de la integración PostgreSQL falló durante arranque temporal del contenedor. Se corrigió el readiness para consultar el puerto TCP publicado; la suite final completa pasó. No se oculta ese intento ni se cuenta como validación final.

Todos los nombres siguientes están en [test_qeef.py](../../tests/test_qeef.py), salvo el test PostgreSQL indicado:

| Requisito | Test ejecutado (prefijo `test_`) |
|---|---|
| 1. HOLD repetido | `repeated_holds_then_buy_remain_one_frozen_thesis`; `thesis_cannot_retroactively_change_target_or_cross_owner` |
| 2. BUY sin ejecución | `buy_without_execution_is_counterfactual_not_realized` |
| 3. Parcial | `partial_fill_uses_actual_quantity_and_preserves_capital_identity`; `observed_partial_execution_does_not_impute_remainder` |
| 4. Postcierre | `after_close_uses_next_session_open` |
| 5. Futuro accidental | `future_input_rejected_even_if_state_claims_high_quality` |
| 6. SNDK sin historia | `source_sndk_history_is_never_extended_with_another_ticker` |
| 7. Historia desigual | `new_or_short_history_never_fills_missing_prices` parametrizado |
| 8. Split | `split_and_cedear_ratio_do_not_create_profit` parametrizado |
| 9. Ratio CEDEAR | anterior + `unexplained_ratio_change_not_identifiable` |
| 10. Precios faltantes | `new_or_short_history_never_fills_missing_prices`; `price_basis_changes_cannot_create_artificial_returns` |
| 11. Owners | `owners_regimes_and_cohorts_cannot_be_pooled`; `monitor_rejects_owner_override_before_any_db_access` |
| 12. Legacy sin owner | `legacy_owner_unknown_cannot_be_promoted` |
| 13–14. HOLD/NO_TRADE | `hold_no_trade_identical_and_cash_sells_whole_portfolio` |
| 15. Costos > ganancia | `costs_exceed_gross_gain_and_cost_monotonicity` |
| 16. Solapamiento | `overlapping_decisions_and_small_samples_have_no_fake_interval` |
| 17. Regímenes | `owners_regimes_and_cohorts_cannot_be_pooled` parametrizado |
| 18. Muestra pequeña | `overlapping_decisions_and_small_samples_have_no_fake_interval`; `verified_defect_singleton_legacy_ci_is_degenerate` |
| 19. No identificable | `missing_corporate_action_coverage_blocks_total_return`; `future_benchmark_rejected_and_missing_benchmark_unknown` |
| 20. Reejecución | `identical_report_rerun_idempotent_and_mutation_rejected`; doble ejecución empírica |
| 21. Migración | `test_qeef_postgres.py::test_migration_idempotence_compatibility_immutable_store_and_owner_rls` |
| 22. Benchmark futuro | `future_benchmark_rejected_and_missing_benchmark_unknown` |

Se probaron además identidades contables, suma de contribuciones de riesgo, invariancia ante cambio de unidad monetaria, monotonicidad de costos, capital insuficiente, detección con universo completo, ablación sin atribución causal y preservación de outcomes legacy. Invariantes parametrizados no equivalen a verificación universal ni a backtest. No se ejecutó toda la suite del repositorio ni se afirma que todos sus tests pasen.

## Bloqueos pendientes exactos

No se cumple todavía validación económica de ≥30 oportunidades canónicas: faltan contratos de tesis ex ante, enlaces completos estado/plan/ejecución, barras ejecutables y corporate actions, fees observadas y NAV/flujos reconciliados. Faltan universo completo/labels para recall real, covarianza/factores/FX/liquidez para atribución amplia, políticas alternativas preregistradas comparables y muestra prospectiva/OOS para inferencia. Resolver esos faltantes requiere captura prospectiva y reconciliación de fuentes, no más tests ni inferir el pasado desde datos actuales.
