# Entrega Quantia Decision Lab — 24/09/2026

Implementado y desplegado en Telegram/scheduler. Infraestructura de auditoría: no ejecuta órdenes ni acredita edge. La historia legacy queda LOW y fuera de inferencia primaria.

## Rama, PR y versión

- Rama: `feature/decision-lab-pit-replay`. HEAD: `2096cdd42e237f90bd93f17cbc1eae288b824c3d`.
- PR en borrador: https://github.com/Francoparodi01/Portfolio-manager-/pull/3, base `feature/agentic-loop-orchestrator`; depende del PR #1 sin modificarlo.
- Git status del worktree: `limpio`. Checkout operativo: `main`, conserva cambios locales anteriores más los archivos desplegados; no se mezclaron en los commits.
- Código numérico de este run: `1541193`; manifest con hashes de implementación y versiones de runtime. Los commits posteriores agregan tests, consultas y documentación.

```text
fbf932a feat: add point-in-time replay contracts and source audit
ca17276 feat: reconstruct frozen states and evaluate common-capital counterfactuals
b65cbf2 feat: add paired DVA inference and versioned walk-forward replay
cae23f6 feat: persist immutable replay evidence and capture future formal plans
8d04689 feat: expose read-only Decision Lab evidence through Telegram agent
f40f999 fix: close replay provenance gaps and preserve blocked missing values
1541193 fix: require complete rotation funding and flag revised macro
ea0d635 test: make Decision Lab fixtures portable across checkouts
65632bb feat: route Telegram queries to specific Decision Lab evidence views
2096cdd docs: document Decision Lab methodology and empirical validation
```

## Arquitectura y persistencia

Exportación DB read-only → evidencia bitemporal → estado en T → política/propuesta congelada → alternativas con capital común → outcomes → DVA y bloques de fechas → manifest/tablas → consultas read-only de /agente.

Tres modos: HISTORICAL_POLICY_REPLAY, CURRENT_POLICY_ON_HISTORICAL_DATA y RECORDED_PLAN_EVALUATION. El caso real utiliza el último. El core moderno es PORTFOLIO_ONLY_NO_RADAR; no se presenta como reejecución integral de la estrategia histórica.

Migración aplicada: decision_lab_objects, decision_lab_runs, decision_lab_plan_captures, índices y triggers append-only. Run real persistido dos veces: una fila, 1.499 objetos. Sin cambios a tablas operativas de órdenes/fills. Captura futura probada contra DB con rollback; no se emitió un plan formal de prueba.

## Validación real

Período 10/08–23/09/2026; cutoff 24/09/2026 11:00 UTC. 112 planes auditados: 82 excluidos por vínculo sobrescrito de otro run, 30 evaluables. Se preservaron órdenes bloqueadas con campos nulos. Velas ARS de TRADINGVIEW_BYMA, sin rellenar huecos.

| Horizonte | n maduro descriptivo | n efectivo proxy | PLAN | HOLD | DVA pp |
|---|---:|---:|---:|---:|---:|
| 5D | 24 | 5 | -0.387% | -0.111% | -0.276 |
| 10D | 21 | 3 | -0.037% | +0.290% | -0.328 |
| 20D | 11 | 1 | +0.256% | +0.909% | -0.653 |
| 40D | 0 | 0 | N/D% | N/D% | N/D |

**n primario = 0**. Calidad LOW; sin CI inferencial válido. Un DVA negativo aquí no demuestra que Quantia sea inferior. Costos simulados: 75 bps por lado; ARS nominal sin intereses, PRICE_ONLY. No son PnL ni fees realmente ejecutados.

Madurez: 5D tiene cinco PENDING y un UNAVAILABLE por cierre faltante de USESPECIE; 10/20/40D tienen 9/19/30 PENDING. Outcomes ausentes nunca se tratan como cero.

## Episodios reales comprobados a mano

| Decisión | Entrada → salida (5 sesiones) | Capital común ARS | HOLD | PLAN | DVA pp |
|---|---|---:|---:|---:|---:|
| 2026-08-11T19:45:47.081880Z | 2026-08-12 → 2026-08-19 | 2,327,924.34 | +0.244% | +0.180% | -0.064 |
| 2026-08-13T19:44:00.022218Z | 2026-08-14 → 2026-08-21 | 2,374,879.81 | -0.705% | -0.452% | +0.253 |
| 2026-08-20T15:58:34.102485Z | 2026-08-21 → 2026-08-27 | 2,298,818.23 | +2.300% | +1.870% | -0.430 |

Ejemplo 13/08: vender 11 ASTS al open 7.540, cierre 7.300 y fee 622,05; vender 15 IREN al open 5.870, cierre 5.560 y fee 660,375. Delta neto frente a HOLD = 2.017,95 + 3.989,625 = 6.007,575 ARS; dividido por capital 2.374.879,81421 da DVA +0,252963 pp. Conserva ventanas y cartera comunes. Es un contrafactual, no un fill humano.

Identidad independiente: delta ARS = suma(qty × (open entrada − close salida)) − fees; PLAN NAV = HOLD NAV + delta. Tres comprobaciones coinciden a 1e-10 en retorno. Los precios/legs y quality completos están en `tmp/decision-lab/release-validation.json`.

## Calidad y límites

- Cartera, plan y features legacy: APPROXIMATE, owner inferido bajo singleton verificado, snapshots sobrescribibles y versiones originales desconocidas.
- Falta historia admisible completa para política, universo, config, FX, macro y sentiment. Ajustes de precios históricos desconocidos; no se certifica ausencia de revisiones retroactivas del provider.
- Los 82 enlaces sobrescritos no se pueden reparar inventando estado. Marzo de 2026 no tiene snapshots de cartera.
- REAL_HUMAN_EXECUTION queda NOT_APPLICABLE hasta disponer de libro completo y atribución. El run real no compara dos versiones; esa capacidad sólo está validada con fixtures.
- Antes de evidencia estadísticamente confiable: fuentes prospectivas completas/versionadas, preregistración, suficiente madurez y ventanas comparables, costos y holdout no visto. La captura nueva por sí sola no otorga HIGH.
- Analytics v2 permanece separado. No se fabrican equity/Sharpe con outcomes solapados ni se completa su roadmap por este trabajo.

## Tests y despliegue

- Suite final del feature: 490 passed, cuatro fallos preexistentes de menú/help/Radar reproducidos en 7558c61.
- Integración Lab + Analytics v2 + agente: 176 passed; compatibilidad Analytics v2: 60 passed; última regresión routing/agente/Telegram: 111 passed. Sin certificación de cobertura global >=90%.
- PostgreSQL real: idempotencia, aislamiento y bloqueo de mutaciones; pruebas sintéticas con rollback. Cuatro consultas por handler Telegram/CLI/DB reales, receptor simulado: COMPLETE, traza completa, cero mensajes reales de prueba.
- Telegram y scheduler activos desde 21:49 UTC, sin reinicios/errores de importación en la verificación. Consulta posterior al despliegue coincide con el run persistido.
- Imágenes: Telegram quantia-telegram-decision-lab:65632bb; scheduler quantia-scheduler-decision-lab:1541193. Se preservaron sus bases previas, agregando sólo el cambio revisado.
- Replay de uno/30 episodios más recálculo: 0,630 s / 2,134 s en entorno local; no extrapolación de performance. Mismos inputs/versiones producen mismos hashes.

## Comandos de uso

En Telegram:

```text
/agente PLAN vs HOLD 20D
/agente Decision Lab contrafactuales CASH 5D
/agente DVA de MSFT 20D
/agente calidad del replay
/agente compara versiones en Decision Lab
```

Validar y abrir el reporte privado desde el checkout operativo:

```powershell
python scripts/run_decision_lab.py validate --folder outputs/decision-lab/737951b6dd461527f8274af304b7fa25d03c74297bb9c41f1721b45c3341d59d --recompute
python scripts/run_decision_lab.py report --folder outputs/decision-lab/737951b6dd461527f8274af304b7fa25d03c74297bb9c41f1721b45c3341d59d
```

Utilizar el runtime del manifest (el entorno local probado está en `$env:TEMP/quantia-analytics-v2-testenv/Scripts/python.exe`). Los comandos completos de export/register/run, ventanas, comparación y dry-run están en `docs/decision-lab.md`.

## Archivos del cambio

```text
M	.gitignore
A	config/decision_lab_costs_v1.json
A	config/decision_lab_exploratory_v1.json
A	docs/decision-lab-source-audit.md
A	docs/decision-lab-validation.md
A	docs/decision-lab.md
M	requirements.txt
A	scripts/check_decision_lab_db.py
M	scripts/run_analysis.py
A	scripts/run_decision_lab.py
M	src/agentic/answer.py
M	src/agentic/diagnostics.py
M	src/agentic/model.py
M	src/agentic/telegram.py
M	src/agentic/tools.py
A	src/analysis/analytics_v2/bootstrap.py
A	src/analysis/date_block_statistics.py
M	src/analysis/optimizer.py
A	src/decision_lab/__init__.py
A	src/decision_lab/capture.py
A	src/decision_lab/counterfactuals.py
A	src/decision_lab/exporter.py
A	src/decision_lab/models.py
A	src/decision_lab/persistence.py
A	src/decision_lab/queries.py
A	src/decision_lab/runner.py
A	src/decision_lab/state.py
A	src/decision_lab/statistics.py
A	src/decision_lab/strategies.py
A	tests/__init__.py
A	tests/test_decision_lab.py
A	tests/test_decision_lab_agent.py
A	tests/test_decision_lab_persistence.py
A	tests/test_decision_lab_statistics.py
```

## Git status operativo preservado

```text
M .gitignore
 M Dockerfile
 M docs/07-analisis-radar-shadow.md
 M docs/10-learning-shadow.md
 M requirements.txt
 M scripts/run_analysis.py
 M scripts/run_shadow_calibration.py
 M scripts/telegram_bot.py
 M src/analysis/learning_shadow.py
 M src/analysis/optimizer.py
 M src/analysis/radar_exploratory.py
 M src/analysis/shadow_calibration_store.py
 M src/collector/cocos_scraper.py
 M src/core/portfolio_refresh.py
 M src/scheduler/runner.py
 M tests/test_analysis_telegram_output.py
 M tests/test_cocos_movement_poll.py
 M tests/test_learning_shadow.py
 M tests/test_shadow_calibration.py
?? .coverage
?? .env.agentic.example
?? config/analytics-v2-policy.json
?? config/decision_lab_costs_v1.json
?? config/decision_lab_exploratory_v1.json
?? docs/15-agentic-loop.md
?? docs/adr/
?? docs/analytics-v2/
?? docs/decision-lab-source-audit.md
?? docs/decision-lab-validation.md
?? docs/decision-lab.md
?? examples/
?? output/
?? requirements-analytics.txt
?? scripts/benchmark_analytics_v2.py
?? scripts/check_decision_lab_db.py
?? scripts/run_agent.py
?? scripts/run_analytics_v2.py
?? scripts/run_decision_lab.py
?? scripts/run_decision_market_audit.py
?? src/agentic/
?? src/analysis/analytics_v2/
?? src/analysis/analytics_v2_live.py
?? src/analysis/date_block_statistics.py
?? src/analysis/decision_market_audit.py
?? src/decision_lab/
?? tests/test_agentic_diagnostics.py
?? tests/test_agentic_guards.py
?? tests/test_agentic_loop.py
?? tests/test_analytics_v2.py
?? tests/test_analytics_v2_telegram.py
?? tests/test_decision_market_audit.py
?? tests/test_opening_portfolio_report.py
?? tmp/
```

La carpeta `outputs/decision-lab` y los recibos de cuenta son privados, ignorados por Git. El reporte público de validación no incluye carteras/owners ni retornos detallados.
