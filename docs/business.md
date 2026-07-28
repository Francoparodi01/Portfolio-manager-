# Documento de negocio

## Problema de negocio

Operar una cartera manualmente puede dejar decisiones dispersas entre precios,
capturas, noticias, intuicion, planes teoricos y fills reales. Cocos Copilot /
Quantia intenta convertir ese flujo en un proceso auditable: observar datos,
analizar, proponer, bloquear cuando no hay condiciones, registrar y medir.

Evidencia:

- [README.md](../README.md) declara que el sistema no ejecuta ordenes
  automaticamente y que observa, analiza, propone, registra y mide.
- [scripts/run_analysis.py](../scripts/run_analysis.py) orquesta el pipeline
  cuantitativo y persiste eventos de `ExecutionPlan`.
- [src/collector/db.py](../src/collector/db.py) persiste snapshots, decisiones,
  fills, movements y outcomes.
- [init.sql](../init.sql) contiene tablas para portfolio, precios, decisiones,
  fills, sentiment, shadow y auditoria.

## Usuarios y stakeholders

- Operador de cartera: recibe reportes por Telegram y monitor.
- Mantenedor tecnico: entiende jobs, datos, fallas, tests y runbook.
- Evaluador externo: puede auditar decisiones contra evidencia de codigo y DB.
- Futuro socio/cliente/inversor: evalua si el sistema separa senial, decision,
  ejecucion y performance con suficiente rigor.

No confirmado en el repo: roles formales de usuarios finales, permisos
multiusuario productivos o SLA comercial. Existe tabla `bot_users`, variables
multiusuario y cifrado de credenciales, pero no se valido un proceso comercial.

## Propuesta de valor

La propuesta de valor real es disciplina operacional:

- Menos decisiones sin contexto.
- Menos confusion entre score teorico, plan operable y fill real.
- Mejor capacidad de explicar por que una operacion fue aprobada, bloqueada,
  ignorada o ejecutada manualmente.
- Medicion de outcomes por horizontes 5/10/20/40 y comparacion bot-only vs
  manual-only mediante [src/analysis/viability_audit.py](../src/analysis/viability_audit.py).

No confirmado en el repo: impacto financiero positivo sostenido. El sistema
tiene herramientas para medirlo, no prueba por si solo que exista edge actual.

## Casos de uso principales

1. Ver cartera actual y concentracion.
   Evidencia: `run_opening_portfolio_report()`, `run_post_open_portfolio_report()`
   en [src/scheduler/runner.py](../src/scheduler/runner.py) y comando
   `/portfolio` en [scripts/telegram_bot.py](../scripts/telegram_bot.py).

2. Generar plan operativo diario.
   Evidencia: [scripts/run_analysis.py](../scripts/run_analysis.py),
   `run_optimizer()` en [src/analysis/optimizer.py](../src/analysis/optimizer.py),
   `derive_decision_intents()` y `reconcile_funding()` en
   [src/analysis/execution_planner.py](../src/analysis/execution_planner.py).

3. Buscar oportunidades fuera de cartera.
   Evidencia: [scripts/run_opportunity.py](../scripts/run_opportunity.py) y
   [src/analysis/opportunity_screener.py](../src/analysis/opportunity_screener.py).

4. Auditar performance y regression.
   Evidencia: [scripts/run_performance.py](../scripts/run_performance.py),
   [scripts/run_regression_audit.py](../scripts/run_regression_audit.py) y
   [src/analysis/regression_audit.py](../src/analysis/regression_audit.py).

5. Comparar decisiones del bot contra accion humana.
   Evidencia: [scripts/run_override_audit.py](../scripts/run_override_audit.py),
   [src/analysis/override_classification.py](../src/analysis/override_classification.py)
   y endpoint `/api/override-audit` en [src/monitor/api.py](../src/monitor/api.py).

6. Registrar eventos/catalysts manuales.
   Evidencia: [scripts/manual_market_events.py](../scripts/manual_market_events.py),
   tabla `manual_market_events` y tests en
   [tests/test_manual_market_events.py](../tests/test_manual_market_events.py).

## Decisiones que ayuda a tomar

- Mantener, reducir, vender parcialmente, bloquear o considerar compra segun
  senial, pesos, cash, nominales enteros y guards.
- Decidir si una oportunidad externa merece entrar como idea/radar o si queda
  fuera por historia, cash, riesgo o falta de senial.
- Identificar si una decision fue seguida, ignorada, sobreseguida, parcial u
  opuesta por la accion humana.
- Separar si performance viene de fills reales o de ideas teoricas.

El sistema ayuda a tomar decisiones; no las ejecuta automaticamente.

## Riesgos del negocio

- Riesgo de datos: scraping puede fallar por cambios en Cocos, credenciales,
  MFA, DOM o cobertura de precios. Evidencia: `SCREENSHOT_ON_FAILURE`,
  `DOM_HASH_TOLERANCE`, `MIN_CONFIDENCE_SCORE` en [.env.example](../.env.example).
- Riesgo de falsa confianza: los reportes pueden parecer convincentes aunque
  falte muestra. Mitigacion parcial: viability, regression, DCL y confidence
  audit.
- Riesgo de mezclar evidencia: radar, shadow, debug y primary tienen que
  mantenerse separados por `metric_scope` e `is_primary_metric`.
- Riesgo operacional: el monitor requiere `MONITOR_API_TOKEN`; exponerlo sin
  controles seria inseguro.
- Riesgo financiero: el sistema puede proponer planes incorrectos si los datos
  son incompletos o si el mercado cambia antes de ejecutar manualmente.

## Metricas de exito

Metricas existentes o inferibles del repo:

- Frescura de ingesta y cobertura de candles: [scripts/run_confidence_audit.py](../scripts/run_confidence_audit.py),
  endpoint `/api/ingestion`.
- EV, win rate, outcomes y path risk: [scripts/run_performance.py](../scripts/run_performance.py),
  endpoint `/api/performance`.
- IC/regression por modo y horizonte: [scripts/run_regression_audit.py](../scripts/run_regression_audit.py).
- Viability bot-only vs manual-only, EV neto con costos, IC y drawdown:
  [scripts/run_viability_audit.py](../scripts/run_viability_audit.py).
- Calidad de shadow: tablas `shadow_thesis_forecasts`,
  `shadow_thesis_outcomes` y tests de quality gate en
  [tests/test_thesis_shadow_store.py](../tests/test_thesis_shadow_store.py).

Pendiente de validar: metas numericas aceptadas como criterio de negocio
productivo. El repo define comandos y calculos, pero no un OKR formal.

## Supuestos no validados

- Que Cocos Capital mantenga estructura de UI/API compatible con el scraper.
- Que las seniales actuales tengan edge neto sostenido despues de costos.
- Que la muestra de fills sea suficiente para inferencias robustas.
- Que los usuarios futuros acepten un flujo sin ejecucion automatica.
- Que multiusuario este listo para uso fuera del operador original.
- Que el monitor pueda exponerse fuera de entorno local sin hardening adicional.
