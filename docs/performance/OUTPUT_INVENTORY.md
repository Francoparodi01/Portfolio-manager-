# Output Inventory

Fecha: 2026-07-23. Repo: `cocos_copilot` live.

## Matriz principal

| Output | Comando/proceso | Entrypoint | Renderer | Datos | Externas | LLM | Tamano base | Latencia base | Duplicacion/riesgo | Prioridad |
|---|---|---|---|---|---|---|---:|---:|---|---|
| Menu | `/start`, `/menu`, callback final | `scripts/telegram_bot.py` | `menu_text()` | config multiuser | Telegram | no | 949 B / 17 l | <1 ms render | se envia despues de casi cada accion | media |
| Ayuda | `/help` | `scripts/telegram_bot.py` | `help_text()` | ninguno | Telegram | no | 2499 B / 41 l | <1 ms render | mensaje largo en mobile | alta |
| Status | `/status`, callback | `scripts/telegram_bot.py` | `action_status()` | Redis, snapshots, market_prices | Telegram | no | no medido antes | no medido antes | acuse previo y dos aperturas DB | alta |
| Portfolio | `/portfolio` | `scripts/telegram_bot.py` | `action_portfolio()` | Redis cache, raw_snapshots | Telegram, DB | no | pendiente | pendiente | tabla pesada, fallback DB | alta |
| Analisis compacto | `/analisis` | `telegram_bot -> run_analysis.py` | `render_report()` | snapshots, market, sentiment, optimizer/planner | DB, market/sentiment | opcional | pendiente | smoke 70.1 s no-persist | subprocess + pipeline completo | alta |
| Analisis test/full/debug | `/analisis_test`, `/analisis_full`, `/analisis_debug` | `telegram_bot -> run_analysis.py` | `render_report()` | idem analisis | DB, market/sentiment | opcional | pendiente | pendiente | alto costo y mensajes grandes | alta |
| Mercado/noticias | `/mercado` | `telegram_bot -> run_market_context.py` | `render_report()` | macro, sentiment | DB, feeds | opcional | pendiente | pendiente | contexto amplio | media |
| Radar | `/radar`, `/oportunidades` | `telegram_bot -> run_opportunity.py` | `render_opportunity_report()` / compactador | market_prices, universe, sentiment | DB | no | pendiente | pendiente | compactacion en bot posterior al CLI | alta |
| Radar full | `/radar_full` | `telegram_bot -> run_opportunity.py` | `render_opportunity_report()` | idem radar | DB | no | pendiente | pendiente | salida extensa | media |
| Performance | `/performance` | `telegram_bot -> run_performance.py` | `render_performance_report()` | decision_log, fills, movements | DB | no | 7896 B | 3090 ms avg | subprocess + query agregada pesada | alta |
| Viability | `/viability` | `telegram_bot -> run_viability_audit.py` | `render_viability_audit()` + PNG | decision_log/outcomes | DB | no | pendiente | pendiente | genera imagen y texto | media |
| Bot vs Humano | `/bot_vs_humano`, `/override` | `telegram_bot -> run_override_audit.py` | `render_report()` | decision_log, broker_movements | DB | no | 8256 B | 1394 ms avg | sync previo en Telegram | alta |
| Decision Ledger | `/ledger` | `telegram_bot -> run_decision_ledger.py` | `render_decision_ledger()` | decision_log, fills, movements | DB | no | 7514 B | 16344 ms avg | cuello de botella principal medido | alta |
| Policy Tree | `/policy` | `telegram_bot -> run_policy_tree.py` | `render_policy_tree()` | decision_log | DB | no | pendiente | pendiente | CLI read-only | media |
| Regression Audit | `/regression` | `telegram_bot -> run_regression_audit.py` | `render_regression_audit()` | decision_log/outcomes | DB | no | pendiente | pendiente | queries y render complejo | alta |
| Confidence Audit | `/confianza` | `telegram_bot -> run_confidence_audit.py` | script renderer | decision_log/stats | DB | no | pendiente | pendiente | CLI read-only | media |
| DCL/calibration | `/dcl` | `telegram_bot -> run_calibration.py` | `render_calibration_report()` | decision outcomes | DB | no | pendiente | pendiente | salida estadistica | media |
| Shadow | `/shadow`, `/shadow TICKER` | `telegram_bot -> run_thesis_shadow.py` | `render_shadow_telegram_report()` | shadow_thesis tables | DB | no | pendiente | pendiente | no ejecutable, debe quedar separado | media |
| Decision Timeline | CLI actual | `scripts/run_decision_timeline.py` | `render_decision_timeline()` / JSON | decision_log, movements, fills | DB | no | 22498 B JSON | 1203 ms avg | filas legacy muestran gaps | alta |
| Monitor HTML | `GET /` | `src/monitor/api.py` | static HTML | `index.html` | HTTP | no | 156468 B | 17.9 ms avg | leia disco por request | media |
| Monitor health | `GET /api/health` | `src/monitor/api.py` | JSON | DB, Redis, calendar | HTTP | no | 570 B | 1197 ms avg | Redis serial | alta |
| Monitor performance | `GET /api/performance` | `src/monitor/api.py` | JSON | decision_log/fills | DB | no | pendiente | pendiente | payload grande | alta |
| Monitor override | `GET /api/override-audit` | `src/monitor/api.py` | JSON | decision_log/movements | DB | no | 9526 B | 121 ms avg | aceptable por ahora | media |
| Monitor ledger | `GET /api/decision-ledger` | `src/monitor/api.py` | JSON via service | DB | no | pendiente | pendiente | comparte cuello con ledger | alta |
| Scheduler alerts | preclose/intraday/EOD | `src/scheduler/runner.py` | multiple renderers | Redis, DB, market data | Telegram, DB | opcional | pendiente | pendiente | procesos automaticos sensibles | alta |
| Outcome/reconciliation | jobs/scripts | `update_outcomes.py`, `sync_cocos_fills.py` | stdout/logs | DB, broker data | Cocos/DB | no | pendiente | pendiente | no debe alterar outcomes existentes | alta |

## Observaciones

- Telegram no concentra toda la presentacion: muchos comandos son wrappers de CLI que devuelven stdout.
- Los caminos pesados pagan startup Python y carga de dependencias por subprocess.
- La arquitectura nueva (`DecisionRunContext`, `FeatureSnapshot`, `DecisionTimeline`) se preservo y quedo como base auditada.
- El primer cambio aplicado se limito a outputs y runtime; no cambia scoring, thresholds, optimizer ni decisiones.
