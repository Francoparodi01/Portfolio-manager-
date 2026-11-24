# Quantia agent data catalog

This is the compact logical catalog exposed to the analytical agent. Runtime schema inspection remains the source for exact live column availability.

Revisión: 2026-10-22. Alcance: mapa de acceso; no habilita tablas ni herramientas nuevas.

## Elegir la fuente según la pregunta

| Pregunta | Herramienta / guía | Límite que debe conservarse |
|---|---|---|
| Cómo funciona el sistema, shadow, gráficos o proveedor LLM | `search_quantia_docs`; índice `docs/agent/README.md` | Documentación no demuestra estado ni resultados actuales. |
| Estado y fecha de la cartera | `get_portfolio_snapshot` | Snapshot con fecha; no asumir refresco en vivo. |
| Por qué se propuso una decisión guardada | `get_persisted_decision_evidence` | Evidencia del análisis persistido, con su corte. |
| Qué propondría ahora | `get_decision_evidence` o `analyze_portfolio` | Recalcula en modo consulta; no sustituye un evento histórico. |
| Analizar un ticker | `analyze_ticker` | Herramienta analítica; no prometer que adjunta los PNG de `/ticker`. |
| PLAN contra HOLD / DVA | `compare_plan_vs_hold`, `get_decision_value_added` | Respetar calidad, madurez y elegibilidad primaria. |
| Historial direccional BUY/SELL | `get_bot_directional_outcomes` | Episodios y costos según el contrato; no cartera realizada. |
| Seguir las propuestas del bot | `get_bot_follow_pnl`, `get_normalized_bot_follow_pnl` | Contrafactual; distinguir planes de episodios deduplicados. |
| Ledger e histórico Telegram importado | `get_decision_ledger`; guía `historical-evidence.md` | Cada bloque conserva su base; una recomendación no acredita fill. |
| Analytics / rendimiento neto reportado | `get_analytics_v2`, `get_net_decision_report` | Leer el tipo de neto y reconciliación; el nombre de la tool no prueba PnL real. |
| Datos fundamentales de empresa | `get_fundamental_evidence`, si está registrada | Preservar emisor/instrumento, período, fuente y estado de cobertura. |
| Auditoría DCL | `get_calibration_audit` | DCL no es Calibración Shadow V3. |
| Patrones exploratorios nuevos | `inspect_quantia_schema`, luego `query_quantia_sql` | Solo relaciones permitidas y owner de la sesión. |

Invocar únicamente herramientas incluidas en el catálogo del run actual y seguir
su schema de argumentos. Una herramienta mencionada aquí puede no estar instalada
en otro entorno. No inventar una tool específica para Learning o Calibración V3.

## Scoped analytical relations

### decision_log
Primary historical decision/audit relation.

Common fields used by the agent:
- `id`
- `owner_chat_id`
- `run_id`
- `decided_at`
- `ticker`
- `decision`
- `final_score`
- `regime`
- `status`
- `metric_scope`
- `source`
- `layers`
- `outcome_5d`
- `outcome_10d`
- `outcome_20d`
- `outcome_40d`
- `outcome_basis`
- `outcome_filled_at`

Use for exploratory decision history. Respect `source`, `metric_scope`, maturity and episode semantics before treating rows as independent recommendations.

### portfolio_snapshots
Account state snapshots.

Typical concepts:
- owner
- snapshot/scrape timestamp
- total account value / NAV-like fields
- cash
- position state stored by the snapshot schema

Use the canonical portfolio tools when exact current account state is requested. SQL exploration is useful for changes over time, but must not infer PnL from two valuations without handling flows/costs.

### broker_fills
Owner-scoped broker fill evidence when present.

Use for execution/fill investigation. Do not assume fills alone reconcile full account PnL.

### position_hold_observations
Chronology of formal HOLD observations used by historical methodologies when available.

Useful for sequence/episode reconstruction. The Historical Edge implementation remains canonical for its exact episode rules.

## Agent metadata relations
Agent audit tables may exist for traces and runs, but they are not financial truth. They can answer questions about what the agent consulted or why a run failed.

## Persistencia conocida fuera del SQL genérico
El explorador genérico permite solo las cuatro relaciones descritas arriba. No
accede a cualquier tabla que aparezca en una guía o mensaje del usuario.

| Dominio | Persistencia documentada | Acceso y semántica |
|---|---|---|
| Calibración Shadow V3 | `shadow_calibration_runs`, `shadow_calibration_models`, `shadow_calibrated_forecasts` | Distinguir ajuste, walk-forward y forecast; buscar fuente autorizada, sin asumir acceso SQL genérico. |
| Learning Shadow | `learning_shadow_cases`, `learning_shadow_metric_snapshots_v2`, `learning_shadow_rule_candidates` | Población y horizonte propios; propuestas no son reglas activadas. |
| Technical V2 / Buy V3 | `decision_log.layers.technical_shadow_v2` y `.technical_buy_shadow_v3` | Separar radar, planner y bloqueos; leer la etiqueta original, no recalcularla retrospectivamente. |
| Telegram importado | `telegram_history_imports`, `telegram_history_observations`, `telegram_history_measurements` en el runtime que tenga esa extensión | Archivos, IDs y mediciones versionadas; no insertar copias en decision_log. |
| Velas | `market_candles` | Identidad, mercado, moneda, intervalo, fuente y ajuste importan. No está expuesta por el SQL genérico. |
| Trazas del agente | `agent_runs`, `agent_steps` | Evidencia de consultas del agente; no resultado financiero. |

Para fundamentales, FIL entrega evidencia de lectura y no decisional. Conservar
`PARTIAL`, `INSUFFICIENT_DATA`, `NOT_COMPARABLE` y `SOURCE_BLOCKED`. No inferir un
ratio CEDEAR, una clase de acción ni una cifra ausente para completar una respuesta.

## Runtime catalog
The agent has a read-only schema inspection tool that exposes allowlisted relation columns from `information_schema`. If this document and the live schema differ, the live schema determines what SQL can execute, while this document determines semantic expectations.

## Access boundary
The generic SQL explorer does not expose arbitrary PostgreSQL relations. It presents only an allowlisted, owner-scoped analytical surface. The model cannot request a different owner or bypass the scope with schema-qualified table names.
