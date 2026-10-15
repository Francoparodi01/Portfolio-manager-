# QEEF Phase 2 — informe final

> **Corrección de alcance — 15 de octubre de 2026:** se contrastó el backup histórico aportado con la captura y con claves de la base local. La [conciliación de Supabase](QEEF_SUPABASE_BACKUP_RECONCILIATION.md) verifica cobertura parcial y documenta cohortes excluidas y cambios de contenido. Los resultados siguientes describen únicamente los 301 casos capturados; no representan toda la historia del proyecto.

Implementación research funcional y validada en PostgreSQL descartable, preparada para revisión. La captura permanece desactivada. **No se demostró alpha ni PnL neto histórico: 0 de 301 casos reales alcanzan el contrato económico VERIFIED.** La suite ampliada de UI conserva tres fallos preexistentes, reproducidos en v1; no se declara todo el repositorio verde.

## A. Estado inicial

Se verificaron checkout principal, worktree v1, árbol, ramas, remotos y commits. Principal: `C:/Users/Franco/OneDrive/Escritorio/backend/cocos_copilot`; `_1-copia` no se utilizó como repositorio. El worktree v1 permaneció en `08623cc9b418d7aef4da89b020ec26737a20d455`; su implementación `d3cca84c06f631773810b4aec5077c35f157aafb` estaba presente. Main remoto seguía en `f2e08096a2a0b6254598243600f522b8608276d5` al verificar.

V1 aportaba contratos, adaptación histórica, contrafactuales, lifecycle, servicio contable y auditoría determinista. Faltaban hooks, entrega durable, recuperación, consumidor automático y diagnóstico prospectivo. Se publicó la rama v1 y se abrió [PR #22](https://github.com/Francoparodi01/Portfolio-manager-/pull/22) draft hacia main, sin merge. Se creó otro worktree/rama exactamente sobre el commit documental v1.

## B. Arquitectura final

Los hooks observan el plan después de su commit y la persistencia de snapshots/fills/mercado. SQLite conserva entregas pendientes. PostgreSQL separa `qeef_events` append-only y `qeef_jobs` con estado de reintento. El job existente de outcomes dispara el consumidor sólo cuando la captura está habilitada. La recuperación consulta fuentes canónicas con owner e inicio explícitos.

El estado/contrafactual se evalúa con Decision Lab; la cuenta con Analytics v2, extendido para costos observados no asociados a fills. Monitor read-only JSON/Markdown y comando `/qeef` opcional. Diagrama y contratos: [pipeline](QEEF_PROSPECTIVE_PIPELINE.md).

## C. Captura prospectiva

Se conservan IDs de plan/decisiones/intents, cartera inicial, señales/scores, macro/eventos disponibles, versiones/hashes, observaciones broker y datos de mercado. Los contextos no suministrados tienen estado UNKNOWN. Scores no se presentan como probabilidad calibrada.

La tesis requiere una declaración explícita del productor con clave estable y objetivos/invalidador congelados. La recurrencia comparte identidad sólo mediante esa clave; no por ticker, tiempo o similitud. OPENED, UPDATED, CONFIRMED, INVALIDATED y CLOSED se proyectan sin modificar el original; vencimiento/censura respetan horizonte y calendario. Condiciones de precio/close se observan automáticamente; otras condiciones necesitan eventos con fuente explícita.

El productor actual no proporciona automáticamente todos los campos de `DecisionBundle`. Por ello, la activación básica conservará evidencia nativa PARTIAL, útil para detectar gaps, pero no garantiza de inmediato casos económicos completos. No se creó una estrategia para suplir hipótesis ausentes.

## D. Reconciliación

Se implementaron compras/ventas parciales, deduplicación, cantidades, cash, costo medio, dividendos, splits/ratio, fees/impuestos, aportes/retiros, NAV y gaps con Decimal. La certificación exige cobertura suficiente y coincidencia con Analytics v2. Costos desconocidos permanecen null; eventos simulados se rechazan para contabilidad observada.

Los retornos ajustados requieren ausencia de flujos o flujos exclusivamente al cierre; con flujos intraperíodo no se fabrica TWR, incluso si la suma neta es cero. Valuación multimoneda usa FX disponible al corte, pero la atribución de PnL multimoneda continúa no identificable. [Contrato contable](QEEF_ACCOUNTING_RECONCILIATION.md).

La auditoría real conservó todos los denominadores: 109/109 fills con fees UNKNOWN; 2.233 snapshots y 28.503 posiciones; 67 diferencias de valuación; 328 pares con gaps de cantidades y 32 con timestamps insuficientes sobre 2.232 pares. Las coincidencias aritméticas restantes no certifican un ledger completo.

## E. Contrafactuales

PLAN, NO_TRADE, HOLD_EXISTING y CASH se congelan con el estado inicial cuando existe factibilidad documentada; REBALANCE_BASELINE y ELIGIBLE_ALTERNATIVE requieren políticas/elegibilidad ex ante. Lo no suministrado queda NOT_IDENTIFIABLE. Se valida el bundle contra snapshot, cash, cantidades/precios y órdenes del plan canónico.

Las evaluaciones reutilizan exactamente el Episode guardado para 5/10/20 sesiones y horizonte propio. No se selecciona el ganador posterior ni se ajustan objetivos después de ver outcomes. Se calculan retornos/diferencias y dinero contrafactual sólo con evidencia suficiente. Riesgo incremental de cuenta queda pendiente sin curva continua reconciliada.

## F. Validación

| Ejecución | Resultado |
|---|---|
| Suite v1 + Phase 2 | **187 passed**, 0 failed, 0 skipped: 140 preservadas + 47 nuevas |
| Regresión adicional collectors/scheduler/Telegram | **54 passed**, 0 failed, 0 skipped |
| UI Telegram ampliada | **9 passed / 3 failed**, los mismos tres fallos en v1 y Phase 2 |
| PostgreSQL real descartable | Migraciones dobles, RLS bajo rol limitado, inmutabilidad, reintento, recuperación y consumidor completo |
| Flujo real de función de persistencia | `_save_execution_plan_events` conserva plan y bundle transaccional con captura desactivada y con hook lanzando excepción |
| Auditoría histórica repetida | Mismos bytes fuente y audit_id; 301 casos, 0 económicos VERIFIED |
| Adapter nuevo sobre los 2 FULL_CONTEXT reales | 2 CASE PARTIAL / 2 evaluaciones NOT_IDENTIFIABLE, sin persistirlos como prospectivos |

Integración sintética: origen canónico → spool → PostgreSQL → CASE → barras con contratos explícitos → evaluación automática VERIFIED → lifecycle; además conciliación de cuenta VERIFIED. Son fixtures, no resultados del historial real. Se probó llegada de outcome antes del padre, reintento, duplicado tras pérdida de acuse, lectura cruzada rechazada, recuperación de fee nueva sin retrodatación y pares de snapshots atrasados.

Evidencia agregada: [phase2-validation.json](evidence/phase2-validation.json), [phase2-readiness.json](evidence/phase2-readiness.json), [phase2-native-adapter.json](evidence/phase2-native-adapter.json). Los XML completos permanecen en `outputs/qeef/`; sus SHA están en el manifiesto. La auditoría usa corte 2026-10-14 04:53:59.810706 UTC, no un supuesto estado actual de producción.

### Cobertura de los 32 escenarios solicitados

| Escenarios | Prueba / evidencia |
|---|---|
| 1–2 tesis y recurrencia | `test_thesis_generation_and_explicit_recurrence_identity` |
| 3 append-only | `test_append_only_thesis_and_out_of_order_updates` + triggers PostgreSQL |
| 4 cierre/invalidación | `test_lifecycle_closure_and_invalidation` + vencimiento automático en integración |
| 5–6 PIT/futuro | `test_market_contract_and_future_revision`, future_signal/thesis y bundle |
| 7 alternativas congeladas | `test_frozen_alternatives_and_complete_synthetic_counterfactual` |
| 8–10 HOLD/BUY sin fill/SELL parcial | hold_without_operation, buy_without_execution, partial_sell_or_buy |
| 11–13 duplicados/fees/simulación | duplicate_fill, unknown_fees_cannot_be_zero_or_simulated |
| 14–17 dividendos/splits/ratio/FX | dividend_enters_cash, corporate_rebase parametrizado, fx_requires_quote |
| 18–19 sesiones/historial corto | fixture de sesiones explícitas, market_missing_metadata, short_history SNDK |
| 20–24 conexión/reintentos/orden/owner/persistencia | temporary_connection_loss, outbox_restart, integración PostgreSQL/RLS y recovery |
| 25–26 NAV/flujos | nav_mismatch, cash_flows, net_zero_intraperiod_flows, end_period_flow_adjusted_return |
| 27–28 contrafactual inválido/completo | invalid_counterfactual y fixture completo, también recorrido automático PostgreSQL |
| 29 caso incompleto real | Ejecución offline del adapter sobre ambos FULL_CONTEXT: PARTIAL/NOT_IDENTIFIABLE |
| 30 reproducibilidad | Evaluación idéntica repetida; auditoría real repetida con SHA idéntico |
| 31–32 captura off/falla sin afectar recomendación | capture_disabled, collector_failure y persistencia formal real en DB descartable |

La regresión de Telegram usa los stubs ya existentes porque el Python local no tiene instalado `python-telegram-bot`; no se enviaron mensajes. Los tres fallos preexistentes son `test_compact_radar_parses_header_with_cash_lines`, `test_help_text_is_mobile_compact_and_scope_safe` y `test_main_menu_prioritizes_common_actions_and_groups_secondary_views`: expectativas de encabezado, longitud de ayuda y menú divergentes de v1. No se modificó esa UI para ocultarlos.

### Reproducción

```powershell
python -m pytest tests/test_qeef.py tests/test_qeef_postgres.py tests/test_analytics_v2.py tests/test_viability_audit.py tests/test_execution_nominals_and_rotation.py tests/test_contextual_e1.py tests/test_historical_analysis_replay.py tests/test_qeef_phase2.py tests/test_qeef_phase2_postgres.py -q --disable-warnings --junitxml=outputs/qeef/phase2-core-final.xml
python -c "import tests.test_telegram_output_quality; import pytest; raise SystemExit(pytest.main(['tests/test_contextual_g2.py','tests/test_broker_fill_db.py','tests/test_scheduler_market_refresh.py','tests/test_scheduler_weekend_policy.py','tests/test_analytics_v2_telegram.py','tests/test_contextual_telegram.py','-q','--disable-warnings','--junitxml=outputs/qeef/phase2-regression-final.xml']))"
python -m pytest tests/test_telegram_output_quality.py -q --disable-warnings
python scripts/audit_qeef_phase2.py --capture <captura-privada-v1> --output <nuevo-json>
```

La tercera orden conserva los tres fallos documentados. Para comparar baseline ejecutar esa misma orden en el worktree v1. Docker necesita la imagen `postgres:17-alpine`; las pruebas crean contenedores etiquetados únicos y los eliminan tras verificar su identidad. No usan base/credenciales operativas.

## G. Bloqueos y límites

Faltan declaraciones ex ante completas del productor, identidad de instrumento verificada, calendarios/ratios/unidades/acciones corporativas, costos observados completos, ledger de flujos exact-owner y atribución formal de ejecución. Los 37 enlaces almacenados decisión→fill no se promueven a causalidad de plan.

La recuperación de movimientos históricos está limitada por su fuente sin owner. Un bundle extra sólo en memoria no es recuperable del FULL_CONTEXT si nunca se escribió allí. La pérdida simultánea de spool y fuente no tiene garantía de recuperación. Fills: scan máximo 5.000 filas con señal explícita de truncamiento; no se certificó rendimiento bajo carga ni SLO operacional. Detalle en el pipeline, sin ocultar estas restricciones tras un status genérico.

No se estiman tamaños efectivos, tasa de éxito económica, intervalos de alpha o promoción desde una muestra sin contratos verificables. Permanecen 0 casos certificados, aunque el software funcione en fixtures completos.

## H. Seguridad

Captura false por defecto; Telegram false por defecto. RESEARCH_ONLY, affects_analysis=false, affects_execution=false y promotion_allowed=false. Migraciones únicamente en PostgreSQL descartable. El monitor usa autenticación existente y owner configurado; evidencia con RLS y triggers, más validación de identidad/hash en servicio. Logs sanitizados.

No se modificaron señales/pesos/políticas/umbrales, no se enviaron órdenes ni mensajes, no se aplicaron migraciones operativas, no hubo merge ni deploy. Se preservaron snapshots/fills reales, el worktree v1 y los cambios ajenos del principal. RLS no reemplaza autorización de una conexión con SQL arbitrario: el rol de aplicación no debe tener privilegios de administrador.

## I. Git y archivos

Rama: `feature/qeef-prospective-evidence-pipeline`. Worktree: `C:/Users/Franco/OneDrive/Escritorio/backend/.worktrees/quantia-qeef-phase2`. Base: `08623cc9b418d7aef4da89b020ec26737a20d455`. Implementación: **`c25e637862924c519931f8fe932bfd495a7c1637`**. Documentación/evidencias se entregan en un commit separado. El PR se publica hacia main y contiene el antecedente v1 mientras #22 no esté integrado; no se duplica ni se mergea automáticamente.

Publicación confirmada: [PR #23 — QEEF Phase 2](https://github.com/Francoparodi01/Portfolio-manager-/pull/23), abierto en draft hacia main, sin merge. Commit documental: `ee2a520fb917c6796fbf42460978756becdc07d4`; este registro de publicación es un cambio documental posterior. El PR v1 #22 también permanece abierto/draft.

Archivos: 10 módulos nuevos QEEF, migración Phase 2, dos CLIs, dos archivos de tests; hooks mínimos en run_analysis/collector/scheduler; vistas monitor/Telegram; extensión contable Analytics v2. Se actualizaron cuatro documentos v1 y se agregaron pipeline, contabilidad, este informe y evidencias agregadas. Los datos privados y salidas de ejecución no se publican.

## J. Próximo paso concreto

Revisar los PRs y autorizar un piloto **sólo de captura** en un entorno definido: migraciones aditivas, owner, instante de inicio y spool persistente; después habilitar únicamente `QEEF_CAPTURE_ENABLED=true` y verificar el siguiente plan normal contra los contadores esperados/capturados. El procedimiento está en [migración y rollback](QEEF_MIGRATION_AND_ROLLBACK.md).

Ese piloto conserva evidencia y revela faltantes; no cambia ninguna decisión. Para obtener casos futuros completamente verificables, el productor debe adjuntar y persistir tesis/bundle en el momento de la recomendación, y las fuentes deben cerrar los contratos de mercado/contabilidad enumerados. Esta entrega no ejecutó la activación.
