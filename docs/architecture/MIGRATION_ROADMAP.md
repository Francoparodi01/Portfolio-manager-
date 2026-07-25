# Roadmap de migracion arquitectonica

## Enfoque

La evolucion de Quantia debe ser incremental. El sistema ya opera, audita y reporta; por eso la migracion correcta agrega contratos, IDs, versionado y servicios read-only antes de tocar decisiones live.

Reglas:

- No hacer migraciones destructivas.
- No cambiar thresholds, optimizer, planner ni risk por accidente.
- Separar audit/shadow de decision productiva.
- Validar cada fase con tests enfocados y smoke checks.
- Mantener `decision_log` como compatibilidad historica.

## Fases

| Fase | Objetivo | Impacto funcional | Riesgo | Dependencias |
|---:|---|---|---|---|
| 0 | Documentar arquitectura actual, brechas y target | Ninguno | Bajo | Auditoria estatica |
| 1 | Agregar contratos de IDs y metadata | Observabilidad | Bajo | `decision_log.layers` |
| 2 | Congelar snapshots/versiones | Reproducibilidad | Medio | Market/portfolio/feature inputs |
| 3 | Decision Timeline read-only | Auditoria | Bajo | Fases 1-2 parciales |
| 4 | Strategy Registry | Base de Strategy Lab | Bajo | Contrato de strategy |
| 5 | Strategy Lab shadow v0 | Comparacion controlada | Medio | Snapshot comun |
| 6 | Champion-challenger | Gobernanza de promocion | Medio | Metricas suficientes |
| 7 | Financial Attribution v0 | Performance explicable | Medio | Plan/fill/outcome IDs |
| 8 | AI governance/explainer | Explicacion evidence-bound | Medio | Evidence packets |
| 9 | Knowledge layer | Aprendizaje historico | Medio | Outcomes + explicaciones |
| 10 | Monitor/productizacion | Uso operativo | Medio | APIs estables |

## Fase 0: documentos

Objetivo:

- Crear la base documental bajo `docs/architecture/`.

Archivos:

- `CURRENT_ARCHITECTURE.md`
- `ARCHITECTURE_GAPS.md`
- `TARGET_ARCHITECTURE.md`
- `DECISION_LIFECYCLE.md`
- `STRATEGY_LAB_DESIGN.md`
- `AI_GOVERNANCE.md`
- `FINANCIAL_ATTRIBUTION.md`
- `MIGRATION_ROADMAP.md`
- `IMPLEMENTATION_BACKLOG.md`

Aceptacion:

- No hay cambios de comportamiento.
- `git diff --check` sin errores.
- Los documentos cubren estado actual, target, gaps y tareas.

Rollback:

- Revertir solo archivos nuevos de documentacion.

## Fase 1: contratos de IDs y metadata

Objetivo:

- Definir IDs logicos sin cambiar el flujo.

Cambios probables:

- Agregar dataclasses o helpers en `src/analysis/decision_context.py`.
- Persistir `planner_version`, `optimizer_version`, `risk_policy_version`, `config_hash`, `code_version` en `decision_log.layers`.
- Crear helper para construir `input_hash`.

Tests:

- Unit tests de hash estable.
- Tests de serializacion de metadata.
- Smoke de `scripts/run_analysis.py --no-telegram --no-llm --no-persist`.

Aceptacion:

- Una decision nueva expone versiones e input hash.
- El output historico sin esos campos sigue funcionando.

Rollback:

- Ignorar campos nuevos en `layers`; no hay migracion destructiva.

## Fase 2: snapshots/versiones

Objetivo:

- Congelar mercado, cartera y features usados por una corrida.

Cambios probables:

- `market_snapshot_id` o vista materializable.
- `feature_snapshot_id` por hash.
- Vinculo consistente con `portfolio_snapshots.snapshot_id`.

Tests:

- No look-ahead: queries con `ts <= as_of`.
- Hash cambia ante cambio de feature.
- Hash no cambia por ordenamiento distinto.

Aceptacion:

- La misma corrida puede reconstruir el packet de inputs.
- Strategy Lab puede usar el mismo packet para multiples estrategias.

Rollback:

- Mantener snapshots como metadata audit-only.

## Fase 3: Decision Timeline read-only

Objetivo:

- Unificar la lectura del ciclo de vida sin modificar tablas productivas.

Cambios probables:

- `src/analysis/decision_timeline.py`
- CLI `scripts/run_decision_timeline.py`
- Tests con fixtures de decision, movement, fill y outcome.

Aceptacion:

- Dado un ticker/run_id, devuelve eventos ordenados.
- Marca gaps explicitamente.
- No escribe en base.

Rollback:

- Desactivar CLI/endpoint; datos intactos.

## Fase 4: Strategy Registry

Objetivo:

- Registrar estrategias y versiones.

Cambios probables:

- Tabla `strategy_registry`.
- Seed inicial para `quantia_core`.
- Dataclass `StrategySpec`.

Tests:

- Validar `can_trade` por estrategia.
- Validar version requerida.

Aceptacion:

- Quantia Core queda registrada como live.
- Estrategias shadow quedan bloqueadas para ejecucion.

Rollback:

- Tabla nueva sin impacto productivo.

## Fase 5: Strategy Lab shadow v0

Objetivo:

- Ejecutar una estrategia shadow minima con el mismo snapshot.

Cambios probables:

- `strategy_runs`
- `strategy_outputs`
- Runner CLI read-only.

Tests:

- Misma entrada para core y shadow.
- No acceso a outcomes futuros.
- `can_trade=false` impide execution plan.

Aceptacion:

- Reporte compara core vs shadow en una ventana.
- Sin escritura en `decision_log` productivo.

Rollback:

- Desactivar runner shadow.

## Fase 6: Champion-challenger

Objetivo:

- Definir promocion gobernada por evidencia.

Cambios probables:

- Reporte de comparacion.
- Estado de estrategia: candidate/shadow/challenger/retired.
- Gates: EV neto, IC, drawdown, turnover, coverage.

Tests:

- Estrategia no promociona sin muestra minima.
- Estrategia con alto turnover falla si costos destruyen EV.

Aceptacion:

- Ningun challenger puede operar automaticamente.
- Promocion requiere decision manual.

Rollback:

- Volver estrategias a `shadow`.

## Fase 7: Financial Attribution v0

Objetivo:

- Explicar resultados por costos, shortfall y benchmark.

Cambios probables:

- `financial_attribution`
- Servicio de benchmark returns.
- CLI de atribucion.

Tests:

- Compra/venta shortfall con signos correctos.
- Costos incluidos una sola vez.
- Manual vs bot no se mezcla.

Aceptacion:

- Una decision ejecutada muestra net return, fees y shortfall.
- Una decision teorica muestra opportunity return separado.

Rollback:

- Tabla/reportes nuevos, sin impacto en planner.

## Fase 8: AI governance/explainer

Objetivo:

- Normalizar evidence packets y explicaciones audit-only.

Cambios probables:

- `EvidencePacket`
- Validadores de output LLM.
- `ai_evidence_packets`
- `ai_explanations`

Tests:

- Rechazo de claims sin evidence IDs.
- Fallback deterministico si falla LLM.
- Versionado de prompt/model/schema.

Aceptacion:

- Las explicaciones no alteran decisiones.
- Monitor/Telegram muestran explicacion con estado auditado.

Rollback:

- Omitir explicaciones AI y mantener resumen deterministico.

## Fase 9: Knowledge layer

Objetivo:

- Convertir outcomes y explicaciones en lecciones consultables.

Cambios probables:

- `decision_lessons`
- Reporte de patrones por regimen/horizonte.
- Vinculo con Strategy Lab.

Tests:

- Lecciones solo desde outcomes cerrados.
- No usar lecciones futuras para decisiones historicas.

Aceptacion:

- Se puede consultar que aprendio Quantia por ticker/regimen.
- La capa es informativa, no decisora.

Rollback:

- Desactivar lecturas de knowledge layer.

## Fase 10: Monitor/productizacion

Objetivo:

- Exponer timeline, Strategy Lab, attribution y explanations en UI/API.

Cambios probables:

- Endpoints nuevos en `src/monitor/api.py`.
- Componentes UI por timeline/strategy/attribution.
- Telegram commands informativos.

Tests:

- Contract tests de endpoints.
- Smoke visual/API.
- Auth existente respetada.

Aceptacion:

- Usuario puede auditar una decision desde snapshot hasta outcome.
- No se mezclan vistas audit-only con acciones live.

Rollback:

- Ocultar endpoints/componentes nuevos.

## Prioridad recomendada

| Orden | Item | Impacto | Esfuerzo | Riesgo |
|---:|---|---|---|---|
| 1 | Versiones y hashes en `layers` | Alto | Bajo | Bajo |
| 2 | Decision Timeline read-only | Alto | Medio | Bajo |
| 3 | Snapshot/feature packet | Alto | Medio | Medio |
| 4 | Strategy Registry | Medio | Bajo | Bajo |
| 5 | Strategy Lab shadow v0 | Alto | Medio | Medio |
| 6 | Attribution costos/shortfall | Alto | Medio | Medio |
| 7 | AI evidence packets | Medio | Medio | Medio |
| 8 | Champion-challenger gates | Alto | Medio | Medio |
| 9 | Knowledge layer | Medio | Alto | Medio |
| 10 | Monitor completo | Alto | Alto | Medio |

## Modulos que no conviene tocar al inicio

- `optimizer.py` para cambiar objetivos o constraints.
- `execution_planner.py` para cambiar guards.
- `synthesis.py` para cambiar scoring live.
- `risk.py` para relajar riesgo.
- `telegram_bot.py` para introducir logica de dominio.
- `monitor/api.py` para duplicar queries nuevas antes de tener servicios compartidos.

La primera mejora debe rodear el motor con contratos, no modificar el motor.
