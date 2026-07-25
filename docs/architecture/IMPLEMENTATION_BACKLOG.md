# Backlog de implementacion arquitectonica

## Criterio

Cada tarea esta pensada para un PR chico, validable y sin cambios productivos implicitos. Las tareas de audit/shadow deben mantenerse separadas de cambios de scoring, optimizer, planner o risk.

## ARCH-001: agregar contrato de DecisionRun

Problema: `run_id` existe, pero no hay entidad que concentre versiones, snapshots y metadata.

Alcance: crear dataclass `DecisionRunContext` y helper de serializacion.

Fuera de alcance: crear tabla nueva obligatoria o migrar historico.

Archivos probables: `src/analysis/decision_context.py`, `scripts/run_analysis.py`, tests nuevos.

Migracion: ninguna.

Tests: hash estable, campos requeridos, compatibilidad si faltan campos.

Aceptacion: una corrida nueva puede construir metadata con `run_id`, `as_of`, versions y hashes.

Riesgos: duplicar datos en `layers`; mantener nombres consistentes.

Dependencias: ninguna.

Tamano: S.

Orden: 1.

## ARCH-002: persistir versiones live en `decision_log.layers`

Problema: planner, optimizer, risk y synthesis no quedan versionados por decision.

Alcance: agregar constantes de version y persistirlas en metadata audit-only.

Fuera de alcance: cambiar thresholds o politicas.

Archivos probables: `execution_planner.py`, `optimizer.py`, `synthesis.py`, `risk.py`, `scripts/run_analysis.py`.

Migracion: ninguna.

Tests: snapshot de layers esperado.

Aceptacion: decisiones nuevas incluyen `planner_version`, `optimizer_version`, `risk_policy_version`, `synthesis_version`.

Riesgos: tocar flujo central; validar que solo cambia metadata.

Dependencias: ARCH-001 recomendado.

Tamano: S.

Orden: 2.

## ARCH-003: centralizar clasificacion bot vs humano

Problema: override classification esta duplicada entre monitor, ledger y scripts.

Alcance: crear helper compartido de clasificacion.

Fuera de alcance: cambiar definiciones historicas.

Archivos probables: `src/analysis/override_classification.py`, `decision_ledger.py`, `run_override_audit.py`, `monitor/api.py`.

Migracion: ninguna.

Tests: fixtures con manual-only, bot-only, matched, unmatched.

Aceptacion: todos los consumidores dan la misma clasificacion.

Riesgos: diferencias actuales pueden estar ocultas por queries distintas.

Dependencias: ninguna.

Tamano: M.

Orden: 3.

## ARCH-004: Decision Timeline read-only

Problema: el lifecycle se reconstruye con logica repartida.

Alcance: servicio read-only que devuelve eventos ordenados y gaps.

Fuera de alcance: tablas nuevas, cambios en monitor UI.

Archivos probables: `src/analysis/decision_timeline.py`, `scripts/run_decision_timeline.py`, tests.

Migracion: ninguna.

Tests: orden temporal, gaps, joins decision/fill/outcome.

Aceptacion: CLI puede mostrar una decision desde input hasta outcome.

Riesgos: queries lentas si no se indexa; empezar con filtros acotados.

Dependencias: ARCH-003 ayuda.

Tamano: M.

Orden: 4.

## ARCH-005: congelar FeatureSnapshot por hash

Problema: features live quedan en JSON/layers sin ID estable.

Alcance: construir hash deterministico de features usadas por decision.

Fuera de alcance: tabla nueva obligatoria.

Archivos probables: `src/analysis/feature_snapshot.py`, `scripts/run_analysis.py`, tests.

Migracion: ninguna.

Tests: hash estable ante ordenamiento distinto, hash cambia ante dato distinto.

Aceptacion: cada decision nueva puede reportar `feature_snapshot_id`.

Riesgos: incluir campos volatiles que rompan reproducibilidad.

Dependencias: ARCH-001.

Tamano: M.

Orden: 5.

## ARCH-006: MarketSnapshot v0

Problema: Strategy Lab necesita un mercado congelado para comparacion justa.

Alcance: crear vista/tabla append-only de snapshot de universo y precios por `as_of`.

Fuera de alcance: reingesta historica masiva.

Archivos probables: `init.sql`, `src/collector/db.py`, nuevo helper.

Migracion: aditiva.

Tests: no look-ahead, universo reproducible, datos faltantes marcados.

Aceptacion: dos estrategias pueden leer el mismo `market_snapshot_id`.

Riesgos: tamano de datos; definir TTL o granularidad.

Dependencias: ARCH-005 recomendado.

Tamano: M.

Orden: 6.

## ARCH-007: Strategy Registry

Problema: no existe entidad de estrategia/version.

Alcance: tabla o registry en codigo con `quantia_core` y shadows iniciales.

Fuera de alcance: implementar estrategias nuevas.

Archivos probables: `init.sql`, `src/analysis/strategy_registry.py`, tests.

Migracion: aditiva.

Tests: version requerida, `can_trade` obligatorio.

Aceptacion: `quantia_core` queda declarado como unico `can_trade=true`.

Riesgos: sobreabstraer; mantener registry minimo.

Dependencias: ninguna estricta.

Tamano: S.

Orden: 7.

## ARCH-008: wrapper de Quantia Core como estrategia

Problema: core esta embebido en scripts, dificil de comparar.

Alcance: exponer output de core bajo `StrategyOutputPacket` sin cambiar calculos.

Fuera de alcance: mover todo `run_analysis.py`.

Archivos probables: `scripts/run_analysis.py`, `src/analysis/strategy_lab.py`.

Migracion: ninguna.

Tests: output wrapper igual al output previo.

Aceptacion: core produce packet comparable.

Riesgos: tocar script grande; hacer PR pequeno.

Dependencias: ARCH-007.

Tamano: M.

Orden: 8.

## ARCH-009: Strategy Lab shadow runner v0

Problema: no hay runner que compare estrategias con mismo snapshot.

Alcance: ejecutar una estrategia shadow trivial/reference y guardar outputs.

Fuera de alcance: estrategia compleja o live.

Archivos probables: `scripts/run_strategy_lab.py`, `src/analysis/strategy_lab.py`, `init.sql`.

Migracion: aditiva: `strategy_runs`, `strategy_outputs`.

Tests: `can_trade=false`, mismo snapshot, sin outcomes futuros.

Aceptacion: reporte CLI muestra core vs shadow.

Riesgos: comparaciones prematuras sin muestra.

Dependencias: ARCH-006, ARCH-007, ARCH-008.

Tamano: M.

Orden: 9.

## ARCH-010: gates champion-challenger

Problema: no hay criterio formal de promocion.

Alcance: definir evaluador read-only con EV neto, IC, drawdown, turnover y coverage.

Fuera de alcance: promocion automatica.

Archivos probables: `src/analysis/strategy_comparison.py`, `scripts/run_strategy_comparison.py`.

Migracion: ninguna.

Tests: falla sin muestra minima, falla con turnover excesivo, aprueba fixture controlado.

Aceptacion: una estrategia puede quedar `candidate`, `shadow`, `challenger_eligible` o `rejected`.

Riesgos: criterios demasiado laxos; empezar conservador.

Dependencias: ARCH-009.

Tamano: M.

Orden: 10.

## ARCH-011: attribution de costos y shortfall

Problema: plan, fill y costos no se comparan de forma sistematica.

Alcance: calcular fees, slippage y shortfall por fill asociado.

Fuera de alcance: atribucion CCL completa.

Archivos probables: `src/analysis/financial_attribution.py`, `scripts/run_financial_attribution.py`, `init.sql`.

Migracion: aditiva: `financial_attribution`.

Tests: compra/venta con signo correcto, costos una vez, fill manual separado.

Aceptacion: reporte muestra shortfall bps y costo neto.

Riesgos: matching plan/fill imperfecto; marcar calidad.

Dependencias: ARCH-004 recomendado.

Tamano: M.

Orden: 11.

## ARCH-012: benchmark returns v0

Problema: EV absoluto puede confundir beta de mercado con valor agregado.

Alcance: calcular benchmark-relative por ticker/universo simple.

Fuera de alcance: factor model completo.

Archivos probables: `src/analysis/benchmarks.py`, `scripts/run_benchmark_report.py`.

Migracion: opcional aditiva.

Tests: retornos por horizonte, feriados/datos faltantes.

Aceptacion: cada outcome puede compararse contra no-trade/buy-and-hold o proxy.

Riesgos: benchmark mal elegido; declarar metodo.

Dependencias: ARCH-011 recomendado.

Tamano: M.

Orden: 12.

## ARCH-013: EvidencePacket para IA

Problema: prompts reciben contexto sin contrato uniforme de evidencia.

Alcance: crear `EvidencePacket` y validador de IDs/tiempos.

Fuera de alcance: cambiar decisiones live.

Archivos probables: `src/analysis/ai_governance.py`, `shadow_causal.py`, `synthesis.py`, tests.

Migracion: ninguna inicial.

Tests: rechaza evidencia futura, rechaza claims sin IDs.

Aceptacion: shadow causal y nuevos explainers usan packet versionado.

Riesgos: romper prompts existentes; adaptar de a uno.

Dependencias: ARCH-001, ARCH-005.

Tamano: M.

Orden: 13.

## ARCH-014: versionar sentiment y synthesis prompts

Problema: no hay trazabilidad suficiente de cambios de prompt/modelo en todos los outputs LLM.

Alcance: persistir `prompt_version`, `schema_version`, `model`, `temperature`, hashes.

Fuera de alcance: cambiar scoring.

Archivos probables: `nlp_scorer.py`, `synthesis.py`, `signal_aggregator.py`, `init.sql`.

Migracion: aditiva o JSON metadata.

Tests: campos presentes, fallback mantiene metadata.

Aceptacion: outputs nuevos de IA son reproducibles/auditables.

Riesgos: columnas nuevas vs JSON; preferir cambio chico.

Dependencias: ARCH-013.

Tamano: M.

Orden: 14.

## ARCH-015: explicador audit-only

Problema: la explicacion live no esta separada como entidad evidence-bound.

Alcance: generar explicaciones audit-only sobre decisiones cerradas o planes nuevos.

Fuera de alcance: usar explicacion para decidir.

Archivos probables: `src/analysis/decision_explainer.py`, `init.sql`, monitor posterior.

Migracion: aditiva: `ai_evidence_packets`, `ai_explanations`.

Tests: no modifica decision, falla cerrado sin evidencia, fallback deterministico.

Aceptacion: se puede consultar explicacion con prompt/schema/model y evidence IDs.

Riesgos: narrativa persuasiva sin soporte; validar claims.

Dependencias: ARCH-013, ARCH-014.

Tamano: M.

Orden: 15.

## ARCH-016: limpiar visibilidad de tests en `.gitignore`

Problema: `.gitignore` ignora `tests/*` y puede ocultar cobertura nueva.

Alcance: revisar excepciones y documentar/ajustar patron para tests versionables.

Fuera de alcance: reordenar toda la suite.

Archivos probables: `.gitignore`, tests existentes.

Migracion: ninguna.

Tests: `rg --files tests` muestra los tests esperados.

Aceptacion: nuevos tests no quedan invisibles por defecto.

Riesgos: apareceran muchos tests no trackeados; separar por commit.

Dependencias: ninguna.

Tamano: S.

Orden: paralelo despues de ARCH-001.

## ARCH-017: endpoint monitor para Decision Timeline

Problema: monitor reconstruye vistas especificas y puede duplicar logica.

Alcance: endpoint que use `decision_timeline.py`.

Fuera de alcance: redisenar UI completa.

Archivos probables: `src/monitor/api.py`, tests de API.

Migracion: ninguna.

Tests: auth, filtros, respuesta con gaps.

Aceptacion: `/api/decision-timeline` devuelve eventos normalizados.

Riesgos: performance; exigir filtros.

Dependencias: ARCH-004.

Tamano: M.

Orden: 16.

## ARCH-018: separar application services de CLI/Telegram

Problema: scripts grandes mezclan orquestacion, dominio, IO y presentacion.

Alcance: extraer un caso de uso chico usado por CLI y Telegram.

Fuera de alcance: refactor masivo de `run_analysis.py` o `telegram_bot.py`.

Archivos probables: nuevo `src/application/`, scripts consumidores.

Migracion: ninguna.

Tests: caso de uso puro con mocks/fixtures.

Aceptacion: una funcionalidad deja de duplicarse entre CLI y Telegram.

Riesgos: refactor transversal; elegir una ruta angosta.

Dependencias: despues de timeline/strategy contracts.

Tamano: L si se amplia; mantener S/M por slice.

Orden: 17.

## Primeros cinco PRs recomendados

1. ARCH-001: contrato `DecisionRunContext`.
2. ARCH-002: versiones live en metadata audit-only.
3. ARCH-003: clasificacion bot/humano compartida.
4. ARCH-004: Decision Timeline read-only.
5. ARCH-005: FeatureSnapshot hash.

Estos cinco no cambian decisiones productivas y preparan el terreno para Strategy Lab y atribucion sin reescribir el motor.
