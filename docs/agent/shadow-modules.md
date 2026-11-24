# Calibración Shadow V3, Learning Shadow, Technical V2 y Technical Buy V3

Revisión: 2026-10-22. Alcance: contratos; este documento no publica resultados actuales.

## Cuatro shadow con preguntas diferentes
`SHADOW_ONLY` significa que el módulo evalúa sin alterar la ejecución. No implica
un único modelo, horizonte, dataset ni win rate común. El agente debe nombrar la
versión, cohorte, período, unidad temporal, casos maduros y métrica que esté leyendo.

| Módulo | Qué evalúa | Cómo leer el resultado |
|---|---|---|
| Calibración Shadow V3 | Ajuste de retornos esperados y probabilidades de subida | MAE y Brier, fit separado de walk-forward, gate por horizonte. |
| Learning Shadow | Decisiones bloqueadas y posibles oportunidades perdidas | Casos maduros, tasa potencial, tasa limpia, riesgo y benchmark. |
| Technical Shadow V2 | Continuidad/reversión según régimen | Sesgo POSITIVE/NEGATIVE/NEUTRAL persistido y resultado por cohorte. |
| Technical Buy Shadow V3 | Candidatos de compra derivados de V2 | A/B elegibles; C sin confirmar; REJECTED; no clasifica ventas. |

## Calibración Shadow V3: MAE, Brier y cobertura del intervalo
Versión `price_trend_calibrated_shadow_v3`, distinta de optimizer calibration y DCL.
Horizontes de evaluación: 5 y 20 sesiones bursátiles. MAE mide error absoluto del
retorno; Brier, error de probabilidades. En ambos menor es mejor. La cobertura del
intervalo cercano al 80% no es una tasa de acierto direccional del 80%.

El walk-forward usa ejemplos cuyo target terminó antes del as-of del caso probado:
`train.target_session_ts < test.as_of_ts`. Reportar sus métricas por separado del
ajuste de entrenamiento. `FAILED_WALK_FORWARD` no puede presentarse como aprobado;
`CANDIDATE_AFTER_FORWARD_TEST` tampoco acredita una promoción operativa.

Persistencia: `shadow_calibration_runs`, `shadow_calibration_models` y
`shadow_calibrated_forecasts`. Implementación: `src/analysis/shadow_calibration.py`
y `shadow_calibration_store.py`. `get_calibration_audit` devuelve DCL: no usarlo
para contestar resultados de Calibración Shadow V3 como si fueran el mismo sistema.

## Learning Shadow: oportunidades bloqueadas y tasa limpia
Versión `learning-shadow-v2`. Población primaria `PLANNER_BLOCKED`; mantener
`RADAR_BLOCKED`, `RADAR_DEBUG` y otras poblaciones separadas. Sus outcomes y recorridos
usan días calendario 5/10/20/40; el campo `shadow_horizon_sessions` pertenece al
forecast enlazado y no cambia la unidad del outcome.

Tasa potencial: proporción madura con movimiento direccional de al menos 75 bps.
Tasa limpia: casos `CLEAN_MISSED_OPPORTUNITY` sobre maduros, después de filtros de
recorrido y benchmark. No son tasas de acierto de compras ejecutadas ni prueban que
el bloqueo fuera incorrecto. Separar el promedio de todos los casos del promedio
de los ganadores seleccionados. Controles retrospectivos reutilizados no prueban causalidad.

Las propuestas `PROPOSED` no son reglas activadas. Fuente: `learning_shadow_cases`,
`learning_shadow_metric_snapshots_v2`, `learning_shadow_rule_candidates`.
Implementación: `src/analysis/learning_shadow.py`; detalles en `docs/10-learning-shadow.md`.
El script de ejecución puede escribir tablas de auditoría: no sugerir ejecutarlo
como si fuera una lectura inocua; el agente debe usar herramientas registradas.

## Technical Shadow V2: dirección y neutralidad
Combina tendencia/reversión según régimen; se guarda en `layers.technical_shadow_v2`.
La clasificación almacenada usa POSITIVE desde +0,20, NEGATIVE desde −0,20 y NEUTRAL
entre ambos. Un score −0,03 no representa una señal bajista activa.

No mezclar planner, radar y bloqueos, ni owners. Para auditar, usar la etiqueta que
existía en la decisión. Un replay con las reglas actuales es otra población. Los
outcomes nativos 5d/10d/20d/40d usan días calendario y primera vela canónica posterior;
un estudio alternativo a 20 ruedas debe identificarse como tal.
Fuente: `src/analysis/technical_shadow_v2.py`, `docs/12-technical-shadow-v2.md`.

## Technical Buy Shadow V3: niveles A, B, C y rechazo
Su objetivo nativo es compra nueva a 20 días calendario. A prioriza continuidad en
STRONG_UPTREND; B, reversión en RANGE; C requiere confirmación; REJECTED no es SELL.
Usar `eligible_for_buy_research` y `priority_tier` originales. Informar A por separado
y aclarar cuándo se agrupan A+B; C/rechazos no son pérdidas.

Se guarda en `layers.technical_buy_shadow_v3`. Declara `affects_analysis=false`,
`affects_execution=false` y `affects_radar_ranking=false`. No modifica el ranking
operativo ni acredita órdenes. El replay inicial de abril-agosto publicado en la
guía V3 es retrospectivo y no equivale a resultados actuales o un holdout estricto.
Fuente: `src/analysis/technical_buy_shadow_v3.py`, `docs/13-technical-buy-shadow-v3.md`.

## Cómo responder sobre resultados auditados
Usar la fuente del módulo exacto y su corte. Declarar denominador, horizonte,
precio de entrada/salida, moneda, costos y tratamiento de repeticiones. Si la
auditoría presenta muestra diaria y sensibilidad sin solapamiento, conservar ambas.
Los cuatro denominadores no se suman: comparten observaciones y miden cosas distintas.
Si el catálogo del run no expone una tabla o informe, informar esa limitación de
acceso. No inventar cifras ni afirmar que el módulo carece de historia.
