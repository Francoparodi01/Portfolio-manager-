# Auditoria de Datos para Frontend Quantia

## Alcance

Este documento es la Fase 0 antes de construir pantallas o endpoints nuevos.
El objetivo es inventariar que datos ya captura el backend, que parte llega hoy
a la API del monitor, que parte se ve en el frontend React y que cruces conviene
priorizar con Franco.

No implementa cambios de UX, API, schema, scoring, thresholds, optimizer,
planner ni logica de decision. Cualquier cruce nuevo debe ser read-only y debe
mantener separados:

- planes del bot (`execution_plan`, `decision_log`);
- ejecuciones reales observadas (`broker_fills`, `broker_movements`);
- movimientos manuales del usuario (`EXECUTED_MANUAL`, actividad de broker);
- radar/shadow/sentiment como evidencia contextual o experimental;
- outcomes y EV solo cuando la muestra y el alcance son comparables.

## Regla de honestidad estadistica

Toda vista de performance, EV, acierto o edge debe mostrar siempre `n`.
Si `n < 30`, la UI debe marcarlo como muestra insuficiente y no debe presentar
edge validado. El caso concreto a respetar es el corte bot-only 5D observado:
`n=25`, por debajo del umbral minimo. Eso puede mostrarse como evidencia en
formacion, pero no como edge confirmado.

## Inventario backend/API/frontend

| Dato capturado en backend | ¿Hoy se expone por API? | ¿Hoy se muestra en el frontend? | Página donde podría vivir | Requiere endpoint nuevo (sí/no) |
|---|---|---|---|---|
| Proyecciones shadow por ticker y horizonte 5/20/40, con outcome maduro cuando existe. Tablas `shadow_thesis_runs`, `shadow_thesis_forecasts`, `shadow_thesis_outcomes`; endpoint en `src/monitor/api.py::shadow_view`. | Si, por `/api/shadow`, con run, forecasts y metrics. | Parcial. `AnalysisPage` consume `useShadowQuery`, pero la vista queda como resumen; no explota una lectura por ticker/horizonte con forecast vs realidad. | `AnalysisPage` / futuro panel "Shadow". | No para MVP basico; si para filtros historicos, drilldown por corrida o causal analysis completa. |
| Sentiment score por ticker/fecha/noticia. Tablas `sentiment_raw`, `sentiment_scored`, `sentiment_aggregated`; pipeline en `scripts/run_sentiment_pipeline.py` y `src/analysis/nlp_scorer.py`. | No se encontro endpoint dedicado en monitor API. Aparece como insumo interno en analysis/shadow causal. | No se encontro vista React dedicada. | `AnalysisPage`, detalle de ticker o timeline de evidencia. | Si. |
| Eventos manuales de mercado/catalysts. Tabla `manual_market_events`; script `scripts/manual_market_events.py`; lectura en scheduler/analysis para riesgo de eventos. | No se encontro endpoint dedicado en monitor API. | No se encontro vista React dedicada. | Timeline de auditoria, `AnalysisPage` o detalle de ticker. | Si. |
| Regimen trend-shadow por ticker (`trend_shadow_regime`, `trend_shadow_score`) guardado en `decision_log.layers`. | Parcial. `/api/performance` lo expone en filas recientes del plan. | Parcial. `PerformancePage` muestra columna "Regimen" en recientes, pero no como serie historica por ticker ni como filtro central. | `PerformancePage` / `AnalysisPage`. | No para mostrar en decisiones recientes; si para historia por ticker y comparacion contra outcome. |
| Pesos Black-Litterman vs pesos previos / fallback `FALLBACK_MAX_SHARPE`. Optimizer en `src/analysis/optimizer.py`; docs mencionan BL y fallback. | No se encontro endpoint dedicado de corridas de optimizer/pesos historicos. Parte puede quedar embebida en planes o logs, pero no como contrato estable de API. | No se encontro vista dedicada de pesos propuestos vs cartera actual. | `PortfolioPage` o panel "Optimizer". | Si, salvo que se acepte un MVP limitado desde `decision_log.layers` si contiene datos suficientes en produccion. |
| Alertas intraday pre-close disparadas. Tabla `intraday_preclose_alerts`; generacion en `src/analysis/preclose_alerts.py` y scheduler. | No se encontro endpoint dedicado en monitor API. | No se encontro vista dedicada. | Timeline operativo, `DataPage` o panel de alertas. | Si. |
| Overrides humanos con justificacion. Auditoria cruza planes aprobados con movimientos reales y clasifica `FOLLOWED`, `PARTIAL`, `OPPOSITE`, `IGNORED`; `override_audit` lee `layers->>'reason'` del plan. | Si, por `/api/override-audit`; incluye estado de seguimiento y razon del plan. No confirma una justificacion humana explicita persistida para cada override. | Parcial. `HumanBenchmarkPage` y `DecisionsPage` muestran bot vs humano, pero falta separar mejor "razon del bot", "accion real del usuario" y "justificacion humana". | `HumanBenchmarkPage` / `DecisionsPage`. | No para auditoria basica; si si queremos guardar y mostrar justificacion humana explicita. |
| Fills reales vs plan del bot. Tablas `broker_fills`, `broker_movements`; endpoints `/api/fills`, `/api/override-audit`, `/api/decision-ledger`. | Si, con fills/movements recientes y auditoria de seguimiento. | Si, parcial. `DecisionsPage` muestra historial de movimientos y `HumanBenchmarkPage` resume seguimiento, pero falta una timeline unificada plan -> ejecucion -> outcome. | `DecisionsPage` / timeline de auditoria. | No para MVP basico; si para una timeline unificada con eventos, alertas y outcomes en una sola respuesta. |
| Snapshots de portfolio y allocation historica. Tabla `portfolio_snapshots`; endpoint `/api/portfolio`. | Si. | Si. `PortfolioPage` y resumen general. | `PortfolioPage`. | No. |
| Cobertura/frescura de velas de mercado. Tabla `market_candles`; endpoint `/api/candles` y `/api/ingestion`. | Si. | Si. `DataPage` y componentes de estado. | `DataPage`. | No. |

## Brechas principales

1. El backend ya captura mas evidencia de la que el frontend muestra: sentiment,
   eventos manuales, pre-close alerts y optimizer no tienen una superficie clara.
2. La UI ya distingue algunas poblaciones, pero todavia necesita una regla
   visual uniforme para `n`, especialmente en EV/performance.
3. "Hecho por Franco" y "decision del bot" no deben mezclarse en una sola fila:
   una pantalla auditable deberia separar plan, accion real, fuente, estado de
   seguimiento y outcome.
4. La justificacion humana no esta confirmada como dato persistido estructurado.
   Hoy puede inferirse accion real por broker movements, pero no conviene
   inventar una razon si la DB no la trae.
5. Shadow, radar, sentiment y trend regime son evidencia; no deben promoverse a
   decision operativa ni contaminar EV bot-only sin contrato explicito.

## Cruces propuestos para priorizar

Esta tabla es una propuesta para que Franco elija. No queda nada aprobado hasta
que Franco confirme que cruces construir primero.

| Prioridad propuesta | Cruce | Valor | Costo estimado | Estado recomendado |
|---|---|---|---|---|
| 1 | EV/performance por poblacion con `n`, scope y badge `n<30`. Separar bot-only, manual-only, aggregate, radar/debug y primary. | Alto: evita conclusiones falsas sobre edge y hace legible la metrica principal. | Bajo/medio: parte ya sale por `/api/performance`, pero hay que endurecer contrato UI. | Recomendar como primer cambio. |
| 2 | Timeline auditable plan -> movimiento real -> outcome. Cada fila debe decir: fuente, estado, accion, monto, fecha, si fue seguido/ignorado y resultado. | Alto: responde directamente "que hice yo" vs "que decidio el bot". | Medio: puede empezar con `/api/fills`, `/api/override-audit` y `/api/decision-ledger`; para timeline completa conviene endpoint agregado read-only. | Recomendar como primer MVP de producto. |
| 3 | Override humano con costo/beneficio real. Mostrar planes aprobados que Franco siguio, ignoro o hizo al reves, con outcome posterior y razon del plan. | Alto: mide si el criterio humano mejora o empeora el plan. | Medio: datos basicos existen; falta capturar justificacion humana si se quiere mostrarla honestamente. | Recomendar despues del timeline basico. |
| 4 | Shadow forecast vs realidad por ticker/horizonte. Ver 5/20/40 como evidencia experimental con muestras y error. | Medio/alto: ayuda a auditar tesis sin tocar planner. | Medio: `/api/shadow` ya existe; falta UX y tal vez historico/filtros. | Aprobable como panel de analisis, no como decision. |
| 5 | Eventos manuales + alertas pre-close sobre decisiones y precio. Marcar cuando una compra/venta ocurrio cerca de catalyst o alerta. | Medio/alto: explica contexto que hoy queda escondido. | Medio/alto: requiere endpoints para `manual_market_events` e `intraday_preclose_alerts`. | Priorizar si Franco usa esos eventos operativamente. |
| 6 | Sentiment por ticker/fecha vs decision tomada. Mostrar si una decision tenia viento textual a favor/en contra. | Medio: mejora explicabilidad, pero tiene riesgo de sobreinterpretacion LLM. | Medio: requiere endpoint nuevo y reglas de frescura/confianza. | Mantener como evidencia contextual. |
| 7 | Trend regime por ticker vs outcome. Comparar regimen/score con resultado posterior. | Medio: puede detectar cuando la lectura de tendencia ayuda o distrae. | Medio: parte existe en `decision_log.layers`; faltan agregados por ticker/historia. | Aprobable despues de EV con `n`. |
| 8 | Optimizer Black-Litterman vs cartera actual vs fallback MAX_SHARPE. Mostrar pesos propuestos, pesos previos y motor usado. | Medio: util para rebalanceo, menos directo para auditoria de ejecucion. | Alto si no hay corrida persistida estructurada; puede requerir contrato nuevo. | No arrancar hasta confirmar datos persistidos. |

## Decision pendiente para Franco

Para avanzar sin construir ocho cosas a la vez, la decision recomendada es elegir
un paquete inicial de 2 o 3 cruces. Mi recomendacion tecnica es:

1. EV/performance por poblacion con `n` visible y bloqueo visual de `n<30`.
2. Timeline auditable plan -> ejecucion real -> outcome.
3. Override humano con costo/beneficio real.

Queda pendiente de aprobacion por Franco. Hasta esa aprobacion, este documento
solo funciona como insumo de priorizacion.

## Contratos API sugeridos, sin implementar

Si se aprueba avanzar, conviene agregar endpoints read-only y no cambiar la
logica de decision:

| Endpoint sugerido | Fuente primaria | Uso |
|---|---|---|
| `/api/audit-timeline?days=...&ticker=...` | `decision_log`, `broker_fills`, `broker_movements`, outcomes | Unificar plan, ejecucion real y resultado. |
| `/api/sentiment-audit?days=...&ticker=...` | `sentiment_raw`, `sentiment_scored`, `sentiment_aggregated` | Mostrar sentimiento como evidencia contextual. |
| `/api/manual-events?days=...&ticker=...` | `manual_market_events` | Mostrar catalysts declarados manualmente. |
| `/api/preclose-alerts?days=...&ticker=...` | `intraday_preclose_alerts` | Mostrar alertas disparadas y evidencia. |
| `/api/optimizer-runs?days=...` | optimizer output persistido o `decision_log.layers` si alcanza | Comparar BL/fallback, pesos previos y pesos propuestos. |

## Criterios de validacion para fases futuras

- Cada card de EV/acierto/perdida evitable debe declarar poblacion, ventana,
  costos incluidos/excluidos y `n`.
- Las filas manuales deben venir de fills/movements reales, no de inferencias de
  UI.
- Las filas del bot deben venir de `execution_plan`/`decision_log` con
  `run_intent`, `decision_stage`, `metric_scope` e `is_primary_metric`.
- Shadow/sentiment/radar deben etiquetarse como evidencia o experimento si no
  forman parte de una decision ejecutable.
- Cualquier endpoint nuevo debe ser read-only y no relajar thresholds.
- Antes de cerrar una implementacion futura: `npm run lint`, `npm run build`,
  smoke de monitor y, si se toca backend, compile/test focal del modulo.
