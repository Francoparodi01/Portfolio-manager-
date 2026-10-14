# Quantia — informe final de auditoría y QEEF v1

**Veredicto: EXPERIMENTAL.** La implementación research funciona y pasó 140 pruebas relevantes. La evidencia histórica accesible no permite demostrar que las decisiones de Quantia mejoren el capital del portfolio después de costos y frente a alternativas factibles. No se afirma ni rentabilidad ni ausencia de valor.

## A. Diagnóstico

El sistema anterior combina piezas razonables con defectos metodológicos concretos y validación económica insuficiente. Decision Lab ya ofrece un núcleo PLAN/HOLD defendible y Analytics v2 separa episodios de PnL reconciliado. El problema no es que todo esté mal concebido: es interpretar métricas de señales como valor de decisiones/capital, más fallas específicas de intervalo, drawdown y partición de cohortes.

Hoy Quantia registra y explica señales, planes y evidencia con mejor linaje prospectivo; todavía no demuestra beneficio económico incremental de forma verificable. QEEF agrega contratos y controles para poder evaluarlo, pero la nueva instrumentación no crea evidencia histórica que nunca se capturó.

## B. Problemas encontrados

1. **Neto real no identificable:** 109 fills exact-owner, 0 con fees conocidas; sin NAV/flujos/componentes reconciliados que sustenten PnL/alpha.
2. **Defecto comprobado:** drawdown de secuencias de señales en viability sin cartera/exposición común; ventanas solapadas impiden interpretarlo como riesgo de cuenta.
3. **Defecto comprobado:** bootstrap DCL con n=1 produce intervalo `[x,x]`.
4. **Defecto comprobado:** constructor de episodios Analytics v2 omite cohorte en partición; test demuestra posible colisión. Frecuencia operativa no demostrada.
5. **Riesgos de evidencia:** calendarios vs sesiones, recurrencias sin tesis canónica, ratios/acciones corporativas y precios ejecutables incompletos, vínculos plan-ejecución incompletos.
6. **Hipótesis no verificadas:** alpha, contribución causal de componentes, sesgo de supervivencia/revisión fundamental generalizado y superioridad fuera de muestra.

Clasificación, archivos/funciones y matriz source→signal→decision→plan→execution→outcome→metric: [ECONOMIC_AUDIT_FINDINGS.md](ECONOMIC_AUDIT_FINDINGS.md).

## C. Cambios implementados

Paquete `src/analysis/qeef/`: contratos inmutables, lifecycle de tesis, adaptador histórico, guardas contrafactuales, seis dimensiones separadas, métricas y confianza, servicio JSON/Markdown y store opt-in. Reutiliza simulación Decision Lab, contabilidad/riesgo Analytics v2 y bootstrap existente.

Alternativas: PLAN, NO_TRADE, HOLD_EXISTING, CASH de portfolio completo y benchmarks registrados REBALANCE_BASELINE/ELIGIBLE_ALTERNATIVE. Objetivos congelados, eventos y censura; sin scores opacos ni promoción. Comparación de asignaciones, ejecución parcial, universo de detección y contrastes de componentes disponibles mediante APIs con requisitos explícitos.

Se agregaron captura read-only, CLI offline, probes empíricos, una migración aditiva `qeef_artifacts`, tests y ruta autenticada de lectura del monitor. No se modificaron las fórmulas históricas ni sus consumidores originales: las tres fallas legacy están reproducidas y QEEF evita sus interpretaciones inválidas; no se presenta un arreglo global del software anterior.

Arquitectura y límites de integración: [QEEF_ARCHITECTURE.md](QEEF_ARCHITECTURE.md). Fórmulas, unidades, costos y estados: [QEEF_METRICS_SPEC.md](QEEF_METRICS_SPEC.md).

## D. Validación real

Captura congelada al 2026-10-14 04:53:59 UTC, owner exacto, repeatable-read y sólo lectura. **301 casos**: 278 decision_log + 23 HOLD; 72 tickers; 254 BUY, 24 SELL, 23 HOLD, manteniendo scopes separados. Se preservaron 30 episodios legacy LOW adicionales como población distinta, no sumados a decisiones ni promovidos.

Resultado: 143 proxies de recurrencia, **0 oportunidades canónicas y 0 pares económicos certificados por este protocolo/captura**. El objetivo de 30 oportunidades defendibles está bloqueado, no completado. Los 301 casos incluyen evidencia inicial, acción, enlaces exactos disponibles, outcomes originales y razones de faltantes. No se rellenan tesis/portfolio/alternativas faltantes con inferencias de alta confianza.

Las 28.615 nuevas barras tienen reloj de disponibilidad; todas carecen de ratio, versión de calendario y unidad de volumen explícitos para el contrato estricto. Hay 2 capturas FULL_CONTEXT de 32; 0 atribuciones plan-ejecución exact-owner. Estos controles no concluyen que no existan casos válidos en fuentes externas aún no incorporadas.

**140 passed; 0 failures/errors/skips.** Incluye 46 pruebas QEEF (45 unitarias/parametrizadas + 1 integración PostgreSQL) y 94 de regresión. Migración doble, aislamiento bajo rol no superuser, idempotencia y rollback por permisos se ejecutaron en PostgreSQL descartable. Dos corridas de auditoría producen el mismo ID `5c3aa7a82b546986768cdc7418c057b84dcb67f964c1ef82609e8a08244507d8`.

Poblaciones, números completos, mapa de los 22 escenarios requeridos y bloqueos: [QEEF_EMPIRICAL_VALIDATION.md](QEEF_EMPIRICAL_VALIDATION.md). Evidencia agregada y hashes: [validation.json](evidence/validation.json). La captura y casos privados están conservados localmente en `outputs/qeef/`, fuera de Git.

## E. Comparación metodológica y económica

Se mantuvieron las mismas filas/outcomes legacy, sus denominadores y definición temporal. QEEF no cambia la media histórica para aparentar mejora: agrega identificación del estimando, razones de no identificabilidad y límites de inferencia. La prueba singleton cambia un CI degenerado por ausencia justificada; drawdown de señales deja de publicarse como cuenta; cohortes incompatibles se rechazan.

Los 30 episodios legacy conservan deltas PLAN−HOLD registrados de −0,2760 pp (24/30 a5 sesiones), −0,3278 pp (21/30 a10) y −0,6531 pp (11/30 a20). Son resultados exploratorios almacenados, no repricing independiente ni pérdidas reales. La media QEEF verificada permanece `null`.

La mejora demostrada es metodológica y técnica. **No se demostró mejora económica, alpha ni reducción de pérdidas de portfolio.** Los números previos 874/626, 50,3%, −0,75% y +5,2% no se revalidaron con sus manifiestos originales; no se comparan con este censo.

## F. Estado operativo y pendientes

Funciona localmente: auditoría histórica offline, reportes deterministas, simulación con inputs completos, lifecycle/API de métricas, persistencia opcional y lectura de monitor implementada. Todo QEEF declara `RESEARCH_ONLY`, `affects_analysis=false`, `affects_execution=false`, `promotion_allowed=false`.

No se desplegó la ruta, no se aplicó migración operativa, no se activó colector de tesis ni se alteró optimizer/planner/política/broker. No hubo órdenes, cambios de balances/posiciones, push, merge o mensajes Telegram. La nueva pantalla de dashboard está especificada como cuatro niveles; la integración implementada es JSON/Markdown sobre la infraestructura existente.

Pendiente por evidencia: tesis prospectivas y universos congelados, enlaces completos, precios/corporate actions verificables, fees y flujos/NAV reconciliados, alternativas registradas y muestra madura/OOS. Pendiente de integración separada: ingesta prospectiva de esos contratos, carga de dimensiones adicionales por CLI, presentación de los paneles existentes y cualquier rollout autorizado. Ninguno de estos puntos se marca como validado.

Reproducción, compatibilidad y rollback preservando historia: [QEEF_MIGRATION_AND_ROLLBACK.md](QEEF_MIGRATION_AND_ROLLBACK.md).

## G. Veredicto y aceptación

**EXPERIMENTAL** es la clasificación integral: hay una base funcional y controles comprobados, pero no evaluación económica defendible de 30 oportunidades ni validación prospectiva. `BLOCKED_BY_EVIDENCE` describe específicamente la validación económica; no significa que la auditoría o implementación no se hayan ejecutado.

| Criterio solicitado | Estado y evidencia |
|---|---|
| Inspeccionar sistema, esquema, historia y PRs | COMPLETADO dentro del alcance accesible; inventario/PR13/PR19 y captura real |
| Identificar definiciones y probar defectos demostrables | COMPLETADO; fórmulas, tres reproducciones, taxonomía y límites |
| Metodología económica coherente | IMPLEMENTADA EN RESEARCH; requisitos y fórmulas explícitos, límites declarados |
| Componentes funcionales y reutilización | COMPLETADO para v1; CLI/servicio/APIs/store/monitor, sin colector o frontend nuevo |
| Preservar historia y separar predicción/decisión/asignación/ejecución | COMPLETADO en nuevo framework; sin reescritura de fuentes |
| Contrafactuales defendibles | MOTOR Y GUARDAS VERIFICADOS; aplicación histórica certificada BLOQUEADA por inputs |
| Horizontes y lifecycle | IMPLEMENTADOS; 5/10/20 conservados con unidades, objetivos inmutables y censura |
| ≥30 oportunidades reales defendibles | NO CUMPLIDO; 301 candidatos y 30 episodios LOW no satisfacen el contrato |
| Tests relevantes | COMPLETADO; 140 ejecutados, no equivale a ventaja económica |
| Comparación OLD vs NEW | COMPLETADA metodológicamente y por mismos registros; superioridad económica NO DEMOSTRADA |
| Atribución causal/OOS | NO IDENTIFICABLE con esta evidencia; API de contrastes limitada, sin porcentajes |
| Documentación, hashes y reproducción | COMPLETADO; seis documentos y agregado verificable, raw privado preservado |
| No alterar operación ni activar experimental | CUMPLIDO; sólo lectura operativa y recursos locales/descartables |

La respuesta a la pregunta del proyecto es: **se construyó una base capaz de exigir evidencia de valor incremental; todavía no se dispone de la evidencia que permita demostrarlo para Quantia**. Esa distinción es el resultado de la auditoría, no un PASS económico.

## H. Git y archivos

Rama: `feature/economic-audit-reengineering-v1`.
Base: `f2e08096a2a0b6254598243600f522b8608276d5`.
Commit de implementación: `d3cca84c06f631773810b4aec5077c35f157aafb` (`feat(qeef): add research-only economic evaluation framework`).
Este informe/evidencia se entrega en un commit documental posterior, identificable en `git log -2`; su SHA final se comunica en la entrega para evitar autorreferencia del commit.

Archivos de implementación (17): `.gitignore`; `migrations/20261014_qeef_v1.sql`; `scripts/capture_qeef.py`, `scripts/run_qeef.py`, `scripts/validate_qeef_empirical.py`; `src/analysis/qeef/__init__.py`, `contracts.py`, `counterfactuals.py`, `historical.py`, `lifecycle.py`, `metrics.py`, `service.py`, `store.py`; `src/monitor/api.py`, `src/monitor/qeef.py`; `tests/test_qeef.py`, `tests/test_qeef_postgres.py`.

Documentación/evidencia (7): los seis documentos de `docs/qeef/` y `docs/qeef/evidence/validation.json`. Los outputs privados y JUnit no se versionan. La comprobación final de working tree se realiza tras el commit documental y se informa en la entrega; no se asume limpio antes de ejecutar esa comprobación.
