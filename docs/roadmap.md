# Roadmap

Este roadmap prioriza mejoras compatibles con el sistema actual. No propone
reescribir el proyecto ni cambiar thresholds productivos sin evidencia.

## Proximas mejoras recomendadas

1. Normalizar lifecycle read-only.
   - Mantener `decision_log` como ledger historico.
   - Ampliar [src/analysis/decision_timeline.py](../src/analysis/decision_timeline.py)
     para reconstruir `market_observed`, `portfolio_observed`,
     `features_built`, `plan_created`, `fill_detected` y `outcome_updated`.
   - No requiere migracion destructiva.

2. Persistir entidad de plan/orden.
   - Crear `execution_plans` y `order_intents` cuando el contrato este probado.
   - Mantener compatibilidad con `decision_log.id`.
   - Prioridad alta porque hoy `OrderIntent` vive en memoria.

3. Versionar estrategia/planner/optimizer/risk.
   - Usar `DecisionRunContext` ya existente.
   - Registrar `strategy_version`, `planner_version`, `optimizer_version`,
     `model_version`, `prompt_version`.

4. Consolidar auditoria de evidence packets.
   - Reusar patron de shadow causal (`prompt_version`, `schema_version`,
     `input_fingerprint`) en sentiment y explicaciones live.
   - Mantener LLM audit-only.

5. Endurecer runbook operativo.
   - Agregar backup/restore Postgres.
   - Agregar smoke Docker documentado.
   - Agregar checklist de despliegue local/remoto.

6. Mejorar QA de integracion.
   - Smoke de `docker compose --profile localdb up`.
   - Tests de monitor API con aiohttp test client.
   - Validacion de comandos CLI `--help`.

## Deuda tecnica

- `decision_log` concentra demasiadas responsabilidades.
- JSONB `layers` necesita contratos versionados mas explicitos.
- Multiusuario existe parcialmente, pero requiere validacion end-to-end.
- Monitor UI estatica tiene mucho codigo en un unico HTML.
- Algunos docs historicos muestran problemas de encoding; conviene normalizar
  documentacion a UTF-8.
- `init.sql` contiene bloques/migraciones idempotentes duplicadas; conviene
  consolidar cuando haya ventana segura.

## Riesgos prioritarios

| Riesgo | Impacto | Mitigacion recomendada |
|---|---|---|
| Scraping Cocos cambia | Alto | Confidence audit, screenshots, tests de parsing, fallback manual. |
| Mezclar radar/shadow/debug con EV | Alto | Tests contractuales de `metric_scope` e `is_primary_metric`. |
| Plan inconsistente reportado | Alto | Mantener `validate_execution_plan()` como gate duro. |
| Fills no reconciliados | Medio/alto | Mejorar decision ledger y alertas de `/api/fills`. |
| LLM con claims sin evidencia | Medio | Evidence packets y validadores. |
| Exposicion remota insegura | Alto | Token/TOTP/firewall/Tailscale, no publicar sin hardening. |
| Muestra insuficiente | Medio | Viability/reportes con min sample y warnings explicitos. |

## Cambios de alto impacto

- Crear una vista/materializacion read-only de decision lifecycle.
- Separar plan y orden como entidades persistidas.
- Agregar contract tests para que radar/shadow nunca sean primary metrics.
- Agregar smoke Docker automatizado.
- Agregar backup/restore documentado y probado.
- Versionar prompts/modelos de sentiment/causal/synthesis.

## Cambios que no conviene hacer todavia

- No automatizar envio de ordenes hasta tener lifecycle, order IDs, reconciliacion
  y controles de riesgo mas cerrados.
- No relajar thresholds por calibracion negativa sin evidencia de EV neto y
  drawdown controlado.
- No fusionar shadow/radar con planner productivo sin muestra suficiente.
- No migrar `decision_log` destructivamente.
- No reemplazar optimizer/planner determinista por LLM.
- No convertir el monitor en servicio publico sin hardening y controles.

## Supuestos pendientes

- Pendiente de validar: volumen real y salud actual de DB.
- Pendiente de validar: ultima performance operativa.
- Pendiente de validar: tiempos reales de jobs y latencia de monitor.
- No confirmado en el repo: politica formal de backups.
- No confirmado en el repo: criterios de promocion de shadow a productivo.
