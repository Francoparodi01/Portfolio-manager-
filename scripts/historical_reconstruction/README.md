# Historical Reconstruction v1

Auditor histórico **DRY-RUN / READ-ONLY** para Quantia.

Su objetivo no es “arreglar” filas legacy en PostgreSQL. Reconstruye qué evidencia
histórica puede defenderse, clasifica su calidad y recalcula outcomes desde precios
crudos. El histórico original queda intacto.

## Fuente de verdad

Para reconstruir una **señal formal registrada**, la fuente primaria es:

```text
execution_plans + order_intents
```

`decision_log` es evidencia auxiliar/compatibilidad. Esto es importante porque el
histórico legacy reutilizó o sobrescribió links de `decision_log` entre corridas.
Un link cross-run/reutilizado se reporta como `BROKEN`, pero no destruye por sí
solo una señal que sigue completa en `execution_plans + order_intents`.

Flujo:

```text
execution_plans + order_intents
        ↓
owner del plan + identidad formal de la intención
        ↓
HIGH / MEDIUM / LOW / UNRECOVERABLE
        ↓
diagnóstico separado del decision_log link
(HEALTHY / BROKEN / MISSING)
        ↓
market_candles + calendario BYMA + corporate events
        ↓
outcomes 5D / 10D / 20D / 40D
```

Reglas principales:

- **HIGH**: existe una captura inmutable `decision_lab_plan_captures` del plan
  para el owner solicitado. Esa captura owner-scoped también puede probar el owner
  aunque la fila mutable legacy de `execution_plans` haya quedado en NULL.
- **MEDIUM**: el plan formal tiene owner explícito y la intención formal es
  autocontenida, pero no existe captura inmutable de esa versión histórica.
- **LOW**: la atribución del plan depende únicamente de la inferencia legacy NULL
  bajo verificación estricta de single-owner.
- **UNRECOVERABLE**: el plan formal mismo no puede atribuirse al owner solicitado
  o no constituye una señal formal evaluable.

Los defectos de `decision_log` (`cross-run`, ticker/source/owner contradictorio,
link reutilizado, superseded o faltante) se conservan como **diagnóstico de
linaje**. No se usan para decidir ticker, side ni outcome de la señal reconstruida.

Sólo **HIGH + MEDIUM** entran en las métricas primarias. LOW se calcula y muestra
por separado. UNRECOVERABLE nunca se rellena por inferencia.

## Invariantes

- PostgreSQL se abre con `default_transaction_read_only=on`.
- La extracción usa `repeatable_read` + `readonly=True`.
- No ejecuta DDL, `INSERT`, `UPDATE`, `DELETE`, backfills ni `ensure_schema`.
- No consulta `outcome_5d`, `outcome_10d`, `outcome_20d`,
  `outcome_40d` ni `executable_outcome_*`.
- Radar y optimizer aparecen sólo como contexto agregado de `decision_log`;
  no entran como fuente de la muestra de planes formales.
- Los NULL owner sin captura inmutable sólo se asocian cuando **el único owner
  explícito en snapshots, decision_log, fills y planes es exactamente el owner
  configurado**. Aun así quedan LOW.
- Los outcomes reutilizan el cálculo auditado de
  `scripts/bot_stats_dashboard/metrics.py`: sesiones BYMA fijas, una sola fuente
  de precios por horizonte, faltantes sin desplazamiento, corporate events y
  discontinuidades fail-closed.
- No calcula PnL realizado de cuenta.

## Ejecutar localmente

Desde la rama/worktree:

```powershell
python scripts/historical_reconstruction/run.py `
  --env-file ..\cocos_copilot\.env `
  --days 180 `
  --cost-bps 75
```

Genera:

```text
output/historical-reconstruction/historical-reconstruction-v1.json
output/historical-reconstruction/historical-reconstruction-v1.md
```

No copia ni modifica el `.env`: sólo lo lee.

## Tests

```powershell
python -m pytest scripts/historical_reconstruction -q
python -m pytest scripts/bot_stats_dashboard -q
git diff --check origin/main...HEAD
```

Después de cambios de reglas de linaje, volver a ejecutar tanto los tests como el
reporte contra la DB real antes de considerar el PR listo para merge.

## Cómo interpretar el resultado

El JSON separa:

- `inventory`: inventario de planes, velas, capturas y contexto de cuenta.
- `source_separation`: Radar/optimizer/etc. sólo como contexto.
- `reconstruction.confidence_counts`: recuperabilidad de las señales formales.
- `reconstruction.decision_link_status_counts`: salud del espejo legacy
  `decision_log` (`HEALTHY`, `BROKEN`, `MISSING`).
- `reconstruction.reason_counts`: causas concretas de degradación o warnings.
- `outcomes.primary`: únicamente HIGH + MEDIUM.
- `outcomes.low`: owner legacy inferido, siempre separado.
- `legacy_comparison`: compara cobertura/linaje sin usar outcomes almacenados.

El reporte no demuestra un backtest point-in-time completo. La ausencia de
vintages históricas de configuración, macro, universo o corporate actions limita
lo que puede reconstruirse. Cuando la identidad/ownership del plan no alcanza,
se conserva como LOW o UNRECOVERABLE en lugar de inventarla.
