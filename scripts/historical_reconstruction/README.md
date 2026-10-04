# Historical Reconstruction v1

Auditor histórico **DRY-RUN / READ-ONLY** para Quantia.

Su objetivo no es “arreglar” filas legacy en PostgreSQL. Reconstruye qué evidencia
histórica puede defenderse, clasifica su calidad y recalcula outcomes desde precios
crudos. El histórico original queda intacto.

## Qué reconstruye

Flujo:

```text
execution_plans + order_intents
        ↓
owner / run / decision link / ticker / source
        ↓
HIGH / MEDIUM / LOW / UNRECOVERABLE
        ↓
market_candles + calendario BYMA + corporate events
        ↓
outcomes 5D / 10D / 20D / 40D
```

Reglas principales:

- **HIGH**: plan formal con owner explícito, linaje coherente y captura inmutable
  `decision_lab_plan_captures`.
- **MEDIUM**: plan formal con owner explícito y linaje coherente, pero sin una
  captura inmutable que pruebe la versión original.
- **LOW**: inferencias legacy controladas, filas mutables/sobrescritas,
  decisiones superseded o links reutilizados.
- **UNRECOVERABLE**: contradicción de owner, source, ticker o run, o evidencia
  que no puede vincularse de forma segura.

Sólo **HIGH + MEDIUM** entran en las métricas primarias. LOW se calcula y muestra
por separado. UNRECOVERABLE nunca se rellena por inferencia.

## Invariantes

- PostgreSQL se abre con `default_transaction_read_only=on`.
- La extracción usa `repeatable_read` + `readonly=True`.
- No ejecuta DDL, `INSERT`, `UPDATE`, `DELETE`, backfills ni `ensure_schema`.
- No consulta `outcome_5d`, `outcome_10d`, `outcome_20d`,
  `outcome_40d` ni `executable_outcome_*`.
- Radar y optimizer aparecen sólo como contexto agregado de `decision_log`;
  no entran en la muestra de planes formales.
- Los NULL owner sólo se asocian cuando **el único owner explícito en snapshots,
  decision_log, fills y planes es exactamente el owner configurado**. Aun así
  quedan LOW.
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

## Cómo interpretar el resultado

El JSON separa:

- `inventory`: inventario de planes, velas, capturas y contexto de cuenta.
- `source_separation`: Radar/optimizer/etc. sólo como contexto.
- `reconstruction.confidence_counts`: recuperabilidad del histórico.
- `reconstruction.reason_counts`: causas concretas de degradación.
- `outcomes.primary`: únicamente HIGH + MEDIUM.
- `outcomes.low`: legacy/inferencias, siempre separado.
- `legacy_comparison`: compara cobertura/linaje sin usar outcomes almacenados.

El reporte no demuestra un backtest point-in-time completo. La ausencia de
vintages históricas de configuración, macro, universo o corporate actions limita
lo que puede reconstruirse. Cuando la evidencia no alcanza, se conserva como
LOW o UNRECOVERABLE en lugar de inventarla.
