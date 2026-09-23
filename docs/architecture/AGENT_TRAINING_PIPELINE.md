# Quantia — Agent/JEV Training Pipeline (staged)

Estado: **PREPARADO / FAIL-CLOSED**. Este bloque es downstream de dos prerequisitos:

1. loop agéntico de Quantia integrado y con trazas persistidas (`agent_runs` + `agent_steps`);
2. JEV integrado y exponiendo el adapter mínimo de entrenamiento.

No debe ejecutarse como entrenamiento efectivo hasta que ambos estén presentes.

## Objetivo

Convertir evidencia prospectiva de Quantia en un dataset temporalmente correcto para evaluar y entrenar challengers sin permitir que un modelo aprendido reemplace controles deterministas.

Flujo:

`Agent trace -> JEV assessment -> matured Ledger outcome -> leakage checks -> temporal dataset -> challenger evaluation -> promotion gate -> human review`

El pipeline **no** coloca órdenes, no modifica pesos, no cambia thresholds y no promueve modelos automáticamente.

## Contrato requerido de JEV

El módulo configurado por `QUANTIA_JEV_MODULE` (default `src.agentic.jev`) debe exponer:

```python
JEV_CAPABILITY_VERSION = "<version>"

def export_training_assessments():
    ...
```

Cada assessment exportado debe poder normalizarse a:

- `run_id`
- `eligible`
- `evidence_complete`
- `label`
- `score`
- `jev_version` o `version`
- `decision_log_id` cuando exista asociación con Ledger
- `action`
- `risk_guard_violations`
- `pre_decision_features`: solamente información conocida al momento de decidir

El exporter puede internamente leer su propia persistencia. El training harness no depende del diseño interno de JEV.

## Dataset

La unión canónica es:

- `agent_runs` / `agent_steps`: proceso agéntico;
- JEV: evaluación de calidad del proceso/decisión;
- Decision Ledger: outcome maduro y auditado.

El dataset generado guarda el hash del objetivo y features estructuradas, pero **no** persiste el prompt/goal crudo ni las observaciones crudas de herramientas. Los outcomes (`5D/10D/20D`, alpha, MAE, etc.) son labels y no pueden aparecer entre las features de decisión.

El builder rechaza claves de features que parezcan contener información futura (`outcome`, `realized`, `future`, `return_5d`, `return_10d`, `return_20d`, etc.). La validación se separa cronológicamente; no hay split aleatorio que mezcle futuro con pasado.

## Preflight

Después de integrar ambas implementaciones:

```bash
python scripts/run_agent_training_pipeline.py --check
```

Si JEV vive en otro módulo:

```bash
python scripts/run_agent_training_pipeline.py --check --jev-module src.<ruta>.jev
```

El comando retorna exit code `2` si falta cualquier prerequisito.

## Ejecución

Los exports JSONL deben contener una fila JSON por línea:

```bash
python scripts/run_agent_training_pipeline.py \
  --agent-runs outputs/training_inputs/agent_runs.jsonl \
  --agent-steps outputs/training_inputs/agent_steps.jsonl \
  --jev-assessments outputs/training_inputs/jev_assessments.jsonl \
  --outcomes outputs/training_inputs/outcomes.jsonl \
  --output-dir outputs/agent_training/latest
```

Artefactos:

- `dataset.jsonl`
- `train.jsonl`
- `validation.jsonl`
- `manifest.json`
- `receipt.json`

## Champion / challenger

La capa de entrenamiento del modelo puede ser local u otro adapter, pero debe producir métricas comparables en holdout temporal. Para evaluar una propuesta:

```bash
python scripts/run_agent_training_pipeline.py \
  ...inputs... \
  --champion-metrics outputs/champion_metrics.json \
  --challenger-metrics outputs/challenger_metrics.json
```

Contrato mínimo de métricas:

```json
{
  "jev_mean_score": 0.0,
  "net_ev": 0.0,
  "max_drawdown": 0.0,
  "risk_guard_violations": 0,
  "data_quality_pass_rate": 1.0
}
```

El gate inicial exige muestra mínima, holdout temporal, cero violaciones de risk guards, calidad de datos y no degradación en JEV/EV/drawdown. Los umbrales iniciales están en `config/agent_training.json` y deben revisarse con evidencia antes de endurecerse o relajarse.

**Resultado máximo del gate:** `CHALLENGER_ELIGIBLE_FOR_REVIEW`. Nunca `PROMOTED`.

## Frontera de seguridad

Permanecen fuera del aprendizaje:

- optimizer determinista;
- risk guards;
- sizing/weight constraints;
- planner guards;
- ejecución de órdenes.

Un challenger sólo puede aprender/evaluar qué evidencia pedir, cómo ordenar candidatos o cómo mejorar el juicio agéntico. Cualquier cambio de cartera sigue pasando por los módulos deterministas de Quantia.

## Secuencia de integración

1. cerrar y congelar el loop agéntico;
2. integrar JEV y su adapter de entrenamiento;
3. rebasear/mergear esta rama sobre el estado que contiene ambos;
4. correr `--check`;
5. exportar trazas + assessments + outcomes maduros;
6. construir el dataset;
7. entrenar challenger fuera de producción;
8. comparar champion/challenger sobre holdout temporal;
9. si el gate pasa, dejar el challenger **sólo como elegible para revisión**;
10. promoción/deploy debe ser una acción separada y explícita.
