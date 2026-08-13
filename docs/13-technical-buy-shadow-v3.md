# Technical Buy Shadow V3

`technical-buy-shadow-v3` convierte la lectura técnica V2 en una herramienta
experimental de análisis de compras nuevas. No intenta decidir ventas: una vez
que el activo está en cartera, esa responsabilidad sigue en `/analisis`.

## Contrato

- Objetivo: descubrir y priorizar candidatos BUY para outcome a 20 días.
- Fuente: `technical-shadow-v2` con sesgo positivo y score >= 0.20.
- Nivel A: `STRONG_UPTREND` sin ruptura estructural.
- Nivel B: `RANGE` con reversión positiva.
- Nivel C: setup BUY todavía no confirmado.
- Rechazo: V2 sin BUY, ruptura estructural o `DOWNTREND`.
- Volumen incompleto, fuente mixta y CEDEAR/CCL son advertencias de calidad;
  no cambian por sí solas la clasificación.

El payload declara:

- `calibration_status=SHADOW_UNVALIDATED`
- `affects_radar_ranking=false`
- `affects_analysis=false`
- `affects_execution=false`

Radar lo persiste como `layers.technical_buy_shadow_v3` y lo muestra como
overlay. No modifica `final_score`, `_classify()`, conviction, sizing, planes ni
órdenes.

La captura productiva es prospectiva y diaria:

- `16:50 ART`: `run_radar_audit_capture` persiste la cohorte Radar.
- `21:30 ART`: `run_update_outcomes` madura outcomes cuando corresponde.
- `/api/radar-audit`: expone capturados V3, nivel A, maduros 20d, win rate y
  EV bruto/neto con costo supuesto de 0.75%.
- `/radar` y `/radar_full`: muestran el nivel V3, pero siguen siendo ideas
  teóricas; sólo `/analisis` puede producir un plan de cartera.

## Replay inicial read-only

Fuente: `technical_replay.csv`, 01/04/2026 al 12/08/2026. Los outcomes usan 20
días calendario y la primera vela disponible en o después de la fecha objetivo;
no equivalen a 20 ruedas exactas.

| Muestra | n | Win | EV bruto | EV neto* | Exceso vs universo |
|---|---:|---:|---:|---:|---:|
| V2 BUY total | 824 | 58.50% | +2.49% | +1.74% | +0.89% |
| V3 nivel A | 273 | 63.37% | +4.24% | +3.49% | +2.59% |
| V3 A antes de 01/07 | 229 | 61.14% | +4.31% | +3.56% | +2.73% |
| V3 A desde 01/07 | 44 | 75.00% | +3.86% | +3.11% | +1.88% |

\* EV neto teórico: retorno bruto menos 0.75% de costo total supuesto. No
incluye sizing, impuestos ni condiciones reales de ejecución.

El corte temporal comprueba estabilidad retrospectiva, pero no es out-of-sample
estricto porque la regla de nivel A se eligió después de inspeccionar el replay.
Por eso V3 debe acumular cohorts prospectivos antes de solicitar promoción.

## Auditoría reproducible

```bash
python scripts/run_technical_buy_shadow_v3_audit.py \
  --csv /ruta/technical_replay.csv \
  --cost-bps 75 \
  --split-date 2026-07-01
```

La auditoría deduplica el primer evento de cada episodio BUY por ticker, compara
contra la mediana del universo elegible del mismo día y reporta intervalo
bootstrap del EV bruto.

## Promoción

V3 no debe modificar Radar productivo hasta cumplir simultáneamente:

1. Outcomes prospectivos de al menos 100 episodios nivel A.
2. Al menos cinco cohorts temporales independientes.
3. EV neto positivo con IC95 inferior mayor que cero.
4. Exceso positivo contra universo contemporáneo.
5. Resultados separados por CEDEAR/acción y calidad de fuente.
6. Revisión humana explícita antes de cambiar ranking o thresholds.
