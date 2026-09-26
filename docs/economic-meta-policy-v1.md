# Economic Meta Policy v1 — preregistro shadow

## Estado

- Base histórica original: `feature/decision-lab-pit-replay` @ `2096cdd42e237f90bd93f17cbc1eae288b824c3d`.
- Modo: `SHADOW_ONLY`.
- Efecto sobre capital: **ninguno**.
- Horizonte económico primario: **20 sesiones**.
- Horizontes secundarios/guardrails: **5D, 10D y 40D**.
- Estado de calibración: `PREREGISTERED_SHADOW_UNVALIDATED`.

## Objetivo

Quantia conserva el Decision Engine vigente como generador de candidatos. Economic Meta Policy v1 es un meta-labeler separado que pregunta si una señal candidata merece ser observada como `ALLOW_SHADOW` o si el challenger la convertiría en `REJECT_TO_HOLD`.

La política v1 **no** reemplaza recomendaciones, no cambia sizing, no crea órdenes y no modifica ninguna ruta de ejecución. Su único propósito es acumular evidencia prospectiva point-in-time sobre si operar menos y de forma más selectiva mejora el resultado económico.

## Motivación y límites de la evidencia histórica

Analytics v2 observacional sugirió que el edge del BOT no es homogéneo por horizonte ni por dirección y que 20D merece ser el endpoint primario de investigación. Ese hallazgo sirve para formular la hipótesis, no para seleccionar retrospectivamente el mejor threshold.

Decision Lab reciente permite comparar PLAN contra HOLD, pero la reconstrucción histórica todavía tiene cobertura primaria insuficiente y calidad limitada para usarla como calibración confirmatoria. Por eso los thresholds base de A/B/C son deliberadamente simples y quedan congelados antes del forward test.

META-D agrega otra hipótesis en shadow: una señal puede ganar evidencia si pertenece a un patrón histórico point-in-time que ya mostró suficiente muestra, win rate y retorno neto. Los buckets y el orden de backoff de META-D son fijos; no se buscan retrospectivamente combinaciones que maximicen PnL.

## Challengers congelados

### META-A — confidence gate común

- BUY: `abs(score) >= 0.10`
- SELL/REDUCE: `abs(score) >= 0.10`
- Edge explícito vs HOLD: no obligatorio.

Sirve como control para responder si un filtro mínimo de convicción mejora al CURRENT.

### META-B — gate asimétrico

- BUY: `abs(score) >= 0.15`
- SELL/REDUCE: `abs(score) >= 0.10`
- Edge explícito vs HOLD: no obligatorio.

Prueba la hipótesis de que BUY y SELL no deberían compartir el mismo umbral.

### META-C — defensive extreme

- BUY: `abs(score) >= 0.20`
- SELL/REDUCE: `abs(score) >= 0.12`
- Requiere evidencia explícita de edge vs HOLD.
- El edge debe superar `estimated_cost_bps + uncertainty_bps + 25 bps`.

META-C es un challenger deliberadamente restrictivo. No constituye recomendación de producción.

### META-D — historical edge

- BUY: `abs(score) >= 0.08`
- SELL/REDUCE: `abs(score) >= 0.08`
- Requiere un match histórico point-in-time de 20D.
- Unidad: episodios direccionales deduplicados, no cada recomendación repetida.
- Muestra mínima: `20` episodios y `8` fechas.
- Win rate neto mínimo: `55%`.
- EV neto mínimo: `+25 bps`.
- Mediana neta: positiva.
- Profit factor mínimo: `1.10`.
- Concentración positiva máxima: Top1 `40%`, Top3 `75%`.
- Costo histórico: el mayor entre `150 bps` y el costo estimado de la corrida actual.

El matcher usa buckets fijos de `abs(final_score)` (`<0.08`, `0.08–0.12`, `0.12–0.18`, `>=0.18`) y un backoff fijo: acción+score+régimen → acción+score → acción+régimen → acción. No elige el bucket que haya rendido mejor.

`outcome_20d` es retorno **direccional** canónico. No es un contrafactual de cartera HOLD, por lo que META-D no lo presenta como DVA vs HOLD. El detalle metodológico está en `docs/historical-edge-meta-d.md`.

## Gates comunes

- `max_estimated_cost_bps = 250`
- `max_portfolio_turnover = 0.35`
- Acción válida: BUY, SELL, REDUCE o HOLD.
- Datos inválidos o incompletos: fail-closed a `REJECT_TO_HOLD`.
- HOLD de origen nunca puede convertirse en trade.

## Evidencia persistida

Cada decisión shadow registra, como mínimo:

- `run_id`
- `as_of`
- `ticker`
- `candidate_action`
- `candidate_score` y `abs_score`
- `policy_name` / `policy_version`
- `decision`
- `rejection_reason`
- `expected_horizon_days`
- `estimated_cost_bps`
- `portfolio_turnover`
- `market_regime`
- `expected_edge_vs_hold_bps`
- `edge_uncertainty_bps`
- `opportunity_id`
- para META-D, snapshot de `historical_edge` dentro de metadata
- flags explícitos de `shadow_only` y `capital_effect=false`

La persistencia de referencia es JSONL append-only en `outputs/economic_meta_policy/shadow_decisions.jsonl`. Está aislada de tablas o rutas de ejecución de producción.

## Evaluación prospectiva

Para cada challenger deben madurar las mismas oportunidades a:

- 5D: guardrail corto.
- 10D: señal temprana.
- **20D: endpoint primario**.
- 40D: persistencia/decay.

Comparadores obligatorios:

1. `CURRENT`: política actual de Quantia.
2. `HOLD`: no modificar la cartera.

Métricas a registrar:

- PnL o retorno neto.
- DVA vs HOLD.
- Delta vs CURRENT.
- Profit factor.
- Turnover.
- Cost drag.
- Tail loss p10.
- Proxy de drawdown de investigación, claramente separado de drawdown NAV ejecutado.
- `n` y `n_effective` con ventanas no solapadas.
- concentración Top1 / Top3 de contribuciones positivas.

## Gates de promoción

Economic Meta Policy v1 **nunca promociona automáticamente**. Incluso si un challenger supera todos los gates cuantitativos, queda bloqueado hasta una revisión manual y una nueva versión explícita.

Antes de considerar cualquier efecto sobre capital se requiere, como mínimo:

1. DVA 20D positivo contra HOLD.
2. Delta 20D positivo contra CURRENT.
3. Muestra efectiva suficiente; el evaluador usa `n_effective >= 20` como gate inicial de research, no como garantía estadística.
4. No deterioro material de drawdown/riesgo de cola.
5. No deterioro material de cost drag.
6. Evidencia prospectiva point-in-time reproducible.
7. Revisión manual del preregistro, sesgos y estabilidad por régimen.
8. Nueva versión explícita; v1 permanece shadow para siempre.

## Runner

Ejemplo con un candidato por stdin:

```bash
printf '%s\n' '{"ticker":"NVDA","candidate_action":"SELL","final_score":-0.14,"as_of":"2026-09-24T18:00:00Z","estimated_cost_bps":75,"portfolio_turnover":0.12,"market_regime":"TRANSITIONAL"}' \
  | python scripts/run_economic_meta_shadow.py --run-id research-20260924
```

El runner emite cuatro registros —META-A, META-B, META-C y META-D— y persiste únicamente evidencia shadow. META-C rechazará el ejemplo mientras no exista una estimación explícita de edge vs HOLD. META-D rechazará una entrada manual que no incluya evidencia histórica estructurada; el watcher automático de base de datos es quien construye ese match point-in-time.

## No objetivos

Esta versión no:

- modifica Decision Engine;
- cambia recomendaciones Telegram;
- cambia execution planner;
- cambia sizing;
- ejecuta operaciones;
- toca cuentas Cocos;
- elige thresholds por mejor resultado histórico;
- declara que SELL tiene edge confirmado;
- convierte Analytics v2 observacional en evidencia causal;
- habilita capital por haber pasado tests de software.

El éxito de esta etapa es operacional: capturar una cohorte prospectiva, auditable y comparable entre CURRENT, META-A/B/C/D y HOLD.
