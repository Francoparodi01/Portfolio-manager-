# Historical Edge · META-D

## Objetivo

META-D prueba en `SHADOW_ONLY` la hipótesis de que Quantia puede mejorar la
calidad económica de sus señales priorizando patrones de decisiones que ya
mostraron resultados históricos favorables.

No modifica el Decision Engine, sizing, ejecución, órdenes ni capital.

## Fuente histórica

La fuente es `decision_log` para propuestas formales `execution_plan` del mismo
`owner_chat_id`.

Para evitar leakage, una observación histórica sólo es utilizable cuando:

- fue decidida antes del `as_of` de la nueva señal;
- tiene `outcome_20d`;
- `outcome_basis` comienza con `canonical_cocos`;
- `outcome_filled_at <= as_of`;
- el registro formal terminó `APPROVED` o `EXECUTED`.

Las decisiones bloqueadas se mantienen en el Learning Shadow separado y no se
mezclan con esta población.

## Unidad de análisis

La unidad es un **episodio direccional**, no cada recomendación diaria.
Repeticiones del mismo BUY/SELL sobre un ticker se deduplican de forma
conservadora. Un HOLD/no-formal registrado o un cambio BUY↔SELL abre un nuevo
episodio.

## Perfil histórico

Cada candidato se asigna de forma determinística a:

- dirección: BUY o SELL;
- bucket fijo de `abs(final_score)`: `<0.08`, `0.08–0.12`, `0.12–0.18`, `>=0.18`;
- `market_regime` registrado;
- horizonte primario 20D.

No se buscan cortes retrospectivos para maximizar PnL. Si la celda exacta tiene
poca muestra se usa un backoff preregistrado y en este orden:

1. acción + score + régimen;
2. acción + score;
3. acción + régimen;
4. acción.

## Métricas

El retorno histórico es el retorno direccional canónico menos un costo research
conservador. Por defecto el watcher usa el mayor entre 150 bps y el costo
estimado de la corrida actual.

Se guardan:

- episodios (`n_episodes`);
- fechas (`n_dates`);
- win rate neto;
- retorno bruto medio;
- EV neto medio;
- mediana neta;
- profit factor;
- concentración top-1 y top-3 de ganancias;
- calidad y razones del gate.

`outcome_20d` **no es** un contrafactual de cartera HOLD. Por eso META-D no lo
presenta como DVA vs HOLD. Ese contraste queda para Decision Lab.

## Gate META-D v1

META-D requiere simultáneamente:

- `n_episodes >= 20`;
- `n_dates >= 8`;
- win rate neto `>= 55%`;
- EV neto `>= +25 bps`;
- profit factor `>= 1.10`;
- concentración top-1 `<= 40%`;
- concentración top-3 `<= 75%`;
- score mínimo original: BUY `0.08`, SELL `0.08`;
- costos y turnover dentro de los guards existentes.

Todo resultado sigue teniendo `capital_effect=false`.

## Lectura en Telegram

`/meta` muestra A/B/C/D. Cuando la evidencia proviene del watcher DB también
muestra una línea como:

```text
Hist20D · ACTION/SCORE/REGIME · n 24 / 11 fechas · win 70,8% · EV net +3,4% · PF 1,72 · MEDIUM
```

Esta línea es evidencia histórica descriptiva para el challenger. No habilita
producción automáticamente.

## Promoción futura

META-D sólo debería pasar a afectar decisiones después de una evaluación
prospectiva suficiente y comparación contra la política actual en EV neto, PnL,
drawdown, turnover y estabilidad temporal. El win rate aislado nunca alcanza
como criterio de promoción.
