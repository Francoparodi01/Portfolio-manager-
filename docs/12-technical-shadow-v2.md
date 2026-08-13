# Technical Shadow V2

`technical-shadow-v2` evalua una combinacion tecnica alternativa sin modificar
el score, las decisiones, el optimizer, los planes ni las ordenes productivas.

## Objetivo

Separar continuidad de tendencia y reversion segun el regimen, en vez de sumar
ambas estrategias dentro de un unico score aditivo.

## Reglas experimentales

| Regimen | Regla shadow |
|---|---|
| `STRONG_UPTREND` | Prioriza `trend_score`; una reversion negativa se registra como riesgo de extension y no como venta. |
| `RANGE` | Prioriza `reversion_score`. |
| `DOWNTREND` | Una lectura de sobreventa positiva no habilita sesgo alcista sin confirmacion. |
| `TRANSITIONAL` | Reduce la magnitud combinada por incertidumbre de regimen. |

Una ruptura estructural confirmada impide que el score shadow quede positivo.

## Persistencia y alcance

El payload se guarda como `layers.technical_shadow_v2` cuando el pipeline ya
persiste un evento auditable. Incluye version, regla, inputs, contribuciones,
sesgo, horizontes de evaluacion y estas garantias:

- `calibration_status=UNVALIDATED`
- `affects_analysis=false`
- `affects_execution=false`
- horizontes canonicos existentes: 5d, 10d, 20d y 40d

Las consultas `/ticker` calculan y muestran el shadow, pero siguen siendo
read-only y no crean filas en `decision_log`.

La comparacion se consulta sin escritura con:

```bash
python scripts/run_technical_shadow_audit.py --days 365
```

El auditor deduplica ticker por dia, reconstruye el retorno bruto cuando el
evento padre era `SELL` y aplica por separado la direccion del baseline y V2.
Los outcomes actuales usan umbrales de dias calendario y la primera vela
canonica disponible posterior; este documento no los presenta como ruedas.

## Criterios de promocion

No promover por win rate aislado ni por resultados repetidos del mismo ticker.
La comparacion debe ser walk-forward y usar episodios deduplicados por ticker.

Requisitos minimos antes de solicitar un cambio productivo:

1. Muestra fuera de entrenamiento con al menos 100 episodios y cobertura de los cuatro regimenes.
2. Rank IC positivo en al menos dos horizontes, sin depender de un unico ticker.
3. Retorno mediano y EV neto superiores al tecnico vigente.
4. MAE y drawdown no peores que el baseline.
5. Resultados separados por calidad de fuente y sin mezclar outcomes no canonicos.
6. Revision humana explicita antes de tocar scoring o thresholds.
