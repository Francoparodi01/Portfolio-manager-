# Auditoria de resultados shadow - 2026-08-09

## Alcance y metodo

- Repo: `cocos_copilot`, commit base `11872b8`.
- Base viva consultada desde `scheduler` mediante `asyncpg`.
- Corte de forecasts: 2026-08-07; la auditoria se ejecuto el domingo 2026-08-09.
- Las metricas de forecast excluyen retornos absolutos mayores a 100% como
  quiebres de base de precio. Las filas se conservan para auditoria.
- No se modificaron scoring, thresholds, optimizer, planner, decision_log,
  forecasts historicos ni outcomes.

## Forecast cuantitativo 5/20/40

Volumen: 34 corridas, 25.590 forecasts y 6.641 outcomes persistidos antes del
filtro de calidad.

| Horizonte | Outcomes validos | Excluidos | Acierto direccion | Retorno medio | MAE |
|---|---:|---:|---:|---:|---:|
| 5 ruedas | 3.763 | 11 | 49,6% | +0,40% | 5,32% |
| 20 ruedas | 2.863 | 4 | 47,8% | +2,12% | 10,64% |
| 40 ruedas | 0 | 0 | Sin muestra | Sin muestra | Sin muestra |

Los 15 outcomes excluidos corresponden a `BAYN`, `C.I.`, `DD` y `FNMA` y
presentan saltos incompatibles con una base de precio comparable.

### Calibracion del modelo vigente

En `price_trend_context_overlay_v2`, la probabilidad no ordena correctamente el
resultado:

- 5 ruedas: bucket medio 35,5% predicho -> 74,5% subas observadas; bucket 66,1%
  predicho -> 44,4% observadas.
- 20 ruedas: bucket 34,6% predicho -> 78,0% subas observadas; bucket 83,0%
  predicho -> 17,0% observadas.
- Acierto del modelo vigente: 47,2% a 5 ruedas y 44,6% a 20 ruedas.

Conclusion: el forecast no demuestra edge direccional y esta descalibrado. No
debe promoverse ni usarse como recomendacion productiva.

### Cobertura

La mediana historica es 259 activos por corrida; la ultima corrida declaro 173
y persistio 170 tickers. La caida contra la mediana es material y debe
monitorearse despues del cambio de cadencia de scraping.

## Learning shadow v2

La ultima corrida fue `COMPLETE`: 356 decisiones vistas y 1.424 casos
actualizados. La poblacion primaria tiene 293 casos `PLANNER_BLOCKED`.

| Horizonte | Maduros | Clean misses | Tasa | Alpha medio vs benchmark |
|---|---:|---:|---:|---:|
| 5 dias | 241 | 77 | 32,0% | -1,65% |
| 10 dias | 216 | 69 | 31,9% | -3,59% |
| 20 dias | 177 | 39 | 22,0% | -6,61% |
| 40 dias | 98 | 9 | 9,2% | -10,56% |

Hay cuatro reglas candidatas `PROPOSED` (`FUNDING`, `MIN_TRADE_OR_NOMINAL`,
`MIN_WEIGHT_DELTA`, `SCORE_GUARD`), pero todas tienen alpha medio negativo. La
cobertura de forecasts sobre `PLANNER_BLOCKED` es 63,5%; sobre `RADAR_BLOCKED`
es 0%.

Conclusion: learning-shadow detecta casos que subieron despues de ser
bloqueados, pero no prueba que relajar los bloqueos mejore al benchmark o al
control. Ninguna regla esta lista para promocion.

## Trend y reversion shadow

`trend_shadow` tiene 192 decisiones etiquetadas. A 5 dias:

- `STRONG_UPTREND`: n=54 maduros, 38,9% positivos, retorno medio -2,08%.
- `TRANSITIONAL`: n=47, 68,1% positivos, retorno medio +1,99%.
- `RANGE`: n=45, 51,1% positivos, retorno medio -0,28%.

La taxonomia no ordena los outcomes en la direccion esperada.

`reversion_shadow` tiene 104 filas. Los scores positivos muestran 63-64% de
outcomes positivos y retornos medios de +1,1% a +1,5%; los scores negativos
muestran 33% y retornos medios de -1,1% a -2,6%.

Conclusion: reversion es la unica separacion prometedora, pero proviene de una
muestra condicionada por decisiones ya generadas. Requiere una cohorte shadow
independiente antes de tocar produccion.

## Otros shadows

- Causal/Ollama: tabla instalada, 0 analisis persistidos. Sin resultados.
- Earnings window: 4 casos `PRE_EARNINGS_WINDOW`, todos audit-only y sin
  outcomes 5D maduros.
- Issuer events: 14 observaciones; sirven como contexto, no tienen evaluacion
  causal propia todavia.

## Estado de promocion

| Capa | Estado |
|---|---|
| Forecast 5/20/40 | No promover; recalibrar |
| Learning shadow v2 | Mantener audit-only |
| Trend shadow | No promover |
| Reversion shadow | Investigar en cohorte independiente |
| Earnings shadow | Muestra insuficiente |
| Causal/Ollama | Inactivo, sin evidencia |
| Intraday range shadow | Nuevo; acumular 60-90 ruedas |
