# Radar Discovery Ledger

## Alcance

Captura prospectiva y audit-only del universo completo evaluado por Radar. No
modifica scoring, thresholds, optimizer, `decision_log`, planes ni ordenes. El
job operativo conserva su corte `--top 6`. Telegram puede emitir alertas shadow
desde esta evidencia, pero esas alertas no se convierten en decisiones o fills.

La version v2 agrega una hipotesis tecnica separada del ranking productivo:

- `trend_component_score` (0-25): alineacion y pendiente de medias, ADX,
  estructura de maximos/minimos y extension;
- `relative_strength_component_score` (0-25): retornos 20/60, percentil dentro
  del universo comparable y exceso local contra QQQ/SPY;
- `compression_component_score` (0-25): percentiles historicos de BB Width y
  ATR, contraccion de rango y, solo con volumen suficiente, contraccion de
  volumen;
- `setup_component_score` (0-25): distancia al trigger, base, invalidacion y
  R/R teorico.

Los agregados `discovery_score` y `setup_score` valen 0-50. El compuesto
0-100 se persiste para auditarlo, no para presentarlo como probabilidad ni
para ordenar candidatos. Tambien se guarda cada feature cruda para poder
explicar y recalibrar el calculo sin reinterpretar la evidencia pasada.

La captura se activa con `RADAR_DISCOVERY_LEDGER_ENABLED=true`. El valor por
defecto es `false` hasta validar costo y persistencia en un ambiente inferior.
Ademas requiere `--capture-discovery`, que agrega el scheduler de 16:50; el
flag de entorno por si solo no convierte una ejecucion manual comun en evidencia
prospectiva.

El Ledger puede completar exclusivamente el volumen shadow con
`TRADINGVIEW_BYMA` cuando la vela canónica no trae volumen. Ese overlay no
reemplaza OHLC: exige la misma rueda, volumen positivo y una diferencia entre
cierres no mayor a 5%. El origen y la cantidad de filas completadas quedan
congelados en `setup_features`; el overlay no se entrega al screener ni al
ranking operativo.

## Fuentes reutilizadas

- `market_candles`: fuente canonica para outcomes por ruedas y drawdown.
- `mature_forecast()`: convencion existente de N sesiones posteriores, no N
  dias calendario.
- normalizacion de corporate actions: rebasa el precio de referencia y las
  velas con las mismas funciones usadas por thesis shadow.
- V2 y V3 actuales: se guardan tal como fueron calculados, sin promoverlos ni
  incorporarlos al ranking productivo.

QQQ y SPY se guardan como controles para resolver sus retornos, pero conservan
el comportamiento vigente del Radar: no compiten como candidatos. El ranking
extendido incluye todo ticker que paso screener y alcanzo scoring; una fila sin
datos suficientes conserva ranking nulo y el motivo exacto de descarte. El
`v3_score` es el `source_score` de V3, heredado de technical-shadow-v2.

No se reutiliza `shadow_thesis_outcomes`: sus filas pertenecen a forecasts de
precio y no identifican una observacion diaria del universo Radar.

## Tablas nuevas

- `radar_discovery_runs`: una cohorte inmutable por usuario, rueda y
  `scoring_version`.
- `radar_discovery_snapshots`: una fila por ticker, incluidos cartera,
  descartados y benchmarks.
- `radar_discovery_outcomes`: retornos 5/10/20/40 ruedas, benchmarks, excesos y
  drawdown intraperiodo medido entre cierres contra el maximo previo.
- `radar_setup_events`: primer trigger, invalidacion, ambiguedad intradiaria o
  vencimiento observado dentro de 10 ruedas desde cada snapshot.
- `radar_setup_outcomes`: retornos 5/10/20/40 anclados al trigger, separados de
  los retornos desde descubrimiento.
- `radar_setup_alerts`: cruce intradia observado, entrega Telegram y accion
  humana `FOLLOW`/`DISMISS`; se mantiene fuera de `decision_log`.

`scoring_version` incluye las versiones V2/V3 y un fingerprint del codigo que
produce tecnico, screener, clasificacion y ranking. Tambien congela periodo,
umbrales del job, top N y uso de sentiment. Las consultas exigen una version
exacta y un usuario; nunca agregan versiones ni usuarios distintos.

## Operacion

La corrida de 16:50 sigue generando el mismo Radar operativo. Con el flag
activo ejecuta despues una segunda evaluacion aislada con cartera incluida y
persiste la cohorte completa bajo el mismo `run_id`. Una repeticion en la misma
rueda no reemplaza evidencia ya capturada.

El job nocturno de outcomes resuelve las observaciones maduras desde
`market_candles`. Las consultas son read-only:

```powershell
python scripts/query_radar_discovery.py --horizon 20 --scoring-version latest
python scripts/query_radar_discovery.py --anchor trigger --horizon 20 --scoring-version latest
python scripts/query_radar_discovery.py --horizon 20 --scoring-version latest --json
python scripts/run_radar_metrics.py
```

El reporte compara top 5, elegibles, V3 A, V3 rechazados, top N seleccionado y
posiciones propias. El retorno neto resta un costo teorico configurable (75 bps
por defecto). Las referencias con mas de cuatro dias calendario de atraso se
conservan pero quedan fuera de comparaciones y benchmarks por defecto. El IC
Spearman se calcula transversalmente por rueda y se informa solo como diagnostico
secundario.

La consulta `--anchor discovery` responde si el Radar descubrio mejores activos.
La consulta `--anchor trigger` responde si esperar la configuracion mejoro la
entrada. Un ticker que toca trigger e invalidacion en la misma vela diaria se
marca `AMBIGUOUS_SAME_SESSION` y no entra en outcomes de trigger, porque con
datos diarios no puede conocerse el orden intradiario.

## Alertas intradia shadow

`RADAR_INTRADAY_SETUP_ALERTS_ENABLED=true` habilita un watcher despues de cada
refresco exitoso de mercado de 10:40, 12:00 y 16:40. No abre otra sesion Cocos:
lee el ultimo snapshot congelado y el precio fresco ya guardado en
`market_prices`.

La version inicial alerta solamente CEDEAR fuera de cartera que:

- estaban `PRE_BREAKOUT` en el ultimo snapshot;
- pertenecen al quintil superior de `setup_percentile`;
- tienen R/R teorico de al menos 2x;
- cruzaron el trigger sin excederlo mas de 6%;
- conservan precio fresco, calidad `GOOD`/`PARTIAL` y no tienen un evento manual
  o advertencia de corporate action bloqueante.
- siguen fuera de la cartera en el ultimo snapshot disponible al momento de
  enviar la alerta y la cohorte Radar no tiene mas de siete dias calendario.

Se envia una sola alerta por ticker durante un cooldown de 14 dias y como maximo
tres por refresco. El mensaje ofrece `Ver analisis`, `Seguir` y `Descartar`.
`Seguir` registra interes shadow, no una compra. `Descartar` silencia la
interaccion humana, pero conserva el trigger y su outcome teorico. El cruce
intradia se ancla al precio observado y declara `price_only`: no inventa una
confirmacion de volumen. A las 16:40 el texto se presenta como validacion para
la proxima rueda, no como invitacion a perseguir el cierre.
La primera accion `Seguir`/`Descartar` queda congelada; un segundo boton no
reescribe la eleccion original.

## Radar manual exploratorio

`RADAR_MANUAL_EXPLORATORY_ENABLED=true` conserva el reporte compacto que el
usuario vio al ejecutar `/radar` y sus candidatos en `radar_exploratory_runs` y
`radar_exploratory_candidates`. El hash del reporte evita duplicar una misma
respuesta servida desde cache. Cada candidato ofrece `Seguir` y `Descartar`; la
primera eleccion es inmutable.

Este circuito tiene `metric_scope=exploratory` e `is_primary_metric=false`. No
escribe `decision_log`, no crea planes u ordenes y no modifica ni alimenta
`radar_discovery_runs`, `radar_discovery_snapshots` o `/radar_metricas`. La
captura oficial de las 16:50 sigue siendo la unica cohorte prospectiva primaria.

Una compra posterior puede vincularse al `Seguir` solo si coinciden usuario,
ticker y secuencia temporal. El reconciliador excluye fills ya atribuidos a una
alerta intradia de Setup, de modo que una ejecucion no tenga dos explicaciones
Radar. Si el broker informa solo la fecha, exige una rueda posterior porque no
puede probar el orden intradiario. No se hace backfill: las elecciones anteriores
a la activacion quedan como casos historicos no causales.

`TRADINGVIEW_BYMA_REFRESH_ENABLED=true` agrega un refresco diario fail-soft a
las 18:00 ART para dejar OHLCV local BYMA disponible para la captura shadow de
la rueda siguiente. Usa 40 ruedas por defecto y no abre Cocos. En la lectura
canónica vigente, Cocos conserva prioridad; TradingView tiene prioridad sobre
velas `internal_snapshot` reconstruidas y por eso también puede mejorar la
entrada técnica del Radar operativo. Los aliases o instrumentos sin historia
suficiente continúan marcados `PARTIAL`/`INSUFFICIENT`.

Si despues de `Seguir` aparece un fill real `BUY`, se vincula solo cuando
coinciden usuario, ticker y secuencia temporal dentro de la ventana. El fill
sigue siendo evidencia del broker; el vinculo no crea una decision del bot ni
escribe en `decision_log`.

Configuracion inicial:

```dotenv
RADAR_INTRADAY_SETUP_ALERTS_ENABLED=false
RADAR_MANUAL_EXPLORATORY_ENABLED=false
RADAR_INTRADAY_SETUP_MIN_PERCENTILE=0.80
RADAR_INTRADAY_SETUP_MIN_RR=2.0
RADAR_INTRADAY_SETUP_MAX_EXTENSION_PCT=0.06
RADAR_INTRADAY_SETUP_MAX_PRICE_AGE_SECONDS=900
RADAR_INTRADAY_SETUP_MAX_SNAPSHOT_AGE_DAYS=7
RADAR_INTRADAY_SETUP_COOLDOWN_DAYS=14
RADAR_INTRADAY_SETUP_MAX_ALERTS=3
```

## Calidad y sesgos controlados

- El volumen faltante produce `feature_quality_flag=PARTIAL` y una advertencia;
  nunca suma puntos de compresion.
- Los percentiles usan exclusivamente datos disponibles en el snapshot.
- Para CEDEAR se usa comparacion local dentro de la misma corrida y se conserva
  `cedear_ccl_not_separated`; no se afirma que sea fuerza del subyacente USD.
- Cada cambio en este calculo cambia el fingerprint de `scoring_version`, por lo
  que las cohortes de versiones diferentes no se mezclan.
- `PRE_BREAKOUT`, `TRIGGERED`, `EXTENDED`, `WATCH` y `DATA_INSUFFICIENT` son
  estados descriptivos shadow, no instrucciones de compra.

## Validacion prospectiva

Solo cuentan snapshots creados despues de activar el Ledger. Los replay V3
previos no se insertan ni se mezclan con esta muestra. Telegram expone un
resumen read-only con `/radar_metricas`; no altera `/radar`, el top 6 operativo
ni la persistencia. Cualquier reduccion futura a 3-5 candidatos requiere una
decision separada basada en outcomes maduros.
