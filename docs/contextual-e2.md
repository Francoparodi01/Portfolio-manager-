# Contextual Analysis E2

## Alcance y estado

E2 agrega evidencia descriptiva de mercado alrededor de una decisión. La capa es `SHADOW_ONLY`: no modifica scores, señales, thresholds, optimizer, planner, cantidades, cash ni ejecución. Sus versiones iniciales son `contextual-market-v1` y `contextual-market-snapshot-v1`.

G1 de E1 quedó en **PASS** sobre PostgreSQL/TimescaleDB real y descartable. La validación creó una corrida nueva, escribió y releyó 260 velas y persistió el paquete formal completo. No reconstruyó historia ni escribió en la base productiva.

## Arquitectura

El recorrido es:

1. `MarketCandle` conserva identidad, símbolo proveedor, OHLCV, fuente y tiempos `bar_start`, `bar_end`, `available_at`, `scraped_at` e `is_closed`. Un dato ausente queda en `NULL`.
2. `candles_to_frame` rechaza identidades ambiguas y conserva unidades, calendario, política de ajuste y ratio depositario.
3. `build_contextual_snapshot` recorta la serie antes de calcular: índice, `bar_end`, `available_at` y `scraped_at` deben ser menores o iguales al cutoff, e `is_closed` debe ser verdadero. Una vela sin esos contratos no es elegible.
4. La capa calcula estructura diaria y semanal, RVOL, breakout, fuerza relativa, distancias y confianza por cobertura.
5. Los invalidadores producen `PASS`, `WARN`, `FAIL` o `UNKNOWN` con motivo. No bloquean producción.
6. `attach_contextual_shadow` adjunta el snapshot a `SynthesisResult`. `_layers_payload_for_decision` lo incorpora a `feature_snapshot_v3` sólo si existe. Los callers productivos actuales no lo crean automáticamente.
7. La captura formal append-only guarda el snapshot dentro de la señal y de `feature_snapshot_v3`, junto con owner, run, plan y hashes.

Archivos de evidencia generada:

- `docs/evidence/contextual-g1.json`: roundtrip completo en Timescale aislado.
- `docs/evidence/contextual-e2-real-readonly.json`: evaluación read-only de una decisión y velas locales reales.
- `docs/evidence/contextual-e2-controlled.json`: snapshot reproducible con datos controlados.
- `docs/evidence/contextual-e2-diagnostic.png`: gráfico diagnóstico controlado.
- `docs/evidence/contextual-e2-non-regression.json`: comparación contra el plan E1 grabado.

## Contratos y definiciones

### Selección PIT

Para un cutoff consciente de zona horaria `T`, una vela entra sólo si:

```text
timestamp <= T
bar_end <= T
available_at <= T
scraped_at <= T
is_closed = true
```

No se infiere `available_at` desde el timestamp ni se supone el cierre por el intervalo. La ausencia produce `UNKNOWN` y se registra en `missingness`.

### Estructura de mercado

Un swing confirmado de ventana `w=2` requiere que el máximo o mínimo central sea único en las cinco observaciones `[t-2, t+2]`. Los dos últimos swings confirmados determinan:

- máximos: `HH`, `LH` o `EH`;
- mínimos: `HL`, `LL` o `EL`;
- tendencia: `UPTREND` para `HH_HL`, `DOWNTREND` para `LH_LL`, `MIXED` para combinaciones restantes y `UNKNOWN` si faltan swings.

La estructura diaria usa hasta 120 velas. La semanal agrega velas diarias cerradas con `W-FRI` y descarta una semana cuyo viernes sea posterior al cutoff. Cada estructura conserva swings, soporte, resistencia, último cierre y timestamps.

Las distancias son:

```text
distance_support    = (close - last_confirmed_swing_low) / close
distance_resistance = (last_confirmed_swing_high - close) / close
```

Un valor negativo indica que el precio atravesó el nivel confirmado.

### Volumen y RVOL

Para una vela diaria cerrada:

```text
expected_volume_t = mean(volume[t-20:t-1])
RVOL_t            = volume_t / expected_volume_t
```

La ventana contiene exactamente las 20 velas cerradas previas y excluye la actual. Si falta un volumen, hay un valor no finito/no positivo o no hay 20 observaciones comparables, RVOL queda `UNKNOWN`.

- `EXPANSION`: RVOL >= 1.50.
- `CONTRACTION`: RVOL <= 0.70.
- `NORMAL`: entre ambos límites.

Un breakout compara el cierre actual contra el máximo de las 20 velas previas. Se confirma con RVOL >= 1.20; de lo contrario queda `BREAKOUT_WITHOUT_VOLUME`. Esos límites sólo clasifican evidencia shadow y no son thresholds productivos.

### Fuerza relativa

Activo y benchmark se alinean por sesión UTC ya conocida al cutoff. Para ventanas de 20, 60 y 120 sesiones alineadas:

```text
asset_return       = A_t / A_t-n - 1
benchmark_return   = B_t / B_t-n - 1
excess_return      = asset_return - benchmark_return
relative_change    = (A_t / B_t) / (A_t-n / B_t-n) - 1
```

Se soporta un benchmark general configurable, por defecto `SPY`, y un benchmark sectorial opcional suministrado por el caller. No hay reglas por ticker. Las monedas deben coincidir o ambas series deben declarar normalización de moneda; una mezcla ARS/USD sin normalización queda `UNKNOWN`.

### Confianza contextual

`context_confidence` es cobertura de evidencia, no convicción direccional ni score:

```text
componentes disponibles / 5
```

Los cinco componentes requeridos son estructura diaria, estructura semanal, RVOL, estado de breakout y fuerza relativa general. `HIGH` requiere al menos 0.80, `MEDIUM` al menos 0.50 y `LOW` menos de 0.50.

### Invalidadores

Se registran por separado:

- breakout sin volumen;
- BUY/ACCUMULATE contra estructura semanal deteriorada;
- deterioro de fuerza relativa de 20 y 60 sesiones;
- precio bajo soporte estructural;
- BUY/ACCUMULATE con volumen desconocido o contraído.

`FAIL` describe evidencia contraria fuerte, `WARN` evidencia débil o mixta, `UNKNOWN` falta de datos y `PASS` ausencia del invalidante definido. Ningún estado altera producción en E2.

## Fuentes y trazabilidad

El snapshot guarda identidad completa, símbolos proveedor, fuentes por serie, cutoff, última vela elegible, última disponibilidad y scraping, calidad E1, componentes, invalidadores y digests SHA-256 de los inputs PIT. El ID `context:<24 hex>` es determinista sobre el payload canónico. La versión de código continúa dentro del manifiesto de `feature_snapshot_v3`, y la captura formal agrega hashes de `contextual_market.py`, persistencia, modelos y schema.

Los eventos de plan, intents, decisiones y capturas siguen append-only por los triggers E1. E2 no actualiza filas históricas.

## Evidencia G1

La prueba `test_new_contextual_run_roundtrips_all_e1_contracts_in_timescale` usa una base creada para cada ejecución dentro de un contenedor Timescale con almacenamiento temporal. La corrida controlada:

- persiste y relee identidad `BYMA:CEDEAR:G1TEST:ARS` y símbolo proveedor;
- conserva intervalo, candle timestamp, `bar_start`, `bar_end`, `available_at`, `scraped_at` e `is_closed`;
- obtiene calidad de precio, volumen y procedencia `VALID`, con missingness vacío;
- persiste `feature_snapshot_v3`, señal, conviction, asset view y régimen;
- vincula owner, run, portfolio snapshot, plan, decisión y captures;
- conserva cash antes/después y un intent bloqueado/no ejecutable;
- persiste `code_version`, `config_hash`, digests de serie, feature snapshot y archivos fuente.

La evidencia se obtiene sin órdenes, red de broker, migraciones productivas ni escritura fuera de la base descartable.

La ejecución final quedó vinculada a `code_version=a528bc69a85324c5c2f2d2c2a452dff4517f3163`, plan `9105757b-2324-44ca-b43f-a3984f1b9d90` y feature snapshot `features:58bd6923aa7d6ed2`. El contenedor reportó TimescaleDB 2.26.4 y fue eliminado al terminar.

## Ejemplos

### Corrida controlada reproducible

El snapshot controlado se genera con `scripts/render_contextual_e2_example.py`. Sus valores se rotulan como fixture y no se presentan como mercado observado. Sirve para inspeccionar todas las dimensiones y producir el gráfico con precio, swings, soporte/resistencia, volumen, media previa de 20 sesiones y cutoff de decisión.

### Decisión real read-only

`scripts/audit_contextual_e2.py` leyó la decisión `1273` de IREN del 6 de octubre de 2026 a las 16:47:10 UTC, con owner `1259412316`, run `18457bc9-b17f-4599-9cd2-d444cb351d5b` y plan `8444cd7f-f2ef-44f5-b834-5b805bc234e1`. Al cutoff había 183 filas `TV:BYMA:IREN` y 313 filas `TV:BYMA:SPY` observables por `scraped_at`.

La tabla real todavía no contiene `available_at`, `bar_end`, `is_closed`, unidad de volumen, calendario, política de ajuste ni ratio depositario. E2 excluyó todas las velas, produjo estructura/RVOL/RS `UNKNOWN`, confianza 0 y no generó gráfico. Esta salida es el comportamiento PIT-safe esperado; completar esos campos por supuestos habría convertido evidencia desconocida en una afirmación falsa.

## Pruebas

`tests/test_contextual_e2.py` cubre:

- no lookahead al agregar velas futuras;
- recorte por cutoff y `available_at`;
- RVOL con ventana anterior exclusiva;
- estructura diaria y semanal;
- fuerza relativa en 20/60/120 sesiones;
- volumen faltante;
- identidad incompleta y benchmark con moneda incompatible;
- timestamps sin zona horaria;
- hash y versión de snapshot;
- invalidadores `PASS`, `WARN`, `FAIL` y `UNKNOWN`;
- adjunto opt-in a `feature_snapshot_v3`;
- gráfico PNG;
- neutralidad de score, señal, órdenes, cantidades y cash.

La integración PostgreSQL de G1 valida la persistencia/relectura. Los tests E1 existentes se mantienen para NaN/infinito, volumen, identidad, autoridad shadow y snapshot v3.

Resultados ejecutados:

- focalizada E1/E2/evidencia: `61 passed, 24 skipped`;
- integración G1 con Timescale opt-in: `1 passed`;
- suite completa E2: `792 passed, 7 failed, 24 skipped`;
- suite completa E1 previa: `721 passed, 7 failed, 23 skipped`;
- baseline anterior a E1: `684 passed, 7 failed, 23 skipped`.

Las siete fallas son las mismas del baseline: tres expectativas de menú/ayuda y dos de radar compacto en Telegram, un mock de radar exploratorio y la dependencia local ausente `pypfopt`. E2 no agregó una falla nueva.

## No regresión contra E1

`scripts/compare_contextual_e2.py` toma la captura E1 grabada, adjunta snapshots E2 a una copia y compara sólo campos productivos. Los hashes productivos baseline/shadow son iguales y la evidencia declara igualdad de:

- scores;
- señales;
- decisiones;
- órdenes;
- cantidades;
- cash.

Ambos hashes son `023079e4d50b97151ef08c735be8f028a07b4fd58f660a5ec4b30d12e73ad081` para el plan E1 `8444cd7f-f2ef-44f5-b834-5b805bc234e1`.

Esto es una comparación de neutralidad sobre el plan grabado, no un replay económico ni una recalibración.

## Supuestos y limitaciones

- Los datos controlados prueban contratos y cálculos; no prueban desempeño de mercado.
- La estructura por pivots necesita dos observaciones posteriores dentro de la serie conocida. Eso confirma el swing con demora y no usa futuro respecto del cutoff.
- La agregación semanal omite la semana incompleta.
- El benchmark sectorial requiere configuración externa explícita; no se asigna por ticker.
- No hay ajuste de FX implícito para benchmarks con distinta moneda.
- Las revisiones de una vela bajo la misma clave siguen siendo un upsert en `market_candles`; el bundle de decisión sí es append-only, pero un historial de revisiones de proveedor aún no existe.
- La base productiva actual no tiene los nuevos campos temporales y de unidades. El código y la migración aditiva están listos, pero no se ejecutaron allí.
- La visualización real histórica queda pendiente hasta que una corrida nueva persista metadatos PIT completos.

## Gaps para E3

Antes de E3 conviene obtener al menos una corrida real nueva con los campos E1 desplegados y validar un gráfico PIT real. También quedan:

- versionar revisiones de velas en lugar de sobrescribir la misma clave;
- registrar el mapeo general/sector benchmark como dato versionado por instrumento;
- usar un calendario BYMA versionado para validar sesiones faltantes y frescura;
- definir normalización FX si se comparan monedas distintas;
- calibrar ventanas y estados sólo con un protocolo de investigación separado, sin ajustar por GDX, IREN ni otro ticker;
- estudiar invalidadores y contexto contra outcomes maduros antes de cualquier promoción fuera de shadow.

La recomendación actual es **no comenzar E3 productivo**. Primero debe existir una corrida real nueva con metadatos temporales completos y una relectura PIT que deje de ser `UNKNOWN`. E3 de investigación puede planificarse, pero no debe alterar decisiones ni ejecución con la evidencia disponible.
