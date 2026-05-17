# Cocos Copilot

Sistema cuantitativo auditable para analizar una cartera real en Cocos Capital, convertir señales en decisiones operativas y medir si esas decisiones agregan valor con el tiempo.

El proyecto no busca hacer trading automático ni prometer predicciones perfectas. Su función es más concreta:

- leer la cartera real,
- separar datos observados de inferencias,
- producir un plan ejecutable o decidir no operar,
- registrar cada decisión,
- medir después si hubo edge.

## Estado actual

El sistema ya puede:

- scrapear la cartera real de Cocos Capital;
- leer el universo de `ACCIONES` y `CEDEARS` por separado;
- guardar precios de mercado actuales y velas históricas de Cocos;
- analizar cartera actual con técnico, macro, riesgo y sentiment opcional;
- generar targets teóricos con optimizer;
- convertir esos targets en `BUY`, `SELL`, `HOLD`, `WATCH` o bloqueos mediante el Execution Planner;
- ejecutar un radar externo de oportunidades;
- clasificar como `EXTERNO` cualquier ticker sin velas Cocos suficientes;
- registrar decisiones, reconciliar fills reales importados y calcular outcomes, IC, EV, win rate y curva de equity;
- operar por CLI o Telegram;
- mantener portfolio, mercado y outcomes con scheduler.

La política de datos ya cambió respecto de versiones anteriores:

- **instrumentos operables**: se evalúan con velas de Cocos en `market_candles`;
- **universo actual**: se descubre desde Cocos en `market_prices`;
- **contexto macro global**: sigue usando fuentes externas como `yfinance` y APIs locales;
- **radar externo**: es estricto-Cocos; si no hay velas suficientes, el activo queda fuera de la operabilidad.

## Cómo se capturan los datos

Hay dos flujos distintos y conviene no mezclarlos:

| Flujo | Origen | Destino | Uso |
|---|---|---|---|
| Scrape global de mercado | `/market/ACCIONES` y `/market/CEDEARS` | `market_prices` | descubrir universo, segmento y precio actual |
| Backfill histórico inicial | página individual de cada ticker + request `historic-data-extended` | `market_candles` con `source = COCOS` | base histórica oficial |
| Continuidad diaria | snapshots propios de `market_prices` | `market_candles` con `source = internal_snapshot` | técnico, optimizer, radar y outcomes |

`market_prices` es una foto del mercado. `market_candles` conserva dos fuentes físicas pero expone una sola serie lógica: si existe una vela oficial `COCOS` para un día, se usa esa; `internal_snapshot` entra solo como fallback.

El proyecto ya tiene scripts para backfill inicial de velas Cocos:

- `scripts/capture_cocos_history.py`
- `scripts/import_cocos_history.py`
- `scripts/backfill_cocos_history.py`

El backfill oficial de Cocos queda congelado como base inicial. La continuidad diaria se reconstruye internamente desde `market_prices`; las capturas `historic-data-extended` quedan para altas nuevas o reparaciones manuales/excepcionales.

## Flujo de decisión

```text
Cocos Capital
  -> portfolio_snapshots / positions
  -> market_prices
  -> market_candles

run_analysis.py
  -> técnico + macro + riesgo + sentiment
  -> síntesis multicapa
  -> optimizer
  -> execution_planner
  -> decision_log
  -> reporte Telegram / stdout
```

Regla central:

```text
El optimizer propone.
El Execution Planner decide qué es operable.
El reporte operativo muestra el plan ejecutable, no el target teórico.
```

## Separación ACCIONES / CEDEARS

La separación por segmento es explícita:

- `ACCIONES`: acciones argentinas;
- `CEDEARS`: exposición local a activos internacionales.

El sistema conserva `asset_type` para no mezclar ambas capas:

- `market_prices.asset_type`
- `market_candles.asset_type`
- carga del universo en análisis y radar.

Esto permite que el análisis futuro modele mejor la diferencia entre comportamiento local argentino y contexto global.

## Stack

| Capa | Tecnología |
|---|---|
| Lenguaje | Python |
| Scraping | Playwright / Chromium |
| Persistencia | PostgreSQL / TimescaleDB |
| Scheduler | APScheduler |
| Bot | python-telegram-bot |
| Datos cuantitativos | pandas, numpy, scipy |
| Contexto macro | yfinance + APIs locales |
| Infraestructura | Docker Compose |
| Coordinación auxiliar | Redis opcional |

## Arranque rápido

1. Crear `.env`:

```bash
cp .env.example .env
```

2. Completar credenciales y `DATABASE_URL`.

3. Construir servicios:

```bash
docker compose build
```

4. Levantar con una base externa ya configurada en `.env`:

```bash
docker compose up -d scheduler telegram_bot
```

5. O levantar también la base local del compose:

```bash
docker compose --profile localdb up -d db scheduler telegram_bot
```

6. Verificar estado:

```bash
docker compose ps
docker compose logs -f scheduler
docker compose logs -f telegram_bot
```

## Comandos principales

Telegram:

```text
/portfolio
/analisis
/radar
/performance
/resumen
/status
```

CLI:

```bash
docker compose exec scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
docker compose exec scheduler python scripts/run_opportunity.py --no-telegram
docker compose exec scheduler python scripts/run_performance.py --no-telegram
```

La referencia completa está en [`COMANDOS.md`](./COMANDOS.md).

## Qué mide el sistema

Las decisiones guardadas en `decision_log` se auditan con:

- outcomes a 5, 10 y 20 días;
- win rate;
- expected value;
- average win / average loss;
- Information Coefficient y Rank IC;
- curva de equity;
- separación entre ideas teóricas del optimizer y ejecución real.

El reporte de performance distingue explícitamente:

- **EV histórico agregado**: evidencia acumulada del modelo;
- **Execution Audit**: evidencia de fills reales confirmados; los planes aprobados quedan separados.

## Limitaciones actuales

- No ejecuta órdenes automáticamente.
- Valida fills reales solo cuando se importan y reconcilian; la captura automática desde Cocos queda pendiente de una fuente estable.
- El camino operativo ya es canónico: sin historia suficiente en `market_candles`, no se reabre un fallback silencioso a otra fuente.
- El dataset de ejecución real todavía está madurando.
- `C.I.` se conserva como `EXTERNO`: la ruta de Cocos no entrega velas utilizables.
- La capa ML existe como stub/experimental, no como parte del runtime principal.

## Documentación

- [`ARQUITECTURA.md`](./ARQUITECTURA.md): diseño técnico, datos, módulos y contratos.
- [`COMANDOS.md`](./COMANDOS.md): operación diaria, CLI, Docker, DB y backfill.

## Disclaimer

Proyecto personal de disciplina cuantitativa. No es asesoramiento financiero ni un producto para terceros.
