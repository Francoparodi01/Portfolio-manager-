# Documentacion de Cocos Copilot

## Proposito

Cocos Copilot es un sistema cuantitativo auditable para una cartera real en Cocos
Capital. Su foco no es automatizar ejecuciones ni vender una prediccion, sino
construir una memoria operacional confiable: datos, decisiones, fills y outcomes.

## Mapa de Documentacion

| Documento | Contenido |
|---|---|
| [01-arquitectura.md](01-arquitectura.md) | servicios, modulos, pipelines y contratos |
| [02-datos-y-persistencia.md](02-datos-y-persistencia.md) | fuentes, tablas, prioridad de velas, fills y outcomes |
| [03-operacion.md](03-operacion.md) | Docker, Telegram, CLI, scheduler, monitor API |
| [04-analitica-y-decision.md](04-analitica-y-decision.md) | analysis, radar, optimizer, planner, performance, regression, DCL |
| [05-mantenimiento.md](05-mantenimiento.md) | backfills, calidad, troubleshooting, limpieza futura |

## Principios del Sistema

1. El scraper observa; no decide.
2. El optimizer propone; no ejecuta.
3. El execution planner es la fuente de verdad operativa.
4. Los fills reales son la fuente de verdad de ejecucion.
5. Los outcomes se calculan solo cuando existe vela suficiente y comparable.
6. Cocos es fuente operativa prioritaria; TradingView/BYMA rellena espalda historica.
7. Un ticker sin historico suficiente no es una oportunidad operable.

## Estado Productivo Actual

| Area | Estado |
|---|---|
| Portfolio Cocos | activo |
| Mercado ACCIONES | activo |
| Mercado CEDEARs Top/ETF/Otros/Nuevos | activo; Crypto excluido |
| Movements/Fills Cocos | activo, read-only |
| Historico Cocos | activo/manual |
| Historico TradingView/BYMA | activo/manual |
| Scheduler | activo |
| Telegram bot | activo |
| Monitor API | activo en `8010` |
| Regression audit | activo |
| DCL/calibration | experimental operativo |
| Ejecucion automatica de ordenes | no implementada por diseno |

## Glosario Minimo

| Termino | Significado |
|---|---|
| `market_prices` | snapshots de precios actuales del universo Cocos |
| `market_candles` | velas OHLCV canonicas usadas por analisis/outcomes |
| `decision_log` | registro auditable de ideas, planes, bloqueos y ejecuciones |
| `broker_fills` | operaciones reales detectadas/importadas desde Cocos |
| `broker_movements` | movimientos de instrumentos/caja desde Cocos |
| `outcome` | resultado posterior de una decision a 5, 10 o 20 ruedas |
| `EXTERNOS` viejo | nombre obsoleto; ahora es "en Cocos / sin historico operable" |
| `TRADINGVIEW_BYMA` | fuente de backfill historico comparable en ARS/BYMA |

