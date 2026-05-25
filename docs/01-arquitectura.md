# Arquitectura

## Vista General

```text
Cocos Capital
  |-- portfolio
  |-- market ACCIONES / CEDEARS
  |-- movements / instrumentos
  |
  v
collector
  |-- portfolio_snapshots / positions / raw_snapshots
  |-- market_prices
  |-- broker_movements / broker_fills
  |
  v
market_candles
  |-- COCOS
  |-- TRADINGVIEW_BYMA
  |-- internal_snapshot
  |
  v
analysis
  |-- technical
  |-- macro
  |-- risk
  |-- sentiment opcional
  |-- synthesis
  |-- optimizer
  |-- execution_planner
  |
  v
decision_log -> outcomes -> performance / regression / DCL
```

## Servicios Docker

| Servicio | Contenedor | Responsabilidad |
|---|---|---|
| `db` | `cocos_db` | PostgreSQL/TimescaleDB local opcional |
| `scheduler` | `cocos_scheduler` | jobs programados, scraping, velas, outcomes |
| `telegram_bot` | `cocos_telegram_bot` | interfaz conversacional y comandos |
| `monitor_api` | `cocos_monitor_api` | API/dashboard read-only de salud e ingesta |

`db` esta bajo perfil `localdb`. Si `DATABASE_URL` apunta a una base externa, se
pueden levantar solo `scheduler`, `telegram_bot` y `monitor_api`.

## Capas de Codigo

| Carpeta | Rol |
|---|---|
| `scripts/` | entrypoints CLI y bot |
| `src/collector/` | scraping, normalizacion, DB, fills, movements |
| `src/analysis/` | senales, riesgo, optimizer, planner, auditorias |
| `src/analysis/dcl/` | decision calibration layer experimental |
| `src/core/` | configuracion, calendario, Redis, credenciales |
| `src/scheduler/` | agenda y loops productivos |
| `src/monitor/` | API y frontend read-only |
| `config/` | feriados y configuracion estatica |

## Entry Points Principales

| Archivo | Uso |
|---|---|
| `scripts/run_once.py` | scrape manual de portfolio/mercado/fills |
| `scripts/run_analysis.py` | analisis integral de cartera |
| `scripts/run_opportunity.py` | radar de oportunidades |
| `scripts/run_performance.py` | performance y outcomes |
| `scripts/run_confidence_audit.py` | salud del dataset operativo |
| `scripts/run_regression_audit.py` | auditoria estadistica de senales |
| `scripts/run_calibration.py` | calibracion/DCL |
| `scripts/sync_cocos_fills.py` | sincronizacion read-only de fills/movements |
| `scripts/backfill_tradingview_byma.py` | backfill OHLCV TradingView/BYMA |
| `scripts/telegram_bot.py` | bot de Telegram |
| `src/monitor/api.py` | monitor API + frontend |

## Contratos de Arquitectura

### Datos

- `market_prices` descubre universo y precios actuales.
- `market_candles` es la serie canonica para tecnico, outcomes y auditorias.
- `decision_log` guarda la memoria de decisiones y resultados.
- `broker_fills` confirma ejecuciones reales.
- `broker_movements` conserva actividad observada de Cocos.

### Decision

- El optimizer produce pesos teoricos.
- El execution planner convierte esos pesos en ordenes operables o bloqueos.
- El bot reporta el plan operativo, no solo la teoria.
- Las decisiones generadas por prueba no deben quedar persistidas si no son parte de una corrida real.

### Seguridad

- El monitor es read-only.
- El monitor requiere token por `Authorization: Bearer` o `X-API-Token`.
- TOTP es opcional mediante `MONITOR_TOTP_SECRET`.
- No hay endpoint para ejecutar trades.

## Scheduler

| Hora ART | Job |
|---|---|
| 10:30 | portfolio inicial |
| 10:31 | inicio de loop intradia |
| 17:00 | full scrape: portfolio + mercado |
| 17:01 | fin de loop intradia |
| 17:05 | construccion de vela interna diaria |
| 17:10 | verificacion de velas |
| 17:12 | analisis diario |
| 21:30 | outcomes |

El scheduler omite jobs de mercado si el calendario local indica feriado o mercado cerrado.

