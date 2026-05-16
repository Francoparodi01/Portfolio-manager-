# Comandos

Referencia operativa de Cocos Copilot: Telegram, CLI, Docker, base de datos y mantenimiento de históricos Cocos.

## Telegram

| Comando | Uso |
|---|---|
| `/start` | menú principal |
| `/menu` | alias de menú |
| `/portfolio` | último snapshot de cartera |
| `/analisis` | análisis cuantitativo completo |
| `/analysis` | alias de `/analisis` |
| `/analisis_semanal` | alias de `/analisis` |
| `/radar` | radar compacto |
| `/radar_full` | radar extendido |
| `/performance` | performance y dataset operativo |
| `/resumen` | resumen semanal |
| `/weekly_summary` | alias de resumen |
| `/resumen_semanal` | alias de resumen |
| `/regression` | auditoría de regresión |
| `/regression_audit` | alias de auditoría |
| `/status` | estado del sistema |
| `/admin_scrape` | scrape manual admin |
| `/admin_refresh_portfolio` | refresco manual de portfolio admin |

## Docker

### Levantar servicios con base externa

```bash
docker compose build
docker compose up -d scheduler telegram_bot
```

### Levantar también la base local

```bash
docker compose --profile localdb up -d db scheduler telegram_bot
```

### Estado y logs

```bash
docker compose ps
docker compose logs -f scheduler
docker compose logs -f telegram_bot
docker compose logs --tail=100 scheduler
docker compose logs --tail=100 telegram_bot
```

### Reiniciar

```bash
docker compose restart scheduler
docker compose restart telegram_bot
```

### Apagar

```bash
docker compose down
```

## CLI de scraping

### Portfolio y mercado global

```bash
docker compose exec scheduler python scripts/run_once.py
docker compose exec scheduler python scripts/run_once.py --full
docker compose exec scheduler python scripts/run_once.py --no-telegram
docker compose exec scheduler python scripts/run_once.py --json snapshot.json
```

| Flag | Efecto |
|---|---|
| sin flags | portfolio y persistencia |
| `--full` | portfolio + `/ACCIONES` + `/CEDEARS` |
| `--no-db` | no persiste |
| `--json FILE` | exporta snapshot |
| `--no-telegram` | ejecución silenciosa |

`--full` alimenta `market_prices`. No crea velas históricas.

## Históricos Cocos

### 1. Abrir Chrome con depuración remota

En Windows:

```powershell
Start-Process chrome.exe -ArgumentList '--remote-debugging-port=9222','--user-data-dir=C:\Temp\cocos-cdp-profile','https://app.cocos.capital'
```

Iniciar sesión en Cocos en esa ventana y dejarla abierta.

### 2. Capturar un ticker

```bash
python scripts/capture_cocos_history.py CEDEARS NVDA --output logs/nvda_history.json
python scripts/capture_cocos_history.py ACCIONES GGAL --output logs/ggal_history.json
```

### 3. Importar un JSON

```bash
python scripts/import_cocos_history.py logs/nvda_history.json
```

### 4. Backfill batch del universo faltante

```bash
python scripts/backfill_cocos_history.py --import-db
python scripts/backfill_cocos_history.py --wait-ms 12000 --pause-ms 20000 --import-db
```

| Flag | Efecto |
|---|---|
| `--cdp-url` | endpoint DevTools, default `127.0.0.1:9222` |
| `--wait-ms` | espera por página/ticker |
| `--pause-ms` | pausa entre activos |
| `--min-rows` | mínimo para considerar un activo cubierto |
| `--all` | recaptura aunque ya exista histórico |
| `--import-db` | guarda cada captura en `market_candles` |

Notas:

- Cocos puede responder con rate limit si se barre demasiado rápido.
- El script corta ante Cloudflare 1015.
- Tickers sin histórico Cocos suficiente quedan `EXTERNO`.
- `C.I.` se conserva como excepción externa conocida.

## CLI de análisis

### Cartera principal

```bash
docker compose exec scheduler python scripts/run_analysis.py --no-telegram
docker compose exec scheduler python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
docker compose exec scheduler python scripts/run_analysis.py --tickers NVDA AMD --period 1y --no-telegram
```

| Flag | Efecto |
|---|---|
| `--tickers A B` | limita cartera analizada |
| `--period` | `1mo`, `3mo`, `6mo`, `1y`, `2y` |
| `--no-telegram` | solo stdout |
| `--no-llm` | omite explicación LLM |
| `--no-sentiment` | omite RSS |
| `--no-optimizer` | omite optimizer |

### Radar de oportunidades

```bash
docker compose exec scheduler python scripts/run_opportunity.py --no-telegram
docker compose exec scheduler python scripts/run_opportunity.py --top 5 --min-score 0.15 --min-rr 1.5 --no-telegram
docker compose exec scheduler python scripts/run_opportunity.py --universe AAPL GGAL MSFT --no-telegram
```

| Flag | Efecto |
|---|---|
| `--universe A B C` | universo manual |
| `--period` | `3mo`, `6mo`, `1y`, `2y` |
| `--top N` | alias de máximo |
| `--max N` | máximo de candidatos |
| `--min-score X` | score mínimo |
| `--min-rr X` | R/R mínimo |
| `--include-portfolio` | incluye holdings |
| `--no-sentiment` | sin RSS |
| `--no-telegram` | solo stdout |

### Performance

```bash
docker compose exec scheduler python scripts/run_performance.py --no-telegram
docker compose exec scheduler python scripts/run_performance.py --days 60 --no-telegram
```

El reporte distingue:

- `EV histórico agregado`;
- `Execution Audit`;
- `Blocked Audit`.

### Outcomes

```bash
docker compose exec scheduler python scripts/update_outcomes.py
docker compose exec scheduler python scripts/update_outcomes.py --days 60
```

### Resumen semanal

```bash
docker compose exec scheduler python scripts/weekly_summary.py --no-telegram
docker compose exec scheduler python scripts/weekly_summary.py --weeks-ago 1 --no-telegram
```

### Auditoría de regresión

```bash
docker compose exec scheduler python scripts/run_regression_audit.py
```

Consultar flags disponibles:

```bash
docker compose exec scheduler python scripts/run_regression_audit.py --help
```

## Scheduler

El scheduler vive en `src/scheduler/runner.py`.

| Hora ART | Trabajo |
|---|---|
| 10:30 | portfolio |
| 10:31 | inicia loops intradía |
| 17:00 | portfolio + mercado global |
| 17:01 | detiene loops intradía |
| 17:05 | construye velas diarias internas |
| 21:30 | outcomes |

Importante:

```text
El backfill Cocos queda manual/excepcional.
La continuidad diaria de market_candles sale de market_prices
con source = internal_snapshot.
```

## Base de datos

### Base local del compose

```bash
docker exec -it cocos_db psql -U portfolio -d portfolio
```

### Consultas útiles

```sql
SELECT COUNT(*) FROM portfolio_snapshots;
SELECT COUNT(*) FROM positions;
SELECT COUNT(*) FROM market_prices;
SELECT COUNT(*) FROM market_candles;
SELECT COUNT(*) FROM decision_log;

SELECT ticker, asset_type, COUNT(*) AS candles
FROM market_candles
GROUP BY ticker, asset_type
ORDER BY ticker;

SELECT decided_at, ticker, decision, final_score, outcome_5d, was_correct
FROM decision_log
ORDER BY decided_at DESC
LIMIT 20;
```

### Último snapshot

```sql
SELECT scraped_at, total_value_ars, cash_ars
FROM portfolio_snapshots
ORDER BY scraped_at DESC
LIMIT 5;
```

## Tests

```bash
python -m pytest tests
```

Si usás un venv local, activarlo antes. Los tests deben correr offline: sin DB real, sin scraper real y sin internet.

## Variables de entorno

Variables principales:

```env
COCOS_USERNAME=
COCOS_PASSWORD=
COCOS_TOTP_SECRET=
DATABASE_URL=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
REDIS_URL=
ADMIN_CHAT_IDS=
HEADLESS=true
```

Nunca subir `.env`.

## Troubleshooting rápido

### `No module named pytest`

Instalar dependencias de desarrollo en el venv activo o usar uno que ya tenga `pytest`.

### Password DB incorrecta

Verificar que `DATABASE_URL` apunte a la base correcta y que no estés mezclando:

- `cocos_db` local del compose;
- DB externa configurada en `.env`.

### Radar con muchos `EXTERNO`

Ejecutar backfill de históricos Cocos y confirmar que haya suficientes filas en `market_candles`.

### Chrome/CDP no responde

Confirmar:

```text
http://127.0.0.1:9222/json/version
```

Si no responde, abrir una nueva ventana de Chrome con `--remote-debugging-port=9222`.
