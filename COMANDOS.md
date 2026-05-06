# REFERENCIA DE COMANDOS

### Cocos Copilot — CLI, Telegram, Docker y mantenimiento

---

## Bot de Telegram

Los comandos están disponibles como slash commands y/o desde botones del menú.

| Comando | Descripción |
|---|---|
| `/start` | Abre el menú principal |
| `/portfolio` | Último snapshot: total cuenta, invertido, cash y posiciones con peso |
| `/analisis` | Pipeline cuantitativo completo |
| `/analisis_rapido` | Pipeline sin LLM ni sentiment, si está habilitado |
| `/radar` | Radar compacto de oportunidades externas |
| `/radar_full` | Radar extendido, comando avanzado |
| `/oportunidades` | Alias o versión extendida del radar, si está habilitado |
| `/performance` | Win rate, EV, outcomes y equity curve |
| `/resumen` | Resumen semanal del portfolio por movimiento de precios |
| `/weekly_summary` | Alias de resumen semanal |
| `/status` | Estado del sistema, DB y freshness |
| `/admin_scrape` | Scrape manual restringido a admin |
| `/admin_refresh_portfolio` | Refresca portfolio y luego muestra `/portfolio` |
| `/ayuda` | Lista de comandos disponibles, si está habilitado |

---

## CLI — Scrape

### `scripts/run_once.py`

```bash
docker compose run --rm scraper python scripts/run_once.py
docker compose run --rm scraper python scripts/run_once.py --full
docker compose run --rm scraper python scripts/run_once.py --no-db
docker compose run --rm scraper python scripts/run_once.py --json output.json
docker compose run --rm scraper python scripts/run_once.py --no-telegram
```

| Flag | Efecto |
|---|---|
| sin flags | Login + scrape portfolio + guardar en DB |
| `--full` | Portfolio + precios de mercado |
| `--no-db` | Scrape sin guardar |
| `--json FILE` | Exportar snapshot como JSON |
| `--no-telegram` | No enviar notificaciones |

---

## CLI — Análisis cuantitativo

### `scripts/run_analysis.py`

```bash
docker compose run --rm scraper python scripts/run_analysis.py
docker compose run --rm scraper python scripts/run_analysis.py --no-llm --no-sentiment
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram
docker compose run --rm scraper python scripts/run_analysis.py --tickers CVX NVDA
docker compose run --rm scraper python scripts/run_analysis.py --period 1y
```

También podés probarlo desde el contenedor del bot:

```bash
docker compose exec telegram_bot python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
```

| Flag | Efecto |
|---|---|
| `--no-llm` | Omite explicación LLM |
| `--no-sentiment` | Omite análisis de noticias |
| `--no-optimizer` | Omite optimizer |
| `--no-telegram` | Imprime por consola |
| `--tickers A B C` | Analiza solo esos tickers |
| `--period 1y` | Período: 1mo, 3mo, 6mo, 1y, 2y |

---

## CLI — Radar de oportunidades

### `scripts/run_opportunity.py`

```bash
docker compose run --rm scraper python scripts/run_opportunity.py
docker compose run --rm scraper python scripts/run_opportunity.py --top 5 --min-score 0.15 --min-rr 1.5
docker compose run --rm scraper python scripts/run_opportunity.py --universe AVGO TSM MSFT AMD
docker compose run --rm scraper python scripts/run_opportunity.py --include-portfolio
docker compose run --rm scraper python scripts/run_opportunity.py --no-telegram
```

| Flag | Efecto |
|---|---|
| `--universe A B C` | Evalúa solo esos tickers |
| `--top N` | Devuelve los N mejores setups |
| `--min-score X` | Score mínimo |
| `--min-rr X` | R/R mínimo |
| `--include-portfolio` | Incluye tickers ya presentes |
| `--period 1y` | Historia de precios |
| `--no-sentiment` | Omite noticias |
| `--no-telegram` | Solo consola |

---

## CLI — Performance

### `scripts/run_performance.py`

```bash
docker compose run --rm scraper python scripts/run_performance.py
docker compose run --rm scraper python scripts/run_performance.py --days 60
docker compose run --rm scraper python scripts/run_performance.py --no-telegram
```

| Flag | Efecto |
|---|---|
| `--days N` | Lookback en días |
| `--no-telegram` | Solo consola |

---

## CLI — Resumen semanal

### `scripts/weekly_summary.py`

```bash
docker compose run --rm scraper python scripts/weekly_summary.py
docker compose run --rm scraper python scripts/weekly_summary.py --weeks-ago 1
docker compose run --rm scraper python scripts/weekly_summary.py --no-telegram
```

Qué muestra:

- P&L estimado por movimiento de precios.
- Variación bruta de cartera.
- Cambios de cantidad.
- Cash inicial/final.
- Mejor y peor posición por precio.

Importante:

```text
La variación bruta incluye compras, ventas y cambios de cantidad.
El P&L por precio es la métrica más limpia para rendimiento semanal.
```

---

## CLI — Outcomes

### `scripts/update_outcomes.py`

Actualiza outcomes a 5, 10 y 20 días de decisiones pasadas.

```bash
docker compose run --rm scraper python scripts/update_outcomes.py
docker compose run --rm scraper python scripts/update_outcomes.py --days 60
```

Recomendación: correrlo periódicamente para mantener `/performance` actualizado.

---

## Scheduler / monitor

El scheduler vive en:

```text
src/scheduler/runner.py
```

Corre:

- Scrape de portfolio.
- Scrape de mercado.
- Loops intradía.
- Risk guard.
- Update outcomes.

Si tu `docker-compose.yml` levanta el servicio `scraper` como scheduler, revisá logs con:

```bash
docker compose logs -f scraper
```

Buscar scheduler en Windows:

```cmd
docker compose logs scraper | findstr /I "Scheduler"
```

---

## Base de datos

Conectarse a Postgres:

```bash
docker exec -it cocos_db psql -U portfolio -d portfolio
```

Consultas útiles:

```sql
SELECT COUNT(*) FROM portfolio_snapshots;
SELECT COUNT(*) FROM positions;
SELECT COUNT(*) FROM market_prices;
SELECT COUNT(*) FROM raw_snapshots;
SELECT COUNT(*) FROM decision_log;

SELECT ticker, decision, final_score, decided_at
FROM decision_log
ORDER BY decided_at DESC
LIMIT 20;
```

Último snapshot:

```sql
SELECT scraped_at, total_value_ars, cash_ars
FROM portfolio_snapshots
ORDER BY scraped_at DESC
LIMIT 5;
```

Últimas decisiones:

```sql
SELECT decided_at, ticker, decision, final_score, outcome_5d, was_correct
FROM decision_log
ORDER BY decided_at DESC
LIMIT 20;
```

---

## Docker — Setup y operación

Setup completo:

```bash
docker compose build --no-cache
docker compose up -d
docker compose ps
```

Rebuild de servicios después de cambiar código:

```bash
docker compose build scraper
docker compose build telegram_bot
docker compose up -d telegram_bot
```

Rebuild fuerte cuando sospechás cache viejo:

```bash
docker compose down
docker compose build --no-cache scraper telegram_bot
docker compose up -d db telegram_bot
```

Logs:

```bash
docker compose logs -f scraper
docker compose logs -f telegram_bot
docker compose logs --tail=100 scraper
docker compose logs --tail=100 telegram_bot
```

Restart:

```bash
docker compose restart telegram_bot
docker compose restart scraper
```

Apagar:

```bash
docker compose down
```

---

## Verificaciones rápidas

Ver si el bot está usando el código nuevo:

```bash
docker compose exec telegram_bot python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
```

Buscar referencias en Windows:

```cmd
findstr /S /I "run_analysis execution_planner Conv Score" scripts\*.py src\*.py
```

Ver contenedores:

```bash
docker compose ps
docker ps -a
```

---

## Backup DB

En Windows CMD:

```cmd
docker exec cocos_db pg_dump -U portfolio portfolio > backup_portfolio.sql
```

Restaurar:

```cmd
type backup_portfolio.sql | docker exec -i cocos_db psql -U portfolio -d portfolio
```

---

## Git — Flujo recomendado

```bash
git branch

git checkout -b dev
git push origin dev
```

Flujo diario:

```bash
git checkout dev
git add .
git commit -m "feat: descripcion del cambio"
git push origin dev
```

Merge a main cuando esté probado:

```bash
git checkout main
git merge dev
git push origin main
git checkout dev
```

Regla:

```text
main = versión estable / demo
dev = desarrollo
```

---

## MFA / TOTP

Si está configurado `COCOS_TOTP_SECRET` en `.env`, el sistema puede generar códigos TOTP automáticamente.

Variables típicas:

```env
COCOS_USERNAME=
COCOS_PASSWORD=
COCOS_TOTP_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
DATABASE_URL=
REDIS_URL=
ADMIN_CHAT_IDS=
```

Nunca subas `.env` al repositorio.

---

## Requirements MVP

Dependencias principales actuales:

```text
playwright
python-telegram-bot
redis
asyncpg
requests
aiohttp
python-dotenv
cryptography
pyotp
pandas
numpy
yfinance
ta
scipy
apscheduler
python-dateutil
pytz
```