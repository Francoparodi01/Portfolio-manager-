# REFERENCIA DE COMANDOS
### Cocos Copilot — Guía completa de CLI y Telegram

---

## Bot de Telegram

Todos los comandos están disponibles tanto como slash-commands como desde el menú de botones inline que aparece al ejecutar `/start`.

| Comando | Descripción |
|---------|-------------|
| `/start` | Abre el menú principal con botones inline |
| `/portfolio` | Muestra el último snapshot: total, cash, posiciones con P&L y peso |
| `/analisis` | Ejecuta el pipeline cuantitativo completo (~2-3 min) |
| `/analisis_rapido` | Pipeline sin LLM ni sentiment (~30-60 seg) |
| `/scrape` | Scrape manual inmediato del portfolio en Cocos Capital |
| `/oportunidades` | Ejecuta el radar de oportunidades externas (~2-3 min) |
| `/status` | Verifica conectividad de Redis Cloud y base de datos |
| `/ayuda` | Lista completa de comandos |

> Para enviar el código MFA, simplemente mandá los 6 dígitos como mensaje de texto cuando el sistema los solicite.

---

## CLI — Scrape

### run_once.py

```bash
docker compose run --rm scraper python scripts/run_once.py
docker compose run --rm scraper python scripts/run_once.py --no-db
docker compose run --rm scraper python scripts/run_once.py --json output.json
docker compose run --rm scraper python scripts/run_once.py --full
```

| Flag | Efecto |
|------|--------|
| (sin flags) | Login + scrape portfolio + guardar en DB + notificar Telegram |
| `--no-db` | Scrape sin guardar en base de datos |
| `--json FILE` | Exportar snapshot completo como JSON |
| `--full` | Portfolio + precios de mercado (acciones + CEDEARs) |
| `--no-telegram` | No enviar notificaciones Telegram |

---

## CLI — Análisis cuantitativo

### run_analysis.py

```bash
docker compose run --rm scraper python scripts/run_analysis.py
docker compose run --rm scraper python scripts/run_analysis.py --no-llm
docker compose run --rm scraper python scripts/run_analysis.py --no-sentiment
docker compose run --rm scraper python scripts/run_analysis.py --no-llm --no-sentiment
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram
docker compose run --rm scraper python scripts/run_analysis.py --tickers CVX NVDA
docker compose run --rm scraper python scripts/run_analysis.py --period 1y
```

| Flag | Efecto |
|------|--------|
| `--no-llm` | Omite el razonamiento LLM |
| `--no-sentiment` | Omite análisis RSS de noticias |
| `--no-optimizer` | Omite el portfolio optimizer |
| `--no-telegram` | Imprime en consola sin enviar a Telegram |
| `--tickers A B C` | Analiza solo los tickers especificados |
| `--period 1y` | Período de historia: 1mo, 3mo, 6mo, 1y, 2y (default: 6mo) |

---

## CLI — Radar de oportunidades

### run_opportunity.py

```bash
docker compose run --rm scraper python scripts/run_opportunity.py
docker compose run --rm scraper python scripts/run_opportunity.py --no-sentiment
docker compose run --rm scraper python scripts/run_opportunity.py --top 5
docker compose run --rm scraper python scripts/run_opportunity.py --min-score 0.15
docker compose run --rm scraper python scripts/run_opportunity.py --min-rr 1.5
docker compose run --rm scraper python scripts/run_opportunity.py --universe AVGO TSM MSFT AMD INTC
docker compose run --rm scraper python scripts/run_opportunity.py --top 5 --min-score 0.15 --min-rr 1.5 --no-sentiment
docker compose run --rm scraper python scripts/run_opportunity.py --include-portfolio
```

| Flag | Efecto |
|------|--------|
| `--universe A B C` | Evaluar solo estos tickers (default: universo Cocos ~80) |
| `--top N` | Devolver solo los N mejores setups |
| `--min-score X` | Score mínimo para aparecer en el reporte (ej: 0.15) |
| `--min-rr X` | R/R mínimo — elimina asimetrías pobres (ej: 1.5) |
| `--include-portfolio` | Incluir tickers actuales del portfolio en el análisis |
| `--period 1y` | Período de historia de precios (default: 1y) |
| `--no-sentiment` | Omitir análisis RSS |
| `--no-telegram` | Solo consola |

---

## CLI — Base de datos

```bash
# Conectarse
docker exec -it cocos_db psql -U portfolio -d portfolio
```

```sql
-- Últimos snapshots
SELECT snapshot_id, scraped_at, total_value_ars, cash_ars, confidence_score
FROM portfolio_snapshots ORDER BY scraped_at DESC LIMIT 10;

-- Posiciones del último snapshot
SELECT ticker, quantity, current_price, market_value, unrealized_pnl_pct
FROM positions WHERE scraped_at = (SELECT MAX(scraped_at) FROM positions)
ORDER BY market_value DESC;

-- Score promedio histórico por ticker
SELECT ticker, AVG(final_score) as avg_score, COUNT(*) as n
FROM decision_log GROUP BY ticker ORDER BY avg_score DESC;

-- Win rate real por ticker
SELECT ticker, was_correct, COUNT(*)
FROM decision_log WHERE outcome_filled_at IS NOT NULL
GROUP BY ticker, was_correct;

-- JSON completo del último snapshot
SELECT payload FROM raw_snapshots ORDER BY scraped_at DESC LIMIT 1;
```

---

## Docker — Operaciones

```bash
# Setup
docker compose build --no-cache
docker compose up -d
docker compose ps

# Logs
docker compose logs -f scraper
docker compose logs -f telegram_bot
docker compose logs --tail=100 scraper

# Restart
docker compose restart telegram_bot
docker compose restart scraper
docker compose down

# Backup DB
docker exec cocos_db pg_dump -U portfolio portfolio > backup_$(date +%Y%m%d).sql
```

### Test Redis Cloud

```bash
docker compose run --rm telegram_bot python3 -c "
import asyncio, os
import redis.asyncio as redis

async def test():
    r = redis.from_url(os.environ['REDIS_URL'], decode_responses=True)
    await r.set('test', 'ok')
    print('Redis OK:', await r.get('test'))
    await r.delete('test')

asyncio.run(test())
"
```

---

## Scheduler automático

| Horario (ART) | Acción |
|---------------|--------|
| 10:30 | Scrape portfolio → guarda DB → notifica Telegram |
| 17:00 | Scrape portfolio + mercado + pipeline cuantitativo completo → Telegram |

> Verificar con `docker compose logs -f scraper` — debe aparecer: `"Scheduler activo: 10:30 ART | 17:00 ART"`
