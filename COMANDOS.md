# TODOS LOS COMANDOS DEL SISTEMA

## Setup inicial (una sola vez)
```bash
# 1. 
# 2. Rebuild Docker con nuevas dependencias
docker compose build --no-cache scraper

# 3. Levantar todo
docker compose up -d

# 4. Verificar que el scheduler arranco bien
docker compose logs -f scraper
# Debe aparecer: "Scheduler activo: 10:30 ART | 17:00 ART"
```

---

## Scrape manual del portfolio
```bash
# Scrape completo (login + portfolio + guardar en DB + Telegram)
docker compose run --rm scraper python scripts/run_once.py

# Solo portfolio, sin guardar en DB
docker compose run --rm scraper python scripts/run_once.py --no-db

# Portfolio + exportar JSON
docker compose run --rm scraper python scripts/run_once.py --json output.json

# Portfolio + precios de mercado (acciones + CEDEARs)
docker compose run --rm scraper python scripts/run_once.py --full
```

---

## Analisis cuantitativo multicapa
```bash
# Pipeline completo (posiciones del ultimo snapshot)
docker compose run --rm scraper python scripts/run_analysis.py

# Sin Claude API (mas rapido, sin razonamiento LLM)
docker compose run --rm scraper python scripts/run_analysis.py --no-llm

# Sin noticias RSS (mas rapido)
docker compose run --rm scraper python scripts/run_analysis.py --no-sentiment

# Modo rapido: solo tecnico + macro + riesgo
docker compose run --rm scraper python scripts/run_analysis.py --no-llm --no-sentiment

# Solo consola, sin enviar a Telegram
docker compose run --rm scraper python scripts/run_analysis.py --no-telegram

# Tickers especificos
docker compose run --rm scraper python scripts/run_analysis.py --tickers CVX NVDA

# Mas historia para el analisis tecnico
docker compose run --rm scraper python scripts/run_analysis.py --period 1y

# Combinaciones
docker compose run --rm scraper python scripts/run_analysis.py --period 1y --no-llm
docker compose run --rm scraper python scripts/run_analysis.py --tickers CVX --period 3mo --no-telegram
```

---

## Base de datos
```bash
# Conectarse a la DB
docker exec -it cocos_db psql -U portfolio -d portfolio

# Queries utiles (dentro de psql):

# Ver todos los snapshots
SELECT snapshot_id, scraped_at, total_value_ars, cash_ars, confidence_score
FROM portfolio_snapshots ORDER BY scraped_at DESC LIMIT 10;

# Ver posiciones del ultimo snapshot
SELECT * FROM latest_positions;

# Ver evolucion del portfolio
SELECT * FROM portfolio_history;

# Ver el JSON completo del ultimo snapshot
SELECT payload FROM raw_snapshots ORDER BY scraped_at DESC LIMIT 1;

# Salir de psql
\q
```

---

## Scheduler (automatico)
```bash
# Levantar scheduler en background (corre 10:30 y 17:00 ART automaticamente)
docker compose up -d

# Ver logs en vivo
docker compose logs -f scraper

# Ver logs de los ultimos 100 registros
docker compose logs --tail=100 scraper

# Detener todo
docker compose down

# Reiniciar solo el scraper
docker compose restart scraper
```

---

## Mantenimiento
```bash
# Rebuild completo (despues de cambiar codigo)
docker compose build --no-cache scraper
docker compose up -d

# Ver estado de los contenedores
docker compose ps

# Ver uso de disco de la DB
docker exec -it cocos_db psql -U portfolio -d portfolio -c "\l+"

# Backup de la DB
docker exec cocos_db pg_dump -U portfolio portfolio > backup_$(date +%Y%m%d).sql
```

---

## Frecuencia de ejecucion automatica
| Horario | Accion |
|---------|--------|
| 10:30 ART | Scrape portfolio → guarda DB → notifica Telegram |
| 17:00 ART | Scrape portfolio + mercado + pipeline cuantitativo completo → Telegram |
