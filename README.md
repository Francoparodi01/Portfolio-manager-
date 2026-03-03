# Cocos Portfolio System

Sistema cuantitativo personal de gestión de portfolio sobre Cocos Capital.

## Arquitectura

```
cocos_portfolio/
├── src/
│   ├── core/
│   │   ├── config.py          # Configuración centralizada desde .env
│   │   └── logger.py          # Logger estructurado con timestamps
│   ├── collector/
│   │   ├── data/
│   │   │   ├── models.py      # Dataclasses: PortfolioSnapshot, Position, MarketAsset
│   │   │   └── normalizer.py  # parse_decimal, normalize_ticker, DOMFingerprint, ConfidenceResult
│   │   └── cocos_scraper.py   # Scraper principal (Playwright async)
│   └── scheduler/
│       └── runner.py          # APScheduler: 10:30 y 17:00
├── scripts/
│   ├── init_db.py             # Crea tablas en TimescaleDB
│   └── run_once.py            # Ejecución manual
├── .env.example
└── requirements.txt
```

## Setup

```bash
cp .env.example .env
# Editar .env con tus credenciales

pip install -r requirements.txt
playwright install chromium

python scripts/init_db.py        # Inicializar base de datos
python scripts/run_once.py       # Scrape manual
python -m src.scheduler.runner   # Modo scheduler automático
```

## Variables de entorno requeridas

```
COCOS_USERNAME=tu@email.com
COCOS_PASSWORD=tupassword
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/portfolio
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```
