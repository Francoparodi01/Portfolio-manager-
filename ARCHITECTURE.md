copiloto-inversiones/
│
├── 📦 docker/
│   ├── docker-compose.yml              # Orquestación completa
│   ├── postgres/
│   │   └── init.sql                    # Schema TimescaleDB
│   ├── collector/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── analyzer/
│       ├── Dockerfile
│       └── requirements.txt
│
├── 🔧 config/
│   ├── __init__.py
│   ├── settings.py                     # Config centralizada
│   └── secrets.example.env             # Template de secretos
│
├── 📡 src/collector/
│   ├── __init__.py
│   ├── cocos_scraper.py                # Web scraping (CONSERVAR)
│   ├── collector_service.py            # Orquestador del collector
│   ├── raw_storage.py                  # Persistencia JSON crudo
│   └── schemas.py                      # Validación de datos crudos
│
├── 🗄️ src/data/
│   ├── __init__.py
│   ├── database.py                     # Conexión TimescaleDB
│   ├── models.py                       # SQLAlchemy models
│   ├── normalizer.py                   # Raw → Normalizado
│   ├── repository.py                   # Data access layer
│   └── migrations/                     # Alembic migrations
│       └── versions/
│
├── 📊 src/analyzer/
│   ├── __init__.py
│   ├── risk_metrics.py                 # Volatilidad, Sharpe, Drawdown
│   ├── performance_metrics.py          # Retornos, benchmarks
│   ├── concentration_metrics.py        # HHI, diversificación
│   ├── projections.py                  # Escenarios futuros
│   ├── anomaly_detector.py             # Detección outliers
│   └── health_score.py                 # Score 0-100
│
├── 📈 src/reporter/
│   ├── __init__.py
│   ├── weekly_report.py                # Generador reporte semanal
│   ├── templates/
│   │   ├── report_template.html        # HTML para email/web
│   │   └── report_template.txt         # Texto plano
│   └── exporters/
│       ├── excel_exporter.py           # Excel (CONSERVAR)
│       └── pdf_exporter.py             # PDF (futuro)
│
├── 🚀 src/services/
│   ├── __init__.py
│   ├── scheduler_service.py            # Cron jobs
│   └── health_check.py                 # Monitoring
│
├── 🧪 tests/
│   ├── unit/
│   │   ├── test_collector.py
│   │   ├── test_normalizer.py
│   │   ├── test_metrics.py
│   │   └── test_projections.py
│   ├── integration/
│   │   ├── test_database.py
│   │   └── test_end_to_end.py
│   └── fixtures/
│       └── sample_data.json
│
├── 📜 scripts/
│   ├── setup_database.sh               # Init DB
│   ├── migrate.sh                      # Run migrations
│   ├── run_collector.sh                # Manual collection
│   └── generate_report.sh              # Manual report
│
├── 📁 data/                            # .gitignore
│   ├── raw/                            # JSON crudo (audit)
│   │   └── YYYY/MM/DD/
│   │       └── snapshot_HHMMSS.json
│   ├── exports/                        # Reportes generados
│   └── logs/                           # Application logs
│
├── 📚 docs/
│   ├── architecture.md
│   ├── database_schema.md
│   ├── deployment.md
│   └── api.md
│
├── .env.example                        # Template
├── .gitignore
├── README.md
├── pyproject.toml                      # Poetry config
└── requirements.txt                    # Fallback pip