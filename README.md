# 🤖 Copiloto Inteligente de Inversiones - Cocos Capital

Sistema automatizado, seguro y escalable para análisis estratégico de inversiones de largo plazo.

![Version](https://img.shields.io/badge/version-2.0.0-blue)
![Python](https://img.shields.io/badge/python-3.11+-green)
![License](https://img.shields.io/badge/license-MIT-blue)
![Status](https://img.shields.io/badge/status-production-success)

---

## 📋 Tabla de Contenidos

- [Visión General](#-visión-general)
- [Filosofía](#-filosofía)
- [Características](#-características-principales)
- [Arquitectura](#-arquitectura)
- [Instalación](#-instalación)
- [Uso](#-uso)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Configuración](#-configuración)
- [Métricas y Análisis](#-métricas-y-análisis)
- [Seguridad](#-seguridad)
- [FAQ](#-faq)

---

## 🎯 Visión General

Sistema profesional que transforma datos operativos del broker **Cocos Capital** en información estratégica para toma de decisiones financieras disciplinadas y basadas en datos.

### **Objetivo Principal**

Proveer análisis continuo del portfolio con enfoque en:
- ✅ Crecimiento sostenible
- ✅ Control de riesgo
- ✅ Escenarios probabilísticos
- ✅ Auditabilidad total
- ✅ Decisiones informadas (no automatizadas)

### **Lo que NO es**

Este sistema:
- ❌ No ejecuta órdenes de compra/venta
- ❌ No hace trading automático
- ❌ No promete "timing perfecto" del mercado
- ❌ No usa deep learning para predicciones mágicas
- ❌ No expone credenciales ni opera sin supervisión

Es una **herramienta de disciplina financiera**, no un bot de trading.

---

## 🧠 Filosofía

| Principio | Implementación |
|-----------|----------------|
| **Métricas robustas** > predicciones puntuales | Volatilidad, Sharpe, Drawdown calculados con ventanas móviles |
| **Escenarios probabilísticos** > certezas falsas | 5 escenarios (mejor caso, optimista, base, pesimista, estrés) |
| **Largo plazo** > especulación | Proyecciones a 4 y 12 semanas, no day-trading |
| **Arquitectura modular** > scripts sueltos | Separación en capas: Collector → Storage → Analyzer → Reporter |
| **Auditabilidad** > caja negra | Raw data + normalized data separados |

---

## ✨ Características Principales

### **📡 Recolección Automática**
- Web scraping de Cocos Capital
- Login con MFA via Telegram Bot
- Snapshots diarios post-cierre (17:30)
- Almacenamiento de raw data (audit trail)

### **📊 Análisis Avanzado**
- **Métricas de Riesgo:**
  - Volatilidad anualizada
  - Max Drawdown
  - Sharpe Ratio
  - Value at Risk (VaR 95%)

- **Métricas de Performance:**
  - Retornos (diarios, semanales, anualizados)
  - Top Winners/Losers
  - Benchmarking

- **Métricas de Concentración:**
  - HHI Index
  - Diversificación
  - Alertas de sobre-concentración

### **🔮 Proyecciones Multi-Escenario**
Proyecciones a **4 y 12 semanas** con:
- **Mejor caso** (+2σ): 2.5% probabilidad
- **Optimista** (+1σ): 16% probabilidad
- **Base** (tendencia): 50% probabilidad
- **Pesimista** (-1σ): 16% probabilidad
- **Estrés** (-2σ): 2.5% probabilidad

### **📈 Reportes Semanales**
- Resumen ejecutivo (3 puntos clave)
- Estado del patrimonio
- Proyecciones outlook
- Alertas activas
- Recomendaciones priorizadas
- Exportación: Excel + JSON + TXT

### **💊 Health Score (0-100)**
- Diversificación (30%)
- Performance (40%)
- Control de Riesgo (30%)
- Clasificación: EXCELENTE / BUENO / ACEPTABLE / NECESITA_MEJORA

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                COPILOTO INTELIGENTE v2.0                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  📊 PRESENTATION LAYER                                      │
│     └── Weekly Reporter                                     │
│         ├── Excel Exporter                                  │
│         ├── JSON (para IA/ML)                               │
│         └── TXT (para email)                                │
│                                                             │
│  🧠 BUSINESS LOGIC LAYER                                    │
│     ├── Risk Metrics (volatility, sharpe, drawdown)        │
│     ├── Performance Metrics (returns, benchmarks)          │
│     ├── Concentration Metrics (HHI, diversification)       │
│     ├── Projections (5 escenarios multi-horizonte)         │
│     ├── Anomaly Detector (outliers)                        │
│     └── Health Score (0-100)                                │
│                                                             │
│  🔄 DATA TRANSFORMATION LAYER                               │
│     └── Normalizer (Raw → Structured)                      │
│                                                             │
│  💾 DATA ACCESS LAYER                                       │
│     └── Repository Pattern                                  │
│         └── SQLAlchemy ORM                                  │
│                                                             │
│  🗄️ DATA PERSISTENCE LAYER                                 │
│     ├── TimescaleDB (time-series optimized)                │
│     │   ├── Hypertables                                     │
│     │   ├── Continuous Aggregates                           │
│     │   └── Retention Policies                              │
│     └── Raw Storage (JSON + HTML audit trail)              │
│                                                             │
│  📡 DATA ACQUISITION LAYER                                  │
│     ├── Collector Service (orchestrator)                    │
│     ├── Cocos Scraper (Selenium WebDriver)                 │
│     └── Telegram MFA Handler                                │
│                                                             │
│  🏭 INFRASTRUCTURE LAYER                                    │
│     ├── Docker Compose                                      │
│     ├── TimescaleDB Container                               │
│     ├── Scheduler (cron/Task Scheduler)                     │
│     └── Telegram Bot (notificaciones)                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### **Flujo de Datos**

```
1️⃣ RECOLECCIÓN (Diaria - 17:30)
   Scheduler → Collector Service
       ↓
   Login (MFA via Telegram) → Scrape Portfolio
       ↓
   Raw Storage (JSON audit) → Normalizer
       ↓
   TimescaleDB (portfolio_snapshots + positions)

2️⃣ ANÁLISIS (Semanal - Viernes 18:00)
   Scheduler → Weekly Reporter
       ↓
   Query TimescaleDB → Calculate Metrics
       ↓
   Risk + Performance + Projections + Health
       ↓
   Export (Excel + JSON + TXT)

3️⃣ NOTIFICACIÓN
   Telegram Bot → Usuario
       ↓
   "✅ Snapshot exitoso: $884,235.00"
   "📊 Reporte semanal generado"
```

---

## 🚀 Instalación

### **Prerrequisitos**

- Python 3.11+
- Docker + Docker Compose
- Google Chrome
- Cuenta en Cocos Capital
- Bot de Telegram (opcional pero recomendado para MFA)

### **1️⃣ Clonar repositorio**

```bash
git clone <repo-url>
cd cocos_copilot
```

### **2️⃣ Crear entorno virtual**

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### **3️⃣ Instalar dependencias**

```bash
pip install -r requirements.txt
```

### **4️⃣ Configurar Telegram Bot (MFA)**

1. Abrir Telegram → Buscar `@BotFather`
2. Enviar: `/newbot`
3. Nombre: `Cocos Copiloto`
4. Copiar el **TOKEN**
5. Enviar mensaje al bot
6. Obtener **CHAT_ID**:

```bash
python scripts/get_telegram_chat_id.py <TU_BOT_TOKEN>
```

### **5️⃣ Configurar variables de entorno**

Crear archivo `.env`:

```bash
# Cocos Capital
COCOS_EMAIL=tu_email@gmail.com
COCOS_PASSWORD=tu_password

# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/cocos_inversiones

# Telegram (MFA)
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789

# Paths
RAW_DATA_PATH=data/raw

# Options
HEADLESS=false  # true en producción
```

### **6️⃣ Levantar TimescaleDB**

```bash
docker-compose up -d
```

Verificar:
```bash
docker ps
docker exec -it cocos_db psql -U postgres -d cocos_inversiones -c "\dt"
```

### **7️⃣ Primera ejecución**

```bash
python run_collector.py
```

**En Telegram recibirás:**
```
🔐 CÓDIGO MFA REQUERIDO
Por favor envía el código de 6 dígitos
```

Envía el código → Sistema completa login y scraping.

---

## 🎮 Uso

### **Recolección Manual**

```bash
python run_collector.py
```

### **Generar Reporte**

```bash
python -m src.reporter.weekly_report
```

### **Automatización (Recomendado)**

#### **Windows - Task Scheduler:**

1. Abrir Task Scheduler
2. Create Task → `Cocos Collector`
3. Trigger: **Lunes-Viernes 17:30**
4. Action: `C:\ruta\venv\Scripts\python.exe run_collector.py`
5. Start in: `C:\ruta\cocos_copilot`

Repetir para reporte semanal (Viernes 18:00).

#### **Linux/Mac - Cron:**

```bash
crontab -e

# Snapshots diarios
30 17 * * 1-5 cd /ruta/cocos_copilot && /ruta/venv/bin/python run_collector.py

# Reporte semanal
0 18 * * 5 cd /ruta/cocos_copilot && /ruta/venv/bin/python -m src.reporter.weekly_report
```

---

## 📁 Estructura del Proyecto

```
cocos_copilot/
│
├── 📡 src/
│   ├── collector/              # Recolección de datos
│   │   ├── cocos_scraper.py           # Selenium scraper
│   │   ├── collector_service.py       # Orquestador
│   │   ├── raw_storage.py             # JSON audit trail
│   │   └── data/
│   │       ├── models.py              # SQLAlchemy models
│   │       ├── normalizer.py          # Raw → Normalized
│   │       └── repository.py          # Data access layer
│   │
│   ├── analyzer/               # Análisis de métricas
│   │   ├── risk_metrics.py            # Volatilidad, Sharpe, Drawdown
│   │   ├── performance_metrics.py     # Retornos, benchmarks
│   │   ├── concentration_metrics.py   # HHI, diversificación
│   │   ├── projections.py             # Escenarios futuros
│   │   ├── health_score.py            # Score 0-100
│   │   └── run_analyzer.py            # Script ejecutable
│   │
│   ├── reporter/               # Generación de reportes
│   │   ├── weekly_report.py           # Reporte semanal
│   │   ├── exporters/
│   │   │   └── excel_exporter.py      # Export Excel
│   │   ├── reports_output/            # Reportes generados
│   │   └── storage/
│   │
│   ├── notifier/               # Notificaciones
│   │   └── telegram_bot.py            # Bot Telegram
│   │
│   └── config/                 # Configuración
│       └── settings.py                # Settings centralizados
│
├── 🐳 docker/
│   ├── docker-compose.yml             # Orquestación
│   └── init.sql                       # Schema TimescaleDB
│
├── 📜 scripts/
│   ├── get_telegram_chat_id.py       # Setup Telegram
│   └── migrate_sqlite_to_timescale.py # Migración datos
│
├── 💾 data/                    # .gitignore
│   ├── raw/                           # JSON snapshots
│   │   └── YYYY/MM/DD/*.json
│   ├── exports/                       # Reportes Excel/PDF
│   └── logs/                          # Application logs
│
├── 📚 docs/
│   ├── ARCHITECTURE.md                # Arquitectura detallada
│   ├── API.md                         # Referencia API
│   └── DEPLOYMENT.md                  # Guía deployment
│
├── 🧪 tests/
│   ├── unit/
│   └── integration/
│
├── .env.example                       # Template config
├── .gitignore
├── docker-compose.yml
├── init.sql
├── requirements.txt
├── run_collector.py                   # ← Entry point collector
└── README.md                          # ← Este archivo
```

---

## ⚙️ Configuración

### **Variables de Entorno (.env)**

```bash
# ============================================
# COCOS CAPITAL
# ============================================
COCOS_EMAIL=tu_email@cocos.com
COCOS_PASSWORD=tu_password_seguro

# ============================================
# DATABASE
# ============================================
DATABASE_URL=postgresql://postgres:password@localhost:5432/cocos_inversiones

# ============================================
# TELEGRAM (MFA + Notificaciones)
# ============================================
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789

# ============================================
# PATHS
# ============================================
RAW_DATA_PATH=data/raw
EXPORTS_PATH=data/exports
LOGS_PATH=data/logs

# ============================================
# OPTIONS
# ============================================
HEADLESS=false              # Chrome headless mode
LOG_LEVEL=INFO              # DEBUG | INFO | WARNING | ERROR
TIMEZONE=America/Argentina/Buenos_Aires

# ============================================
# SCHEDULER
# ============================================
SNAPSHOT_TIME=17:30         # Hora de snapshot diario
REPORT_DAY=5                # Día reporte semanal (5=Viernes)
REPORT_TIME=18:00
```

### **Configuración de TimescaleDB**

El archivo `init.sql` crea:
- ✅ Extensión TimescaleDB
- ✅ Tablas: `portfolio_snapshots`, `positions`, `raw_snapshots`
- ✅ Hypertables (particionado por tiempo)
- ✅ Continuous Aggregates (pre-agregación)
- ✅ Retention Policies (auto-delete datos viejos)
- ✅ Compression Policies (compresión automática)

---

## 📊 Métricas y Análisis

### **Risk Metrics**

| Métrica | Descripción | Fórmula |
|---------|-------------|---------|
| **Volatilidad** | Variabilidad anualizada | σ_daily × √252 |
| **Max Drawdown** | Pérdida máxima desde pico | (Trough - Peak) / Peak |
| **Sharpe Ratio** | Retorno ajustado por riesgo | (R - Rf) / σ × √252 |
| **VaR 95%** | Pérdida máxima esperada (95% confianza) | Percentil 5 |

### **Projections (Multi-Escenario)**

Proyecciones estadísticas basadas en:
- Regresión lineal de tendencia
- Distribución normal de retornos históricos
- Ventana móvil de 90 días

**Ejemplo de Output:**
```
PROYECCIONES A 4 SEMANAS:

Valor Actual: $884,235.00

Escenarios:
  Mejor Caso:  $952,450 (+7.71%)   - Probabilidad: 2.5%
  Optimista:   $915,320 (+3.52%)   - Probabilidad: 16%
  Base:        $891,100 (+0.78%)   - Probabilidad: 50%
  Pesimista:   $865,200 (-2.15%)   - Probabilidad: 16%
  Estrés:      $825,800 (-6.60%)   - Probabilidad: 2.5%

Interpretación:
- Rango probable (68%): $865K - $915K
- Prepararse para estrés: hasta $826K
```

### **Health Score**

Score de **0-100** que evalúa:

```
Score = (Diversificación × 0.3) + (Performance × 0.4) + (Riesgo × 0.3)

Diversificación:
  HHI < 0.15 → 100 puntos
  HHI < 0.25 → 80 puntos
  HHI < 0.35 → 60 puntos
  HHI > 0.35 → 40 puntos

Performance:
  Retorno > 20% anual → 100 puntos
  Retorno > 10% anual → 80 puntos
  Retorno > 5% anual → 60 puntos
  Retorno > 0% anual → 40 puntos

Control de Riesgo:
  Sharpe > 1 → 50 puntos
  Drawdown < 15% → 50 puntos
```

**Clasificación:**
- **80-100**: EXCELENTE
- **60-79**: BUENO
- **40-59**: ACEPTABLE
- **<40**: NECESITA_MEJORA

---

## 🔐 Seguridad

### **Principios**

1. ✅ **Solo lectura**: No ejecuta órdenes en Cocos
2. ✅ **Secrets en archivos**: No hardcoding de passwords
3. ✅ **Local-first**: Datos en tu PC, no cloud
4. ✅ **Auditabilidad**: Raw data preservado
5. ✅ **MFA via Telegram**: Códigos 2FA seguros

### **Implementación**

```bash
# Secrets con permisos restrictivos
chmod 600 .env

# .gitignore
.env
secrets/
data/
*.log
```

### **Telegram MFA Flow**

```
1. Sistema detecta MFA requerido
2. Envía mensaje Telegram: "🔐 Código MFA requerido"
3. Usuario responde: "123456"
4. Sistema captura código
5. Completa login automáticamente
6. Notifica: "✅ Login exitoso"
```

---

## 🐛 Troubleshooting

### **Error: Login falló**

```bash
# Verificar credenciales
cat .env | grep COCOS_EMAIL

# Verificar Telegram configurado
cat .env | grep TELEGRAM_BOT_TOKEN

# Ejecutar en modo visible (no headless)
# En .env: HEADLESS=false
```

### **Error: Database connection refused**

```bash
# Verificar DB corriendo
docker ps | grep cocos_db

# Ver logs
docker logs cocos_db

# Reiniciar
docker-compose restart
```

### **Error: No se recibió código MFA**

```bash
# Verificar bot Telegram respondiendo
# Enviar mensaje manual al bot

# Ver logs
tail -f data/logs/collector.log

# Timeout default: 120 segundos
# Extender en collector_service.py si necesario
```

---

## 📈 Roadmap

### **Fase 3: Machine Learning** (Futuro)
- [ ] Modelos predictivos calibrados
- [ ] Optimización de portfolio con RL
- [ ] Sentiment analysis de noticias
- [ ] Backtesting automatizado

### **Fase 4: Dashboard Web** (Futuro)
- [ ] FastAPI backend
- [ ] React frontend
- [ ] Real-time updates (WebSockets)
- [ ] Mobile responsive

### **Fase 5: Multi-broker** (Futuro)
- [ ] Soporte para otros brokers (IOL, PPI, etc)
- [ ] Consolidación multi-cuenta
- [ ] Benchmarking cross-broker

---

## ❓ FAQ

### **¿Puedo usar esto sin Docker?**

Sí, pero necesitas instalar PostgreSQL + TimescaleDB manualmente. Docker simplifica el deployment.

### **¿Funciona en Mac/Linux?**

Sí. Ajustar paths en comandos (usar `/` en vez de `\`).

### **¿Cuántos datos históricos necesito?**

Mínimo **30 días** para métricas robustas. Ideal: **90+ días**.

### **¿Puedo agregar otros brokers?**

Sí. Crear nuevo scraper en `src/collector/` siguiendo patrón de `cocos_scraper.py`.

### **¿El bot ejecuta órdenes de compra/venta?**

**NO**. Solo genera recomendaciones. Las decisiones son manuales y tuyas.

---

## 👨‍💻 Autor

Sistema diseñado desde la perspectiva de **Ingeniería de Software** e **Ingeniería de Requerimientos**, con enfoque en:
- Arquitectura modular y escalable
- Buenas prácticas de desarrollo
- Seguridad y auditabilidad
- Disciplina financiera

---

**⭐ Si este proyecto te resulta útil, considera darle una estrella en GitHub!**

---

*Última actualización: Febrero 2026*
