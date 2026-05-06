# COCOS COPILOT

### Sistema cuantitativo auditable para gestión de portfolio CEDEARs

*Cocos Capital · Argentina · 2026*

---

## ¿Por qué existe este proyecto?

La mayoría de las herramientas de análisis financiero responden qué está pasando en el mercado, pero no qué hacer con una cartera real.

Cocos Copilot nace para cerrar ese gap.

No busca ser un bot mágico de trading ni prometer predicciones perfectas. Su objetivo es transformar datos reales de portfolio en planes de decisión auditables:

- Qué mantener.
- Qué reducir.
- Qué comprar.
- Qué bloquear.
- Qué dejar en observación.
- Cómo medir si esas decisiones tuvieron edge real.

Cada recomendación queda registrada, explicada y luego evaluada con métricas como IC, EV, win rate y outcomes a distintos horizontes.

---

## ¿Qué es Cocos Copilot?

Cocos Copilot es un asistente cuantitativo personal para gestión de portfolio CEDEARs en Cocos Capital.

Integra scraping automatizado, análisis técnico, contexto macro, gestión de riesgo, optimizer matemático, radar de oportunidades, memoria de decisiones y un bot de Telegram como interfaz operativa.

El sistema responde tres preguntas principales:

- **¿Qué hago con lo que ya tengo?** → Portfolio Analyzer
- **¿Qué oportunidades hay afuera?** → Opportunity Radar
- **¿Cómo roto el capital de forma disciplinada?** → Execution Planner / Rotation Engine

> El núcleo de decisión es cuantitativo. El LLM, si se utiliza, solo explica el resultado y no modifica las decisiones.

---

## Estado actual del MVP

El MVP está enfocado en ser un sistema de decisión auditable, no un sistema de auto-trading.

Actualmente puede:

- Leer el portfolio real desde Cocos Capital.
- Mostrar total cuenta, capital invertido, cash y posiciones.
- Generar análisis semanal multicapa.
- Construir un plan de rotación operativo.
- Bloquear compras con score negativo.
- Bloquear operaciones con señales débiles.
- Separar targets teóricos del optimizer de órdenes ejecutables.
- Calcular IC, EV, win rate y outcomes históricos.
- Mostrar radar compacto de oportunidades externas.
- Operar desde Telegram y desde CLI.
- Mantener datos frescos con scheduler y monitoreo intradía.

El sistema no solo recomienda operaciones: también puede decir **no operar** cuando la señal no justifica el riesgo.

---

## Qué lo hace distinto

- No obedece ciegamente al optimizer.
- Todo target teórico pasa por una capa de ejecución.
- Cada decisión se clasifica como ejecutable, bloqueada, watch o hold.
- El reporte diferencia entre score, señal operativa y alineación de capas.
- El IC histórico ayuda a interpretar si el modelo está mostrando poder predictivo.
- Cada decisión se registra para medir resultados reales.
- El portfolio actual se muestra limpio: total cuenta, invertido, cash y pesos; el P&L se evalúa en `/performance`.

Ejemplo:

```text
Optimizer sugiere aumentar AMD.
Score: +0.048 → señal neutral / ruido.
Resultado: WATCH, no compra ejecutable.
```

El objetivo no es operar más. El objetivo es operar mejor y medir si existe edge real.

---

## Flujo del sistema

```text
Scraper
  ↓
Portfolio Snapshot
  ↓
Análisis técnico + macro + riesgo + sentiment
  ↓
Síntesis multicapa
  ↓
Optimizer
  ↓
Execution Planner
  ↓
Plan ejecutable / bloqueos / watch
  ↓
Decision Memory
  ↓
Performance: IC, EV, win rate, outcomes
```

---

## Comandos principales

Desde Telegram:

```text
/portfolio      Estado actual de cartera
/analisis       Análisis semanal y plan de decisión
/radar          Radar de oportunidades externas
/performance    Win rate, EV y outcomes del sistema
/status         Estado del sistema y conectividad
/resumen        Resumen semanal de performance por precio
/admin_scrape   Scrape manual restringido a admin
```

También puede ejecutarse desde CLI con Docker Compose.

Ver detalle completo en [`COMANDOS.md`](./COMANDOS.md).

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Orquestación | Docker Compose |
| Scraper | Playwright / Chromium headless |
| Base de datos | TimescaleDB / PostgreSQL |
| Bot | python-telegram-bot |
| Comunicación | Redis |
| Datos de mercado | yfinance + APIs locales |
| Análisis | pandas, numpy, ta |
| Optimización | scipy / numpy |
| Scheduler | APScheduler |
| MFA | pyotp / TOTP |
| Lenguaje | Python |

---

## Instalación rápida

```bash
git clone <repo>
cd cocos_copilot

cp .env.example .env
# completar variables del entorno

docker compose build --no-cache
docker compose up -d
```

Ver estado:

```bash
docker compose ps
docker compose logs -f telegram_bot
docker compose logs -f scraper
```

Ejecutar análisis rápido por consola:

```bash
docker compose exec telegram_bot python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
```

---

## Documentación

- [`ARQUITECTURA.md`](./ARQUITECTURA.md) — diseño técnico, pipelines, guards, DB y estructura.
- [`COMANDOS.md`](./COMANDOS.md) — comandos de Telegram, CLI, Docker, Git y mantenimiento.

---

## Limitaciones actuales

- No ejecuta órdenes automáticamente.
- No valida fills reales del broker.
- La muestra estadística todavía es limitada.
- Las reglas de salida y position sizing siguen madurando.
- Algunas decisiones históricas pertenecen a versiones previas del sistema actual de guards.
- La capa ML (`feature_builder.py` / `ml_model.py`) queda como experimental/post-MVP.
- El proyecto está diseñado para uso personal y validación técnica.

---

## Disclaimer

Cocos Copilot es un proyecto personal de disciplina cuantitativa. No es asesoramiento financiero ni un producto para terceros.

Su objetivo es validar si un sistema basado en datos puede generar decisiones trazables, medibles y auditables sobre una cartera real.
