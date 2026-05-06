# COCOS COPILOT

### Sistema cuantitativo auditable para gestión de portfolio CEDEARs

*Cocos Capital · Argentina · 2026*

---

## 🚀 ¿Por qué existe este proyecto?

La mayoría de las herramientas de análisis financiero responden qué está pasando en el mercado, pero no qué hacer con una cartera real.

**Cocos Copilot nace para cerrar ese gap.**

No busca ser un bot mágico de trading ni prometer predicciones perfectas. Su objetivo es transformar datos reales de portfolio en planes de decisión auditables:

- Qué mantener.
- Qué reducir.
- Qué comprar.
- Qué bloquear.
- Qué dejar en observación.
- Cómo medir si esas decisiones tuvieron edge real.

Cada recomendación queda registrada, explicada y luego evaluada con métricas como IC, EV, win rate y outcomes a distintos horizontes.

> Cocos Copilot no intenta operar más; intenta operar mejor. Y cuando la señal no alcanza, también sabe no operar.

---

## 🧠 ¿Qué es Cocos Copilot?

Cocos Copilot es un **asistente cuantitativo personal** para gestión de portfolio CEDEARs en Cocos Capital.

Integra scraping automatizado, análisis técnico, contexto macro, análisis de riesgo, radar de oportunidades, optimización matemática y memoria de decisiones en un flujo operativo único.

El sistema responde tres preguntas fundamentales:

- **¿Qué hago con lo que ya tengo?** → Portfolio Analyzer.
- **¿Qué oportunidades hay afuera?** → Opportunity Radar.
- **¿Cómo roto el capital de forma disciplinada?** → Rotation Engine + Execution Planner.

El núcleo de decisión es cuantitativo. El LLM, si se utiliza, cumple únicamente un rol explicativo: no modifica decisiones, scores ni órdenes sugeridas.

---

## 🎯 Objetivo del proyecto

Cocos Copilot busca validar si una cartera gestionada con reglas cuantitativas puede construir una ventaja estadística real.

El objetivo no es solamente analizar activos, sino cerrar el ciclo completo:

```text
Datos → Señales → Optimizer → Guards → Plan ejecutable → Registro → Outcome → Métrica
```

Cada decisión queda asociada a:

- Score cuantitativo.
- Señal operativa.
- Alineación entre capas.
- Peso actual y peso objetivo.
- Monto teórico y monto ejecutable.
- Motivo de ejecución, bloqueo o espera.
- Resultado posterior a 5, 10 y 20 días.

El foco principal no es operar constantemente, sino validar si el sistema tiene **edge real**.

---

## 🧩 ¿Qué lo hace distinto?

Cocos Copilot no es solo un sistema de análisis. Es una capa de decisión auditable.

Sus diferencias principales:

- No obedece ciegamente al optimizer.
- Separa targets teóricos de órdenes ejecutables.
- Puede bloquear compras con score negativo o débil.
- Puede bloquear ventas si la señal del activo sigue siendo positiva.
- Diferencia entre `Score`, `Señal` y `Alineación de capas`.
- Registra cada recomendación y la evalúa luego con datos reales.
- Expone por qué una acción se ejecuta, se bloquea o queda en observación.

Una recomendación puede terminar como:

```text
BUY      → compra ejecutable
SELL     → venta ejecutable
HOLD     → mantener posición
WATCH    → señal interesante, pero no operable todavía
BLOCKED  → operación bloqueada por guardias de calidad o riesgo
```

---

## 🛡️ Execution Planner y guardias de calidad

El optimizer genera pesos objetivo teóricos, pero esos targets no se ejecutan directamente.

Antes de transformarse en una acción operativa, cada cambio pasa por una capa de validación llamada **Execution Planner**.

Esta capa aplica guardias como:

### BUY_SCORE_GUARD

Bloquea compras con score negativo.

```text
Optimizer sugiere comprar XOM.
Score: -0.122.
Resultado: BLOCKED.
```

### TRADE_QUALITY_GUARD

Evita operar señales débiles o ruido estadístico.

```text
Optimizer sugiere aumentar AMD.
Score: +0.048.
Señal: NEUTRAL / RUIDO.
Resultado: WATCH, no compra ejecutable.
```

### SELL_PROTECTION_GUARD

Evita reducir una posición cuando el scorer muestra una señal positiva y no hay concentración excesiva.

```text
Optimizer sugiere reducir NVDA.
Score: +0.094.
Señal: POSITIVA OPERABLE.
Resultado: HOLD.
```

### IC Regime

Interpreta el Information Coefficient reciente del sistema.

Si el IC viene negativo, el reporte lo marca como régimen de cautela y el sistema evita operar señales débiles.

```text
IC 5d: -0.173
Régimen IC: CAUTELA ALTA
Acción: mantener y observar
```

El objetivo de estas guardias no es hacer el sistema más conservador por defecto, sino evitar overtrading cuando la señal no justifica costos, slippage ni riesgo.

---

## ⚙️ ¿Cómo funciona? Alto nivel

El sistema se organiza en cinco capas principales:

### 1. Data Layer

- Scraping del portfolio real en Cocos Capital.
- Persistencia de snapshots históricos.
- Captura de posiciones, cash y valor total de cuenta.
- Registro de precios y datos de mercado.

### 2. Analysis Layer

Combina distintas fuentes de señal:

- Técnico: tendencia, momentum, volatilidad y volumen.
- Macro: VIX, S&P 500, petróleo, tasas, DXY y variables locales.
- Riesgo: volatilidad, drawdown, concentración y sizing.
- Sentiment: capa opcional para contexto externo.

### 3. Optimization Layer

El optimizer genera pesos objetivo teóricos usando modelos como:

- Black-Litterman.
- Min Variance fallback.
- Restricciones por riesgo y concentración.

### 4. Execution Planner

Convierte los targets teóricos en un plan operativo real:

- Reconciliación de cash.
- Ventas primero, compras después.
- Cálculo de fees y slippage.
- Bloqueo de operaciones incoherentes.
- Separación entre órdenes ejecutables, WATCH y BLOCKED.

### 5. Evaluation Layer

Cada decisión se registra y luego se evalúa con métricas como:

- Information Coefficient (IC).
- Expected Value (EV).
- Win rate.
- Outcomes a 5, 10 y 20 días.
- Equity curve.
- Max drawdown.

---

## 🧱 Componentes principales

### Scraper automatizado

Automatiza la lectura del portfolio real en Cocos Capital mediante Playwright.

Incluye soporte para login, MFA y captura de snapshots históricos.

### Portfolio Analyzer

Analiza las posiciones actuales y genera una lectura cuantitativa por activo.

Evalúa:

- Score final.
- Señal operativa.
- Peso actual.
- Peso objetivo.
- Riesgo de concentración.
- Acción sugerida.

### Opportunity Radar

Escanea el universo disponible de CEDEARs y detecta oportunidades externas.

Clasifica candidatos en categorías como:

- Compra fuerte.
- En vigilancia.
- En observación.
- No operable.

### Risk Engine

Evalúa riesgo individual y de portfolio.

Incluye:

- Volatilidad anualizada.
- Sharpe estimado.
- Gates operativos: `NORMAL`, `CAUTIOUS`, `BLOCKED`.
- Control de concentración.
- Sizing sugerido.

### Portfolio Optimizer

Genera pesos objetivo teóricos según condiciones de mercado y restricciones del portfolio.

Importante: estos pesos son informativos. No se transforman automáticamente en órdenes.

### Execution Planner

Es la capa que decide si una operación es realmente ejecutable.

Puede convertir una sugerencia del optimizer en:

- Orden ejecutable.
- HOLD.
- WATCH.
- BLOCKED.

### Decision Memory

Registra decisiones y resultados para medir si el sistema realmente tiene edge.

Guarda:

- Ticker.
- Decisión.
- Score.
- Confidence / alineación.
- Precio al momento de decisión.
- Stop loss.
- Target.
- Horizonte.
- Outcomes.
- Resultado correcto o incorrecto.

### Telegram Bot

Interfaz operativa principal del sistema.

Comandos principales:

```text
/portfolio    → estado actual de cartera
/analisis     → análisis semanal y plan de decisión
/radar        → oportunidades externas
/performance  → EV, win rate y outcomes reales
/status       → estado del sistema y frescura de datos
```

---

## 📊 Ejemplo de decisión bloqueada

```text
Optimizer:
  Comprar AMD: +$60.491 ARS
  Peso: 18.3% → 21.3%

Scorer:
  Score: +0.048
  Señal: NEUTRAL / RUIDO

Execution Planner:
  Acción final: WATCH
  Motivo: BUY requiere score >= +0.08

Resultado:
  No se ejecuta compra.
```

Este comportamiento es central para el proyecto: el sistema no solo recomienda, también sabe frenar.

---

## 📈 Métricas de evaluación

Cocos Copilot evalúa sus decisiones con métricas cuantitativas.

### Information Coefficient (IC)

Mide la relación entre score asignado y resultado posterior.

```text
IC > 0 → el score tiene poder predictivo positivo.
IC < 0 → el score no está prediciendo correctamente en ese período.
```

### Expected Value (EV)

Evalúa si la expectativa matemática del sistema es positiva.

```text
EV = (win_rate × avg_win) − (loss_rate × avg_loss)
```

### Win Rate

Porcentaje de decisiones correctas según dirección esperada.

### Equity Curve

Simulación acumulada del desempeño de las decisiones registradas.

### Outcomes

Seguimiento de cada decisión a distintos horizontes:

- 5 días.
- 10 días.
- 20 días.

---

## 🧰 Stack tecnológico

| Componente | Tecnología |
|---|---|
| Lenguaje principal | Python |
| Orquestación | Docker Compose |
| Scraper | Playwright / Chromium headless |
| Base de datos | TimescaleDB / PostgreSQL |
| Bot | python-telegram-bot |
| Datos de mercado | yfinance + APIs locales |
| Análisis | pandas / numpy / ta |
| Optimización | scipy / numpy |
| Scheduler | APScheduler |
| Runtime | Docker |

---

## 📁 Arquitectura conceptual

```text
cocos_copilot/
│
├── scripts/
│   ├── run_analysis.py        # Pipeline cuantitativo principal
│   ├── telegram_bot.py        # Interfaz Telegram
│   └── run_performance.py     # Métricas y outcomes
│
├── src/
│   ├── collector/
│   │   ├── db.py              # TimescaleDB / PostgreSQL
│   │   └── scraper.py         # Scraping portfolio
│   │
│   ├── analysis/
│   │   ├── technical.py       # Indicadores técnicos
│   │   ├── macro.py           # Contexto macro
│   │   ├── risk.py            # Risk engine
│   │   ├── synthesis.py       # Score multicapa
│   │   ├── optimizer.py       # Portfolio optimizer
│   │   ├── execution_planner.py
│   │   └── validators.py
│   │
│   └── core/
│       ├── config.py
│       └── logger.py
│
├── docker-compose.yml
├── init.sql
└── README.md
```

---

## ⚠️ Estado actual del MVP

El sistema se encuentra en fase de validación real.

Actualmente ya puede:

- Leer portfolio real desde Cocos Capital.
- Separar correctamente invertido, cash y valor total de cuenta.
- Generar análisis semanal con plan operativo.
- Bloquear compras con score negativo.
- Bloquear operaciones con señal débil.
- Marcar operaciones como WATCH o BLOCKED.
- Calcular IC, EV, win rate y outcomes.
- Mostrar un radar compacto de oportunidades.
- Ejecutarse desde Docker y Telegram.

Limitaciones actuales:

- No ejecuta órdenes automáticamente.
- No valida todavía fills reales de mercado.
- La muestra estadística todavía es chica.
- Las reglas de salida, stop y target siguen madurando.
- Algunas métricas históricas pueden incluir decisiones legacy previas a las guardias actuales.
- El sistema está diseñado para uso personal y experimental.

---

## 🚀 Instalación rápida

```bash
git clone <repo>
cd cocos_copilot

cp .env.example .env
# completar variables de entorno

docker compose build --no-cache
docker compose up -d
```

Ver logs:

```bash
docker compose logs -f telegram_bot
```

Ejecutar análisis manual:

```bash
docker compose exec telegram_bot python scripts/run_analysis.py --no-telegram --no-llm --no-sentiment
```

Ejecutar performance:

```bash
docker compose exec telegram_bot python scripts/run_performance.py
```

---

## 🧪 Comandos principales

Desde Telegram:

```text
/portfolio
/analisis
/radar
/performance
/status
```

### `/portfolio`

Muestra:

- Total cuenta.
- Capital invertido.
- Cash disponible.
- Posiciones actuales.
- Peso por activo.
- Estado de concentración.

### `/analisis`

Genera:

- Resumen ejecutivo.
- Information Coefficient.
- Acción principal.
- Resultado esperado.
- Plan de rotación.
- WATCH / BLOCKED por guardias.
- Contexto macro.
- Optimizer informativo.
- Radar externo compacto.
- Veredicto final.

### `/radar`

Muestra oportunidades externas del universo Cocos.

### `/performance`

Evalúa si las decisiones pasadas tuvieron edge real.

Incluye:

- Win rate.
- EV.
- Avg win / avg loss.
- Retornos por horizonte.
- Últimas decisiones.
- Equity curve.

### `/status`

Muestra estado operativo del sistema, frescura de datos y monitor.

---

## 🛣️ Roadmap

### MVP actual

- Portfolio limpio y auditable.
- Análisis semanal con execution planner.
- Score guards.
- Trade quality guard.
- IC explicado en reporte.
- Radar compacto.
- Performance básica.

### Próximas mejoras

- Separar métricas legacy vs post-guard.
- Mejorar reglas de salida.
- Validar ejecución real de órdenes.
- Mejorar position sizing.
- Incorporar dashboard web opcional.
- Optimizar tiempos del radar.
- Reforzar backtesting walk-forward.

### Fuera de alcance por ahora

- Auto-trading.
- Ejecución automática de órdenes.
- Promesa de rentabilidad.
- Uso como producto financiero para terceros.

---

## 📌 Disclaimer

Este proyecto es personal, experimental y educativo.

No constituye asesoramiento financiero, recomendación de inversión ni sistema de trading automático para terceros.

Las decisiones generadas por Cocos Copilot deben ser revisadas manualmente antes de cualquier operación real.

---

## 🧠 Idea central

Cocos Copilot no intenta predecir el mercado de forma perfecta.

Intenta construir un proceso disciplinado para responder una pregunta concreta:

> ¿Existe una ventaja estadística real cuando las decisiones de portfolio se generan, filtran, registran y evalúan de forma sistemática?

Ese es el objetivo del proyecto.
