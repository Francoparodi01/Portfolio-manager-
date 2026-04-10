# COCOS COPILOT
### Sistema Cuantitativo Multicapa para Gestión de Portfolio
*Cocos Capital · Argentina · 2026*

---

## 🚀 ¿Por qué existe este proyecto?

La mayoría de las herramientas de análisis financiero responden qué está pasando en el mercado, pero no qué hacer.

Cocos Copilot nace para resolver ese problema.

Es un sistema cuantitativo que no solo analiza, sino que:
- Decide (BUY / SELL / HOLD)
- Define cuánto operar (position sizing)
- Define riesgo (stop loss / target)
- Y mide si sus decisiones funcionan

Todo de forma automática, trazable y basada en datos.

---

## 🧠 ¿Qué es Cocos Copilot?

Cocos Copilot es un sistema autónomo de análisis y gestión de portfolio para CEDEARs en Cocos Capital. Integra scraping automatizado, análisis técnico, contexto macro, análisis de sentimiento, gestión de riesgo y optimización matemática en un único flujo operativo.

El sistema responde tres preguntas fundamentales:

- **¿Qué hago con lo que ya tengo?** → Portfolio Analyzer  
- **¿Qué debería incorporar?** → Opportunity Radar  
- **¿Adónde va el capital que se libera?** → Rotation Engine  

> El sistema es cuantitativo puro: las decisiones las toma el modelo matemático. El LLM se utiliza únicamente para explicación.

---

## 🎯 ¿Qué intenta lograr?

Cocos Copilot busca construir un sistema cuantitativo capaz de gestionar un portfolio de inversión de forma autónoma, disciplinada y basada en evidencia.

El proyecto parte de una limitación clara: la mayoría de los sistemas analizan el mercado, pero no traducen ese análisis en decisiones ejecutables y medibles.

Este sistema cierra ese gap, transformando señales en acciones concretas:
- Comprar, vender o mantener
- Definir porcentaje del portfolio
- Establecer condiciones de riesgo
- Determinar horizonte temporal

Cada decisión incluye:
- Tamaño de posición  
- Stop loss  
- Target  
- Criterios de invalidación  

Pero el foco principal no es solo decidir, sino validar.

Cada operación se registra junto con su contexto y luego se evalúa con métricas como:
- **Information Coefficient (IC)** → capacidad predictiva  
- **Expected Value (EV)** → expectativa matemática  
- **Equity Curve** → evolución del capital  

El objetivo final es determinar si el sistema tiene una ventaja estadística real (edge).

---

## 🧩 ¿Qué lo hace distinto?

- No es un sistema de análisis → es un sistema de decisión  
- Cada señal es ejecutable (no ambigua)  
- Cada decisión se mide y se valida con datos reales  
- Integra todo el flujo: datos → análisis → decisión → evaluación  
- No depende de intuición, sino de evidencia empírica  

---

## ⚙️ ¿Cómo funciona? (alto nivel)

El sistema está compuesto por tres pipelines principales:

1. **Portfolio Analyzer**
   - Analiza posiciones actuales
   - Genera decisiones (BUY / SELL / HOLD)

2. **Opportunity Radar**
   - Detecta oportunidades externas
   - Evalúa asimetría riesgo/retorno

3. **Rotation Engine**
   - Decide cómo redistribuir capital
   - Prioriza entre posiciones actuales y nuevas oportunidades

Todo el sistema corre sobre:
- Scraping automatizado del portfolio
- Análisis multicapa (técnico, macro, riesgo, sentiment)
- Optimización matemática (Black-Litterman / Min Variance)
- Persistencia en base de datos
- Evaluación histórica de decisiones

---

## 🧱 Componentes principales

### Scraper automatizado
Login en Cocos Capital con soporte MFA. Comunicación scraper-bot vía Redis (event-driven, sin race conditions).

### Análisis técnico
Indicadores de tendencia, momentum, volatilidad y volumen:
SMA, EMA, RSI, MACD, ADX, Bollinger Bands, OBV, ATR.

### Contexto macro
Datos globales (VIX, S&P500, petróleo, tasas) y variables locales (CCL, MEP, reservas, riesgo país).

### Risk Engine
Sizing dinámico con Kelly fraccionario y control por volatilidad. Sistema de gates:
- NORMAL
- CAUTIOUS
- BLOCKED

### Portfolio Optimizer
Modelo Black-Litterman con fallback a mínima varianza. Respetando siempre el Risk Gate.

### Opportunity Radar
Screener + scoring + análisis de asimetría → identifica oportunidades reales.

### Rotation Engine
Asigna capital entre posiciones actuales y nuevas oportunidades.

### Decision Memory
Registro de decisiones + cálculo de IC, EV y performance histórica.

---

## 🧰 Stack tecnológico

| Componente | Tecnología |
|------------|------------|
| Orquestación | Docker Compose |
| Scraper | Playwright (Chromium headless) |
| Base de datos | TimescaleDB (PostgreSQL) |
| Comunicación | Redis (event-driven) |
| Bot | python-telegram-bot |
| Datos de mercado | yfinance + APIs locales |
| Análisis | pandas / numpy / ta |
| Optimización | scipy / numpy |
| Scheduler | APScheduler |

---

## ⚠️ Estado actual y desafíos

El sistema se encuentra en fase de validación real.

Actualmente existen desafíos en:
- Definición de ejecución (parcial vs total)
- Reglas claras de salida
- Validación de ejecución real de órdenes

Estos puntos son clave para lograr un sistema completamente operativo y están en proceso de mejora.

---

## 🚀 Instalación rápida

```bash
git clone <repo>
cd cocos_copilot

cp .env.example .env
# completar variables

docker compose build --no-cache
docker compose up -d
docker compose logs -f scraper