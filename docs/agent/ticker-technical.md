# Imágenes y gráficos del análisis técnico por ticker

Revisión: 2026-10-22. Alcance: circuito `/ticker` y `/tecnico`, verificado en código.

## Cómo hace las imágenes del análisis técnico / technical analysis chart image generation
`/ticker NVDA` o `/tecnico NVDA` llama `scripts/run_ticker_analysis.py` con ticker,
owner y ruta PNG. Python genera las imágenes usando Matplotlib y mplfinance.
Nemotron/OpenRouter no genera, interpreta ni dibuja esos PNG dentro de este circuito.
El agente conversacional y el renderer técnico son componentes separados.
Se entregan dos PNG: precio/volumen con SMA20/50/200 y niveles técnicos; y momentum
con RSI14 y MACD. Los indicadores se calculan desde las velas disponibles.

## Precios, velas y fuentes
El informe lee hasta 260 velas diarias de `market_candles`. La selección de una vela
por día prioriza COCOS, TRADINGVIEW_BYMA y luego internal_snapshot. Se controla la
identidad de serie: no mezclar monedas, instrumentos, mercados o ajustes incompatibles.
Se requieren al menos 60 velas para calcular el análisis.

Si faltan velas y está habilitada la alternativa, `fetch_history(ticker, period="1y")`
consulta yfinance. El informe declara fuente externa y moneda USD. Esto no demuestra
precio local de un CEDEAR en ARS. Citar fuente y fecha final, no llamarlo precio en vivo.

## Imagen 1: precio, medias móviles y volumen
Velas japonesas OHLC, volumen, SMA20, SMA50 y SMA200 cuando hay historia suficiente.
También muestra niveles calculados de soporte crítico y recuperación, fecha de
las velas y score técnico. Los niveles son reglas del motor, no objetivos elegidos
por el LLM. La ausencia de una SMA por falta de datos no equivale a valor cero.

## Imagen 2: momentum, RSI y MACD
RSI de 14 períodos con referencias 30/50/70; MACD, línea de señal e histograma.
La lectura técnica y las leyendas se calculan desde la serie. No interpretar un
color de vela, RSI o cruce de MACD aislado como orden ejecutada o retorno esperado.

## Envío y persistencia
El renderer devuelve dos rutas PNG. Telegram envía ambas fotos y después el informe
de texto; elimina los PNG temporales al terminar. La consulta `/ticker` es de lectura:
no escribe `decision_log` ni cambia umbrales. Puede mostrar V2/V3 como información
shadow calculada en la consulta; eso no crea una señal histórica persistida.

La tool conversacional `analyze_ticker` reutiliza el análisis determinístico de un
activo. No asumir que envía las mismas dos imágenes: el envío PNG pertenece al
handler `/ticker`.

## Implementación
- `scripts/telegram_bot.py`: `action_ticker_analysis`, envío y limpieza.
- `scripts/run_ticker_analysis.py`: consulta y rutas de salida.
- `src/analysis/ticker_technical_report.py`: informe, indicadores gráficos y PNG.
- `src/analysis/technical.py`: motor técnico reutilizado.
- `src/collector/db.py` y `src/collector/cocos_history.py`: velas y selección de fuente.
