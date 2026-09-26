# Auditoría de cuatro respuestas de /agente

Fuente: agent_runs y agent_steps, leídos con conexión PostgreSQL protegida contra escritura y filtro de owner. No se recalcularon precios ni planes. La captura contiene información privada de la cuenta.

Los cuatro runs ocurrieron el 23/09/2026 entre 15:34 y 16:18 ART, antes del despliegue de evidence_renderer_v1 a las 21:23 ART. Metadata histórica agent_version=quantia-agent-v1; no registra el hash exacto del código por run.

| Run | Consultas | Hallazgo |
|---|---|---|
| `806eb374-404c-4eb3-b002-d0eadf619f8d` | get_portfolio_snapshot, get_macro_context, analyze_portfolio | Confunde scores con rendimientos: el snapshot tiene pnl_pct=null. Describe SP500 alcista aunque get_macro_context registra sp500_trend=-0.0061 y régimen market=neutral. No aporta una medida que sostenga cartera equilibrada. |
| `d879e507-e9b5-42b6-bbf0-9a2009d89bb8` | analyze_ticker, analyze_ticker, get_portfolio_snapshot, get_macro_context | Inventó analyze_ticker(ticker=CDEEAR). Repitió la consulta, bloqueada por el guard. La primera herramienta devolvió cartera no evaluable, aunque el subprocess quedó ok=True. Atribuye crítico a 555 pb sin comprobar la regla. |
| `39b72a63-869f-40f7-b7bb-2faf8a949e30` | analyze_portfolio | Sólo consultó analyze_portfolio. No verificó recesión, retornos de dos meses ni caídas significativas. El texto sobre recesión proviene del objetivo del usuario, no de evidencia validada. |
| `befa2215-57c8-4d77-8195-b41174a70010` | analyze_portfolio | Reproduce scores y bloqueos reales, pero no determina si comprar es económicamente inconveniente. Ventas propuestas y BUY_SCORE_GUARD explican comportamiento del sistema; no demuestran superioridad frente a hold/comprar. |

## Diagnóstico del código desplegado

- Cada comando Telegram crea un proceso CLI con el goal actual; el historial que recibe el controlador contiene pasos de ese run, no la conversación anterior.
- get_macro_regime marca argentina=crítico si riesgo_pais>800 O ccl>1450. Con los valores observados (555 y 1604.1), la condición verdadera es CCL. Esto identifica la regla informática; no valida económicamente el umbral.
- Las reglas macro directas de NVDA y AMD usan SP500, Dow, VIX, TNX y DXY. IREN usa el mapa default con esos indicadores. No hay factor riesgo_pais directo en esos mapas; esto no audita todos los canales de riesgo del sistema.
- COMPLETE indica que el controlador eligió terminar, no que sus afirmaciones fueron verificadas. Auditoría completa indica persistencia de la traza.
- La mitigación actual reemplaza la prosa libre del modelo por un cierre descriptivo de fuentes. No agrega memoria conversacional, verificación de hipótesis ni un análisis causal de decisiones.

## Casos de aceptación para un rediseño

1. Reconocer CEDEAR como clase de instrumento; no crear un ticker de esa palabra.
2. No llamar retorno a un score, ni afirmar pérdidas con pnl_pct ausente.
3. Verificar o dejar explícitamente sin verificar las premisas temporales del usuario.
4. Separar subyacente extranjero, vehículo local y cash ARS al explicar macro/riesgo.
5. Vincular preguntas de seguimiento al run anterior y conservar hipótesis/correcciones por cuenta, con límites de retención explícitos.
6. Explicar la regla que bloquea una compra y distinguir esa explicación de una recomendación económica.
7. Reconocer objetivos pendientes incluso cuando el proceso técnico finaliza.

Fuente conceptual CEDEAR: https://www.byma.com.ar/productos/productos-financieros/cedears

Hash SHA256 de la captura: `b1cd7f553c7bb0fcacb2b6714e9cd58d752525b8d56eadd7f60b3d1cae2dd29e`.
Hash del macro.py desplegado verificado: `74780ec1fa89963785b1116d87b29eef46cd9d62dca6801f4bb45f4ae08e1009`.
