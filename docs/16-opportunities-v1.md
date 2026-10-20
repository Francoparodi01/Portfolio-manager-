# Quantia Opportunities V1

## Objetivo
Mostrar hasta diez **candidatos de investigación prospectivos**, fuera de cartera, desde la **última captura completa** de Radar Discovery. No es una nueva estrategia ni un recomendador de compra.

## Fuente y orden
- Endpoint read-only `GET /api/opportunities/top10` protegido por la autenticación del monitor.
- El owner se obtiene **exclusivamente** de `get_config().scraper.telegram_chat_id`, nunca de parámetros enviados por el navegador.
- Se consulta la última fila de `radar_discovery_runs` con `status=COMPLETE` y `captured_at <= NOW()`; **una única versión/rueda**.
- Se filtra el mismo `run_id` con `radar_eligible=TRUE`, `screener_passed=TRUE`, `in_portfolio=FALSE`, `rank_position IS NOT NULL`, sin QQQ/SPY controles.
- Orden **original congelado**: `rank_position ASC, ticker ASC`. No se usa `outcome_5d`, `outcome_10d`, `outcome_20d`, precios futuros ni `composite_shadow_score` para reordenar.
- Si faltan tablas, owner, captura o elegibles, se declara explícitamente. No se reemplaza con `decision_log` ni información demo inventada.
- `stale=true` cuando la captura supera 7 días calendario. Se conserva el dato, marcado como vencido.
- `trigger_price`, `target_price`, `invalidation_price` y `setup_risk_reward` son niveles teóricos; `null` no significa cero. La moneda/unidad puede no estar verificada.

## Límites
- La captura sólo existe si se habilitó explícitamente `RADAR_DISCOVERY_LEDGER_ENABLED=true` y el job `--capture-discovery`. **Este PR no activa ninguno.**
- La evidencia técnica/fundamental no siempre está completa; no se fabrica research fundamental, valoración, noticias, probabilidades o autoridad operativa.
- Esta vista no modifica optimizer, planner, scores guardados, pesos, cron, flags, órdenes, fills, QEEF ni base de datos. Las correcciones del motor propuestas en la misma rama se documentan por separado en la auditoría enlazada abajo.
- La UI anterior de auditoría histórica se conserva debajo del panel prospectivo.
- Tests cubren ausencia de datos, owner, ranking histórico y vencimiento; el build de frontend debe verificarse antes de merge.

## Contrato de uso
Esta pantalla responde: *«¿Qué nombres propone investigar la última captura completa del Radar?»*. No responde: *«¿Qué conviene comprar hoy?»*. Una futura ficha por ticker deberá vincular fuente/fecha, validación de instrumento CEDEAR, research fundamental y análisis de cartera antes de proponer capital.

## Top 10 compartido con Telegram

`/top10` usa el mismo `fetch_top_opportunities` y `present_top_opportunities` que el endpoint. Ambos leen una transacción consistente de sólo lectura; el bot acepta únicamente el chat de la cuenta configurada en el monitor. El comando no refresca mercado, inicia ingestas ni realiza operaciones.

La presentación agrega moneda, fecha/antigüedad del precio, score técnico, tendencia, volumen y definición, motivo, riesgo, calidad y campos faltantes. `metadata.top10_evidence` conserva los valores disponibles en las capturas futuras, sin cambiar schema, ranking ni decisiones históricas. Los campos ausentes en versiones anteriores siguen desconocidos: ni el score shadow ni el ranking sustituyen al score técnico.

COMPRABLE_AHORA se presenta como COMPRAR; COMPRA_HABILITADA, SWAP_CANDIDATO y VIGILANCIA_A/B/C como VIGILAR; el resto como ESPERAR. La evidencia incompleta, vencida o no canónica obliga a mostrar ESPERAR, conservando el estado original. Se requiere calidad VALID de precio, volumen y procedencia, moneda y score conservados, precio positivo y timestamp verificable. No se crean señales operativas con este mapeo.

El orden no se altera para ocultar faltantes: puede haber diez candidatos y ninguna compra verificable. Las posiciones originales pueden contener saltos. La fecha y versión permiten comprobar si dos consultas corresponden al mismo run. El panel prospectivo sigue disponible aunque falle la consulta del historial.

Resultados, límites y reproducción: [Correcciones técnicas y Top 10 V1](audits/RADAR_CORRECTIONS_TOP10_V1.md).
