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
- No se modifica optimizer, planner, scores, pesos, cron, flags, órdenes, fills, QEEF ni base de datos.
- La UI anterior de auditoría histórica se conserva debajo del panel prospectivo.
- Tests cubren ausencia de datos, owner, ranking histórico y vencimiento; el build de frontend debe verificarse antes de merge.

## Contrato de uso
Esta pantalla responde: *«¿Qué nombres propone investigar la última captura completa del Radar?»*. No responde: *«¿Qué conviene comprar hoy?»*. Una futura ficha por ticker deberá vincular fuente/fecha, validación de instrumento CEDEAR, research fundamental y análisis de cartera antes de proponer capital.
