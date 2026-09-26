# Marketaux entity-matched news retrieval

## Objetivo

Reemplazar el supuesto legacy `feed de ticker => noticia del ticker` por una capa de retrieval auditable que exija resolución explícita de entidad antes de permitir que una noticia afecte el sentiment de un activo.

Marketaux se usa únicamente para discovery y entity resolution. El sentiment del proveedor se conserva en `raw_payload` para auditoría, pero **no participa del score activo**. El score económico/contextual sigue siendo `ProsusAI/finbert` local.

## Contrato activo

- Provider ticker-news: `marketaux`
- Retrieval policy: `marketaux_entity_v1`
- Scorer: `finbert`
- Aggregation policy: `event_time_finbert_entity_v1`
- Gate default: `entity_match_score >= 0.65`
- PIT: `published_at <= as_of` y dentro del lookback solicitado
- Fallback ticker RSS: ninguno; falla cerrada
- Feeds generales: continúan para macro/mercado, pero no pueden alimentar un ticker salvo que tengan el retrieval policy activo

## Flujo

```text
portfolio CEDEAR
  -> underlying symbol mapping
  -> get_news_context(symbols, as_of, lookback)
  -> Marketaux symbols + filter_entities + min_match_score
  -> local relevance/PIT revalidation
  -> sentiment_raw (article + entity identity)
  -> FinBERT local
  -> sentiment_scored
  -> entity-match weighted aggregation
  -> /mercado, sentiment_tool, Decision Engine context
```

## Identidad artículo + entidad

Una misma nota puede ser evidencia válida para más de una empresa. Para evitar que el `url_hash` legacy haga colisionar asociaciones, la URL persistida agrega un fragmento local `#quantia_entity=SYMBOL`; la URL canónica original queda preservada en `raw_payload.canonical_url`. Los fragmentos no se envían al servidor del publisher.

## Variables de entorno

```env
SENTIMENT_TICKER_NEWS_PROVIDER=marketaux
SENTIMENT_ACTIVE_TICKER_RETRIEVAL_POLICY=marketaux_entity_v1
MARKETAUX_API_TOKEN=
SENTIMENT_MARKETAUX_MIN_MATCH_SCORE=0.65
SENTIMENT_MARKETAUX_LOOKBACK_HOURS=72
SENTIMENT_MARKETAUX_LIMIT=3
SENTIMENT_MARKETAUX_BATCH_SIZE=20
```

Los defaults `LIMIT=3` y `BATCH_SIZE=20` están elegidos para que un scheduler cada 15 minutos realice como máximo una request de ticker news por ciclo cuando el universo activo no supera 20 símbolos. En planes superiores se puede aumentar `SENTIMENT_MARKETAUX_LIMIT` sin cambiar el contrato PIT/relevance.

## Seguridad económica

- Marketaux no modifica thresholds ni planner.
- `provider_entity_sentiment` es metadata, no input del score.
- Una noticia sin entidad válida no produce evidencia ticker.
- Una respuesta sin `published_at` no produce evidencia ticker.
- Una noticia posterior a `as_of` no produce evidencia ticker.
- Scores FinBERT históricos de feeds ticker legacy quedan en DB, pero los lectores activos los ignoran por policy.

## Cutover

1. Configurar `MARKETAUX_API_TOKEN` en `.env`.
2. Rebuild de `scheduler` y `telegram_bot`.
3. Ejecutar una pasada de fetch/score/aggregate.
4. Verificar en stdout `ticker_news_provider=marketaux` y `ticker_retrieval_status=ok`.
5. Verificar `/mercado`: los eventos ticker activos deben tener fuentes `marketaux:*`; los RSS generales pueden seguir apareciendo en macro.
6. Verificar que ejemplos contaminados legacy (por ejemplo una nota sin relación directa con el ticker) ya no aparezcan como evento activo del ticker.

## Rollback

Revertir el merge y reconstruir `scheduler`/`telegram_bot`. No se elimina evidencia histórica. Las filas Marketaux quedan auditables en `sentiment_raw` y `sentiment_scored`; la policy activa determina qué evidencia puede leerse.
