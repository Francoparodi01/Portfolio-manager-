# FinBERT sentiment cutover

## Objetivo

Reemplazar el scorer legacy de sentiment (Ollama + heurística) por un clasificador
local y versionado basado en `ProsusAI/finbert`, manteniendo la ingesta raw, la
trazabilidad en Postgres y la interfaz de contexto que consume Quantia.

El cambio no borra filas históricas. La separación se hace por:

- `sentiment_scored.scorer = 'finbert'`
- `sentiment_scored.model = '<model>@<revision>'`
- `sentiment_aggregated.sources._policy = 'event_time_finbert_v1'`
- `sentiment_aggregated.sources._scorer = 'finbert'`

Por lo tanto los scores legacy quedan disponibles para auditoría pero no entran
en el agregado activo nuevo.

## Contrato point-in-time

Para cualquier `as_of`:

1. solo se usan noticias con `COALESCE(published_at, fetched_at) <= as_of`;
2. si `published_at` falta se usa `fetched_at`, que es conservador;
3. el agregador y `sentiment_tool.get_sentiment()` aplican el mismo límite temporal;
4. la tool expone ventanas 6h, 24h y 72h sin mirar eventos futuros.

## Mapping CEDEAR -> underlying

`src/analysis/sentiment_symbols.py` separa el símbolo de portfolio del símbolo de
noticias. Defaults actuales:

- `YPFD -> YPF`
- `PAMP -> PAM`
- `BRKB / BRK.B -> BRK-B`

Se puede extender sin código usando `SENTIMENT_UNDERLYING_MAP_JSON`.

## Modelo

Defaults:

```env
SENTIMENT_ACTIVE_SCORER=finbert
SENTIMENT_FINBERT_MODEL=ProsusAI/finbert
SENTIMENT_FINBERT_REVISION=db38d3727cbaed87c9aed72df7b3519e2ba5cca1
SENTIMENT_FINBERT_MAX_LENGTH=384
SENTIMENT_FINBERT_TORCH_THREADS=2
SENTIMENT_FINBERT_LOCAL_ONLY=false
HF_HOME=/app/.cache/huggingface
```

El score persistido es:

```text
score = P(positive) - P(negative)
```

`raw_response` conserva `positive`, `negative`, `neutral`, modelo, revision y
fórmula para poder reproducir el cálculo.

## Cutover de producción

Después de mergear la rama/PR a `main`, desde el directorio de Quantia:

```bash
git pull --ff-only origin main && \
docker compose build scheduler telegram_bot && \
docker compose up -d scheduler telegram_bot && \
docker compose exec scheduler python scripts/run_sentiment_pipeline.py \
  --no-fetch --rescore-hours 72 --score-limit 500 && \
docker compose exec scheduler python scripts/run_sentiment_pipeline.py \
  --aggregate-only
```

El primer `rescore` descarga el modelo si no existe en el volumen `hf_cache`.
El scheduler y Telegram comparten ese cache.

Para validar el corte:

```bash
docker compose exec scheduler python scripts/run_sentiment_pipeline.py \
  --score-only --score-limit 20

docker compose logs --tail=200 scheduler | grep -i -E "finbert|sentiment"
```

En DB, la evidencia activa debe aparecer como `scorer='finbert'` y los nuevos
agregados con policy `event_time_finbert_v1`.

## Rollback

No hay migración destructiva. Si el merge de esta feature es el último cambio:

```bash
git revert <MERGE_COMMIT> && \
docker compose build scheduler telegram_bot && \
docker compose up -d scheduler telegram_bot
```

Las filas FinBERT permanecen como evidencia histórica. Al volver al código
anterior, el agregador legacy vuelve a filtrar su propia policy y no consume los
agregados FinBERT.

## Comportamiento ante fallas

- Si Torch/Transformers/modelo no está disponible, la fila queda pendiente.
- No se usa una heurística de polaridad como reemplazo silencioso.
- El planner no se bloquea por una falla de NLP; el sentiment queda sin evidencia
  nueva hasta que el scorer se recupere.
- No se borran ni reescriben decisiones históricas.
