import os

try:
    import redis.asyncio as redis
except ImportError:  # pragma: no cover - local lean env fallback
    redis = None

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

class _MissingRedisClient:
    def __getattr__(self, _name):
        async def _missing(*_args, **_kwargs):
            raise RuntimeError("redis no instalado")

        return _missing


client = (
    redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )
    if redis is not None
    else _MissingRedisClient()
)
