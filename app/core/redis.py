from typing import Annotated

import structlog
from fastapi import Depends
from redis.asyncio import Redis, from_url

from app.core.config import Settings, get_settings

logger = structlog.get_logger(__name__)

# Module-level client — created once during lifespan, reused across requests
_redis: Redis | None = None  # type: ignore[type-arg]


async def connect_redis(settings: Settings) -> None:
    """Create the Redis client. Called from app lifespan startup."""
    global _redis
    _redis = from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    # Verify connection
    await _redis.ping()
    logger.info("redis.connected", url=settings.redis_url)


async def disconnect_redis() -> None:
    """Close the Redis client. Called from app lifespan shutdown."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
        logger.info("redis.disconnected")


def get_redis_client() -> Redis:  # type: ignore[type-arg]
    if _redis is None:
        raise RuntimeError("Redis client is not initialised. Was connect_redis() called?")
    return _redis


async def get_redis(
    settings: Annotated[Settings, Depends(get_settings)],  # noqa: ARG001
) -> Redis:  # type: ignore[type-arg]
    """FastAPI dependency — injects the Redis client for the request."""
    return get_redis_client()
