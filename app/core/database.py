from typing import Annotated

import structlog
from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import Settings, get_settings

logger = structlog.get_logger(__name__)

# Module-level client — created once during lifespan, reused across requests
_client: AsyncIOMotorClient | None = None  # type: ignore[type-arg]


async def connect_db(settings: Settings) -> None:
    """Create the Motor client. Called from app lifespan startup."""
    global _client
    _client = AsyncIOMotorClient(
        settings.mongodb_uri,
        maxPoolSize=settings.mongodb_max_pool_size,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )
    # Verify connection
    await _client.admin.command("ping")
    logger.info("mongodb.connected", uri=settings.mongodb_uri, db=settings.mongodb_db_name)


async def disconnect_db() -> None:
    """Close the Motor client. Called from app lifespan shutdown."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("mongodb.disconnected")


def get_motor_client() -> AsyncIOMotorClient:  # type: ignore[type-arg]
    if _client is None:
        raise RuntimeError("MongoDB client is not initialised. Was connect_db() called?")
    return _client


async def get_database(
    settings: Annotated[Settings, Depends(get_settings)],
) -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
    """FastAPI dependency — injects the Motor database for the request."""
    return get_motor_client()[settings.mongodb_db_name]


# Convenience alias for use inside services that don't go through Depends
def get_db_direct(db_name: str | None = None) -> AsyncIOMotorDatabase:  # type: ignore[type-arg]
    settings = get_settings()
    return get_motor_client()[db_name or settings.mongodb_db_name]
