"""
Redis response cache for performance endpoints.

Design
──────
@cached(ttl, key_prefix)
    Decorator for async FastAPI route handlers.

    Cache key: {prefix}:{brand_id}:{func_name}:{params_hash}
        - brand_id  — extracted from resolved kwargs (always present on
                      performance routes via BrandAccess dependency)
        - params_hash — SHA-256 of JSON-serialised remaining kwargs
                        (db and brand_id excluded), first 12 hex chars

    On HIT  → json.loads(cached_str) dict returned; FastAPI coerces it
              via the route's response_model annotation.
    On MISS → call through, store Pydantic model as JSON, return model.

    Fail-open: any Redis error is logged at WARNING and the handler is
    called through normally so the request always succeeds.

invalidate_brand_cache(redis, brand_id, key_prefix)
    Delete all cached responses for a brand.  Used by BaseIngestionService
    after a successful ingestion run so dashboards see fresh data
    immediately.

    Implementation: SCAN MATCH + UNLINK in batches of 500.
    Fail-safe: errors are logged, never raised.

TTL conventions (seconds):
    CACHE_TTL_SUMMARY     = 3600   — KPI summary card (1 h)
    CACHE_TTL_ROLLUP      = 3600   — rollup aggregates (1 h)
    CACHE_TTL_ATTRIBUTION = 3600   — attribution pie (1 h)
    CACHE_TTL_TOP         = 1800   — top-campaigns (30 min)
    CACHE_TTL_DAILY       =  900   — raw daily rows (15 min)
    CACHE_TTL_TREND       =  900   — trend time-series (15 min)
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import typing
from typing import Any, Callable

# functools.wraps copies __annotations__ as strings (from __future__ annotations).
# FastAPI evaluates those strings using the WRAPPER's __globals__, but the wrapper
# lives in app.core.cache where Depends/BrandAccess are not defined → NameError.
# Fix: exclude __annotations__ from wraps assignments, then build wrapper.__signature__
# with annotations already resolved in the ORIGINAL function's namespace via
# typing.get_type_hints(). inspect.signature() uses __signature__ directly so
# FastAPI sees actual type objects and skips forward-ref evaluation entirely.
_WRAPPER_ASSIGNMENTS = tuple(
    a for a in functools.WRAPPER_ASSIGNMENTS if a != "__annotations__"
)


def _resolved_signature(func: Callable) -> inspect.Signature | None:
    """Return a Signature for `func` with all annotations resolved to actual types.

    Uses get_type_hints() which evaluates string annotations in func's own module
    namespace.  Returns None if resolution fails (e.g. during import cycle).
    """
    try:
        hints = typing.get_type_hints(func, include_extras=True)
        original_sig = inspect.signature(func, follow_wrapped=False)
        new_params = [
            p.replace(annotation=hints.get(name, p.annotation))
            for name, p in original_sig.parameters.items()
        ]
        return original_sig.replace(
            parameters=new_params,
            return_annotation=hints.get("return", original_sig.return_annotation),
        )
    except Exception:
        return None

import structlog

logger = structlog.get_logger(__name__)

# ── TTL constants (exported for use in routers) ────────────────────────────────

CACHE_TTL_SUMMARY     = 3600   # 1 h  — summary card
CACHE_TTL_ROLLUP      = 3600   # 1 h  — pre-computed rollup
CACHE_TTL_ATTRIBUTION = 3600   # 1 h  — attribution breakdown
CACHE_TTL_TOP         = 1800   # 30 m — top-campaigns
CACHE_TTL_DAILY       =  900   # 15 m — raw daily rows
CACHE_TTL_TREND       =  900   # 15 m — trend time-series

# Default prefix for performance endpoints
PERF_PREFIX = "perf"

_UNLINK_BATCH = 500   # keys per UNLINK call during invalidation


# ── Key helpers ────────────────────────────────────────────────────────────────

def _params_hash(kwargs: dict[str, Any], exclude: frozenset[str]) -> str:
    """Return first 12 hex chars of SHA-256 over serialisable kwargs.

    `exclude` contains kwarg names that must never be part of the key
    (brand_id — already in the key prefix; db — motor handle).
    Date / datetime / Enum values are converted via their str() repr so
    they serialise deterministically.
    """
    params = {k: _serialisable(v) for k, v in sorted(kwargs.items()) if k not in exclude}
    raw = json.dumps(params, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _serialisable(v: Any) -> Any:
    """Convert non-JSON-native types to their string representation."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


def build_key(prefix: str, brand_id: str, func_name: str, params_hash: str) -> str:
    return f"{prefix}:{brand_id}:{func_name}:{params_hash}"


# ── Decorator ──────────────────────────────────────────────────────────────────

def cached(ttl: int, key_prefix: str = PERF_PREFIX) -> Callable:
    """Decorator factory — wraps an async FastAPI route handler with Redis caching.

    Usage::

        @router.get("/{brand_id}/performance/summary", response_model=KpiSummary)
        @cached(ttl=CACHE_TTL_SUMMARY)
        async def get_summary(brand_id: ..., db: ..., ...) -> KpiSummary:
            ...

    Requirements on the decorated function:
        - Must be async.
        - Must have a `brand_id` kwarg (resolved from BrandAccess).
        - Must have a `db` kwarg (the Motor DB handle — excluded from hash).
        - Must return a Pydantic BaseModel instance (or a dict) so that
          ``model_dump_json()`` / ``json.dumps()`` serialisation works.

    Redis errors (connection refused, timeout, etc.) are caught and logged
    at WARNING level; the handler is then called through normally so the
    request always completes successfully.
    """
    _EXCLUDE = frozenset({"brand_id", "db"})

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func, assigned=_WRAPPER_ASSIGNMENTS)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:  # type: ignore[return]
            brand_id: str = kwargs.get("brand_id", "")
            phash = _params_hash(kwargs, _EXCLUDE)
            key = build_key(key_prefix, brand_id, func.__name__, phash)

            # ── Try cache read ────────────────────────────────────────────────
            try:
                from app.core.redis import get_redis_client
                redis = get_redis_client()
                cached_val = await redis.get(key)
                if cached_val is not None:
                    logger.debug("cache.hit", key=key)
                    # Return as dict — FastAPI coerces via response_model
                    return json.loads(cached_val)
            except Exception as exc:
                logger.warning("cache.read_failed", key=key, error=str(exc))

            # ── Cache miss — call through ─────────────────────────────────────
            result = await func(*args, **kwargs)

            # ── Try cache write ───────────────────────────────────────────────
            try:
                from app.core.redis import get_redis_client
                redis = get_redis_client()
                payload = (
                    result.model_dump_json()
                    if hasattr(result, "model_dump_json")
                    else json.dumps(result, default=str)
                )
                await redis.set(key, payload, ex=ttl)
                logger.debug("cache.set", key=key, ttl=ttl)
            except Exception as exc:
                logger.warning("cache.write_failed", key=key, error=str(exc))

            return result

        # Give the wrapper a __signature__ with resolved (non-string) annotations.
        # This ensures FastAPI's dependency injection works even when the decorated
        # function uses `from __future__ import annotations` (which makes all
        # annotations lazy strings that would otherwise be evaluated in THIS
        # module's namespace, where Depends / BrandAccess are not defined).
        sig = _resolved_signature(func)
        if sig is not None:
            wrapper.__signature__ = sig  # type: ignore[attr-defined]

        return wrapper
    return decorator


# ── Invalidation ───────────────────────────────────────────────────────────────

async def invalidate_brand_cache(
    brand_id: str,
    key_prefix: str = PERF_PREFIX,
) -> int:
    """Delete all cached responses for a brand.

    Uses SCAN to avoid blocking the Redis event loop.  Keys are deleted
    in batches via UNLINK (non-blocking delete).

    Returns the total number of keys deleted, or 0 if Redis is unavailable.
    Errors are logged at WARNING and never re-raised.
    """
    pattern = f"{key_prefix}:{brand_id}:*"
    deleted = 0

    try:
        from app.core.redis import get_redis_client
        redis = get_redis_client()

        cursor: int = 0
        batch: list[str] = []

        while True:
            cursor, keys = await redis.scan(cursor, match=pattern, count=200)
            batch.extend(keys)

            if len(batch) >= _UNLINK_BATCH:
                await redis.unlink(*batch)
                deleted += len(batch)
                batch = []

            if cursor == 0:
                break

        if batch:
            await redis.unlink(*batch)
            deleted += len(batch)

        if deleted:
            logger.info(
                "cache.invalidated",
                brand_id=brand_id,
                prefix=key_prefix,
                keys_deleted=deleted,
            )

    except Exception as exc:
        logger.warning(
            "cache.invalidation_failed",
            brand_id=brand_id,
            prefix=key_prefix,
            error=str(exc),
        )

    return deleted
