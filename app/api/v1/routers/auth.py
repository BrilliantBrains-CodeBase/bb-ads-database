"""
Auth router

  POST /auth/token       — login (rate-limited: 5 attempts / min per IP)
  POST /auth/refresh     — rotate refresh token, blocklist old jti
  POST /auth/logout      — blocklist both jtis
  POST /auth/api-keys    — create API key
  GET  /auth/api-keys    — list API keys (metadata only)
  DELETE /auth/api-keys/{key_id} — revoke API key
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

import jwt
import structlog
from fastapi import APIRouter, Depends, Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.api.v1.schemas.auth import (
    ApiKeyCreatedResponse,
    ApiKeyListResponse,
    ApiKeyMetadata,
    CreateApiKeyRequest,
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    TokenResponse,
)
from app.core.config import Settings, get_settings
from app.core.database import get_database
from app.core.exceptions import NotFoundError, RateLimitError, UnauthorizedError
from app.core.redis import get_redis
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    dummy_password_verify,
    generate_api_key,
    verify_password,
)
from app.middleware.auth import AuthUser
from app.repositories.users import UsersRepository

router = APIRouter(prefix="/auth", tags=["auth"])
logger = structlog.get_logger(__name__)


# ── Rate-limit helper ─────────────────────────────────────────────────────────

async def _check_auth_rate_limit(ip: str, redis: Redis, settings: Settings) -> None:  # type: ignore[type-arg]
    """Increment attempt counter for this IP; raise RateLimitError if exceeded."""
    key = f"rate_limit:auth:{ip}"
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, settings.auth_rate_limit_window_seconds)
    if count > settings.auth_rate_limit_attempts:
        ttl = await redis.ttl(key)
        raise RateLimitError(
            f"Too many login attempts. Try again in {max(ttl, 1)} seconds.",
            retry_after=max(ttl, 1),
        )


# ── Blocklist helpers ─────────────────────────────────────────────────────────

async def _blocklist_jti(jti: str, ttl_seconds: int, redis: Redis) -> None:  # type: ignore[type-arg]
    key = f"blocklist:jti:{jti}"
    await redis.setex(key, ttl_seconds, "1")


# ── POST /auth/token ──────────────────────────────────────────────────────────

@router.post("/token", response_model=TokenResponse, status_code=200)
async def login(
    body: LoginRequest,
    request: Request,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    redis: Annotated[Redis, Depends(get_redis)],  # type: ignore[type-arg]
    settings: Annotated[Settings, Depends(get_settings)],
) -> TokenResponse:
    """Authenticate with email + password; returns access + refresh tokens."""
    ip = request.client.host if request.client else "unknown"
    await _check_auth_rate_limit(ip, redis, settings)

    repo = UsersRepository(db)
    user = await repo.find_by_email(body.email)

    # Constant-time path: always run a bcrypt operation regardless of whether
    # the email exists, so response time cannot be used to enumerate users.
    if user:
        password_ok = verify_password(body.password, user["hashed_password"])
    else:
        dummy_password_verify()
        password_ok = False

    if not user or not password_ok:
        logger.warning("auth.login_failed", email=body.email, ip=ip)
        raise UnauthorizedError("Invalid email or password.")

    user_id = str(user["_id"])
    access_token, _access_jti = create_access_token(
        user_id=user_id,
        role=user["role"],
        allowed_brands=[str(b) for b in user.get("allowed_brands", [])],
    )
    refresh_token, _refresh_jti = create_refresh_token(user_id=user_id)

    logger.info("auth.login_success", user_id=user_id, ip=ip)
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=settings.jwt_access_token_expire_minutes * 60,
    )


# ── POST /auth/refresh ────────────────────────────────────────────────────────

@router.post("/refresh", response_model=TokenResponse, status_code=200)
async def refresh_token(
    body: RefreshRequest,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    redis: Annotated[Redis, Depends(get_redis)],  # type: ignore[type-arg]
    settings: Annotated[Settings, Depends(get_settings)],
) -> TokenResponse:
    """Rotate a refresh token; the old jti is immediately blocklisted."""
    try:
        claims = decode_refresh_token(body.refresh_token)
    except jwt.ExpiredSignatureError:
        raise UnauthorizedError("Refresh token has expired.")
    except jwt.InvalidTokenError as exc:
        raise UnauthorizedError(f"Invalid refresh token: {exc}")

    # Blocklist check
    if await redis.exists(f"blocklist:jti:{claims.jti}"):
        logger.warning("auth.refresh_blocked", jti=claims.jti)
        raise UnauthorizedError("Refresh token has been revoked.")

    # Blocklist old jti immediately (rotation)
    remaining = int((claims.exp - datetime.now(UTC)).total_seconds())
    await _blocklist_jti(claims.jti, max(remaining, 1), redis)

    # Load user to get fresh role / allowed_brands
    repo = UsersRepository(db)
    user = await repo.find_by_id(claims.sub)
    if user is None:
        raise UnauthorizedError("User account not found.")

    access_token, _access_jti = create_access_token(
        user_id=claims.sub,
        role=user["role"],
        allowed_brands=[str(b) for b in user.get("allowed_brands", [])],
    )
    new_refresh_token, _new_refresh_jti = create_refresh_token(user_id=claims.sub)

    logger.info("auth.token_refreshed", user_id=claims.sub)
    return TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        expires_in=settings.jwt_access_token_expire_minutes * 60,
    )


# ── POST /auth/logout ─────────────────────────────────────────────────────────

@router.post("/logout", status_code=204)
async def logout(
    body: LogoutRequest,
    redis: Annotated[Redis, Depends(get_redis)],  # type: ignore[type-arg]
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    """Blocklist both access and refresh jtis."""
    access_ttl = settings.jwt_access_token_expire_minutes * 60
    refresh_ttl = settings.jwt_refresh_token_expire_days * 86400

    # Blocklist access token jti (best-effort — may already be expired)
    try:
        access_claims = jwt.decode(
            body.access_token,
            options={"verify_exp": False, "verify_signature": False},
            algorithms=["RS256"],
        )
        if jti := access_claims.get("jti"):
            await _blocklist_jti(jti, access_ttl, redis)
    except Exception:
        pass  # malformed token — nothing to blocklist

    # Blocklist refresh token jti
    try:
        refresh_claims = decode_refresh_token(body.refresh_token)
        remaining = int((refresh_claims.exp - datetime.now(UTC)).total_seconds())
        await _blocklist_jti(refresh_claims.jti, max(remaining, refresh_ttl), redis)
    except Exception:
        pass

    logger.info("auth.logout")


# ── POST /auth/api-keys ───────────────────────────────────────────────────────

@router.post("/api-keys", response_model=ApiKeyCreatedResponse, status_code=201)
async def create_api_key(
    body: CreateApiKeyRequest,
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> ApiKeyCreatedResponse:
    """Generate a new API key. The raw key is shown exactly once."""
    raw_key, key_hash = generate_api_key()
    repo = UsersRepository(db)
    record = await repo.add_api_key(user.user_id, body.name, key_hash)

    return ApiKeyCreatedResponse(
        key_id=record["key_id"],
        name=record["name"],
        raw_key=raw_key,
        created_at=record["created_at"],
    )


# ── GET /auth/api-keys ────────────────────────────────────────────────────────

@router.get("/api-keys", response_model=ApiKeyListResponse, status_code=200)
async def list_api_keys(
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> ApiKeyListResponse:
    """List all API key metadata for the authenticated user (no raw keys)."""
    repo = UsersRepository(db)
    keys = await repo.list_api_keys(user.user_id)
    return ApiKeyListResponse(
        keys=[ApiKeyMetadata(**k) for k in keys]
    )


# ── DELETE /auth/api-keys/{key_id} ────────────────────────────────────────────

@router.delete("/api-keys/{key_id}", status_code=204)
async def revoke_api_key(
    key_id: str,
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> None:
    """Revoke an API key by its key_id."""
    repo = UsersRepository(db)
    found = await repo.revoke_api_key(user.user_id, key_id)
    if not found:
        raise NotFoundError("API key not found.", details={"key_id": key_id})
