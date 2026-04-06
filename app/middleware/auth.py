"""
Auth dependency — get_current_user

Resolves the caller's identity from either:
  1. Bearer JWT  (Authorization: Bearer <token>)
  2. API key     (Authorization: Bearer bbads_<key>)

Returns a CurrentUser injected into every route that Depends on it.
Also binds user_id / brand_id to structlog contextvars so all log lines
in the request automatically carry those fields.

Usage:
    from app.middleware.auth import get_current_user, require_role
    from app.middleware.auth import CurrentUser

    @router.get("/...")
    async def my_endpoint(user: CurrentUser = Depends(get_current_user)):
        ...

    # Role-gated:
    @router.get("/admin/...")
    async def admin_only(user: CurrentUser = Depends(require_role("super_admin", "admin"))):
        ...
"""

from __future__ import annotations

from typing import Annotated

import jwt
import structlog
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from app.core.database import get_database
from app.core.exceptions import ForbiddenError, UnauthorizedError
from app.core.redis import get_redis
from app.core.security import decode_access_token, hash_api_key
from app.repositories.users import UsersRepository

logger = structlog.get_logger(__name__)

_bearer = HTTPBearer(auto_error=False)


# ── CurrentUser model ─────────────────────────────────────────────────────────

class CurrentUser(BaseModel):
    user_id: str
    role: str
    allowed_brands: list[str]
    auth_method: str  # "jwt" | "api_key"

    def can_access_brand(self, brand_id: str) -> bool:
        if self.role == "super_admin":
            return True
        return brand_id in self.allowed_brands


# ── Core dependency ───────────────────────────────────────────────────────────

async def get_current_user(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    redis: Annotated[object, Depends(get_redis)],
) -> CurrentUser:
    if credentials is None:
        raise UnauthorizedError("Authorization header missing.")

    token = credentials.credentials

    # ── Route: API key ─────────────────────────────────────────────
    if token.startswith("bbads_"):
        return await _auth_via_api_key(token, db)

    # ── Route: JWT ─────────────────────────────────────────────────
    return await _auth_via_jwt(token, redis)  # type: ignore[arg-type]


async def _auth_via_jwt(token: str, redis: object) -> CurrentUser:
    from redis.asyncio import Redis as AsyncRedis

    try:
        claims = decode_access_token(token)
    except jwt.ExpiredSignatureError:
        raise UnauthorizedError("Access token has expired.")
    except jwt.InvalidTokenError as exc:
        raise UnauthorizedError(f"Invalid access token: {exc}")

    # Check blocklist (logout / rotation)
    blocklist_key = f"blocklist:jti:{claims.jti}"
    if await (redis).exists(blocklist_key):  # type: ignore[union-attr]
        raise UnauthorizedError("Token has been revoked.")

    user = CurrentUser(
        user_id=claims.sub,
        role=claims.role,
        allowed_brands=claims.allowed_brands,
        auth_method="jwt",
    )
    _bind_log_context(user)
    return user


async def _auth_via_api_key(
    raw_key: str, db: AsyncIOMotorDatabase  # type: ignore[type-arg]
) -> CurrentUser:
    key_hash = hash_api_key(raw_key)
    repo = UsersRepository(db)
    doc = await repo.find_by_api_key_hash(key_hash)
    if doc is None:
        raise UnauthorizedError("Invalid or revoked API key.")

    user_id = str(doc["_id"])

    # Fire-and-forget touch (last_used_at) — find the matching key_id
    for key_rec in doc.get("api_keys", []):
        if key_rec.get("key_hash") == key_hash and not key_rec.get("revoked"):
            await repo.touch_api_key(user_id, key_rec["key_id"])
            break

    user = CurrentUser(
        user_id=user_id,
        role=doc.get("role", "viewer"),
        allowed_brands=[str(b) for b in doc.get("allowed_brands", [])],
        auth_method="api_key",
    )
    _bind_log_context(user)
    return user


def _bind_log_context(user: CurrentUser) -> None:
    structlog.contextvars.bind_contextvars(user_id=user.user_id)


# ── Role-gated dependency factory ─────────────────────────────────────────────

def require_role(*roles: str):
    """
    Returns a FastAPI dependency that ensures the caller has one of the
    given roles.

    Usage:
        Depends(require_role("super_admin", "admin"))
    """
    async def _check(
        user: Annotated[CurrentUser, Depends(get_current_user)],
    ) -> CurrentUser:
        if user.role not in roles:
            raise ForbiddenError(
                f"This action requires one of the following roles: {', '.join(roles)}."
            )
        return user

    return _check


# ── Convenience type alias ────────────────────────────────────────────────────

AuthUser = Annotated[CurrentUser, Depends(get_current_user)]
