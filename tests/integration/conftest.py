"""
Shared fixtures for integration tests.

Architecture
────────────
• RSA keys          — generated once per session; patched into app.core.security
                      so JWT sign/verify works without real key files.
• FakeRedis         — in-memory dict-backed Redis supporting all used commands
                      (incr/expire/ttl/exists/setex/get/set/scan/unlink).
                      Also written to app.core.redis._redis so the @cached
                      decorator (which calls get_redis_client() directly) and
                      cache-invalidation code see the same instance as the
                      FastAPI dependency.
• mongomock-motor   — fresh database per test (function scope) → no state leaks.
• test_app          — minimal FastAPI with all routers, no lifespan, overridden
                      get_database / get_redis dependencies.
• client            — httpx.AsyncClient wired to the test_app via ASGITransport.

Helper functions (not fixtures)
────────────────────────────────
  make_user(db, ...)      — insert a users document, return str _id
  make_brand(db, ...)     — insert a brands document, return str _id
  login(client, email)    — POST /api/v1/auth/token, return TokenResponse dict
  auth_headers(token)     — {"Authorization": "Bearer <token>"}
"""

from __future__ import annotations

import fnmatch
import time
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

from app.core.security import hash_password

# ── RSA key pair — generated once for the entire test session ─────────────────

@pytest.fixture(scope="session", autouse=True)
def _patch_jwt_keys():
    """Generate a throw-away RSA-2048 key pair and patch the loaders."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public_pem = private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()

    with patch("app.core.security._load_private_key", return_value=private_pem), \
         patch("app.core.security._load_public_key",  return_value=public_pem):
        yield


# ── In-memory Redis ────────────────────────────────────────────────────────────

class FakeRedis:
    """Minimal async-compatible dict-backed Redis for tests.

    Supports: ping, get, set, setex, exists, incr, expire, ttl, scan, unlink.
    Expiry is tracked via real wall-clock time so TTL tests work correctly.
    """

    def __init__(self) -> None:
        # key → (value: str, expires_at: float | None)
        self._store: dict[str, tuple[str, float | None]] = {}

    # ── Internal ──────────────────────────────────────────────────────────────

    def _alive(self, key: str) -> bool:
        if key not in self._store:
            return False
        _, exp = self._store[key]
        if exp is not None and time.monotonic() > exp:
            del self._store[key]
            return False
        return True

    def _val(self, key: str) -> str | None:
        return self._store[key][0] if self._alive(key) else None

    # ── Commands ──────────────────────────────────────────────────────────────

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self._val(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        exp = time.monotonic() + ex if ex else None
        self._store[key] = (str(value), exp)
        return True

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self._store[key] = (str(value), time.monotonic() + ttl)
        return True

    async def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if self._alive(k))

    async def incr(self, key: str) -> int:
        cur = int(self._val(key) or 0) + 1
        _, exp = self._store.get(key, (None, None))
        self._store[key] = (str(cur), exp)
        return cur

    async def expire(self, key: str, ttl: int) -> bool:
        if self._alive(key):
            val, _ = self._store[key]
            self._store[key] = (val, time.monotonic() + ttl)
            return True
        return False

    async def ttl(self, key: str) -> int:
        if not self._alive(key):
            return -2
        _, exp = self._store[key]
        if exp is None:
            return -1
        return max(0, int(exp - time.monotonic()))

    async def scan(self, cursor: int, match: str = "*", count: int = 100) -> tuple[int, list[str]]:
        """Return (0, matching_keys) — single-page scan (cursor always 0 on exit)."""
        alive = [k for k in list(self._store.keys()) if self._alive(k)]
        matched = [k for k in alive if fnmatch.fnmatch(k, match)]
        return 0, matched

    async def unlink(self, *keys: str) -> int:
        removed = 0
        for k in keys:
            if k in self._store:
                del self._store[k]
                removed += 1
        return removed

    async def delete(self, *keys: str) -> int:
        return await self.unlink(*keys)

    async def aclose(self) -> None:
        pass


# ── Database + Redis fixtures ──────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db():
    """Fresh mongomock database per test."""
    client = AsyncMongoMockClient()
    database = client["test_bb_ads"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def fake_redis():
    """Fresh FakeRedis per test; also patched into app.core.redis._redis so
    direct calls (cache decorator, cache invalidation) use the same instance."""
    redis = FakeRedis()
    import app.core.redis as _redis_mod
    old = _redis_mod._redis
    _redis_mod._redis = redis  # type: ignore[assignment]
    yield redis
    _redis_mod._redis = old


# ── Test FastAPI application ───────────────────────────────────────────────────

@pytest_asyncio.fixture
async def test_app(db, fake_redis):
    """Minimal FastAPI app with all routers and no lifespan (no real connections)."""
    from fastapi import FastAPI

    from app.api.v1.routers import auth, brands, campaigns, performance
    from app.core.database import get_database
    from app.core.error_handlers import register_error_handlers
    from app.core.redis import get_redis

    app = FastAPI()

    # Dependency overrides — point to in-process fakes
    async def _get_db():
        return db

    async def _get_redis():
        return fake_redis

    app.dependency_overrides[get_database] = _get_db
    app.dependency_overrides[get_redis] = _get_redis

    # Mount routers (same prefixes as production)
    api = "/api/v1"
    app.include_router(auth.router,        prefix=api)
    app.include_router(brands.router,      prefix=api)
    app.include_router(campaigns.router,   prefix=api)
    app.include_router(performance.router, prefix=api)

    register_error_handlers(app)
    return app


@pytest_asyncio.fixture
async def client(test_app):
    """AsyncClient wired to the test_app via in-process transport."""
    async with AsyncClient(
        transport=ASGITransport(app=test_app),
        base_url="http://test",
    ) as ac:
        yield ac


# ── Data helpers (plain functions, not fixtures) ───────────────────────────────

_AGENCY_OID = ObjectId("aaaaaaaaaaaaaaaaaaaaaaaa")
_AGENCY_ID  = str(_AGENCY_OID)
_PASSWORD   = "TestPass1!"


async def make_user(
    db: Any,
    email: str,
    role: str,
    allowed_brands: list[str] | None = None,
    is_active: bool = True,
) -> str:
    """Insert a user document and return its string _id."""
    oid = ObjectId()
    await db["users"].insert_one({
        "_id": oid,
        "agency_id": _AGENCY_OID,
        "email": email.lower(),
        "hashed_password": hash_password(_PASSWORD),
        "role": role,
        "is_active": is_active,
        "allowed_brands": [ObjectId(b) for b in (allowed_brands or [])],
        "api_keys": [],
    })
    return str(oid)


async def make_brand(
    db: Any,
    name: str,
    slug: str | None = None,
) -> str:
    """Insert a brand document and return its string _id."""
    oid = ObjectId()
    await db["brands"].insert_one({
        "_id": oid,
        "agency_id": _AGENCY_OID,
        "name": name,
        "slug": slug or name.lower().replace(" ", "-"),
        "is_active": True,
        "onboarding_status": "completed",
        "created_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
    })
    return str(oid)


async def login(client: AsyncClient, email: str) -> dict[str, Any]:
    """POST /api/v1/auth/token with the shared test password.  Returns body dict."""
    resp = await client.post(
        "/api/v1/auth/token",
        json={"email": email, "password": _PASSWORD},
    )
    return resp


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
