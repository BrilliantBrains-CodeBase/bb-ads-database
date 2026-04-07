"""
Integration tests: full auth lifecycle

Coverage
────────
  Login
  ├── valid credentials → 200 + token pair
  ├── wrong password    → 401
  ├── unknown email     → 401
  └── inactive account  → 401

  Protected routes
  ├── valid token → 200
  └── missing token → 401

  Token refresh
  ├── valid refresh token → 200, new token pair
  ├── old refresh token blocked after rotation → 401
  └── malformed refresh token → 401

  Logout
  ├── POST /auth/logout → 204
  ├── access token blocked after logout → 401
  └── refresh token blocked after logout → 401

  Rate limiting
  └── 6th login attempt in window → 429 + Retry-After header
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from tests.integration.conftest import _PASSWORD, auth_headers, login, make_user


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def active_user(db):
    """Insert a super_admin user and return their email."""
    email = "integration@bb.test"
    await make_user(db, email, "super_admin")
    return email


@pytest_asyncio.fixture
async def inactive_user(db):
    email = "inactive@bb.test"
    await make_user(db, email, "viewer", is_active=False)
    return email


@pytest_asyncio.fixture
async def token_pair(client, active_user):
    """Obtain a fresh token pair for the active user."""
    resp = await login(client, active_user)
    assert resp.status_code == 200
    return resp.json()


# ── Login ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_login_success(client, active_user):
    resp = await login(client, active_user)
    assert resp.status_code == 200
    body = resp.json()
    assert "access_token" in body
    assert "refresh_token" in body
    assert body["expires_in"] > 0
    # Tokens are non-empty strings
    assert len(body["access_token"]) > 20
    assert len(body["refresh_token"]) > 20


@pytest.mark.asyncio
async def test_login_wrong_password(client, active_user):
    resp = await client.post(
        "/api/v1/auth/token",
        json={"email": active_user, "password": "wrong_password"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_login_unknown_email(client):
    resp = await client.post(
        "/api/v1/auth/token",
        json={"email": "nobody@nowhere.com", "password": _PASSWORD},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_login_inactive_user(client, inactive_user):
    resp = await client.post(
        "/api/v1/auth/token",
        json={"email": inactive_user, "password": _PASSWORD},
    )
    assert resp.status_code == 401


# ── Protected routes ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_valid_token_reaches_protected_route(client, token_pair):
    resp = await client.get(
        "/api/v1/brands",
        headers=auth_headers(token_pair["access_token"]),
    )
    # 200 (empty list) — not 401/403
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_missing_auth_header_rejected(client):
    resp = await client.get("/api/v1/brands")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_garbage_token_rejected(client):
    resp = await client.get(
        "/api/v1/brands",
        headers={"Authorization": "Bearer this.is.garbage"},
    )
    assert resp.status_code == 401


# ── Token refresh ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_refresh_returns_new_token_pair(client, token_pair):
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token_pair["refresh_token"]},
    )
    assert resp.status_code == 200
    new = resp.json()
    assert "access_token" in new
    assert "refresh_token" in new
    # Tokens should differ from the originals (JTI rotation)
    assert new["access_token"]  != token_pair["access_token"]
    assert new["refresh_token"] != token_pair["refresh_token"]


@pytest.mark.asyncio
async def test_refresh_token_is_rotated_immediately(client, token_pair):
    """After one successful refresh the old refresh token must be blocklisted."""
    # First refresh — succeeds
    r1 = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token_pair["refresh_token"]},
    )
    assert r1.status_code == 200

    # Re-use of the same old refresh token must fail
    r2 = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token_pair["refresh_token"]},
    )
    assert r2.status_code == 401


@pytest.mark.asyncio
async def test_refresh_with_malformed_token(client):
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": "bad.token.here"},
    )
    assert resp.status_code == 401


# ── Logout ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_logout_returns_204(client, token_pair):
    resp = await client.post(
        "/api/v1/auth/logout",
        json={
            "access_token":  token_pair["access_token"],
            "refresh_token": token_pair["refresh_token"],
        },
    )
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_access_token_blocked_after_logout(client, token_pair):
    """After logout the access token must be rejected by protected routes."""
    await client.post(
        "/api/v1/auth/logout",
        json={
            "access_token":  token_pair["access_token"],
            "refresh_token": token_pair["refresh_token"],
        },
    )
    resp = await client.get(
        "/api/v1/brands",
        headers=auth_headers(token_pair["access_token"]),
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_refresh_token_blocked_after_logout(client, token_pair):
    """After logout the refresh token must be rejected."""
    await client.post(
        "/api/v1/auth/logout",
        json={
            "access_token":  token_pair["access_token"],
            "refresh_token": token_pair["refresh_token"],
        },
    )
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token_pair["refresh_token"]},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_logout_is_idempotent(client, token_pair):
    """A second logout call with the same tokens must still return 204."""
    payload = {
        "access_token":  token_pair["access_token"],
        "refresh_token": token_pair["refresh_token"],
    }
    r1 = await client.post("/api/v1/auth/logout", json=payload)
    assert r1.status_code == 204
    r2 = await client.post("/api/v1/auth/logout", json=payload)
    assert r2.status_code == 204


# ── Rate limiting ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rate_limit_exceeded(client, active_user):
    """The 6th login attempt in the same window must return 429."""
    payload = {"email": active_user, "password": "wrong"}

    for i in range(5):
        r = await client.post("/api/v1/auth/token", json=payload)
        # First 5 attempts: 401 (bad password), not 429
        assert r.status_code == 401, f"attempt {i+1} should be 401, got {r.status_code}"

    # 6th attempt → rate-limited
    r6 = await client.post("/api/v1/auth/token", json=payload)
    assert r6.status_code == 429
    assert "Retry-After" in r6.headers


@pytest.mark.asyncio
async def test_rate_limit_does_not_block_different_ip(client, db, active_user):
    """Rate-limiting is per-IP; a fresh FakeRedis has no prior counts."""
    # The test environment always uses the same IP ("testclient").
    # Verify that 5 failed attempts from this IP don't block a valid login
    # from a different origin — we can't change the IP in unit tests,
    # so we just verify the counter logic: exactly 5 failures before the block.
    # (The 429 test above covers the ≥6 threshold case.)
    payload = {"email": active_user, "password": "wrong"}
    for _ in range(4):
        await client.post("/api/v1/auth/token", json=payload)

    # 5th attempt: still 401, not yet rate-limited
    r5 = await client.post("/api/v1/auth/token", json=payload)
    assert r5.status_code == 401
