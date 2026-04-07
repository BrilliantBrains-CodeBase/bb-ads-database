"""
Integration tests: brand-level access isolation

Scenario
────────
  Two brands (A, B) owned by the same agency.

  Users:
    viewer_a   — viewer role, allowed_brands: [brand_A]
    viewer_b   — viewer role, allowed_brands: [brand_B]
    analyst_ab — analyst role, allowed_brands: [brand_A, brand_B]
    super_admin — super_admin role, allowed_brands: [] (bypass via role)

Coverage
────────
  GET /brands/{id}
  ├── viewer_a  → brand_A → 200
  ├── viewer_a  → brand_B → 403
  ├── viewer_b  → brand_A → 403
  └── super_admin → both → 200

  GET /brands/{id}/performance/daily
  ├── viewer_a  → brand_A → 200 (empty but allowed)
  ├── viewer_a  → brand_B → 403
  └── analyst_ab → brand_A + brand_B → 200, 200

  POST /brands (requires admin role)
  ├── viewer_a  → 403
  └── analyst_ab → 403

  Cross-brand data leakage
  └── viewer_a owns data in brand_A; querying brand_B returns 403,
      not 200-with-empty-data (the boundary is enforced before DB access)
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId

from tests.integration.conftest import auth_headers, login, make_brand, make_user


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def brands(db):
    """Two brands in the same agency. Returns {brand_A: str, brand_B: str}."""
    a = await make_brand(db, "Alpha Corp",   "alpha-corp")
    b = await make_brand(db, "Beta Corp",    "beta-corp")
    return {"brand_A": a, "brand_B": b}


@pytest_asyncio.fixture
async def users(db, brands):
    """Four users with varying brand access. Returns dict of role→email."""
    users = {
        "viewer_a":    "viewer_a@bb.test",
        "viewer_b":    "viewer_b@bb.test",
        "analyst_ab":  "analyst@bb.test",
        "super_admin": "superadmin@bb.test",
    }
    await make_user(db, users["viewer_a"],   "viewer",      [brands["brand_A"]])
    await make_user(db, users["viewer_b"],   "viewer",      [brands["brand_B"]])
    await make_user(db, users["analyst_ab"], "analyst",     [brands["brand_A"], brands["brand_B"]])
    await make_user(db, users["super_admin"],"super_admin", [])
    return users


async def _get_token(client, email: str) -> str:
    resp = await login(client, email)
    assert resp.status_code == 200, f"login failed for {email}: {resp.json()}"
    return resp.json()["access_token"]


# ── GET /brands/{id} ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_viewer_can_access_allowed_brand(client, brands, users):
    token = await _get_token(client, users["viewer_a"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_A']}",
        headers=auth_headers(token),
    )
    assert resp.status_code == 200
    assert resp.json()["id"] == brands["brand_A"]


@pytest.mark.asyncio
async def test_viewer_forbidden_on_other_brand(client, brands, users):
    token = await _get_token(client, users["viewer_a"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_B']}",
        headers=auth_headers(token),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden"


@pytest.mark.asyncio
async def test_viewer_b_forbidden_on_brand_a(client, brands, users):
    token = await _get_token(client, users["viewer_b"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_A']}",
        headers=auth_headers(token),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_super_admin_can_access_any_brand(client, brands, users):
    token = await _get_token(client, users["super_admin"])
    for brand_id in brands.values():
        resp = await client.get(
            f"/api/v1/brands/{brand_id}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200, f"super_admin denied brand {brand_id}"


@pytest.mark.asyncio
async def test_analyst_can_access_both_assigned_brands(client, brands, users):
    token = await _get_token(client, users["analyst_ab"])
    for brand_id in brands.values():
        resp = await client.get(
            f"/api/v1/brands/{brand_id}",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200


# ── GET /brands/{id}/performance/daily ────────────────────────────────────────

@pytest.mark.asyncio
async def test_viewer_can_read_performance_of_allowed_brand(client, brands, users):
    token = await _get_token(client, users["viewer_a"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_A']}/performance/daily",
        headers=auth_headers(token),
    )
    assert resp.status_code == 200
    assert "items" in resp.json()   # DailyResponse.items


@pytest.mark.asyncio
async def test_viewer_forbidden_on_other_brand_performance(client, brands, users):
    token = await _get_token(client, users["viewer_a"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_B']}/performance/daily",
        headers=auth_headers(token),
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_analyst_can_read_performance_for_both_brands(client, brands, users):
    token = await _get_token(client, users["analyst_ab"])
    for brand_id in brands.values():
        resp = await client.get(
            f"/api/v1/brands/{brand_id}/performance/daily",
            headers=auth_headers(token),
        )
        assert resp.status_code == 200


# ── Boundary enforcement happens before DB access ─────────────────────────────

@pytest.mark.asyncio
async def test_brand_b_forbidden_not_empty(client, db, brands, users):
    """viewer_a should get 403, never 200-with-empty-data, on brand_B.

    Seed perf data in brand_A so there's something to potentially leak,
    then verify the access boundary blocks before any DB query.
    """
    from datetime import date
    from app.repositories.performance import PerformanceRepository

    perf_repo = PerformanceRepository(db, brands["brand_A"])
    camp_id = str(ObjectId())
    await perf_repo.upsert(
        source="google_ads",
        campaign_id=camp_id,
        record_date=date.today(),
        metrics={"spend_paise": 100_000, "impressions": 5000, "clicks": 50},
        ingestion_run_id="test-run-1",
    )

    token = await _get_token(client, users["viewer_a"])
    resp = await client.get(
        f"/api/v1/brands/{brands['brand_B']}/performance/daily",
        headers=auth_headers(token),
    )
    # Must be 403, not 200 (even though brand_B has zero rows — the gate is role/brand)
    assert resp.status_code == 403


# ── Permission gating (role-based) ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_viewer_cannot_create_brand(client, brands, users):
    """POST /brands requires MANAGE_BRANDS = admin+."""
    token = await _get_token(client, users["viewer_a"])
    with patch("app.services.brand_storage.create_brand_folders"), \
         patch("app.services.clickup.create_onboarding_task"):
        resp = await client.post(
            "/api/v1/brands",
            json={"name": "New Brand", "slug": "new-brand"},
            headers=auth_headers(token),
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_analyst_cannot_create_brand(client, brands, users):
    token = await _get_token(client, users["analyst_ab"])
    with patch("app.services.brand_storage.create_brand_folders"), \
         patch("app.services.clickup.create_onboarding_task"):
        resp = await client.post(
            "/api/v1/brands",
            json={"name": "New Brand", "slug": "new-brand"},
            headers=auth_headers(token),
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_unauthenticated_cannot_access_any_brand(client, brands):
    for brand_id in brands.values():
        resp = await client.get(f"/api/v1/brands/{brand_id}")
        assert resp.status_code == 401


# ── list_brands respects role scope ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_viewer_list_only_sees_allowed_brands(client, brands, users):
    """viewer_a has only brand_A — list endpoint must NOT expose brand_B."""
    token = await _get_token(client, users["viewer_a"])
    resp = await client.get("/api/v1/brands", headers=auth_headers(token))
    assert resp.status_code == 200
    ids = {b["id"] for b in resp.json()["brands"]}
    assert brands["brand_A"] in ids
    assert brands["brand_B"] not in ids


@pytest.mark.asyncio
async def test_super_admin_list_sees_all_agency_brands(client, brands, users):
    token = await _get_token(client, users["super_admin"])
    resp = await client.get("/api/v1/brands", headers=auth_headers(token))
    assert resp.status_code == 200
    ids = {b["id"] for b in resp.json()["brands"]}
    assert brands["brand_A"] in ids
    assert brands["brand_B"] in ids
