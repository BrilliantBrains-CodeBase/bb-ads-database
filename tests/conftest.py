"""
Root pytest conftest — shared fixtures and mock activation.

Mock activation via env vars
─────────────────────────────
Set these before running pytest to swap real SDK calls for fixtures:

    USE_MOCK_GOOGLE_ADS=1 pytest   # patch GoogleAdsIngestionService._run_gaql_sync
    USE_MOCK_META_ADS=1   pytest   # patch MetaAdsIngestionService._run_insights_sync

Both can be active simultaneously:

    USE_MOCK_GOOGLE_ADS=1 USE_MOCK_META_ADS=1 pytest

When a mock is active every test in the session uses it automatically —
no per-test patching required.  Tests that need to override behaviour
(e.g. simulate a rate-limit error) can still use patch.object() locally
and it will take precedence over the session-level patch.

Shared fixtures
───────────────
- db                 — mongomock-motor AsyncIOMotorDatabase (function scope)
- brand_id           — ObjectId string of a minimal brand doc inserted into db
- google_ads_brand   — brand with platforms.google_ads credentials
- meta_ads_brand     — brand with platforms.meta_ads credentials
- sample_gaql_rows   — one day of Google Ads fixture rows (list[dict])
- sample_insight_rows — one day of Meta Ads fixture rows (list[dict])
- date_range_rows_google — 7 days of Google Ads rows
- date_range_rows_meta   — 7 days of Meta Ads rows
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from tests.mocks.google_ads import (
    FIXTURE_CAMPAIGNS as GOOGLE_CAMPAIGNS,
    MockGoogleAdsClient,
    build_date_range_rows as google_date_range,
)
from tests.mocks.meta_ads import (
    FIXTURE_CAMPAIGNS as META_CAMPAIGNS,
    MockMetaAdsClient,
    build_date_range_rows as meta_date_range,
)

# ── Constants ──────────────────────────────────────────────────────────────────

TODAY = date(2026, 4, 6)
YESTERDAY = TODAY - timedelta(days=1)

_GOOGLE_CUSTOMER_ID = "1234567890"
_META_AD_ACCOUNT_ID = "act_9876543210"

# ── Session-level mock activation ─────────────────────────────────────────────

def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    """Register custom markers so pytest doesn't warn about unknown ones."""
    config.addinivalue_line("markers", "uses_google_mock: test uses the Google Ads mock")
    config.addinivalue_line("markers", "uses_meta_mock: test uses the Meta Ads mock")


@pytest.fixture(scope="session", autouse=True)
def _activate_google_mock():
    """If USE_MOCK_GOOGLE_ADS=1, patch _run_gaql_sync for the entire session."""
    if not os.getenv("USE_MOCK_GOOGLE_ADS"):
        yield
        return

    mock_client = MockGoogleAdsClient()

    # We patch at the class level so ALL instances pick it up automatically.
    with patch(
        "app.services.ingestion.google_ads.GoogleAdsIngestionService._run_gaql_sync",
        new=mock_client.run,
    ):
        yield


@pytest.fixture(scope="session", autouse=True)
def _activate_meta_mock():
    """If USE_MOCK_META_ADS=1, patch _run_insights_sync for the entire session."""
    if not os.getenv("USE_MOCK_META_ADS"):
        yield
        return

    mock_client = MockMetaAdsClient()

    with patch(
        "app.services.ingestion.meta_ads.MetaAdsIngestionService._run_insights_sync",
        new=mock_client.run,
    ):
        yield


# ── Database fixture ───────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db():
    """Fresh mongomock-motor database, discarded after each test."""
    client = AsyncMongoMockClient()
    database = client["test_bb_ads"]
    yield database
    client.close()


# ── Brand fixtures ─────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def brand_id(db) -> str:
    """Insert a minimal brand doc and return its string _id."""
    oid = ObjectId()
    await db["brands"].insert_one({
        "_id": oid,
        "name": "Fixture Brand",
        "slug": "fixture-brand",
        "agency_id": "agency001",
        "is_active": True,
        "onboarding_status": "completed",
    })
    return str(oid)


@pytest_asyncio.fixture
async def google_ads_brand(db) -> str:
    """Brand pre-seeded with valid Google Ads credentials. Returns brand_id string."""
    oid = ObjectId()
    await db["brands"].insert_one({
        "_id": oid,
        "name": "Google Brand",
        "slug": "google-brand",
        "agency_id": "agency001",
        "is_active": True,
        "onboarding_status": "completed",
        "platforms": {
            "google_ads": {
                "customer_id": _GOOGLE_CUSTOMER_ID,
                "refresh_token": "mock_refresh_token",
                "login_customer_id": _GOOGLE_CUSTOMER_ID,
            }
        },
    })
    return str(oid)


@pytest_asyncio.fixture
async def meta_ads_brand(db) -> str:
    """Brand pre-seeded with valid Meta credentials. Returns brand_id string."""
    oid = ObjectId()
    await db["brands"].insert_one({
        "_id": oid,
        "name": "Meta Brand",
        "slug": "meta-brand",
        "agency_id": "agency001",
        "is_active": True,
        "onboarding_status": "completed",
        "platforms": {
            "meta_ads": {
                "access_token": "mock_access_token",
                "ad_account_id": _META_AD_ACCOUNT_ID,
                "currency": "INR",
            }
        },
    })
    return str(oid)


@pytest_asyncio.fixture
async def all_platforms_brand(db) -> str:
    """Brand with both Google Ads and Meta credentials."""
    oid = ObjectId()
    await db["brands"].insert_one({
        "_id": oid,
        "name": "Full Brand",
        "slug": "full-brand",
        "agency_id": "agency001",
        "is_active": True,
        "onboarding_status": "completed",
        "platforms": {
            "google_ads": {
                "customer_id": _GOOGLE_CUSTOMER_ID,
                "refresh_token": "mock_refresh_token",
            },
            "meta_ads": {
                "access_token": "mock_access_token",
                "ad_account_id": _META_AD_ACCOUNT_ID,
                "currency": "INR",
            },
        },
    })
    return str(oid)


# ── Fixture row helpers ────────────────────────────────────────────────────────

@pytest.fixture
def sample_gaql_rows() -> list[dict]:
    """One day of Google Ads fixture rows for TODAY."""
    client = MockGoogleAdsClient()
    return client.run(
        {"customer_id": _GOOGLE_CUSTOMER_ID},
        TODAY.strftime("%Y-%m-%d"),
    )


@pytest.fixture
def sample_insight_rows() -> list[dict]:
    """One day of Meta Ads fixture rows for TODAY."""
    client = MockMetaAdsClient()
    return client.run(
        {"ad_account_id": _META_AD_ACCOUNT_ID},
        TODAY.strftime("%Y-%m-%d"),
    )


@pytest.fixture
def date_range_rows_google() -> list[dict]:
    """Seven days of Google Ads fixture rows ending on TODAY."""
    return google_date_range(
        start_date=TODAY - timedelta(days=6),
        end_date=TODAY,
        customer_id=_GOOGLE_CUSTOMER_ID,
    )


@pytest.fixture
def date_range_rows_meta() -> list[dict]:
    """Seven days of Meta Ads fixture rows ending on TODAY."""
    return meta_date_range(
        start_date=TODAY - timedelta(days=6),
        end_date=TODAY,
        ad_account_id=_META_AD_ACCOUNT_ID,
    )


# ── Mock client fixtures (for per-test customisation) ─────────────────────────

@pytest.fixture
def google_mock_client() -> MockGoogleAdsClient:
    """Default MockGoogleAdsClient — override fields in tests as needed."""
    return MockGoogleAdsClient()


@pytest.fixture
def google_mock_rate_limit() -> MockGoogleAdsClient:
    """MockGoogleAdsClient that raises RESOURCE_EXHAUSTED on the first call."""
    return MockGoogleAdsClient(simulate_rate_limit=True)


@pytest.fixture
def google_mock_auth_error() -> MockGoogleAdsClient:
    """MockGoogleAdsClient that raises UNAUTHENTICATED."""
    return MockGoogleAdsClient(simulate_auth_error=True)


@pytest.fixture
def meta_mock_client() -> MockMetaAdsClient:
    """Default MockMetaAdsClient."""
    return MockMetaAdsClient()


@pytest.fixture
def meta_mock_rate_limit() -> MockMetaAdsClient:
    """MockMetaAdsClient that raises MetaRateLimitError (code 17)."""
    return MockMetaAdsClient(simulate_rate_limit=True)


@pytest.fixture
def meta_mock_auth_error() -> MockMetaAdsClient:
    """MockMetaAdsClient that raises MetaAuthError (code 190)."""
    return MockMetaAdsClient(simulate_auth_error=True)
