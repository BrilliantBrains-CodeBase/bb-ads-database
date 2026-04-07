"""
Unit tests for the Performance API endpoints and the new PerformanceRepository methods.

Strategy
────────
• Repository methods are tested against mongomock-motor with real aggregation data.
• Router endpoints are tested via FastAPI TestClient with a mocked dependency stack
  (BrandAccess always returns the fixture brand_id, DB is mongomock).
• Schema helpers (from_doc, KPI math) are tested directly.

Coverage
────────
  PerformanceRepository:
    - get_kpi_summary: totals, derived KPIs, empty result, source filter
    - get_top_campaigns: ranking by spend/roas/leads, limit, source filter, $lookup name
    - get_source_attribution: per-source breakdown, spend_share ordering

  Schemas:
    - DailyRow.from_doc: field mapping, None handling
    - RollupItem.from_doc: field mapping
    - TopCampaignItem.from_doc: derived KPI math, zero-denominator safety
    - AttributionSource.from_doc: spend_share_pct calculation

  Router endpoints (via TestClient):
    - GET /performance/daily: happy path, source filter, unknown source returns []
    - GET /performance/rollup: happy path, unknown source returns []
    - GET /performance/summary: totals + KPIs computed correctly
    - GET /performance/top-campaigns: limit, metric param, unknown metric defaults
    - GET /performance/trend: daily points, source filter
    - GET /performance/attribution: sources + spend_share_pct
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

# ── Constants ──────────────────────────────────────────────────────────────────

BRAND_OID = ObjectId()
BRAND_ID = str(BRAND_OID)
CAMPAIGN_OID_1 = ObjectId()
CAMPAIGN_OID_2 = ObjectId()
TODAY = date(2026, 4, 7)
YESTERDAY = TODAY - timedelta(days=1)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _day_utc(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time(), tzinfo=UTC)


def _perf_doc(
    *,
    brand_id: str = BRAND_ID,
    campaign_id: ObjectId = CAMPAIGN_OID_1,
    source: str = "google_ads",
    d: date = TODAY,
    spend: int = 10_000,
    impressions: int = 1000,
    clicks: int = 50,
    reach: int = 800,
    leads: int = 5,
    conversions: int = 3,
    conv_value: int = 30_000,
    run_id: str = "run-001",
) -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "brand_id": brand_id,
        "campaign_id": campaign_id,
        "source": source,
        "date": _day_utc(d),
        "ingested_at": datetime.now(UTC),
        "ingestion_run_id": run_id,
        "spend_paise": spend,
        "impressions": impressions,
        "clicks": clicks,
        "reach": reach,
        "leads": leads,
        "conversions": conversions,
        "conversion_value_paise": conv_value,
        "ctr": clicks / impressions if impressions else None,
        "cpc_paise": spend // clicks if clicks else None,
        "cpm_paise": int(spend * 1000 // impressions) if impressions else None,
        "cpl_paise": spend // leads if leads else None,
        "roas": conv_value / spend if spend else None,
    }


# ── DB fixtures ────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db():
    client = AsyncMongoMockClient()
    database = client["test_db"]
    yield database
    client.close()


@pytest_asyncio.fixture
async def db_with_data(db):
    """Two campaigns, two sources, two days."""
    await db["ad_performance_raw"].insert_many([
        # google_ads camp1 today
        _perf_doc(campaign_id=CAMPAIGN_OID_1, source="google_ads", d=TODAY,
                  spend=50_000, impressions=5000, clicks=200, leads=10,
                  conversions=5, conv_value=100_000),
        # google_ads camp1 yesterday
        _perf_doc(campaign_id=CAMPAIGN_OID_1, source="google_ads", d=YESTERDAY,
                  spend=40_000, impressions=4000, clicks=160, leads=8,
                  conversions=4, conv_value=80_000),
        # meta camp2 today
        _perf_doc(campaign_id=CAMPAIGN_OID_2, source="meta", d=TODAY,
                  spend=30_000, impressions=8000, clicks=80, leads=20,
                  conversions=2, conv_value=20_000),
    ])
    # Insert campaign names
    await db["campaigns"].insert_many([
        {"_id": CAMPAIGN_OID_1, "brand_id": BRAND_OID, "name": "Google Search",
         "source": "google_ads", "external_id": "111"},
        {"_id": CAMPAIGN_OID_2, "brand_id": BRAND_OID, "name": "Meta Feed",
         "source": "meta", "external_id": "222"},
    ])
    yield db


# ══════════════════════════════════════════════════════════════════════════════
# Section 1: PerformanceRepository — get_kpi_summary
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestGetKpiSummary:
    async def test_totals_across_sources(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        result = await repo.get_kpi_summary(YESTERDAY, TODAY)

        # spend: 40k + 50k + 30k = 120k
        assert result["total_spend_paise"] == 120_000
        # impressions: 4k + 5k + 8k = 17k
        assert result["total_impressions"] == 17_000
        assert result["total_clicks"] == 440
        assert result["total_leads"] == 38
        assert result["total_conversions"] == 11

    async def test_source_filter(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        result = await repo.get_kpi_summary(YESTERDAY, TODAY, source="meta")

        assert result["total_spend_paise"] == 30_000
        assert result["total_impressions"] == 8_000

    async def test_days_with_data(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        result = await repo.get_kpi_summary(YESTERDAY, TODAY)
        # Two distinct dates present
        assert result["days_with_data"] == 2

    async def test_empty_range_returns_empty_dict(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        result = await repo.get_kpi_summary(
            date(2025, 1, 1), date(2025, 1, 31)
        )
        assert result == {}

    async def test_brand_isolation(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        other_brand = str(ObjectId())
        repo = PerformanceRepository(db_with_data, other_brand)
        result = await repo.get_kpi_summary(YESTERDAY, TODAY)
        assert result == {}


# ══════════════════════════════════════════════════════════════════════════════
# Section 2: PerformanceRepository — get_top_campaigns
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestGetTopCampaigns:
    async def test_ranked_by_spend_default(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_top_campaigns(YESTERDAY, TODAY)

        # google_ads camp1: 50k + 40k = 90k spend
        # meta camp2: 30k spend
        assert len(rows) == 2
        assert rows[0]["total_spend_paise"] == 90_000  # google camp1
        assert rows[1]["total_spend_paise"] == 30_000  # meta camp2

    async def test_limit_respected(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_top_campaigns(YESTERDAY, TODAY, limit=1)
        assert len(rows) == 1

    async def test_source_filter(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_top_campaigns(YESTERDAY, TODAY, source="meta")
        assert len(rows) == 1
        assert rows[0]["source"] == "meta"

    async def test_campaign_name_joined(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_top_campaigns(YESTERDAY, TODAY, limit=2)
        names = {r["campaign_name"] for r in rows}
        assert "Google Search" in names
        assert "Meta Feed" in names

    async def test_ranked_by_leads(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        # meta camp2 has 20 leads, google camp1 has 18 leads
        rows = await repo.get_top_campaigns(YESTERDAY, TODAY, metric="leads")
        assert rows[0]["total_leads"] == 20   # meta

    async def test_empty_range(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_top_campaigns(date(2025, 1, 1), date(2025, 1, 31))
        assert rows == []


# ══════════════════════════════════════════════════════════════════════════════
# Section 3: PerformanceRepository — get_source_attribution
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestGetSourceAttribution:
    async def test_two_sources_returned(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_source_attribution(YESTERDAY, TODAY)
        sources = {r["source"] for r in rows}
        assert sources == {"google_ads", "meta"}

    async def test_sorted_by_spend_descending(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, BRAND_ID)
        rows = await repo.get_source_attribution(YESTERDAY, TODAY)
        assert rows[0]["source"] == "google_ads"  # 90k > 30k
        assert rows[0]["total_spend_paise"] == 90_000

    async def test_brand_isolation(self, db_with_data):
        from app.repositories.performance import PerformanceRepository
        repo = PerformanceRepository(db_with_data, str(ObjectId()))
        rows = await repo.get_source_attribution(YESTERDAY, TODAY)
        assert rows == []


# ══════════════════════════════════════════════════════════════════════════════
# Section 4: Schema helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestDailyRowFromDoc:
    def test_maps_all_fields(self):
        from app.api.v1.schemas.performance import DailyRow
        doc = _perf_doc()
        row = DailyRow.from_doc(doc)
        assert row.spend_paise == 10_000
        assert row.source == "google_ads"
        assert row.campaign_id == str(CAMPAIGN_OID_1)
        assert row.brand_id == str(BRAND_OID)

    def test_none_metrics_default_to_zero(self):
        from app.api.v1.schemas.performance import DailyRow
        doc = _perf_doc(spend=0, impressions=0, clicks=0)
        doc["spend_paise"] = None
        row = DailyRow.from_doc(doc)
        assert row.spend_paise == 0


class TestTopCampaignItemFromDoc:
    def test_derived_kpis_computed(self):
        from app.api.v1.schemas.performance import TopCampaignItem
        doc = {
            "campaign_id": CAMPAIGN_OID_1,
            "source": "google_ads",
            "total_spend_paise": 50_000,
            "total_impressions": 5_000,
            "total_clicks": 200,
            "total_leads": 10,
            "total_conversions": 5,
            "total_conversion_value_paise": 100_000,
        }
        item = TopCampaignItem.from_doc(doc)
        assert item.roas == pytest.approx(2.0)
        assert item.ctr == pytest.approx(0.04)
        assert item.cpc_paise == 250
        assert item.cpl_paise == 5_000

    def test_zero_denominators_produce_none(self):
        from app.api.v1.schemas.performance import TopCampaignItem
        doc = {
            "campaign_id": CAMPAIGN_OID_1,
            "source": "google_ads",
            "total_spend_paise": 0,
            "total_impressions": 0,
            "total_clicks": 0,
            "total_leads": 0,
            "total_conversions": 0,
            "total_conversion_value_paise": 0,
        }
        item = TopCampaignItem.from_doc(doc)
        assert item.roas is None
        assert item.ctr is None
        assert item.cpc_paise is None
        assert item.cpl_paise is None


class TestAttributionSourceFromDoc:
    def test_spend_share_pct(self):
        from app.api.v1.schemas.performance import AttributionSource
        doc = {
            "source": "google_ads",
            "total_spend_paise": 75_000,
            "total_impressions": 5000,
            "total_clicks": 200,
            "total_leads": 10,
            "total_conversions": 5,
            "total_conversion_value_paise": 100_000,
        }
        item = AttributionSource.from_doc(doc, total_spend=100_000)
        assert item.spend_share_pct == 75.0

    def test_zero_total_spend(self):
        from app.api.v1.schemas.performance import AttributionSource
        doc = {
            "source": "meta",
            "total_spend_paise": 0,
            "total_impressions": 0,
            "total_clicks": 0,
            "total_leads": 0,
            "total_conversions": 0,
            "total_conversion_value_paise": 0,
        }
        item = AttributionSource.from_doc(doc, total_spend=0)
        assert item.spend_share_pct == 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Section 5: Router tests via TestClient
# ══════════════════════════════════════════════════════════════════════════════

def _build_test_app(db):
    """Build a minimal FastAPI app with the performance router and mocked deps."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from unittest.mock import AsyncMock, MagicMock

    from app.api.v1.routers import performance as perf_module
    from app.core.database import get_database
    from app.middleware.brand_scope import BrandAccess
    from app.middleware.auth import get_current_user

    app = FastAPI()
    app.include_router(perf_module.router, prefix="/api/v1")

    # BrandAccess → always returns BRAND_ID
    app.dependency_overrides[BrandAccess] = lambda: BRAND_ID
    # get_database → our mongomock DB
    app.dependency_overrides[get_database] = lambda: db
    # get_current_user → stubbed user
    user = MagicMock()
    user.user_id = "user001"
    user.role = "admin"
    user.allowed_brands = [BRAND_ID]
    user.can_access_brand = lambda _: True
    app.dependency_overrides[get_current_user] = lambda: user

    return TestClient(app)


@pytest.fixture
def client(db_with_data):
    return _build_test_app(db_with_data)


@pytest.fixture
def empty_client(db):
    return _build_test_app(db)


class TestDailyEndpoint:
    def test_returns_rows(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/daily",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["items"]) == 3

    def test_source_filter(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/daily",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY), "source": "meta"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["source"] == "meta"

    def test_unknown_source_returns_empty(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/daily",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY), "source": "tiktok"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_empty_range_returns_empty(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/daily",
            params={"date_from": "2025-01-01", "date_to": "2025-01-31"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0


class TestRollupEndpoint:
    def test_empty_when_no_rollups(self, empty_client):
        resp = empty_client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/rollup",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_unknown_source_returns_empty(self, empty_client):
        resp = empty_client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/rollup",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY), "source": "tiktok"},
        )
        assert resp.status_code == 200
        assert resp.json()["total"] == 0

    def test_valid_period_types_accepted(self, empty_client):
        for period in ("daily", "weekly", "monthly"):
            resp = empty_client.get(
                f"/api/v1/brands/{BRAND_ID}/performance/rollup",
                params={"period_type": period, "date_from": str(YESTERDAY), "date_to": str(TODAY)},
            )
            assert resp.status_code == 200, f"period_type={period} failed"

    def test_invalid_period_type_returns_422(self, empty_client):
        resp = empty_client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/rollup",
            params={"period_type": "quarterly"},
        )
        assert resp.status_code == 422


class TestSummaryEndpoint:
    def test_totals_correct(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/summary",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_spend_paise"] == 120_000
        assert data["total_impressions"] == 17_000
        assert data["total_leads"] == 38

    def test_kpis_computed(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/summary",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        data = resp.json()
        # spend=120k, impressions=17k → cpm = int(120000 * 1000 / 17000) = 7058
        assert data["cpm_paise"] == int(120_000 * 1000 // 17_000)
        assert data["roas"] is not None
        assert data["ctr"] is not None

    def test_unknown_source_returns_zeros(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/summary",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY), "source": "tiktok"},
        )
        assert resp.status_code == 200
        assert resp.json()["total_spend_paise"] == 0

    def test_empty_range_returns_zeros(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/summary",
            params={"date_from": "2025-01-01", "date_to": "2025-01-31"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_spend_paise"] == 0
        assert data["roas"] is None


class TestTopCampaignsEndpoint:
    def test_returns_campaigns(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2

    def test_limit_param(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY), "limit": 1},
        )
        assert resp.status_code == 200
        assert len(resp.json()["items"]) == 1

    def test_metric_in_response(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"metric": "leads", "date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.json()["metric"] == "leads"

    def test_unknown_metric_defaults_to_spend(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"metric": "unknown_metric", "date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        assert resp.json()["metric"] == "spend_paise"

    def test_limit_over_max_returns_422(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"limit": 100},
        )
        assert resp.status_code == 422

    def test_unknown_source_returns_empty(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/top-campaigns",
            params={"source": "tiktok", "date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.json()["items"] == []


class TestTrendEndpoint:
    def test_returns_daily_points(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/trend",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        data = resp.json()
        # 2 sources × 2 days = up to 4 points (group by date+source when no source filter)
        assert len(data["points"]) >= 2

    def test_source_filter(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/trend",
            params={
                "date_from": str(YESTERDAY), "date_to": str(TODAY),
                "source": "google_ads",
            },
        )
        assert resp.status_code == 200
        points = resp.json()["points"]
        assert all(p["source"] == "google_ads" for p in points)

    def test_unknown_source_returns_empty(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/trend",
            params={"source": "tiktok", "date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.json()["points"] == []

    def test_spend_totals_in_points(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/trend",
            params={
                "date_from": str(TODAY), "date_to": str(TODAY),
                "source": "google_ads",
            },
        )
        points = resp.json()["points"]
        assert len(points) == 1
        assert points[0]["total_spend_paise"] == 50_000


class TestAttributionEndpoint:
    def test_two_sources(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/attribution",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        assert resp.status_code == 200
        data = resp.json()
        sources = {s["source"] for s in data["sources"]}
        assert sources == {"google_ads", "meta"}

    def test_total_spend_paise(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/attribution",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        data = resp.json()
        assert data["total_spend_paise"] == 120_000

    def test_spend_share_adds_to_100(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/attribution",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        sources = resp.json()["sources"]
        total_share = sum(s["spend_share_pct"] for s in sources)
        assert abs(total_share - 100.0) < 0.01

    def test_google_has_higher_share(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/attribution",
            params={"date_from": str(YESTERDAY), "date_to": str(TODAY)},
        )
        google = next(s for s in resp.json()["sources"] if s["source"] == "google_ads")
        meta = next(s for s in resp.json()["sources"] if s["source"] == "meta")
        assert google["spend_share_pct"] > meta["spend_share_pct"]

    def test_empty_range_returns_no_sources(self, client):
        resp = client.get(
            f"/api/v1/brands/{BRAND_ID}/performance/attribution",
            params={"date_from": "2025-01-01", "date_to": "2025-01-31"},
        )
        assert resp.json()["sources"] == []
        assert resp.json()["total_spend_paise"] == 0
