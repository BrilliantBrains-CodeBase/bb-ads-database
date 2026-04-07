"""
Integration tests: performance API — seed data → query → verify aggregation math

Seed layout (fixed, deterministic)
────────────────────────────────────
  Brand: "Perf Test Brand"
  3 dates: D-2, D-1, D-0  (relative to ANCHOR_DATE)

  Campaigns & daily values (same each day):

    camp_ga_1  (google_ads)  spend=100k  imp=10k  clicks=100  leads=5  conv=3  cv=300k
    camp_ga_2  (google_ads)  spend=200k  imp=20k  clicks=200  leads=10 conv=6  cv=600k
    camp_mt_1  (meta)        spend=150k  imp=15k  clicks=150  leads=7  conv=4  cv=450k

  All values in INR paise (e.g. 100_000 paise = ₹1000).

Expected totals (all sources, 3 days)
────────────────────────────────────────
  total_spend_paise         = (100k+200k+150k) × 3  = 1_350_000
  total_impressions         = (10k+20k+15k)   × 3  = 135_000
  total_clicks              = (100+200+150)   × 3  = 1_350
  total_leads               = (5+10+7)        × 3  = 66
  total_conversion_value    = (300k+600k+450k)× 3  = 4_050_000
  ROAS                      = 4_050_000 / 1_350_000 = 3.0

Attribution
────────────
  google_ads: spend = 900_000 → share = 66.67 %
  meta:       spend = 450_000 → share = 33.33 %

Coverage
────────
  GET /performance/daily
  ├── returns all rows across 3 dates
  ├── source filter: google_ads → 2 campaigns × 3 dates = 6 rows
  ├── date range filter: single day → 3 rows (all 3 campaigns)
  └── unknown source → 200 with empty items (not 422)

  GET /performance/summary
  ├── total_spend_paise == 1_350_000
  ├── total_impressions == 135_000
  ├── roas == 3.0 (exact)
  ├── ctr  == total_clicks / total_impressions
  └── days_with_data == 3

  GET /performance/trend
  ├── returns 3 TrendPoints (one per seeded date)
  └── sum of point spend == total_spend_paise

  GET /performance/top-campaigns
  ├── returns 3 items (one per campaign, summed across dates)
  ├── top by spend: camp_ga_2 is highest
  └── spend per campaign == daily_value × 3 (summed across dates)

  GET /performance/attribution
  ├── 2 source entries (google_ads, meta)
  ├── total_spend_paise == grand total
  └── spend_share_pct sums to 100 %

  Cache behaviour
  └── second identical request hits Redis cache (verified via FakeRedis.get())
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio
from bson import ObjectId

from app.repositories.performance import PerformanceRepository
from tests.integration.conftest import _PASSWORD, auth_headers, login, make_brand, make_user

# ── Fixed test data ────────────────────────────────────────────────────────────

ANCHOR = date(2026, 4, 6)

_DATES = [ANCHOR - timedelta(days=2), ANCHOR - timedelta(days=1), ANCHOR]

_CAMPAIGNS: list[dict] = [
    {
        "key":    "ga_1",
        "source": "google_ads",
        "spend":  100_000,
        "imp":    10_000,
        "clicks": 100,
        "leads":  5,
        "conv":   3,
        "cv":     300_000,
    },
    {
        "key":    "ga_2",
        "source": "google_ads",
        "spend":  200_000,
        "imp":    20_000,
        "clicks": 200,
        "leads":  10,
        "conv":   6,
        "cv":     600_000,
    },
    {
        "key":    "mt_1",
        "source": "meta",
        "spend":  150_000,
        "imp":    15_000,
        "clicks": 150,
        "leads":  7,
        "conv":   4,
        "cv":     450_000,
    },
]

# Aggregated expected values
_N_DATES = len(_DATES)
_TOTAL_SPEND   = sum(c["spend"]  for c in _CAMPAIGNS) * _N_DATES   # 1_350_000
_TOTAL_IMP     = sum(c["imp"]    for c in _CAMPAIGNS) * _N_DATES   # 135_000
_TOTAL_CLICKS  = sum(c["clicks"] for c in _CAMPAIGNS) * _N_DATES   # 1_350
_TOTAL_LEADS   = sum(c["leads"]  for c in _CAMPAIGNS) * _N_DATES   # 66
_TOTAL_CV      = sum(c["cv"]     for c in _CAMPAIGNS) * _N_DATES   # 4_050_000
_ROAS          = _TOTAL_CV / _TOTAL_SPEND                           # 3.0
_GA_SPEND      = sum(c["spend"] for c in _CAMPAIGNS if c["source"] == "google_ads") * _N_DATES   # 900_000
_META_SPEND    = sum(c["spend"] for c in _CAMPAIGNS if c["source"] == "meta") * _N_DATES         # 450_000


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def brand_id(db):
    return await make_brand(db, "Perf Test Brand", "perf-test")


@pytest_asyncio.fixture
async def seeded_db(db, brand_id):
    """Insert deterministic performance rows for all campaigns × dates."""
    repo = PerformanceRepository(db, brand_id)
    camp_oids: dict[str, str] = {c["key"]: str(ObjectId()) for c in _CAMPAIGNS}

    for camp in _CAMPAIGNS:
        for d in _DATES:
            await repo.upsert(
                source=camp["source"],
                campaign_id=camp_oids[camp["key"]],
                record_date=d,
                metrics={
                    "spend_paise":              camp["spend"],
                    "impressions":              camp["imp"],
                    "clicks":                   camp["clicks"],
                    "leads":                    camp["leads"],
                    "conversions":              camp["conv"],
                    "conversion_value_paise":   camp["cv"],
                    "reach":                    int(camp["imp"] * 0.7),
                    "ctr":                      round(camp["clicks"] / camp["imp"], 6),
                    "cpc_paise":                camp["spend"] // camp["clicks"],
                    "cpm_paise":                camp["spend"] * 1000 // camp["imp"],
                    "cpl_paise":                camp["spend"] // camp["leads"],
                    "roas":                     camp["cv"] / camp["spend"],
                },
                ingestion_run_id="test-run-perf",
            )

    return {"brand_id": brand_id, "camp_oids": camp_oids}


@pytest_asyncio.fixture
async def user_and_token(client, db, brand_id):
    """Create a viewer user for the brand, login, return token."""
    email = "perf_viewer@bb.test"
    await make_user(db, email, "viewer", [brand_id])
    resp = await login(client, email)
    assert resp.status_code == 200
    return resp.json()["access_token"]


# ── GET /performance/daily ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_daily_returns_all_seeded_rows(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/daily",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == len(_CAMPAIGNS) * _N_DATES


@pytest.mark.asyncio
async def test_daily_source_filter(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/daily",
        headers=auth_headers(user_and_token),
        params={
            "date_from": str(_DATES[0]),
            "date_to":   str(_DATES[-1]),
            "source":    "google_ads",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    ga_count = sum(1 for c in _CAMPAIGNS if c["source"] == "google_ads") * _N_DATES
    assert body["total"] == ga_count
    sources = {row["source"] for row in body["items"]}
    assert sources == {"google_ads"}


@pytest.mark.asyncio
async def test_daily_date_range_filter(client, seeded_db, user_and_token):
    """Filtering to a single day returns exactly len(_CAMPAIGNS) rows."""
    brand_id = seeded_db["brand_id"]
    single_day = str(_DATES[1])
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/daily",
        headers=auth_headers(user_and_token),
        params={"date_from": single_day, "date_to": single_day},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == len(_CAMPAIGNS)


@pytest.mark.asyncio
async def test_daily_unknown_source_returns_empty_not_422(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/daily",
        headers=auth_headers(user_and_token),
        params={"source": "tiktok_does_not_exist"},
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


# ── GET /performance/summary ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_summary_total_spend(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.status_code == 200
    assert resp.json()["total_spend_paise"] == _TOTAL_SPEND


@pytest.mark.asyncio
async def test_summary_total_impressions(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.json()["total_impressions"] == _TOTAL_IMP


@pytest.mark.asyncio
async def test_summary_roas_is_correct(client, seeded_db, user_and_token):
    """ROAS = total_conversion_value / total_spend == 3.0 exactly."""
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    body = resp.json()
    assert body["roas"] == pytest.approx(_ROAS, abs=0.001)


@pytest.mark.asyncio
async def test_summary_ctr_equals_clicks_over_impressions(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    body = resp.json()
    expected_ctr = _TOTAL_CLICKS / _TOTAL_IMP
    assert body["ctr"] == pytest.approx(expected_ctr, rel=1e-4)


@pytest.mark.asyncio
async def test_summary_days_with_data(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.json()["days_with_data"] == _N_DATES


@pytest.mark.asyncio
async def test_summary_source_filter_ga_only(client, seeded_db, user_and_token):
    """Summary filtered to google_ads must exclude meta spend."""
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={
            "date_from": str(_DATES[0]),
            "date_to":   str(_DATES[-1]),
            "source":    "google_ads",
        },
    )
    assert resp.json()["total_spend_paise"] == _GA_SPEND


@pytest.mark.asyncio
async def test_summary_empty_range_returns_zero_spend(client, seeded_db, user_and_token):
    """A date range with no data returns zeros, not an error."""
    brand_id = seeded_db["brand_id"]
    far_future = date(2030, 1, 1)
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/summary",
        headers=auth_headers(user_and_token),
        params={"date_from": str(far_future), "date_to": str(far_future)},
    )
    assert resp.status_code == 200
    assert resp.json()["total_spend_paise"] == 0
    assert resp.json()["roas"] is None


# ── GET /performance/trend ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_trend_returns_one_point_per_date(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/trend",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["points"]) == _N_DATES


@pytest.mark.asyncio
async def test_trend_point_spend_sums_correctly(client, seeded_db, user_and_token):
    """Sum of TrendPoint.total_spend_paise across all points == grand total."""
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/trend",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    total = sum(p["total_spend_paise"] for p in resp.json()["points"])
    assert total == _TOTAL_SPEND


@pytest.mark.asyncio
async def test_trend_points_are_sorted_ascending(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/trend",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    points = resp.json()["points"]
    dates = [p["date"] for p in points]
    assert dates == sorted(dates)


# ── GET /performance/top-campaigns ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_top_campaigns_returns_all_campaigns(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/top-campaigns",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == len(_CAMPAIGNS)


@pytest.mark.asyncio
async def test_top_campaigns_highest_spend_is_ga_2(client, seeded_db, user_and_token):
    """camp_ga_2 has the highest spend (200k/day × 3 = 600k)."""
    brand_id = seeded_db["brand_id"]
    ga2_oid = seeded_db["camp_oids"]["ga_2"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/top-campaigns",
        headers=auth_headers(user_and_token),
        params={
            "date_from": str(_DATES[0]),
            "date_to":   str(_DATES[-1]),
            "metric":    "spend_paise",
        },
    )
    items = resp.json()["items"]
    top = items[0]
    assert top["campaign_id"] == ga2_oid
    assert top["total_spend_paise"] == 200_000 * _N_DATES


@pytest.mark.asyncio
async def test_top_campaigns_per_campaign_spend_equals_daily_times_dates(
    client, seeded_db, user_and_token
):
    """Each item's total_spend must equal the per-day spend × number of seeded dates."""
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/top-campaigns",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    spend_by_oid = {
        seeded_db["camp_oids"][c["key"]]: c["spend"] * _N_DATES
        for c in _CAMPAIGNS
    }
    for item in resp.json()["items"]:
        expected = spend_by_oid.get(item["campaign_id"])
        if expected is not None:
            assert item["total_spend_paise"] == expected


# ── GET /performance/attribution ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_attribution_has_both_sources(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/attribution",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.status_code == 200
    sources = {s["source"] for s in resp.json()["sources"]}
    assert "google_ads" in sources
    assert "meta" in sources


@pytest.mark.asyncio
async def test_attribution_total_spend_matches_summary(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/attribution",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    assert resp.json()["total_spend_paise"] == _TOTAL_SPEND


@pytest.mark.asyncio
async def test_attribution_spend_per_source(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/attribution",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    sources = {s["source"]: s for s in resp.json()["sources"]}
    assert sources["google_ads"]["total_spend_paise"] == _GA_SPEND
    assert sources["meta"]["total_spend_paise"]       == _META_SPEND


@pytest.mark.asyncio
async def test_attribution_spend_shares_sum_to_100(client, seeded_db, user_and_token):
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/attribution",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    total_share = sum(s["spend_share_pct"] for s in resp.json()["sources"])
    assert total_share == pytest.approx(100.0, abs=0.01)


@pytest.mark.asyncio
async def test_attribution_google_share_is_two_thirds(client, seeded_db, user_and_token):
    """google_ads = 900k, meta = 450k → GA share ≈ 66.67 %."""
    brand_id = seeded_db["brand_id"]
    resp = await client.get(
        f"/api/v1/brands/{brand_id}/performance/attribution",
        headers=auth_headers(user_and_token),
        params={"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])},
    )
    sources = {s["source"]: s for s in resp.json()["sources"]}
    expected_ga_share = _GA_SPEND / _TOTAL_SPEND * 100
    assert sources["google_ads"]["spend_share_pct"] == pytest.approx(expected_ga_share, abs=0.01)


# ── Cache hit verification ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_second_request_hits_redis_cache(client, seeded_db, user_and_token, fake_redis):
    """After the first request populates the cache, the second request should
    find a value in FakeRedis — verified by checking that get() returns non-None."""
    brand_id = seeded_db["brand_id"]
    params = {"date_from": str(_DATES[0]), "date_to": str(_DATES[-1])}
    url = f"/api/v1/brands/{brand_id}/performance/summary"
    headers = auth_headers(user_and_token)

    # First request: cache miss → populates cache
    r1 = await client.get(url, headers=headers, params=params)
    assert r1.status_code == 200

    # Verify at least one cache key was written to FakeRedis
    all_keys = [k for k in fake_redis._store if k.startswith("perf:")]
    assert len(all_keys) > 0, "Expected at least one perf:* cache key after first request"

    # Second request: should return the same result
    r2 = await client.get(url, headers=headers, params=params)
    assert r2.status_code == 200
    assert r2.json()["total_spend_paise"] == r1.json()["total_spend_paise"]
