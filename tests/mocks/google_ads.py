"""
Google Ads API mock for local development and testing.

Replaces _run_gaql_sync inside GoogleAdsIngestionService so tests never
need the google-ads SDK or real credentials.

Fixture data mirrors the shape returned by _row_to_dict():

    {
        "campaign_id":               str,
        "campaign_name":             str,
        "status":                    "ENABLED" | "PAUSED",
        "advertising_channel_type":  "SEARCH" | "DISPLAY" | "SHOPPING" | "VIDEO",
        "impressions":               int,
        "clicks":                    int,
        "cost_micros":               int,   # INR micros (1 INR = 1_000_000 micros)
        "conversions":               float,
        "conversion_value":          float, # INR rupees
        "date":                      str,   # YYYY-MM-DD
    }

Usage (in tests or local dev):

    from tests.mocks.google_ads import MockGoogleAdsClient, FIXTURE_CAMPAIGNS
    with patch.object(svc, "_run_gaql_sync", MockGoogleAdsClient().run):
        rows = await svc.fetch(brand_id, target_date)

Or activate globally via env var (handled in tests/conftest.py):

    USE_MOCK_GOOGLE_ADS=1 pytest
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

# ── Fixture campaign catalogue ─────────────────────────────────────────────────

FIXTURE_CAMPAIGNS: list[dict[str, Any]] = [
    {
        "id": "111000001",
        "name": "Brand Awareness — Search",
        "status": "ENABLED",
        "advertising_channel_type": "SEARCH",
        # Typical Search campaign: moderate spend, high CTR
        "_base_impressions": 12_000,
        "_base_clicks": 480,
        "_base_cost_inr": 4_200.0,   # INR per day
        "_base_conversions": 18.0,
        "_base_conv_value_inr": 36_000.0,
    },
    {
        "id": "111000002",
        "name": "Retargeting — Display",
        "status": "ENABLED",
        "advertising_channel_type": "DISPLAY",
        # Display: high impressions, low CTR
        "_base_impressions": 85_000,
        "_base_clicks": 340,
        "_base_cost_inr": 1_800.0,
        "_base_conversions": 6.0,
        "_base_conv_value_inr": 12_000.0,
    },
    {
        "id": "111000003",
        "name": "Product Shopping — All SKUs",
        "status": "ENABLED",
        "advertising_channel_type": "SHOPPING",
        "_base_impressions": 22_000,
        "_base_clicks": 1_100,
        "_base_cost_inr": 9_500.0,
        "_base_conversions": 55.0,
        "_base_conv_value_inr": 165_000.0,
    },
    {
        "id": "111000004",
        "name": "YouTube — Brand Video",
        "status": "PAUSED",
        "advertising_channel_type": "VIDEO",
        "_base_impressions": 50_000,
        "_base_clicks": 200,
        "_base_cost_inr": 3_000.0,
        "_base_conversions": 2.0,
        "_base_conv_value_inr": 4_000.0,
    },
    {
        "id": "111000005",
        "name": "Competitor Keywords — Search",
        "status": "ENABLED",
        "advertising_channel_type": "SEARCH",
        "_base_impressions": 5_000,
        "_base_clicks": 150,
        "_base_cost_inr": 2_500.0,
        "_base_conversions": 8.0,
        "_base_conv_value_inr": 14_000.0,
    },
]

# ── Seeded RNG — deterministic per (customer_id, date) ────────────────────────

def _seed(customer_id: str, date_str: str) -> random.Random:
    """Return a deterministic Random instance so the same inputs always yield
    the same fixture rows — essential for idempotency tests."""
    key = hash(f"{customer_id}:{date_str}") & 0xFFFF_FFFF
    return random.Random(key)


def _jitter(rng: random.Random, base: float, pct: float = 0.15) -> float:
    """Apply ±pct random variation around base."""
    return base * (1.0 + rng.uniform(-pct, pct))


# ── Mock client ────────────────────────────────────────────────────────────────

class MockGoogleAdsClient:
    """Drop-in replacement for _run_gaql_sync.

    Call signature matches the real method::

        rows = mock_client.run(creds, date_str)

    Behaviour:
    - Returns rows only for ENABLED campaigns by default.
    - Applies ±15 % random jitter seeded on (customer_id, date) so values are
      stable across multiple calls with the same inputs.
    - PAUSED campaigns are included with zero spend/metrics.
    - Raises GoogleAdsRateLimitError when simulate_rate_limit=True.
    - Raises GoogleAdsAuthError when simulate_auth_error=True.
    """

    def __init__(
        self,
        *,
        campaigns: list[dict[str, Any]] | None = None,
        simulate_rate_limit: bool = False,
        simulate_auth_error: bool = False,
        include_paused: bool = True,
    ) -> None:
        self._campaigns = campaigns if campaigns is not None else FIXTURE_CAMPAIGNS
        self._simulate_rate_limit = simulate_rate_limit
        self._simulate_auth_error = simulate_auth_error
        self._include_paused = include_paused

    def run(self, creds: dict[str, Any], date_str: str) -> list[dict[str, Any]]:
        """Simulate _run_gaql_sync — returns fixture rows."""
        if self._simulate_auth_error:
            raise GoogleAdsAuthError(
                "UNAUTHENTICATED: OAuth token expired.",
                code="UNAUTHENTICATED",
            )
        if self._simulate_rate_limit:
            raise GoogleAdsRateLimitError(
                "RESOURCE_EXHAUSTED: Quota exceeded.",
                code="RESOURCE_EXHAUSTED",
            )

        customer_id: str = creds.get("customer_id", "unknown")
        rng = _seed(customer_id, date_str)

        rows: list[dict[str, Any]] = []
        for camp in self._campaigns:
            if camp["status"] == "PAUSED" and not self._include_paused:
                continue

            if camp["status"] == "PAUSED":
                # Paused campaigns appear in the API with zero metrics
                rows.append({
                    "campaign_id": camp["id"],
                    "campaign_name": camp["name"],
                    "status": "PAUSED",
                    "advertising_channel_type": camp["advertising_channel_type"],
                    "impressions": 0,
                    "clicks": 0,
                    "cost_micros": 0,
                    "conversions": 0.0,
                    "conversion_value": 0.0,
                    "date": date_str,
                })
                continue

            impr = max(0, int(_jitter(rng, camp["_base_impressions"])))
            clicks = max(0, int(_jitter(rng, camp["_base_clicks"])))
            cost_inr = max(0.0, _jitter(rng, camp["_base_cost_inr"]))
            cost_micros = int(cost_inr * 1_000_000)
            conversions = max(0.0, _jitter(rng, camp["_base_conversions"], pct=0.25))
            conv_value_inr = max(0.0, _jitter(rng, camp["_base_conv_value_inr"], pct=0.25))

            rows.append({
                "campaign_id": camp["id"],
                "campaign_name": camp["name"],
                "status": camp["status"],
                "advertising_channel_type": camp["advertising_channel_type"],
                "impressions": impr,
                "clicks": clicks,
                "cost_micros": cost_micros,
                "conversions": round(conversions, 1),
                "conversion_value": round(conv_value_inr, 2),
                "date": date_str,
            })

        return rows


def build_date_range_rows(
    start_date: date,
    end_date: date,
    customer_id: str = "1234567890",
) -> list[dict[str, Any]]:
    """Generate fixture rows for a date range (for backfill tests)."""
    client = MockGoogleAdsClient()
    creds = {"customer_id": customer_id}
    all_rows: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        all_rows.extend(client.run(creds, current.strftime("%Y-%m-%d")))
        current += timedelta(days=1)
    return all_rows


# ── Fake exception classes (mimic google-ads SDK error shape) ─────────────────

class _FakeGoogleAdsError(Exception):
    """Base fake error that exposes .failure.errors so _extract_google_error_code works."""

    def __init__(self, message: str, code: str = "") -> None:
        super().__init__(message)
        self._code = code
        # Build a .failure.errors list mirroring the real SDK structure
        _err = _FakeErrorDetail(code)
        self.failure = _FakeFailure([_err])


class _FakeErrorDetail:
    def __init__(self, code: str) -> None:
        self.error_code = _FakeErrorCode(code)


class _FakeErrorCode:
    def __init__(self, code: str) -> None:
        self._code = code

    def WhichOneof(self, _field: str) -> str:
        return self._code


class _FakeFailure:
    def __init__(self, errors: list) -> None:
        self.errors = errors


class GoogleAdsAuthError(_FakeGoogleAdsError):
    """Simulates UNAUTHENTICATED / auth failure."""


class GoogleAdsRateLimitError(_FakeGoogleAdsError):
    """Simulates RESOURCE_EXHAUSTED / quota failure."""
