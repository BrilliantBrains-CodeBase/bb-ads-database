"""
Meta Ads API mock for local development and testing.

Replaces _run_insights_sync inside MetaAdsIngestionService so tests never
need the facebook-business SDK or real credentials.

Fixture data mirrors the shape returned by _insight_to_dict():

    {
        "campaign_id":        str,
        "campaign_name":      str,
        "date":               str,   # YYYY-MM-DD
        "currency":           str,   # "INR"
        "impressions":        int,
        "clicks":             int,
        "spend":              float, # INR rupees
        "reach":              int,
        "frequency":          float,
        "leads":              int,
        "conversions":        int,
        "conversion_value":   float, # INR rupees
    }

Usage (in tests or local dev):

    from tests.mocks.meta_ads import MockMetaAdsClient, FIXTURE_CAMPAIGNS
    with patch.object(svc, "_run_insights_sync", MockMetaAdsClient().run):
        rows = await svc.fetch(brand_id, target_date)

Or activate globally via env var (handled in tests/conftest.py):

    USE_MOCK_META_ADS=1 pytest
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

# ── Fixture campaign catalogue ─────────────────────────────────────────────────

FIXTURE_CAMPAIGNS: list[dict[str, Any]] = [
    {
        "id": "222000001",
        "name": "Traffic — Feed",
        "status": "ACTIVE",
        # High impressions, moderate CTR, no conversion tracking
        "_base_impressions": 45_000,
        "_base_clicks": 900,
        "_base_spend_inr": 3_500.0,
        "_base_reach": 38_000,
        "_base_frequency": 1.18,
        "_base_leads": 0,
        "_base_conversions": 0,
        "_base_conv_value_inr": 0.0,
    },
    {
        "id": "222000002",
        "name": "Lead Gen — Lookalike",
        "status": "ACTIVE",
        # Dedicated lead gen campaign
        "_base_impressions": 28_000,
        "_base_clicks": 560,
        "_base_spend_inr": 6_000.0,
        "_base_reach": 25_000,
        "_base_frequency": 1.12,
        "_base_leads": 42,
        "_base_conversions": 0,
        "_base_conv_value_inr": 0.0,
    },
    {
        "id": "222000003",
        "name": "Conversion — Retargeting",
        "status": "ACTIVE",
        # Pixel-based purchase conversion campaign
        "_base_impressions": 12_000,
        "_base_clicks": 360,
        "_base_spend_inr": 8_200.0,
        "_base_reach": 10_500,
        "_base_frequency": 1.14,
        "_base_leads": 0,
        "_base_conversions": 28,
        "_base_conv_value_inr": 98_000.0,
    },
    {
        "id": "222000004",
        "name": "Brand Awareness — Reels",
        "status": "ACTIVE",
        # Very high reach, near-zero direct response
        "_base_impressions": 120_000,
        "_base_clicks": 480,
        "_base_spend_inr": 5_000.0,
        "_base_reach": 110_000,
        "_base_frequency": 1.09,
        "_base_leads": 0,
        "_base_conversions": 0,
        "_base_conv_value_inr": 0.0,
    },
    {
        "id": "222000005",
        "name": "Catalogue Sales — DPA",
        "status": "PAUSED",
        # Dynamic product ads — currently paused
        "_base_impressions": 0,
        "_base_clicks": 0,
        "_base_spend_inr": 0.0,
        "_base_reach": 0,
        "_base_frequency": 0.0,
        "_base_leads": 0,
        "_base_conversions": 0,
        "_base_conv_value_inr": 0.0,
    },
]

# ── Seeded RNG — deterministic per (ad_account_id, date) ──────────────────────

def _seed(ad_account_id: str, date_str: str) -> random.Random:
    key = hash(f"{ad_account_id}:{date_str}") & 0xFFFF_FFFF
    return random.Random(key)


def _jitter(rng: random.Random, base: float, pct: float = 0.12) -> float:
    return base * (1.0 + rng.uniform(-pct, pct))


# ── Mock client ────────────────────────────────────────────────────────────────

class MockMetaAdsClient:
    """Drop-in replacement for _run_insights_sync.

    Call signature matches the real method::

        rows = mock_client.run(creds, date_str)

    Behaviour:
    - Returns rows for all non-paused campaigns (paused → zero metrics).
    - Applies ±12 % random jitter seeded on (ad_account_id, date).
    - Raises MetaRateLimitError when simulate_rate_limit=True.
    - Raises MetaAuthError when simulate_auth_error=True.
    - Supports simulate_non_inr_currency to return USD rows.
    """

    def __init__(
        self,
        *,
        campaigns: list[dict[str, Any]] | None = None,
        simulate_rate_limit: bool = False,
        simulate_auth_error: bool = False,
        currency: str = "INR",
    ) -> None:
        self._campaigns = campaigns if campaigns is not None else FIXTURE_CAMPAIGNS
        self._simulate_rate_limit = simulate_rate_limit
        self._simulate_auth_error = simulate_auth_error
        self._currency = currency

    def run(self, creds: dict[str, Any], date_str: str) -> list[dict[str, Any]]:
        """Simulate _run_insights_sync — returns fixture rows."""
        if self._simulate_auth_error:
            raise MetaAuthError("OAuthException: invalid access token", code=190)
        if self._simulate_rate_limit:
            raise MetaRateLimitError("User Request Limit Reached", code=17)

        ad_account_id: str = creds.get("ad_account_id", "act_unknown")
        rng = _seed(ad_account_id, date_str)

        rows: list[dict[str, Any]] = []
        for camp in self._campaigns:
            if camp["status"] == "PAUSED":
                rows.append({
                    "campaign_id": camp["id"],
                    "campaign_name": camp["name"],
                    "date": date_str,
                    "currency": self._currency,
                    "impressions": 0,
                    "clicks": 0,
                    "spend": 0.0,
                    "reach": 0,
                    "frequency": 0.0,
                    "leads": 0,
                    "conversions": 0,
                    "conversion_value": 0.0,
                })
                continue

            impr = max(0, int(_jitter(rng, camp["_base_impressions"])))
            clicks = max(0, int(_jitter(rng, camp["_base_clicks"])))
            spend = max(0.0, round(_jitter(rng, camp["_base_spend_inr"]), 2))
            reach = max(0, int(_jitter(rng, camp["_base_reach"])))
            freq_base = camp["_base_frequency"]
            frequency = round(_jitter(rng, freq_base, pct=0.05), 4) if freq_base else 0.0
            leads = max(0, int(_jitter(rng, camp["_base_leads"], pct=0.30)))
            conversions = max(0, int(_jitter(rng, camp["_base_conversions"], pct=0.30)))
            conv_value = max(0.0, round(_jitter(rng, camp["_base_conv_value_inr"], pct=0.30), 2))

            rows.append({
                "campaign_id": camp["id"],
                "campaign_name": camp["name"],
                "date": date_str,
                "currency": self._currency,
                "impressions": impr,
                "clicks": clicks,
                "spend": spend,
                "reach": reach,
                "frequency": frequency,
                "leads": leads,
                "conversions": conversions,
                "conversion_value": conv_value,
            })

        return rows


def build_date_range_rows(
    start_date: date,
    end_date: date,
    ad_account_id: str = "act_1234567890",
    currency: str = "INR",
) -> list[dict[str, Any]]:
    """Generate fixture rows for a date range (for backfill tests)."""
    client = MockMetaAdsClient(currency=currency)
    creds = {"ad_account_id": ad_account_id}
    all_rows: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        all_rows.extend(client.run(creds, current.strftime("%Y-%m-%d")))
        current += timedelta(days=1)
    return all_rows


# ── Fake exception classes (mimic facebook-business SDK error shape) ──────────

class _FakeMetaError(Exception):
    """Base fake Meta error that exposes .api_error_code() so
    _extract_meta_error_code() resolves the code correctly."""

    def __init__(self, message: str, code: int = 0) -> None:
        super().__init__(message)
        self._code = code

    def api_error_code(self) -> int:
        return self._code


class MetaAuthError(_FakeMetaError):
    """Simulates OAuthException (code 190) — invalid / expired token."""


class MetaRateLimitError(_FakeMetaError):
    """Simulates User Request Limit Reached (code 17)."""


class MetaPermissionError(_FakeMetaError):
    """Simulates Permission error (code 200)."""
