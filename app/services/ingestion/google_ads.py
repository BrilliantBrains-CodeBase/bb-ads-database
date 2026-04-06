"""
Google Ads Connector
====================

Pulls daily campaign-level performance data from the Google Ads API using
the official google-ads Python SDK (v24+) and GAQL.

Credential model
────────────────
Shared across all brands (from app.core.config.Settings):
  GOOGLE_ADS_DEVELOPER_TOKEN   — developer token issued by Google
  GOOGLE_ADS_CLIENT_ID         — OAuth2 client ID
  GOOGLE_ADS_CLIENT_SECRET     — OAuth2 client secret

Per-brand (stored in MongoDB  brands.platforms.google_ads):
  customer_id        — 10-digit Google Ads Customer ID (no dashes)
  refresh_token      — OAuth2 refresh token (AES-256-GCM encrypted at rest)
  login_customer_id  — MCC / manager account ID, optional
                       (defaults to customer_id for standalone accounts)

Currency conversion
───────────────────
Google Ads returns monetary values in *micros* of the account currency.
Assuming accounts are billed in INR:
    1 INR  = 1,000,000 micros
    1 INR  = 100 paise
    1 paise = 10,000 micros
    ⇒ paise = cost_micros // 10_000

Conversion value from Google is a float in account currency (INR rupees):
    paise = int(conversion_value_inr * 100)

GAQL query
──────────
We fetch at campaign granularity with date segmentation:

    SELECT campaign.id, campaign.name, campaign.status,
           campaign.advertising_channel_type,
           metrics.impressions, metrics.clicks, metrics.cost_micros,
           metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE segments.date = 'YYYY-MM-DD'
      AND campaign.status != 'REMOVED'

Rate limit handling
───────────────────
Google Ads limits: ~1,500 API calls/day/developer token, 15 QPS.
On RESOURCE_EXHAUSTED (quota) or transient errors: exponential backoff
with up to _MAX_RETRIES attempts (1s → 2s → 4s → 8s).
UNAUTHENTICATED / PERMISSION_DENIED errors raise immediately (not retried).
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import get_settings
from app.services.ingestion.base import BaseIngestionService, PlatformRecord

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 4
_BACKOFF_BASE = 1.0  # seconds; doubles each attempt

# Google Ads error codes that should NOT be retried
_FATAL_ERROR_CODES = {
    "UNAUTHENTICATED",
    "PERMISSION_DENIED",
    "INVALID_CUSTOMER_ID",
    "CUSTOMER_NOT_ENABLED",
    "AUTHORIZATION_ERROR",
}


class GoogleAdsIngestionService(BaseIngestionService):
    """Ingestion connector for Google Ads.

    Usage::

        svc = GoogleAdsIngestionService(db)
        result = await svc.run(brand_id="...", target_date=date(2026, 4, 5))
    """

    source: str = "google_ads"

    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        super().__init__(db)
        self._settings = get_settings()

    # ── Abstract interface implementation ─────────────────────────────────────

    async def fetch(
        self,
        brand_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """Load brand credentials, build a GoogleAdsClient, and run GAQL.

        Retries on transient quota / server errors with exponential backoff.
        Raises immediately on auth / permission failures.

        Returns raw GAQL row dicts (see _row_to_dict for shape).
        """
        creds = await self._load_credentials(brand_id)
        date_str = target_date.strftime("%Y-%m-%d")

        log = logger.bind(
            brand_id=brand_id,
            source=self.source,
            date=date_str,
            customer_id=creds["customer_id"],
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                rows = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._run_gaql_sync,
                    creds,
                    date_str,
                )
                log.debug(
                    "google_ads.fetch.success",
                    rows=len(rows),
                    attempt=attempt,
                )
                return rows

            except Exception as exc:
                error_code = _extract_google_error_code(exc)
                if error_code in _FATAL_ERROR_CODES:
                    log.error(
                        "google_ads.fetch.fatal",
                        error_code=error_code,
                        error=str(exc),
                    )
                    raise

                if attempt == _MAX_RETRIES:
                    log.error(
                        "google_ads.fetch.exhausted",
                        attempt=attempt,
                        error=str(exc),
                    )
                    raise

                wait = _BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "google_ads.fetch.retry",
                    attempt=attempt,
                    wait_s=wait,
                    error=str(exc),
                )
                await asyncio.sleep(wait)

        # Unreachable — loop always raises or returns inside
        raise RuntimeError("fetch: unexpected exit from retry loop")

    def transform(
        self,
        raw_records: list[dict[str, Any]],
        brand_id: str,
    ) -> list[PlatformRecord]:
        """Convert GAQL row dicts into PlatformRecord instances.

        Monetary conversion:
          cost_micros → paise  :  cost_micros // 10_000
          conversion_value_inr → paise  :  int(value * 100)
        """
        records: list[PlatformRecord] = []
        for row in raw_records:
            try:
                rec = self._map_row(row, brand_id)
                records.append(rec)
            except Exception as exc:
                logger.warning(
                    "google_ads.transform.row_failed",
                    brand_id=brand_id,
                    campaign_id=row.get("campaign_id"),
                    error=str(exc),
                )
        return records

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _load_credentials(self, brand_id: str) -> dict[str, Any]:
        """Read per-brand Google Ads credentials from MongoDB.

        Expected document path: brands.platforms.google_ads
        Fields required: customer_id, refresh_token
        Fields optional: login_customer_id
        """
        doc = await self._db["brands"].find_one(
            {"_id": _to_object_id(brand_id)},
            {"platforms.google_ads": 1},
        )
        if not doc:
            raise ValueError(f"Brand '{brand_id}' not found.")

        creds: dict[str, Any] = (doc.get("platforms") or {}).get("google_ads") or {}
        if not creds.get("customer_id"):
            raise ValueError(
                f"Brand '{brand_id}' has no Google Ads customer_id configured."
            )
        if not creds.get("refresh_token"):
            raise ValueError(
                f"Brand '{brand_id}' has no Google Ads refresh_token configured."
            )

        # Decrypt refresh token if AES-GCM encryption is in place.
        # Currently a pass-through; plug in app.core.crypto.decrypt() here.
        creds["refresh_token"] = _decrypt_token(creds["refresh_token"])
        return creds

    def _run_gaql_sync(
        self,
        creds: dict[str, Any],
        date_str: str,
    ) -> list[dict[str, Any]]:
        """Synchronous GAQL execution (runs in a thread pool executor).

        Builds a GoogleAdsClient from per-brand credentials combined with
        the shared developer token / OAuth app credentials from settings.

        Returns a list of plain dicts (one per campaign row).
        """
        # Import here to keep startup fast when Google Ads SDK is unused
        from google.ads.googleads.client import GoogleAdsClient  # type: ignore[import]
        from google.ads.googleads.errors import GoogleAdsException  # type: ignore[import]

        customer_id: str = creds["customer_id"]
        login_customer_id: str = creds.get("login_customer_id") or customer_id

        client_config = {
            "developer_token": self._settings.google_ads_developer_token,
            "client_id": self._settings.google_ads_client_id,
            "client_secret": self._settings.google_ads_client_secret,
            "refresh_token": creds["refresh_token"],
            "login_customer_id": login_customer_id,
            "use_proto_plus": True,
        }

        client = GoogleAdsClient.load_from_dict(client_config)
        gas = client.get_service("GoogleAdsService")

        query = _build_gaql(date_str)

        try:
            response = gas.search_stream(customer_id=customer_id, query=query)
            rows: list[dict[str, Any]] = []
            for batch in response:
                for row in batch.results:
                    rows.append(_row_to_dict(row))
            return rows
        except GoogleAdsException as exc:
            # Re-raise with a normalised code so the retry logic can inspect it
            raise _wrap_google_exception(exc) from exc

    def _map_row(self, row: dict[str, Any], brand_id: str) -> PlatformRecord:
        """Map a single GAQL result dict to a PlatformRecord."""
        record_date = _parse_gaql_date(row["date"])

        cost_micros: int = row.get("cost_micros", 0) or 0
        spend_paise: int = cost_micros // 10_000

        conversion_value_inr: float = row.get("conversion_value", 0.0) or 0.0
        conversion_value_paise: int = int(conversion_value_inr * 100)

        impressions: int = row.get("impressions", 0) or 0
        clicks: int = row.get("clicks", 0) or 0
        conversions: int = int(row.get("conversions", 0) or 0)

        channel_type: str = row.get("advertising_channel_type", "")
        campaign_status: str = row.get("status", "")

        return PlatformRecord(
            external_campaign_id=str(row["campaign_id"]),
            campaign_name=row.get("campaign_name", ""),
            date=record_date,
            spend_paise=spend_paise,
            impressions=impressions,
            clicks=clicks,
            conversions=conversions,
            conversion_value_paise=conversion_value_paise,
            # reach and frequency require a separate Reach & Frequency report
            # and are not available in the standard campaign report
            reach=0,
            frequency=0.0,
            leads=0,
            campaign_meta={
                "advertising_channel_type": channel_type,
                "platform_status": campaign_status,
            },
        )


# ── Module-level helpers ───────────────────────────────────────────────────────

def _build_gaql(date_str: str) -> str:
    """Return the GAQL query for a single date."""
    return (
        "SELECT "
        "  campaign.id, "
        "  campaign.name, "
        "  campaign.status, "
        "  campaign.advertising_channel_type, "
        "  metrics.impressions, "
        "  metrics.clicks, "
        "  metrics.cost_micros, "
        "  metrics.conversions, "
        "  metrics.conversions_value, "
        "  segments.date "
        "FROM campaign "
        f"WHERE segments.date = '{date_str}' "
        "  AND campaign.status != 'REMOVED'"
    )


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Flatten a proto-plus GoogleAdsRow into a plain dict."""
    return {
        "campaign_id": str(row.campaign.id),
        "campaign_name": str(row.campaign.name),
        "status": str(row.campaign.status.name),
        "advertising_channel_type": str(row.campaign.advertising_channel_type.name),
        "impressions": int(row.metrics.impressions),
        "clicks": int(row.metrics.clicks),
        "cost_micros": int(row.metrics.cost_micros),
        "conversions": float(row.metrics.conversions),
        "conversion_value": float(row.metrics.conversions_value),
        "date": str(row.segments.date),
    }


def _parse_gaql_date(date_str: str) -> date:
    """Parse GAQL segment date string 'YYYY-MM-DD' into a date object."""
    from datetime import date as _date
    return _date.fromisoformat(date_str)


def _extract_google_error_code(exc: Exception) -> str:
    """Extract the Google Ads error code string from an exception, if present."""
    # GoogleAdsException wraps one or more GoogleAdsError objects
    try:
        for error in exc.failure.errors:  # type: ignore[attr-defined]
            return error.error_code.WhichOneof("error_code") or ""
    except AttributeError:
        pass
    return ""


def _wrap_google_exception(exc: Exception) -> Exception:
    """Re-raise GoogleAdsException with a normalised message including the code."""
    code = _extract_google_error_code(exc)
    msg = f"GoogleAdsError({code}): {exc}"
    wrapped = RuntimeError(msg)
    wrapped.__cause__ = exc
    # Stash the code so the retry loop can check it
    wrapped.google_error_code = code  # type: ignore[attr-defined]
    return wrapped


def _decrypt_token(token: str) -> str:
    """Decrypt a stored OAuth2 refresh token.

    Currently a no-op pass-through.  When AES-256-GCM encryption is wired up
    in app.core.crypto, replace this with:

        from app.core.crypto import decrypt
        return decrypt(token)
    """
    return token


def _to_object_id(brand_id: str) -> Any:
    """Convert a string brand_id to a BSON ObjectId."""
    from bson import ObjectId
    return ObjectId(brand_id)
