"""
Meta Ads Connector
==================

Pulls daily campaign-level performance data from the Meta Marketing API using
the facebook-business Python SDK (v20+).

Credential model
────────────────
Shared across all brands (from app.core.config.Settings):
  META_APP_ID       — Facebook App ID
  META_APP_SECRET   — Facebook App Secret

Per-brand (stored in MongoDB  brands.platforms.meta_ads):
  access_token      — System User Token (permanent) or long-lived page token
                      (AES-256-GCM encrypted at rest)
  ad_account_id     — Meta Ad Account ID with "act_" prefix (e.g. "act_123456")
  token_expires_at  — ISO-8601 datetime; None for permanent System User Tokens
  currency          — Account billing currency code (e.g. "INR", "USD")

Token strategy
──────────────
System User Tokens (permanent) are preferred. If token_expires_at is set and
within EXPIRY_WARN_DAYS days of expiry, a warning is logged so the caller can
surface an alert — but ingestion still proceeds.

Currency conversion
───────────────────
Meta returns spend in the ad account's billing currency.
If currency == "INR":  spend_paise = int(spend_float * 100)
Otherwise:             spend_paise = 0 and a warning is logged.
(Full FX conversion is out of scope for this connector.)

Conversion value follows the same logic.

Rate limit handling
───────────────────
Meta's Business Use Case Usage rate limit is returned in the header:
  X-Business-Use-Case-Usage: { "<account_id>": [{ "call_count": N, ... }] }

At 75% call_count we pause for THROTTLE_SLEEP_SECONDS.
On 429 / error code 17 (User Request Limit Reached): exponential backoff
with up to _MAX_RETRIES attempts (1s → 2s → 4s → 8s).
OAUTH_EXCEPTION (code 190) and PERMISSION errors raise immediately.

Fields fetched (campaign Insights)
───────────────────────────────────
  impressions, clicks, spend, reach, frequency,
  actions[action_type=lead], actions[action_type=offsite_conversion.fb_pixel_purchase],
  action_values[action_type=offsite_conversion.fb_pixel_purchase]

Attribution window: 7d_click (default Meta attribution).
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.config import get_settings
from app.services.ingestion.base import BaseIngestionService, PlatformRecord

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 4
_BACKOFF_BASE = 1.0          # seconds; doubles each attempt
_THROTTLE_SLEEP_SECONDS = 30
_RATE_LIMIT_THRESHOLD = 0.75  # pause at 75% call_count
_EXPIRY_WARN_DAYS = 7

# Meta error codes that should NOT be retried
_FATAL_CODES = {190, 200, 273, 10, 100}
# 190 = OAuthException (invalid/expired token)
# 200, 273 = Permission errors
# 10, 100 = API permission / developer policy

_RATE_LIMIT_CODE = 17  # User Request Limit Reached


class MetaAdsIngestionService(BaseIngestionService):
    """Ingestion connector for Meta (Facebook) Ads.

    Usage::

        svc = MetaAdsIngestionService(db)
        result = await svc.run(brand_id="...", target_date=date(2026, 4, 5))
    """

    source: str = "meta"

    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        super().__init__(db)
        self._settings = get_settings()

    # ── Abstract interface implementation ─────────────────────────────────────

    async def fetch(
        self,
        brand_id: str,
        target_date: date,
    ) -> list[dict[str, Any]]:
        """Load brand credentials, call Meta Insights API, return raw records.

        Checks token expiry, applies rate limit throttling, and retries on
        transient quota errors with exponential backoff.
        """
        creds = await self._load_credentials(brand_id)
        _warn_if_token_expiring(creds, brand_id)

        date_str = target_date.strftime("%Y-%m-%d")
        log = logger.bind(
            brand_id=brand_id,
            source=self.source,
            date=date_str,
            ad_account_id=creds["ad_account_id"],
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                rows = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._run_insights_sync,
                    creds,
                    date_str,
                )
                log.debug("meta_ads.fetch.success", rows=len(rows), attempt=attempt)
                return rows

            except Exception as exc:
                code = _extract_meta_error_code(exc)
                if code in _FATAL_CODES:
                    log.error("meta_ads.fetch.fatal", error_code=code, error=str(exc))
                    raise

                if attempt == _MAX_RETRIES:
                    log.error("meta_ads.fetch.exhausted", attempt=attempt, error=str(exc))
                    raise

                wait = _BACKOFF_BASE * (2 ** (attempt - 1))
                log.warning(
                    "meta_ads.fetch.retry",
                    attempt=attempt,
                    wait_s=wait,
                    error_code=code,
                    error=str(exc),
                )
                await asyncio.sleep(wait)

        raise RuntimeError("fetch: unexpected exit from retry loop")

    def transform(
        self,
        raw_records: list[dict[str, Any]],
        brand_id: str,
    ) -> list[PlatformRecord]:
        """Convert Meta Insights dicts into PlatformRecord instances.

        Monetary fields are extracted from the 'spend' and action_values
        fields and converted to INR paise.
        """
        records: list[PlatformRecord] = []
        for row in raw_records:
            try:
                rec = self._map_row(row, brand_id)
                records.append(rec)
            except Exception as exc:
                logger.warning(
                    "meta_ads.transform.row_failed",
                    brand_id=brand_id,
                    campaign_id=row.get("campaign_id"),
                    error=str(exc),
                )
        return records

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _load_credentials(self, brand_id: str) -> dict[str, Any]:
        """Read per-brand Meta credentials from MongoDB.

        Expected document path: brands.platforms.meta_ads
        Fields required: access_token, ad_account_id
        Fields optional: token_expires_at, currency (defaults to "INR")
        """
        doc = await self._db["brands"].find_one(
            {"_id": _to_object_id(brand_id)},
            {"platforms.meta_ads": 1},
        )
        if not doc:
            raise ValueError(f"Brand '{brand_id}' not found.")

        creds: dict[str, Any] = (doc.get("platforms") or {}).get("meta_ads") or {}
        if not creds.get("access_token"):
            raise ValueError(
                f"Brand '{brand_id}' has no Meta access_token configured."
            )
        if not creds.get("ad_account_id"):
            raise ValueError(
                f"Brand '{brand_id}' has no Meta ad_account_id configured."
            )

        # Normalise ad_account_id — ensure it has the "act_" prefix
        acct = creds["ad_account_id"]
        if not acct.startswith("act_"):
            creds["ad_account_id"] = f"act_{acct}"

        # Default currency to INR if not specified
        creds.setdefault("currency", "INR")

        # Decrypt token (pass-through until crypto module is wired up)
        creds["access_token"] = _decrypt_token(creds["access_token"])
        return creds

    def _run_insights_sync(
        self,
        creds: dict[str, Any],
        date_str: str,
    ) -> list[dict[str, Any]]:
        """Synchronous Meta Insights call (runs in a thread pool executor).

        Uses the facebook-business SDK's AdAccount.get_insights() with
        campaign-level granularity and 7d_click attribution.

        Returns a list of plain dicts (one per campaign per day).
        """
        # Import here to keep startup fast when Meta SDK is unused
        from facebook_business.adobjects.adaccount import AdAccount  # type: ignore[import]
        from facebook_business.api import FacebookAdsApi  # type: ignore[import]

        FacebookAdsApi.init(
            app_id=self._settings.meta_app_id,
            app_secret=self._settings.meta_app_secret,
            access_token=creds["access_token"],
        )

        account = AdAccount(creds["ad_account_id"])

        params = {
            "level": "campaign",
            "time_range": {"since": date_str, "until": date_str},
            "fields": [
                "campaign_id",
                "campaign_name",
                "impressions",
                "clicks",
                "spend",
                "reach",
                "frequency",
                "actions",
                "action_values",
            ],
            "action_attribution_windows": ["7d_click"],
            "time_increment": 1,  # daily breakdown
            "limit": 500,
        }

        try:
            cursor = account.get_insights(params=params)
            rows: list[dict[str, Any]] = []
            for insight in cursor:
                rows.append(_insight_to_dict(insight, creds["currency"]))

            # Check rate limit from response headers (best-effort)
            _check_rate_limit_header(cursor)
            return rows

        except Exception as exc:
            raise _wrap_meta_exception(exc) from exc

    def _map_row(self, row: dict[str, Any], brand_id: str) -> PlatformRecord:
        """Map a single Meta Insights dict to a PlatformRecord."""
        record_date = _parse_meta_date(row["date"])
        currency: str = row.get("currency", "INR")

        spend_paise = _to_paise(row.get("spend", 0.0), currency, brand_id, "spend")
        conv_value_paise = _to_paise(
            row.get("conversion_value", 0.0), currency, brand_id, "conversion_value"
        )

        impressions: int = int(row.get("impressions", 0) or 0)
        clicks: int = int(row.get("clicks", 0) or 0)
        reach: int = int(row.get("reach", 0) or 0)
        frequency: float = float(row.get("frequency", 0.0) or 0.0)
        leads: int = int(row.get("leads", 0) or 0)
        conversions: int = int(row.get("conversions", 0) or 0)

        return PlatformRecord(
            external_campaign_id=str(row["campaign_id"]),
            campaign_name=row.get("campaign_name", ""),
            date=record_date,
            spend_paise=spend_paise,
            impressions=impressions,
            clicks=clicks,
            reach=reach,
            frequency=frequency,
            leads=leads,
            conversions=conversions,
            conversion_value_paise=conv_value_paise,
            campaign_meta={
                "currency": currency,
            },
        )


# ── Module-level helpers ───────────────────────────────────────────────────────

def _insight_to_dict(insight: Any, currency: str) -> dict[str, Any]:
    """Flatten a Meta Insights cursor row into a plain dict.

    Extracts lead and purchase actions/action_values from the nested lists.
    """
    actions: list[dict] = insight.get("actions") or []
    action_values: list[dict] = insight.get("action_values") or []

    leads = _extract_action_value(actions, "lead")
    conversions = _extract_action_value(actions, "offsite_conversion.fb_pixel_purchase")
    conversion_value = _extract_action_value(
        action_values, "offsite_conversion.fb_pixel_purchase"
    )

    return {
        "campaign_id": str(insight.get("campaign_id", "")),
        "campaign_name": str(insight.get("campaign_name", "")),
        "date": str(insight.get("date_start", "")),
        "currency": currency,
        "impressions": int(insight.get("impressions", 0) or 0),
        "clicks": int(insight.get("clicks", 0) or 0),
        "spend": float(insight.get("spend", 0.0) or 0.0),
        "reach": int(insight.get("reach", 0) or 0),
        "frequency": float(insight.get("frequency", 0.0) or 0.0),
        "leads": int(leads),
        "conversions": int(conversions),
        "conversion_value": float(conversion_value),
    }


def _extract_action_value(
    action_list: list[dict[str, Any]], action_type: str
) -> float:
    """Sum the '7d_click' value for a given action_type in an actions list.

    Falls back to the plain 'value' key if '7d_click' is not present.
    """
    for item in action_list:
        if item.get("action_type") == action_type:
            # Prefer 7d_click attribution; fall back to default 'value'
            val = item.get("7d_click") or item.get("value") or 0
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _to_paise(
    amount: float | int | str | None,
    currency: str,
    brand_id: str,
    field_name: str,
) -> int:
    """Convert a monetary amount to INR paise.

    Only INR is supported; other currencies log a warning and return 0.
    """
    if not amount:
        return 0
    try:
        amount_float = float(amount)
    except (TypeError, ValueError):
        return 0

    if currency.upper() == "INR":
        return int(amount_float * 100)

    logger.warning(
        "meta_ads.currency_not_supported",
        brand_id=brand_id,
        currency=currency,
        field=field_name,
    )
    return 0


def _parse_meta_date(date_str: str) -> date:
    """Parse Meta API date string 'YYYY-MM-DD' into a date object."""
    return date.fromisoformat(date_str)


def _warn_if_token_expiring(creds: dict[str, Any], brand_id: str) -> None:
    """Log a warning if the access token expires within EXPIRY_WARN_DAYS days."""
    expires_at_raw = creds.get("token_expires_at")
    if not expires_at_raw:
        return  # permanent / no expiry info

    try:
        if isinstance(expires_at_raw, str):
            expires_at = datetime.fromisoformat(expires_at_raw)
        elif isinstance(expires_at_raw, datetime):
            expires_at = expires_at_raw
        else:
            return

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        days_left = (expires_at - datetime.now(timezone.utc)).days
        if days_left <= _EXPIRY_WARN_DAYS:
            logger.warning(
                "meta_ads.token_expiring_soon",
                brand_id=brand_id,
                days_left=days_left,
                expires_at=expires_at.isoformat(),
            )
    except Exception:
        pass  # malformed expiry field — ignore, don't abort ingestion


def _check_rate_limit_header(cursor: Any) -> None:
    """Parse X-Business-Use-Case-Usage from the last response and throttle if needed."""
    try:
        headers = cursor.headers()  # type: ignore[attr-defined]
        usage_raw = headers.get("x-business-use-case-usage") or ""
        if not usage_raw:
            return

        import json
        usage = json.loads(usage_raw)
        for _account_id, entries in usage.items():
            for entry in entries:
                call_count = entry.get("call_count", 0)
                if call_count >= int(_RATE_LIMIT_THRESHOLD * 100):
                    logger.warning(
                        "meta_ads.rate_limit_approaching",
                        call_count_pct=call_count,
                        threshold_pct=int(_RATE_LIMIT_THRESHOLD * 100),
                    )
                    # Note: sleeping in a sync context is acceptable here
                    # because this runs in a thread pool executor
                    import time
                    time.sleep(_THROTTLE_SLEEP_SECONDS)
                    return
    except Exception:
        pass  # header parsing failure must never abort ingestion


def _extract_meta_error_code(exc: Exception) -> int:
    """Extract the Meta error code integer from a FacebookRequestError, if present."""
    try:
        return exc.api_error_code()  # type: ignore[attr-defined]
    except AttributeError:
        pass
    # Some wrappers expose .http_status or ._api_error_code
    try:
        return exc._api_error_code  # type: ignore[attr-defined]
    except AttributeError:
        pass
    return 0


def _wrap_meta_exception(exc: Exception) -> Exception:
    """Re-raise Meta SDK exception with a normalised message including the code."""
    code = _extract_meta_error_code(exc)
    msg = f"MetaAdsError({code}): {exc}"
    wrapped = RuntimeError(msg)
    wrapped.__cause__ = exc
    wrapped.meta_error_code = code  # type: ignore[attr-defined]
    return wrapped


def _decrypt_token(token: str) -> str:
    """Decrypt a stored OAuth2 access token.

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
