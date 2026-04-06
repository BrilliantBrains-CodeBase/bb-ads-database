"""
ClickUp integration service.

Responsibilities:
  - create_onboarding_task  — create a task + 8-item checklist in the
                               configured onboarding list
  - update_task_status      — move a task through New → In Progress → Live
  - get_task                — fetch a task including checklists (for progress)
  - map_clickup_status      — translate ClickUp status names to our enum
  - sync_brand_status       — fetch live ClickUp status and update brand in DB
  - verify_webhook_signature— HMAC-SHA256 guard for incoming webhooks

All functions degrade gracefully: if CLICKUP_API_TOKEN is not set, or if
any HTTP call fails, the function logs a warning and returns a safe default.
Brand creation / onboarding transitions are never blocked by ClickUp errors.

ClickUp API reference: https://clickup.com/api
Auth header: "Authorization: {token}"  (no "Bearer" prefix for personal tokens)
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import httpx
import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)

_BASE = "https://api.clickup.com/api/v2"
_TIMEOUT = 10.0  # seconds — ClickUp can be slow; don't hold request threads too long

# The 8 onboarding checklist items (plan § 2.4)
_CHECKLIST_ITEMS = [
    "Link Google Ads account",
    "Link Meta Ads account",
    "Link Interakt account",
    "Upload initial CSV data",
    "Set KPI targets (ROAS, CPL)",
    "Configure budget alert thresholds",
    "Verify first ingestion run",
    "Review initial anomaly baselines",
]

# ClickUp status → our onboarding_status mapping (case-insensitive match)
_CLICKUP_TO_INTERNAL: dict[str, str] = {
    "new":         "pending",
    "to do":       "pending",
    "open":        "pending",
    "in progress": "in_progress",
    "live":        "completed",
    "complete":    "completed",
    "closed":      "completed",
    "blocked":     "blocked",
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_configured() -> bool:
    s = get_settings()
    return bool(s.clickup_api_token and s.clickup_onboarding_list_id)


def _headers() -> dict[str, str]:
    return {
        "Authorization": get_settings().clickup_api_token,
        "Content-Type": "application/json",
    }


# ── Public API ────────────────────────────────────────────────────────────────

async def create_onboarding_task(brand: dict[str, Any]) -> str | None:
    """Create a ClickUp task with the onboarding checklist for a new brand.

    Steps:
      1. Create task in the onboarding list
      2. Create a checklist named "Onboarding Checklist"
      3. Add all 8 checklist items

    Returns the task_id string, or None if ClickUp is not configured or any
    call fails (failure must not abort brand creation).
    """
    if not _is_configured():
        logger.debug(
            "clickup.skipped",
            reason="CLICKUP_API_TOKEN or CLICKUP_ONBOARDING_LIST_ID not set",
            brand_slug=brand.get("slug"),
        )
        return None

    settings = get_settings()
    task_id: str | None = None

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            # ── 1. Create task ────────────────────────────────────
            task_resp = await client.post(
                f"{_BASE}/list/{settings.clickup_onboarding_list_id}/task",
                headers=_headers(),
                json={
                    "name": f"Onboarding: {brand['name']}",
                    "description": (
                        f"Onboarding checklist for brand **{brand['name']}**\n"
                        f"Slug: `{brand.get('slug', '')}` | "
                        f"Industry: {brand.get('industry', 'N/A')}"
                    ),
                    "status": "New",
                    "tags": ["onboarding"],
                },
            )
            task_resp.raise_for_status()
            task_id = task_resp.json()["id"]

            # ── 2. Create checklist ───────────────────────────────
            cl_resp = await client.post(
                f"{_BASE}/task/{task_id}/checklist",
                headers=_headers(),
                json={"name": "Onboarding Checklist"},
            )
            cl_resp.raise_for_status()
            checklist_id: str = cl_resp.json()["checklist"]["id"]

            # ── 3. Add items (sequential — ClickUp rate limit) ───
            for item_name in _CHECKLIST_ITEMS:
                item_resp = await client.post(
                    f"{_BASE}/checklist/{checklist_id}/checklist_item",
                    headers=_headers(),
                    json={"name": item_name, "resolved": False},
                )
                item_resp.raise_for_status()

        logger.info(
            "clickup.task_created",
            task_id=task_id,
            brand_slug=brand.get("slug"),
            checklist_items=len(_CHECKLIST_ITEMS),
        )
        return task_id

    except httpx.HTTPStatusError as exc:
        logger.warning(
            "clickup.task_creation_failed",
            brand_slug=brand.get("slug"),
            status_code=exc.response.status_code,
            detail=exc.response.text[:200],
        )
        return task_id  # return partial task_id if task was created but checklist failed
    except Exception as exc:
        logger.warning(
            "clickup.task_creation_error",
            brand_slug=brand.get("slug"),
            error=str(exc),
        )
        return None


async def update_task_status(task_id: str, clickup_status: str) -> bool:
    """Set the ClickUp task to the given status name.

    Status names are workspace-specific; the plan uses:
      "New" | "In Progress" | "Live" | "Blocked"

    Returns True on success, False on failure / not configured.
    """
    if not _is_configured() or not task_id:
        return False

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.put(
                f"{_BASE}/task/{task_id}",
                headers=_headers(),
                json={"status": clickup_status},
            )
            resp.raise_for_status()
        logger.info("clickup.status_updated", task_id=task_id, status=clickup_status)
        return True

    except httpx.HTTPStatusError as exc:
        logger.warning(
            "clickup.status_update_failed",
            task_id=task_id,
            status=clickup_status,
            status_code=exc.response.status_code,
            detail=exc.response.text[:200],
        )
        return False
    except Exception as exc:
        logger.warning("clickup.status_update_error", task_id=task_id, error=str(exc))
        return False


async def get_task(task_id: str) -> dict[str, Any] | None:
    """Fetch a ClickUp task including its checklists and current status.

    Returns the raw task dict from the ClickUp API, or None on failure.

    Useful fields in the result:
      result["status"]["status"]       — current status name (str)
      result["checklists"]             — list of checklist objects
        checklist["items"]             — list of items
          item["name"]                 — item label
          item["resolved"]             — True if ticked
    """
    if not _is_configured() or not task_id:
        return None

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                f"{_BASE}/task/{task_id}",
                headers=_headers(),
            )
            resp.raise_for_status()
            return resp.json()

    except httpx.HTTPStatusError as exc:
        logger.warning(
            "clickup.get_task_failed",
            task_id=task_id,
            status_code=exc.response.status_code,
        )
        return None
    except Exception as exc:
        logger.warning("clickup.get_task_error", task_id=task_id, error=str(exc))
        return None


def map_clickup_status(clickup_status: str) -> str:
    """Map a ClickUp status name to our `brands.onboarding_status` value.

    Unknown statuses map to "in_progress" (safe default — better than
    resetting to pending for an actively-worked task).
    """
    return _CLICKUP_TO_INTERNAL.get(clickup_status.lower(), "in_progress")


async def sync_brand_status(brand_id: str) -> str | None:
    """Fetch the live ClickUp status for a brand and update MongoDB.

    Looks up the brand's clickup_task_id, queries ClickUp, maps the status,
    and writes `onboarding_status` back.  Uses `get_db_direct()` so it can
    be called from background workers (no FastAPI request context needed).

    Returns the mapped status string, or None if sync is not possible.
    """
    if not _is_configured():
        return None

    from app.core.database import get_db_direct
    from app.repositories.brands import BrandsRepository

    db = get_db_direct()
    repo = BrandsRepository(db)

    brand = await repo.find_by_id(brand_id)
    if not brand:
        logger.warning("clickup.sync_brand_not_found", brand_id=brand_id)
        return None

    task_id: str | None = brand.get("clickup_task_id")
    if not task_id:
        logger.debug("clickup.sync_no_task_id", brand_id=brand_id)
        return None

    task = await get_task(task_id)
    if not task:
        return None

    raw_status: str = task.get("status", {}).get("status", "")
    mapped = map_clickup_status(raw_status)

    if mapped != brand.get("onboarding_status"):
        await repo.set_onboarding_status(brand_id, mapped)
        logger.info(
            "clickup.brand_status_synced",
            brand_id=brand_id,
            clickup_status=raw_status,
            mapped_status=mapped,
        )

    return mapped


# ── Webhook signature verification ───────────────────────────────────────────

def verify_webhook_signature(payload: bytes, signature_header: str) -> bool:
    """Verify a ClickUp webhook request using HMAC-SHA256.

    ClickUp signs the raw request body with the webhook secret and sends
    the hex digest in the `X-Signature` header.

    Returns True if the signature is valid, False otherwise.
    Raises nothing — invalid signatures return False silently.
    """
    secret = get_settings().clickup_webhook_secret
    if not secret:
        # If no secret is configured, skip verification (dev / local testing)
        logger.debug("clickup.webhook_signature_skipped", reason="no secret configured")
        return True

    try:
        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header)
    except Exception:
        return False
