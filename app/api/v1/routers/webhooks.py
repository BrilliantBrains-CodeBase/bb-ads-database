"""
Webhooks router

  POST /webhooks/clickup — receive ClickUp task status-change events,
                           auto-update brand onboarding_status in MongoDB.

Security:
  Every incoming request is verified with HMAC-SHA256 against
  CLICKUP_WEBHOOK_SECRET before any DB writes happen.  Requests with an
  invalid or missing signature are rejected with 401.

  If CLICKUP_WEBHOOK_SECRET is not configured the signature check is skipped
  (dev / local testing only — always set the secret in production).

ClickUp webhook payload (taskStatusUpdated):
  {
    "event":       "taskStatusUpdated",
    "webhook_id":  "...",
    "task_id":     "abc123",
    "history_items": [
      {
        "field": "status",
        "before": { "status": "In Progress", ... },
        "after":  { "status": "Live", ... }
      }
    ]
  }

Other event types (taskCreated, taskUpdated, etc.) are acknowledged with 200
but not acted upon — only "taskStatusUpdated" triggers a DB write.
"""

from __future__ import annotations

from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, Header, Request, Response, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.database import get_database
from app.core.exceptions import UnauthorizedError
from app.repositories.brands import BrandsRepository
from app.services.clickup import map_clickup_status, verify_webhook_signature

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
logger = structlog.get_logger(__name__)


# ── POST /webhooks/clickup ────────────────────────────────────────────────────

@router.post("/clickup", status_code=status.HTTP_200_OK)
async def clickup_webhook(
    request: Request,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    x_signature: Annotated[str | None, Header(alias="x-signature")] = None,
) -> dict[str, str]:
    """Receive ClickUp webhook events.

    Verifies the HMAC-SHA256 signature, then processes status-change events
    to keep `brands.onboarding_status` in sync with ClickUp automatically.

    Always returns 200 for any successfully-verified event (even if no DB
    write is needed) so ClickUp does not retry the delivery.
    """
    raw_body: bytes = await request.body()

    # ── Signature verification ────────────────────────────────────
    sig = x_signature or ""
    if not verify_webhook_signature(raw_body, sig):
        logger.warning(
            "webhook.clickup.invalid_signature",
            remote=request.client.host if request.client else "unknown",
        )
        raise UnauthorizedError("Invalid webhook signature.")

    # ── Parse payload ─────────────────────────────────────────────
    try:
        payload: dict[str, Any] = await request.json()
    except Exception:
        # Malformed JSON — acknowledge to avoid ClickUp retries
        logger.warning("webhook.clickup.malformed_json")
        return {"status": "ignored", "reason": "malformed json"}

    event: str = payload.get("event", "")
    task_id: str = payload.get("task_id", "")

    logger.info("webhook.clickup.received", event=event, task_id=task_id)

    # ── Only handle status changes ────────────────────────────────
    if event != "taskStatusUpdated" or not task_id:
        return {"status": "ignored", "reason": f"unhandled event: {event}"}

    # Extract the new status from history_items
    new_clickup_status: str | None = None
    for item in payload.get("history_items", []):
        if item.get("field") == "status":
            new_clickup_status = (
                item.get("after", {}).get("status")
                or item.get("after", {}).get("status_type")
            )
            break

    if not new_clickup_status:
        return {"status": "ignored", "reason": "status not found in history_items"}

    mapped_status = map_clickup_status(new_clickup_status)

    # ── Find brand by clickup_task_id ─────────────────────────────
    repo = BrandsRepository(db)
    brand = await _find_brand_by_task_id(db, task_id)

    if not brand:
        logger.info(
            "webhook.clickup.no_matching_brand",
            task_id=task_id,
            status=new_clickup_status,
        )
        return {"status": "ignored", "reason": "no brand found for task_id"}

    brand_id = str(brand["_id"])
    current_status: str = brand.get("onboarding_status", "pending")

    if mapped_status == current_status:
        return {"status": "ok", "reason": "status unchanged"}

    # ── Update brand onboarding_status ────────────────────────────
    await repo.set_onboarding_status(
        brand_id,
        mapped_status,
        onboarded_by=None,  # system-driven update, not a user action
    )

    logger.info(
        "webhook.clickup.brand_status_updated",
        brand_id=brand_id,
        clickup_status=new_clickup_status,
        mapped_status=mapped_status,
        previous_status=current_status,
    )

    return {
        "status": "ok",
        "brand_id": brand_id,
        "onboarding_status": mapped_status,
    }


async def _find_brand_by_task_id(
    db: AsyncIOMotorDatabase,  # type: ignore[type-arg]
    task_id: str,
) -> dict[str, Any] | None:
    """Find a brand document where clickup_task_id matches."""
    return await db["brands"].find_one({"clickup_task_id": task_id})
