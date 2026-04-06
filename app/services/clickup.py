"""
ClickUp integration service — stub for 2.3 wiring.

Full implementation (API calls, checklist creation, status sync) is
delivered in section 2.4.  This stub exposes the interface so the brands
router can call it today without breaking.

If CLICKUP_API_TOKEN is not configured the functions are no-ops that log
a debug message and return None — brand creation never fails because of
missing ClickUp config.
"""

from __future__ import annotations

from typing import Any

import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)


async def create_onboarding_task(brand: dict[str, Any]) -> str | None:
    """Create a ClickUp onboarding task for a newly created brand.

    Returns the ClickUp task_id string, or None if ClickUp is not
    configured or the call fails (brand creation must not be blocked).

    Full implementation: section 2.4.
    """
    settings = get_settings()
    if not settings.clickup_api_token or not settings.clickup_onboarding_list_id:
        logger.debug(
            "clickup.skipped",
            reason="CLICKUP_API_TOKEN or CLICKUP_ONBOARDING_LIST_ID not set",
            brand_slug=brand.get("slug"),
        )
        return None

    # 2.4 will implement the HTTP call here.
    logger.info(
        "clickup.task_creation_pending",
        brand_slug=brand.get("slug"),
        note="Full implementation in section 2.4",
    )
    return None


async def update_task_status(task_id: str, status: str) -> bool:
    """Move a ClickUp task to the given status.

    Returns True on success, False on failure / not configured.
    Full implementation: section 2.4.
    """
    settings = get_settings()
    if not settings.clickup_api_token or not task_id:
        return False
    # 2.4 will implement the HTTP call here.
    return False


async def sync_brand_status(brand_id: str) -> str | None:
    """Pull the current ClickUp task status for a brand and return it.

    Returns the ClickUp status string or None if unavailable.
    Full implementation: section 2.4.
    """
    return None
