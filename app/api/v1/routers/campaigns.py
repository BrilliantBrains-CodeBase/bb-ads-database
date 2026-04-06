"""
Campaigns router

  GET   /brands/{brand_id}/campaigns          — list campaigns for a brand
  GET   /brands/{brand_id}/campaigns/{cid}    — get single campaign
  PATCH /brands/{brand_id}/campaigns/{cid}    — update campaign metadata (analyst+)
"""

from __future__ import annotations

from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.v1.schemas.campaigns import (
    CampaignListResponse,
    CampaignResponse,
    CampaignUpdate,
)
from app.core.database import get_database
from app.core.exceptions import NotFoundError
from app.core.permissions import Permission, require_permission
from app.middleware.auth import AuthUser
from app.middleware.brand_scope import BrandAccess
from app.repositories.campaigns import CampaignsRepository

router = APIRouter(prefix="/brands", tags=["campaigns"])
logger = structlog.get_logger(__name__)

_VALID_SOURCES = ("google_ads", "meta", "interakt", "manual")
_VALID_STATUSES = ("active", "paused", "archived")


# ── GET /brands/{brand_id}/campaigns ─────────────────────────────────────────

@router.get("/{brand_id}/campaigns", response_model=CampaignListResponse)
async def list_campaigns(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    source: Annotated[str | None, Query(description="Filter by source platform")] = None,
    status: Annotated[str | None, Query(description="Filter by our_status")] = None,
) -> CampaignListResponse:
    """List all campaigns for a brand, with optional source and status filters."""
    repo = CampaignsRepository(db, brand_id)

    filter_: dict = {}
    if source:
        if source not in _VALID_SOURCES:
            # Return empty rather than raising — unknown source = no results
            return CampaignListResponse(campaigns=[], total=0)
        filter_["source"] = source
    if status:
        if status not in _VALID_STATUSES:
            return CampaignListResponse(campaigns=[], total=0)
        filter_["our_status"] = status

    docs = await repo.find(filter_, sort=[("name", 1)])
    campaigns = [CampaignResponse.from_doc(d) for d in docs]
    return CampaignListResponse(campaigns=campaigns, total=len(campaigns))


# ── GET /brands/{brand_id}/campaigns/{cid} ────────────────────────────────────

@router.get("/{brand_id}/campaigns/{cid}", response_model=CampaignResponse)
async def get_campaign(
    brand_id: Annotated[str, Depends(BrandAccess)],
    cid: str,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> CampaignResponse:
    repo = CampaignsRepository(db, brand_id)
    doc = await repo.find_by_id(cid)
    if not doc:
        raise NotFoundError("Campaign not found.", details={"campaign_id": cid})
    return CampaignResponse.from_doc(doc)


# ── PATCH /brands/{brand_id}/campaigns/{cid} ─────────────────────────────────

@router.patch(
    "/{brand_id}/campaigns/{cid}",
    response_model=CampaignResponse,
    dependencies=[Depends(require_permission(Permission.TRIGGER_INGESTION))],
)
async def update_campaign(
    brand_id: Annotated[str, Depends(BrandAccess)],
    cid: str,
    body: CampaignUpdate,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> CampaignResponse:
    """Update campaign metadata (name, our_status, labels). Analyst+ required."""
    repo = CampaignsRepository(db, brand_id)

    # Verify the campaign exists within this brand before updating
    existing = await repo.find_by_id(cid)
    if not existing:
        raise NotFoundError("Campaign not found.", details={"campaign_id": cid})

    fields = body.model_dump(exclude_none=True)
    if fields:
        updated = await repo.update(cid, fields)
        if not updated:
            raise NotFoundError("Campaign not found.", details={"campaign_id": cid})

    doc = await repo.find_by_id(cid)
    return CampaignResponse.from_doc(doc)  # type: ignore[arg-type]
