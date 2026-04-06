"""
Brands router

  GET    /brands                       — list brands visible to the caller
  POST   /brands                       — create brand (admin+)
  GET    /brands/{brand_id}            — get brand detail
  PATCH  /brands/{brand_id}            — update brand (admin+)
  POST   /brands/{brand_id}/onboard    — start onboarding flow (admin+)
  GET    /brands/{brand_id}/onboarding-status
  POST   /brands/{brand_id}/onboard/complete  — mark onboarding done (admin+)
"""

from __future__ import annotations

from typing import Annotated

import structlog
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.v1.schemas.brands import (
    BrandCreate,
    BrandListResponse,
    BrandResponse,
    BrandUpdate,
    ChecklistItem,
    OnboardingStatusResponse,
)
from app.core.database import get_database
from app.core.exceptions import ConflictError, NotFoundError
from app.core.permissions import Permission, require_permission
from app.middleware.auth import AuthUser, CurrentUser
from app.middleware.brand_scope import BrandAccess
from app.repositories.brands import BrandsRepository
from app.repositories.users import UsersRepository
from app.services import brand_storage, clickup

router = APIRouter(prefix="/brands", tags=["brands"])
logger = structlog.get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _get_agency_id(user: CurrentUser, db: AsyncIOMotorDatabase) -> str:  # type: ignore[type-arg]
    """Fetch the authenticated user's agency_id from the DB."""
    repo = UsersRepository(db)
    doc = await repo.find_by_id(user.user_id)
    if not doc:
        raise NotFoundError("User account not found.")
    return str(doc["agency_id"])


# ── GET /brands ───────────────────────────────────────────────────────────────

@router.get("", response_model=BrandListResponse)
async def list_brands(
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> BrandListResponse:
    """List brands visible to the caller.

    super_admin and admin: all active brands.
    analyst / viewer: only brands in their allowed_brands list.
    """
    repo = BrandsRepository(db)

    if user.role in ("super_admin", "admin"):
        agency_id = await _get_agency_id(user, db)
        brands = await repo.find_all(agency_id)
    else:
        brands = await repo.find_by_ids(user.allowed_brands)

    return BrandListResponse(
        brands=[BrandResponse.from_doc(b) for b in brands],
        total=len(brands),
    )


# ── POST /brands ──────────────────────────────────────────────────────────────

@router.post(
    "",
    response_model=BrandResponse,
    status_code=201,
    dependencies=[Depends(require_permission(Permission.MANAGE_BRANDS))],
)
async def create_brand(
    body: BrandCreate,
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> BrandResponse:
    """Create a brand, provision storage folders, and queue a ClickUp onboarding task."""
    repo = BrandsRepository(db)
    agency_id = await _get_agency_id(user, db)

    # Slug uniqueness check (before DB insert for a clean error message)
    if await repo.slug_exists(agency_id, body.slug):
        raise ConflictError(
            f"A brand with slug '{body.slug}' already exists.",
            details={"slug": body.slug},
        )

    # Persist brand
    doc: dict = {
        "agency_id": agency_id,
        "name": body.name,
        "slug": body.slug,
        "industry": body.industry,
        "is_active": True,
        "created_by": user.user_id,
        "onboarding_status": "pending",
        "settings": body.settings.model_dump() if body.settings else {},
    }
    brand_id = await repo.create(doc)

    # Create storage folders (best-effort — failure must not roll back brand creation)
    storage_path: str | None = None
    try:
        root = brand_storage.create_brand_folders(body.slug)
        storage_path = str(root)
        await repo.update(brand_id, {"storage_path": storage_path})
    except Exception as exc:
        logger.warning(
            "brand.storage_creation_failed",
            brand_id=brand_id,
            slug=body.slug,
            error=str(exc),
        )

    # Queue ClickUp onboarding task (best-effort)
    try:
        fresh_doc = await repo.find_by_id(brand_id)
        task_id = await clickup.create_onboarding_task(fresh_doc or doc)
        if task_id:
            await repo.set_onboarding_status(
                brand_id, "pending", clickup_task_id=task_id
            )
    except Exception as exc:
        logger.warning(
            "brand.clickup_task_failed",
            brand_id=brand_id,
            error=str(exc),
        )

    created = await repo.find_by_id(brand_id)
    if not created:
        raise NotFoundError("Brand was created but could not be retrieved.")
    return BrandResponse.from_doc(created)


# ── GET /brands/{brand_id} ────────────────────────────────────────────────────

@router.get("/{brand_id}", response_model=BrandResponse)
async def get_brand(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> BrandResponse:
    repo = BrandsRepository(db)
    doc = await repo.find_by_id(brand_id)
    if not doc:
        raise NotFoundError("Brand not found.", details={"brand_id": brand_id})
    return BrandResponse.from_doc(doc)


# ── PATCH /brands/{brand_id} ──────────────────────────────────────────────────

@router.patch(
    "/{brand_id}",
    response_model=BrandResponse,
    dependencies=[Depends(require_permission(Permission.MANAGE_BRANDS))],
)
async def update_brand(
    brand_id: Annotated[str, Depends(BrandAccess)],
    body: BrandUpdate,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> BrandResponse:
    repo = BrandsRepository(db)
    fields = body.model_dump(exclude_none=True)
    if "settings" in fields and isinstance(fields["settings"], dict):
        pass  # already a dict from model_dump

    if not fields:
        doc = await repo.find_by_id(brand_id)
        if not doc:
            raise NotFoundError("Brand not found.")
        return BrandResponse.from_doc(doc)

    updated = await repo.update(brand_id, fields)
    if not updated:
        raise NotFoundError("Brand not found.", details={"brand_id": brand_id})

    doc = await repo.find_by_id(brand_id)
    return BrandResponse.from_doc(doc)  # type: ignore[arg-type]


# ── POST /brands/{brand_id}/onboard ──────────────────────────────────────────

@router.post(
    "/{brand_id}/onboard",
    response_model=OnboardingStatusResponse,
    status_code=200,
    dependencies=[Depends(require_permission(Permission.MANAGE_BRANDS))],
)
async def start_onboarding(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> OnboardingStatusResponse:
    """Idempotently trigger the full onboarding flow for an existing brand."""
    repo = BrandsRepository(db)
    doc = await repo.find_by_id(brand_id)
    if not doc:
        raise NotFoundError("Brand not found.")

    slug = doc["slug"]

    # Ensure storage exists
    storage_path: str | None = doc.get("storage_path")
    if not storage_path or not brand_storage.brand_exists(slug):
        try:
            root = brand_storage.create_brand_folders(slug)
            storage_path = str(root)
        except Exception as exc:
            logger.warning("brand.storage_creation_failed", brand_id=brand_id, error=str(exc))

    # Ensure ClickUp task exists
    task_id: str | None = doc.get("clickup_task_id")
    if not task_id:
        try:
            task_id = await clickup.create_onboarding_task(doc)
        except Exception as exc:
            logger.warning("brand.clickup_task_failed", brand_id=brand_id, error=str(exc))

    await repo.set_onboarding_status(
        brand_id,
        "in_progress",
        clickup_task_id=task_id,
        storage_path=storage_path,
    )

    updated = await repo.find_by_id(brand_id)
    return OnboardingStatusResponse(
        brand_id=brand_id,
        onboarding_status=updated.get("onboarding_status", "in_progress"),  # type: ignore[union-attr]
        clickup_task_id=updated.get("clickup_task_id"),  # type: ignore[union-attr]
        storage_path=updated.get("storage_path"),  # type: ignore[union-attr]
    )


# ── GET /brands/{brand_id}/onboarding-status ─────────────────────────────────

@router.get("/{brand_id}/onboarding-status", response_model=OnboardingStatusResponse)
async def get_onboarding_status(
    brand_id: Annotated[str, Depends(BrandAccess)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> OnboardingStatusResponse:
    """Return onboarding status + live checklist progress from ClickUp."""
    repo = BrandsRepository(db)
    doc = await repo.find_by_id(brand_id)
    if not doc:
        raise NotFoundError("Brand not found.")

    task_id: str | None = doc.get("clickup_task_id")
    checklist_items: list[ChecklistItem] | None = None
    checklist_resolved = 0
    checklist_total = 0

    # Fetch live checklist from ClickUp when task exists (best-effort)
    if task_id:
        try:
            task = await clickup.get_task(task_id)
            if task:
                # Sync status back to DB if it changed in ClickUp
                raw_status: str = task.get("status", {}).get("status", "")
                if raw_status:
                    mapped = clickup.map_clickup_status(raw_status)
                    if mapped != doc.get("onboarding_status"):
                        await repo.set_onboarding_status(brand_id, mapped)
                        doc["onboarding_status"] = mapped

                # Flatten all checklists into one item list
                all_items: list[ChecklistItem] = []
                for cl in task.get("checklists", []):
                    for item in cl.get("items", []):
                        ci = ChecklistItem(
                            name=item.get("name", ""),
                            resolved=bool(item.get("resolved", False)),
                        )
                        all_items.append(ci)
                if all_items:
                    checklist_items = all_items
                    checklist_resolved = sum(1 for i in all_items if i.resolved)
                    checklist_total = len(all_items)
        except Exception as exc:
            logger.warning(
                "brand.checklist_fetch_failed",
                brand_id=brand_id,
                task_id=task_id,
                error=str(exc),
            )

    return OnboardingStatusResponse(
        brand_id=brand_id,
        onboarding_status=doc.get("onboarding_status", "pending"),
        clickup_task_id=task_id,
        storage_path=doc.get("storage_path"),
        onboarded_at=doc.get("onboarded_at"),
        checklist=checklist_items,
        checklist_resolved=checklist_resolved,
        checklist_total=checklist_total,
    )


# ── POST /brands/{brand_id}/onboard/complete ─────────────────────────────────

@router.post(
    "/{brand_id}/onboard/complete",
    response_model=OnboardingStatusResponse,
    dependencies=[Depends(require_permission(Permission.MANAGE_BRANDS))],
)
async def complete_onboarding(
    brand_id: Annotated[str, Depends(BrandAccess)],
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> OnboardingStatusResponse:
    """Mark onboarding as completed and activate the brand for ingestion."""
    repo = BrandsRepository(db)
    doc = await repo.find_by_id(brand_id)
    if not doc:
        raise NotFoundError("Brand not found.")

    task_id = doc.get("clickup_task_id")
    if task_id:
        try:
            await clickup.update_task_status(task_id, "Live")
        except Exception as exc:
            logger.warning("brand.clickup_status_update_failed", brand_id=brand_id, error=str(exc))

    await repo.set_onboarding_status(
        brand_id,
        "completed",
        onboarded_by=user.user_id,
    )
    logger.info("brand.onboarding_completed", brand_id=brand_id, completed_by=user.user_id)

    updated = await repo.find_by_id(brand_id)
    return OnboardingStatusResponse(
        brand_id=brand_id,
        onboarding_status="completed",
        clickup_task_id=updated.get("clickup_task_id"),  # type: ignore[union-attr]
        storage_path=updated.get("storage_path"),  # type: ignore[union-attr]
        onboarded_at=updated.get("onboarded_at"),  # type: ignore[union-attr]
    )
