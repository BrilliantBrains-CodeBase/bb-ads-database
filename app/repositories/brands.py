"""
BrandsRepository

Agency-level collection — does NOT extend BrandScopedRepository because
brands themselves are the tenant boundary, not data within one.

Every write stamps `updated_at`; `created_at` is set on insert only.
Slug uniqueness is enforced by the MongoDB index `brands_agency_slug_unique`
plus the `slug_exists()` pre-check for a cleaner error message.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]


def _oid(brand_id: str) -> ObjectId:
    return ObjectId(brand_id)


class BrandsRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        self._col = db["brands"]

    # ── Queries ───────────────────────────────────────────────────────────────

    async def find_all(
        self,
        agency_id: str,
        *,
        active_only: bool = True,
    ) -> list[Doc]:
        """Return all brands for an agency, ordered by name."""
        q: Doc = {"agency_id": _oid(agency_id)}
        if active_only:
            q["is_active"] = True
        cursor = self._col.find(q).sort("name", 1)
        return await cursor.to_list(length=None)

    async def find_by_id(self, brand_id: str) -> Doc | None:
        return await self._col.find_one({"_id": _oid(brand_id)})

    async def find_by_slug(self, agency_id: str, slug: str) -> Doc | None:
        return await self._col.find_one(
            {"agency_id": _oid(agency_id), "slug": slug}
        )

    async def slug_exists(self, agency_id: str, slug: str) -> bool:
        """True if a brand with this slug already exists under the agency."""
        count = await self._col.count_documents(
            {"agency_id": _oid(agency_id), "slug": slug}
        )
        return count > 0

    # ── Writes ────────────────────────────────────────────────────────────────

    async def create(self, doc: Doc) -> str:
        """Insert a new brand document. Returns the new brand's string id.

        Caller must supply at minimum: agency_id (str), name, slug, created_by (str).
        created_at, updated_at, is_active are set here if absent.
        """
        now = datetime.now(UTC)
        doc = {
            **doc,
            "agency_id": _oid(doc["agency_id"]),
            "created_by": _oid(doc["created_by"]),
            "is_active": doc.get("is_active", True),
            "onboarding_status": doc.get("onboarding_status", "pending"),
            "created_at": doc.get("created_at", now),
            "updated_at": now,
        }
        result = await self._col.insert_one(doc)
        brand_id = str(result.inserted_id)
        logger.info("brand.created", brand_id=brand_id, slug=doc.get("slug"))
        return brand_id

    async def update(self, brand_id: str, fields: Doc) -> bool:
        """Partial update. Returns True if a document was modified."""
        fields = {k: v for k, v in fields.items() if k not in ("_id", "agency_id")}
        fields["updated_at"] = datetime.now(UTC)
        result = await self._col.update_one(
            {"_id": _oid(brand_id)},
            {"$set": fields},
        )
        return result.modified_count > 0

    async def deactivate(self, brand_id: str) -> bool:
        """Soft-delete: set is_active=False. Returns True if found and updated."""
        result = await self._col.update_one(
            {"_id": _oid(brand_id), "is_active": True},
            {"$set": {"is_active": False, "updated_at": datetime.now(UTC)}},
        )
        updated = result.modified_count > 0
        if updated:
            logger.info("brand.deactivated", brand_id=brand_id)
        return updated

    async def set_onboarding_status(
        self,
        brand_id: str,
        status: str,
        *,
        clickup_task_id: str | None = None,
        storage_path: str | None = None,
        onboarded_by: str | None = None,
    ) -> bool:
        """Update onboarding progress fields atomically."""
        now = datetime.now(UTC)
        fields: Doc = {"onboarding_status": status, "updated_at": now}
        if clickup_task_id is not None:
            fields["clickup_task_id"] = clickup_task_id
        if storage_path is not None:
            fields["storage_path"] = storage_path
        if status == "completed":
            fields["onboarded_at"] = now
            if onboarded_by:
                fields["onboarded_by"] = _oid(onboarded_by)
        result = await self._col.update_one(
            {"_id": _oid(brand_id)},
            {"$set": fields},
        )
        return result.modified_count > 0
