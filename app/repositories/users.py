"""
UsersRepository

Agency-level collection — does NOT extend BrandScopedRepository.
Users belong to an agency and may be granted access to specific brands
via `allowed_brands` (list of brand ObjectIds).

Auth-specific methods (find_by_email, find_by_api_key_hash, API key
management) are preserved from Week 1 exactly as implemented.
Full CRUD (find_all, create, update, deactivate, email_exists) added here
for the admin router (Week 2).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = structlog.get_logger(__name__)

Doc = dict[str, Any]


def _oid(id_str: str) -> ObjectId:
    return ObjectId(id_str)


class UsersRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        self._col = db["users"]

    # ── Auth lookups (Week 1 — unchanged) ────────────────────────────────────

    async def find_by_email(self, email: str) -> Doc | None:
        return await self._col.find_one({"email": email.lower(), "is_active": True})

    async def find_by_id(self, user_id: str) -> Doc | None:
        try:
            oid = _oid(user_id)
        except Exception:
            return None
        return await self._col.find_one({"_id": oid, "is_active": True})

    async def find_by_api_key_hash(self, key_hash: str) -> Doc | None:
        """Find an active user that owns the given non-revoked API key hash."""
        return await self._col.find_one(
            {
                "api_keys": {
                    "$elemMatch": {"key_hash": key_hash, "revoked": False}
                },
                "is_active": True,
            }
        )

    # ── API key management (Week 1 — unchanged) ───────────────────────────────

    async def add_api_key(
        self, user_id: str, name: str, key_hash: str
    ) -> Doc:
        """Append a new API key record; return the record (without hash)."""
        key_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        record: Doc = {
            "key_id": key_id,
            "name": name,
            "key_hash": key_hash,
            "created_at": now,
            "last_used_at": None,
            "revoked": False,
        }
        await self._col.update_one(
            {"_id": _oid(user_id)},
            {
                "$push": {"api_keys": record},
                "$set": {"updated_at": now},
            },
        )
        logger.info("api_key.created", user_id=user_id, key_id=key_id)
        return record

    async def list_api_keys(self, user_id: str) -> list[Doc]:
        """Return all API key metadata (no hashes) for a user."""
        doc = await self._col.find_one(
            {"_id": _oid(user_id)}, {"api_keys": 1}
        )
        if not doc:
            return []
        return [
            {k: v for k, v in key.items() if k != "key_hash"}
            for key in doc.get("api_keys", [])
        ]

    async def revoke_api_key(self, user_id: str, key_id: str) -> bool:
        """Mark a key as revoked. Returns True if found and updated."""
        result = await self._col.update_one(
            {"_id": _oid(user_id), "api_keys.key_id": key_id},
            {
                "$set": {
                    "api_keys.$.revoked": True,
                    "updated_at": datetime.now(UTC),
                }
            },
        )
        updated = result.modified_count > 0
        if updated:
            logger.info("api_key.revoked", user_id=user_id, key_id=key_id)
        return updated

    async def touch_api_key(self, user_id: str, key_id: str) -> None:
        """Update last_used_at — called after successful API key auth."""
        await self._col.update_one(
            {"_id": _oid(user_id), "api_keys.key_id": key_id},
            {"$set": {"api_keys.$.last_used_at": datetime.now(UTC)}},
        )

    # ── CRUD (Week 2) ─────────────────────────────────────────────────────────

    async def find_all(
        self,
        agency_id: str,
        *,
        role: str | None = None,
        active_only: bool = True,
    ) -> list[Doc]:
        """Return users for an agency, optionally filtered by role."""
        q: Doc = {"agency_id": _oid(agency_id)}
        if active_only:
            q["is_active"] = True
        if role:
            q["role"] = role
        cursor = self._col.find(q, {"hashed_password": 0, "api_keys.key_hash": 0}).sort(
            "email", 1
        )
        return await cursor.to_list(length=None)

    async def email_exists(self, email: str) -> bool:
        """True if any user (active or not) has this email."""
        count = await self._col.count_documents({"email": email.lower()})
        return count > 0

    async def create(self, doc: Doc) -> str:
        """Insert a new user. Returns the new user's string id.

        Caller must supply: agency_id (str), email, hashed_password, role.
        Normalises email to lowercase; sets timestamps and defaults.
        """
        now = datetime.now(UTC)
        allowed = [_oid(b) for b in doc.get("allowed_brands", [])]
        insert_doc: Doc = {
            **doc,
            "email": doc["email"].lower(),
            "agency_id": _oid(doc["agency_id"]),
            "allowed_brands": allowed,
            "is_active": doc.get("is_active", True),
            "api_keys": doc.get("api_keys", []),
            "created_at": doc.get("created_at", now),
            "updated_at": now,
        }
        result = await self._col.insert_one(insert_doc)
        user_id = str(result.inserted_id)
        logger.info("user.created", user_id=user_id, email=insert_doc["email"])
        return user_id

    async def update(self, user_id: str, fields: Doc) -> bool:
        """Partial update. Strips immutable fields. Returns True if modified."""
        fields = {
            k: v
            for k, v in fields.items()
            if k not in ("_id", "agency_id", "hashed_password", "api_keys")
        }
        if "email" in fields:
            fields["email"] = fields["email"].lower()
        if "allowed_brands" in fields:
            fields["allowed_brands"] = [_oid(b) for b in fields["allowed_brands"]]
        fields["updated_at"] = datetime.now(UTC)
        result = await self._col.update_one(
            {"_id": _oid(user_id)}, {"$set": fields}
        )
        return result.modified_count > 0

    async def update_password(self, user_id: str, hashed_password: str) -> bool:
        """Replace hashed password. Returns True if modified."""
        result = await self._col.update_one(
            {"_id": _oid(user_id)},
            {"$set": {"hashed_password": hashed_password, "updated_at": datetime.now(UTC)}},
        )
        return result.modified_count > 0

    async def deactivate(self, user_id: str) -> bool:
        """Soft-delete: set is_active=False. Returns True if found and updated."""
        result = await self._col.update_one(
            {"_id": _oid(user_id), "is_active": True},
            {"$set": {"is_active": False, "updated_at": datetime.now(UTC)}},
        )
        updated = result.modified_count > 0
        if updated:
            logger.info("user.deactivated", user_id=user_id)
        return updated
