"""
Users repository — auth-focused subset.

Full CRUD (Week 2) will expand this. For now it exposes exactly what
the auth router and auth middleware need:
  - find_by_email
  - find_by_id
  - find_by_api_key_hash
  - add_api_key / list_api_keys / revoke_api_key
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = structlog.get_logger(__name__)

# Type alias — raw MongoDB document dict
UserDoc = dict[str, Any]


class UsersRepository:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:  # type: ignore[type-arg]
        self._col = db["users"]

    # ── Lookups ───────────────────────────────────────────────────────────────

    async def find_by_email(self, email: str) -> UserDoc | None:
        return await self._col.find_one({"email": email.lower(), "is_active": True})

    async def find_by_id(self, user_id: str) -> UserDoc | None:
        from bson import ObjectId

        try:
            oid = ObjectId(user_id)
        except Exception:
            return None
        return await self._col.find_one({"_id": oid, "is_active": True})

    async def find_by_api_key_hash(self, key_hash: str) -> UserDoc | None:
        """Find an active user that owns the given (non-revoked) API key hash."""
        return await self._col.find_one(
            {
                "api_keys": {
                    "$elemMatch": {"key_hash": key_hash, "revoked": False}
                },
                "is_active": True,
            }
        )

    # ── API key management ────────────────────────────────────────────────────

    async def add_api_key(
        self, user_id: str, name: str, key_hash: str
    ) -> dict[str, Any]:
        """Append a new API key record; return the record (without hash)."""
        from bson import ObjectId

        key_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        record: dict[str, Any] = {
            "key_id": key_id,
            "name": name,
            "key_hash": key_hash,
            "created_at": now,
            "last_used_at": None,
            "revoked": False,
        }
        await self._col.update_one(
            {"_id": ObjectId(user_id)},
            {
                "$push": {"api_keys": record},
                "$set": {"updated_at": now},
            },
        )
        logger.info("api_key.created", user_id=user_id, key_id=key_id)
        return record

    async def list_api_keys(self, user_id: str) -> list[dict[str, Any]]:
        """Return all API key metadata (no hashes) for a user."""
        from bson import ObjectId

        doc = await self._col.find_one(
            {"_id": ObjectId(user_id)}, {"api_keys": 1}
        )
        if not doc:
            return []
        return [
            {k: v for k, v in key.items() if k != "key_hash"}
            for key in doc.get("api_keys", [])
        ]

    async def revoke_api_key(self, user_id: str, key_id: str) -> bool:
        """Mark a key as revoked. Returns True if found and updated."""
        from bson import ObjectId

        result = await self._col.update_one(
            {"_id": ObjectId(user_id), "api_keys.key_id": key_id},
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
        from bson import ObjectId

        await self._col.update_one(
            {"_id": ObjectId(user_id), "api_keys.key_id": key_id},
            {"$set": {"api_keys.$.last_used_at": datetime.now(UTC)}},
        )
