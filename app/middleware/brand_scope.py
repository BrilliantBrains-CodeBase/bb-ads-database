"""
Brand-scope dependency — BrandAccess

Every route that operates on a specific brand should declare:

    @router.get("/brands/{brand_id}/performance/daily")
    async def daily(
        brand: Annotated[str, Depends(BrandAccess)],
        user: AuthUser,
        ...
    ):
        # brand is the validated brand_id string

`BrandAccess` reads the `brand_id` path parameter, verifies the authenticated
user is allowed to access it (super_admin bypasses the check), and binds
`brand_id` to structlog contextvars so every log line in the request
automatically carries it.

It raises:
  - 401 if no authenticated user (propagated from get_current_user)
  - 403 if the user's allowed_brands does not include this brand_id

Usage
-----
Single dependency:

    from app.middleware.brand_scope import BrandAccess

    @router.get("/brands/{brand_id}/campaigns")
    async def list_campaigns(brand_id: Annotated[str, Depends(BrandAccess)]):
        ...

With explicit user object too:

    @router.patch("/brands/{brand_id}/campaigns/{cid}")
    async def update_campaign(
        brand_id: Annotated[str, Depends(BrandAccess)],
        cid: str,
        user: AuthUser,            # same CurrentUser, no second DB trip
        ...
    ):
        ...
"""

from __future__ import annotations

from typing import Annotated

import structlog
from fastapi import Depends, Path

from app.core.exceptions import ForbiddenError
from app.middleware.auth import AuthUser, CurrentUser

logger = structlog.get_logger(__name__)


async def _brand_access(
    brand_id: Annotated[str, Path(description="Brand ObjectId")],
    user: AuthUser,
) -> str:
    """Validate brand access and bind brand_id to log context."""
    if not user.can_access_brand(brand_id):
        logger.warning(
            "brand_scope.denied",
            user_id=user.user_id,
            role=user.role,
            brand_id=brand_id,
        )
        raise ForbiddenError(
            "You do not have access to this brand.",
            details={"brand_id": brand_id},
        )

    # Bind to structlog — from this point every log line in the request
    # carries brand_id alongside the correlation_id and user_id set earlier.
    structlog.contextvars.bind_contextvars(brand_id=brand_id)
    return brand_id


# Public alias used as Depends(BrandAccess) in route signatures
BrandAccess = _brand_access
