"""
Role-based access control.

Role hierarchy (highest → lowest):
    super_admin  >  admin  >  analyst  >  viewer

Permissions are additive — a higher role inherits all permissions below it.

Usage in routes:

    from app.core.permissions import Permission, require_permission

    @router.post("/ingest/trigger")
    async def trigger(user: Annotated[CurrentUser, Depends(require_permission(Permission.TRIGGER_INGESTION))]):
        ...

Or check inline:

    from app.core.permissions import can
    if not can(user.role, Permission.MANAGE_BRANDS):
        raise ForbiddenError(...)
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

import structlog
from fastapi import Depends

from app.core.exceptions import ForbiddenError
from app.middleware.auth import AuthUser, CurrentUser, get_current_user

logger = structlog.get_logger(__name__)


# ── Role hierarchy ────────────────────────────────────────────────────────────

ROLE_LEVELS: dict[str, int] = {
    "super_admin": 40,
    "admin": 30,
    "analyst": 20,
    "viewer": 10,
}


def has_minimum_role(user_role: str, required_role: str) -> bool:
    """Return True if user_role is at least as privileged as required_role."""
    return ROLE_LEVELS.get(user_role, 0) >= ROLE_LEVELS.get(required_role, 0)


# ── Permission catalogue ──────────────────────────────────────────────────────

class Permission(StrEnum):
    # Read-only — viewer and above
    READ_PERFORMANCE = "read_performance"
    READ_CAMPAIGNS   = "read_campaigns"
    READ_ANOMALIES   = "read_anomalies"
    READ_REPORTS     = "read_reports"

    # Write — analyst and above
    TRIGGER_INGESTION    = "trigger_ingestion"
    UPLOAD_CSV           = "upload_csv"
    ACKNOWLEDGE_ANOMALY  = "acknowledge_anomaly"
    RUN_REPORT           = "run_report"
    ASK_CLAUDE           = "ask_claude"

    # Brand management — admin and above
    MANAGE_BRANDS  = "manage_brands"
    MANAGE_REPORTS = "manage_reports"   # create / edit scheduled reports

    # User and system management — admin and above
    MANAGE_USERS   = "manage_users"
    VIEW_ADMIN     = "view_admin"

    # Super-admin only
    SYSTEM_ADMIN   = "system_admin"     # data export/delete, reset, cross-brand ops


# Map each permission to the minimum role required
_PERMISSION_MIN_ROLE: dict[Permission, str] = {
    Permission.READ_PERFORMANCE:    "viewer",
    Permission.READ_CAMPAIGNS:      "viewer",
    Permission.READ_ANOMALIES:      "viewer",
    Permission.READ_REPORTS:        "viewer",

    Permission.TRIGGER_INGESTION:   "analyst",
    Permission.UPLOAD_CSV:          "analyst",
    Permission.ACKNOWLEDGE_ANOMALY: "analyst",
    Permission.RUN_REPORT:          "analyst",
    Permission.ASK_CLAUDE:          "analyst",

    Permission.MANAGE_BRANDS:       "admin",
    Permission.MANAGE_REPORTS:      "admin",
    Permission.MANAGE_USERS:        "admin",
    Permission.VIEW_ADMIN:          "admin",

    Permission.SYSTEM_ADMIN:        "super_admin",
}


def can(role: str, permission: Permission) -> bool:
    """Return True if `role` is allowed to perform `permission`."""
    required = _PERMISSION_MIN_ROLE.get(permission, "super_admin")
    return has_minimum_role(role, required)


# ── FastAPI dependency factory ────────────────────────────────────────────────

def require_permission(permission: Permission):
    """
    Returns a FastAPI dependency that enforces a single permission.

    Usage:
        Depends(require_permission(Permission.TRIGGER_INGESTION))
    """
    async def _check(
        user: Annotated[CurrentUser, Depends(get_current_user)],
    ) -> CurrentUser:
        if not can(user.role, permission):
            raise ForbiddenError(
                f"Permission '{permission}' requires at least "
                f"'{_PERMISSION_MIN_ROLE[permission]}' role."
            )
        return user

    _check.__name__ = f"require_{permission}"
    return _check


# ── Convenience aliases (typed Depends) ───────────────────────────────────────

# Drop-in replacements for Depends(get_current_user) with permission enforcement.
# Usage:  async def my_route(user: AnalystUser): ...

ViewerUser   = Annotated[CurrentUser, Depends(require_permission(Permission.READ_PERFORMANCE))]
AnalystUser  = Annotated[CurrentUser, Depends(require_permission(Permission.TRIGGER_INGESTION))]
AdminUser    = Annotated[CurrentUser, Depends(require_permission(Permission.MANAGE_BRANDS))]
SuperAdmin   = Annotated[CurrentUser, Depends(require_permission(Permission.SYSTEM_ADMIN))]
