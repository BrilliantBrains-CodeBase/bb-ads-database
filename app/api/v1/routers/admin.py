"""
Admin router

  GET    /admin/users           — list users (admin+)
  POST   /admin/users           — create user (admin+)
  PATCH  /admin/users/{id}      — update user role / brand access (admin+)
  GET    /admin/health          — detailed health check (admin+)
  GET    /admin/metrics         — metrics placeholder (admin+); full Prometheus in Week 10
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, Query, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.api.v1.schemas.admin import (
    HealthDetailResponse,
    LastIngestion,
    ServiceStatus,
    UserCreate,
    UserListResponse,
    UserResponse,
    UserUpdate,
)
from app.core.database import get_database
from app.core.exceptions import ConflictError, NotFoundError
from app.core.permissions import Permission, require_permission
from app.core.redis import get_redis
from app.core.security import hash_password
from app.middleware.auth import AuthUser
from app.repositories.users import UsersRepository

router = APIRouter(prefix="/admin", tags=["admin"])
logger = structlog.get_logger(__name__)

# Shared admin+ dependency
_admin = Depends(require_permission(Permission.MANAGE_USERS))


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _get_agency_id(user_id: str, db: AsyncIOMotorDatabase) -> str:  # type: ignore[type-arg]
    repo = UsersRepository(db)
    doc = await repo.find_by_id(user_id)
    if not doc:
        raise NotFoundError("Admin user account not found.")
    return str(doc["agency_id"])


# ── GET /admin/users ──────────────────────────────────────────────────────────

@router.get("/users", response_model=UserListResponse, dependencies=[_admin])
async def list_users(
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    role: Annotated[str | None, Query(description="Filter by role")] = None,
    active_only: Annotated[bool, Query()] = True,
) -> UserListResponse:
    agency_id = await _get_agency_id(user.user_id, db)
    repo = UsersRepository(db)
    docs = await repo.find_all(agency_id, role=role, active_only=active_only)
    users = [UserResponse.from_doc(d) for d in docs]
    return UserListResponse(users=users, total=len(users))


# ── POST /admin/users ─────────────────────────────────────────────────────────

@router.post("/users", response_model=UserResponse, status_code=201, dependencies=[_admin])
async def create_user(
    body: UserCreate,
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> UserResponse:
    """Create a new agency user. Password is hashed before storage."""
    repo = UsersRepository(db)
    email = str(body.email).lower()

    if await repo.email_exists(email):
        raise ConflictError(
            f"A user with email '{email}' already exists.",
            details={"email": email},
        )

    agency_id = await _get_agency_id(user.user_id, db)
    user_id = await repo.create({
        "agency_id": agency_id,
        "email": email,
        "hashed_password": hash_password(body.password),
        "role": body.role,
        "allowed_brands": body.allowed_brands,
    })
    logger.info("admin.user_created", new_user_id=user_id, created_by=user.user_id)

    doc = await repo.find_by_id(user_id)
    return UserResponse.from_doc(doc)  # type: ignore[arg-type]


# ── PATCH /admin/users/{id} ───────────────────────────────────────────────────

@router.patch("/users/{uid}", response_model=UserResponse, dependencies=[_admin])
async def update_user(
    uid: str,
    body: UserUpdate,
    user: AuthUser,
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> UserResponse:
    """Update a user's role, allowed_brands, or active status."""
    repo = UsersRepository(db)

    # Non-super_admin cannot elevate a user to super_admin
    if body.role == "super_admin" and user.role != "super_admin":
        from app.core.exceptions import ForbiddenError
        raise ForbiddenError("Only super_admin can grant the super_admin role.")

    fields = body.model_dump(exclude_none=True)
    if not fields:
        doc = await repo.find_by_id(uid)
        if not doc:
            raise NotFoundError("User not found.", details={"user_id": uid})
        return UserResponse.from_doc(doc)

    updated = await repo.update(uid, fields)
    if not updated:
        raise NotFoundError("User not found.", details={"user_id": uid})

    doc = await repo.find_by_id(uid)
    logger.info("admin.user_updated", target_user_id=uid, updated_by=user.user_id)
    return UserResponse.from_doc(doc)  # type: ignore[arg-type]


# ── GET /admin/health ─────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthDetailResponse,
    dependencies=[Depends(require_permission(Permission.VIEW_ADMIN))],
)
async def detailed_health(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
    redis: Annotated[Redis, Depends(get_redis)],  # type: ignore[type-arg]
) -> HealthDetailResponse:
    """Detailed health check: MongoDB latency, Redis status, last ingestion age."""
    # ── MongoDB ───────────────────────────────────────────────────
    mongo_status: ServiceStatus
    try:
        t0 = time.monotonic()
        await db.command("ping")
        mongo_ms = round((time.monotonic() - t0) * 1000, 2)
        mongo_status = ServiceStatus(status="ok", latency_ms=mongo_ms)
    except Exception as exc:
        mongo_status = ServiceStatus(status="down", detail=str(exc))

    # ── Redis ─────────────────────────────────────────────────────
    redis_status: ServiceStatus
    try:
        t0 = time.monotonic()
        await redis.ping()
        redis_ms = round((time.monotonic() - t0) * 1000, 2)
        redis_status = ServiceStatus(status="ok", latency_ms=redis_ms)
    except Exception as exc:
        redis_status = ServiceStatus(status="down", detail=str(exc))

    # ── Last ingestion ────────────────────────────────────────────
    last_ingestion: LastIngestion | None = None
    try:
        last_log = await db["ingestion_logs"].find_one(
            {"status": {"$in": ["success", "partial"]}},
            sort=[("completed_at", -1)],
        )
        if last_log and last_log.get("completed_at"):
            completed = last_log["completed_at"]
            if completed.tzinfo is None:
                completed = completed.replace(tzinfo=UTC)
            hours_ago = (datetime.now(UTC) - completed).total_seconds() / 3600
            last_ingestion = LastIngestion(
                hours_since=round(hours_ago, 2),
                status=last_log.get("status"),
                brand_id=str(last_log.get("brand_id", "")),
                source=last_log.get("source"),
            )
    except Exception:
        pass  # non-critical

    # ── Overall status ────────────────────────────────────────────
    if mongo_status.status == "down" or redis_status.status == "down":
        overall = "down"
    elif last_ingestion and last_ingestion.hours_since and last_ingestion.hours_since > 26:
        overall = "degraded"
    else:
        overall = "ok"

    return HealthDetailResponse(
        status=overall,
        mongodb=mongo_status,
        redis=redis_status,
        last_ingestion=last_ingestion,
        checked_at=datetime.now(UTC),
    )


# ── GET /admin/metrics ────────────────────────────────────────────────────────

@router.get(
    "/metrics",
    dependencies=[Depends(require_permission(Permission.VIEW_ADMIN))],
    response_class=Response,
)
async def metrics(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],  # type: ignore[type-arg]
) -> Response:
    """Basic Prometheus-format metrics placeholder.

    Full per-request latency histograms and error-rate counters are added
    in Week 10 (app/middleware/metrics.py).  This endpoint returns collection
    document counts that are cheap to compute and useful for capacity monitoring.
    """
    try:
        brand_count = await db["brands"].count_documents({"is_active": True})
        user_count = await db["users"].count_documents({"is_active": True})
        campaign_count = await db["campaigns"].count_documents({})
        perf_count = await db["ad_performance_raw"].count_documents({})

        lines = [
            "# HELP bb_brands_active Number of active brands",
            "# TYPE bb_brands_active gauge",
            f"bb_brands_active {brand_count}",
            "# HELP bb_users_active Number of active users",
            "# TYPE bb_users_active gauge",
            f"bb_users_active {user_count}",
            "# HELP bb_campaigns_total Total campaigns (all statuses)",
            "# TYPE bb_campaigns_total gauge",
            f"bb_campaigns_total {campaign_count}",
            "# HELP bb_performance_raw_docs Total raw performance documents",
            "# TYPE bb_performance_raw_docs gauge",
            f"bb_performance_raw_docs {perf_count}",
        ]
        body = "\n".join(lines) + "\n"
    except Exception as exc:
        body = f"# error collecting metrics: {exc}\n"

    return Response(content=body, media_type="text/plain; version=0.0.4")
