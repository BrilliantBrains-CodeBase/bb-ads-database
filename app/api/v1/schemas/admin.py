"""Pydantic request / response models for the admin router."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field, field_validator

_VALID_ROLES = ("super_admin", "admin", "analyst", "viewer")


# ── Requests ──────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    role: str = "viewer"
    allowed_brands: list[str] = Field(default_factory=list)

    @field_validator("role")
    @classmethod
    def _valid_role(cls, v: str) -> str:
        if v not in _VALID_ROLES:
            raise ValueError(f"role must be one of {_VALID_ROLES}")
        return v


class UserUpdate(BaseModel):
    role: str | None = None
    allowed_brands: list[str] | None = None
    is_active: bool | None = None

    @field_validator("role")
    @classmethod
    def _valid_role(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_ROLES:
            raise ValueError(f"role must be one of {_VALID_ROLES}")
        return v


# ── Responses ─────────────────────────────────────────────────────────────────

class UserResponse(BaseModel):
    id: str
    email: str
    role: str
    allowed_brands: list[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime | None = None

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "UserResponse":
        return cls(
            id=str(doc["_id"]),
            email=doc["email"],
            role=doc["role"],
            allowed_brands=[str(b) for b in doc.get("allowed_brands", [])],
            is_active=doc.get("is_active", True),
            created_at=doc["created_at"],
            updated_at=doc.get("updated_at"),
        )


class UserListResponse(BaseModel):
    users: list[UserResponse]
    total: int


# ── Health ────────────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    status: str          # "ok" | "degraded" | "down"
    latency_ms: float | None = None
    detail: str | None = None


class LastIngestion(BaseModel):
    hours_since: float | None = None
    status: str | None = None
    brand_id: str | None = None
    source: str | None = None


class HealthDetailResponse(BaseModel):
    status: str          # "ok" | "degraded" | "down"
    mongodb: ServiceStatus
    redis: ServiceStatus
    last_ingestion: LastIngestion | None = None
    checked_at: datetime
