"""Pydantic request / response models for the brands router."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


# ── Shared sub-models ─────────────────────────────────────────────────────────

class BrandSettings(BaseModel):
    target_roas: float | None = None
    target_cpl: int | None = None                 # INR paise
    budget_alert_threshold: float = 0.9           # 0–1 fraction
    anomaly_sensitivity: str = "medium"           # low | medium | high

    @field_validator("anomaly_sensitivity")
    @classmethod
    def _valid_sensitivity(cls, v: str) -> str:
        if v not in ("low", "medium", "high"):
            raise ValueError("anomaly_sensitivity must be low, medium, or high")
        return v

    @field_validator("budget_alert_threshold")
    @classmethod
    def _valid_threshold(cls, v: float) -> float:
        if not (0 < v <= 1):
            raise ValueError("budget_alert_threshold must be between 0 and 1")
        return v


# ── Requests ──────────────────────────────────────────────────────────────────

class BrandCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    slug: str = Field(
        min_length=3,
        max_length=63,
        pattern=r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$",
        description="URL-safe identifier: lowercase alphanumeric and hyphens",
    )
    industry: str | None = Field(default=None, max_length=100)
    settings: BrandSettings | None = None


class BrandUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    industry: str | None = Field(default=None, max_length=100)
    settings: BrandSettings | None = None
    is_active: bool | None = None


# ── Responses ─────────────────────────────────────────────────────────────────

class BrandResponse(BaseModel):
    id: str
    name: str
    slug: str
    industry: str | None = None
    is_active: bool
    onboarding_status: str
    clickup_task_id: str | None = None
    storage_path: str | None = None
    settings: BrandSettings | None = None
    created_at: datetime
    created_by: str | None = None

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "BrandResponse":
        return cls(
            id=str(doc["_id"]),
            name=doc["name"],
            slug=doc["slug"],
            industry=doc.get("industry"),
            is_active=doc.get("is_active", True),
            onboarding_status=doc.get("onboarding_status", "pending"),
            clickup_task_id=doc.get("clickup_task_id"),
            storage_path=doc.get("storage_path"),
            settings=BrandSettings(**doc["settings"]) if doc.get("settings") else None,
            created_at=doc["created_at"],
            created_by=str(doc["created_by"]) if doc.get("created_by") else None,
        )


class BrandListResponse(BaseModel):
    brands: list[BrandResponse]
    total: int


class OnboardingStatusResponse(BaseModel):
    brand_id: str
    onboarding_status: str
    clickup_task_id: str | None = None
    storage_path: str | None = None
    onboarded_at: datetime | None = None
