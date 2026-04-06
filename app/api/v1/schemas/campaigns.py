"""Pydantic request / response models for the campaigns router."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


_VALID_STATUSES = ("active", "paused", "archived")
_VALID_SOURCES = ("google_ads", "meta", "interakt", "manual")


# ── Requests ──────────────────────────────────────────────────────────────────

class CampaignUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=300)
    our_status: str | None = None
    labels: list[str] | None = None

    @field_validator("our_status")
    @classmethod
    def _valid_status(cls, v: str | None) -> str | None:
        if v is not None and v not in _VALID_STATUSES:
            raise ValueError(f"our_status must be one of {_VALID_STATUSES}")
        return v


# ── Responses ─────────────────────────────────────────────────────────────────

class CampaignResponse(BaseModel):
    id: str
    brand_id: str
    source: str
    external_id: str
    name: str
    objective: str | None = None
    platform_status: str | None = None
    our_status: str = "active"
    start_date: datetime | None = None
    end_date: datetime | None = None
    budget_type: str | None = None
    budget_paise: int | None = None
    labels: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime | None = None

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "CampaignResponse":
        return cls(
            id=str(doc["_id"]),
            brand_id=str(doc["brand_id"]) if doc.get("brand_id") else "",
            source=doc["source"],
            external_id=doc["external_id"],
            name=doc["name"],
            objective=doc.get("objective"),
            platform_status=doc.get("platform_status"),
            our_status=doc.get("our_status", "active"),
            start_date=doc.get("start_date"),
            end_date=doc.get("end_date"),
            budget_type=doc.get("budget_type"),
            budget_paise=doc.get("budget_paise"),
            labels=doc.get("labels", []),
            created_at=doc["created_at"],
            updated_at=doc.get("updated_at"),
        )


class CampaignListResponse(BaseModel):
    campaigns: list[CampaignResponse]
    total: int
