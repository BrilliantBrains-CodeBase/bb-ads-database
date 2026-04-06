"""Pydantic request / response models for the auth router."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


# ── Requests ──────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1)


class RefreshRequest(BaseModel):
    refresh_token: str


class LogoutRequest(BaseModel):
    access_token: str
    refresh_token: str


class CreateApiKeyRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100, description="Human-readable label")


class RevokeApiKeyRequest(BaseModel):
    key_id: str


# ── Responses ─────────────────────────────────────────────────────────────────

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # access token TTL in seconds


class ApiKeyCreatedResponse(BaseModel):
    key_id: str
    name: str
    raw_key: str = Field(description="Shown exactly once — store it now.")
    created_at: datetime


class ApiKeyMetadata(BaseModel):
    key_id: str
    name: str
    created_at: datetime
    last_used_at: datetime | None = None
    revoked: bool = False


class ApiKeyListResponse(BaseModel):
    keys: list[ApiKeyMetadata]
