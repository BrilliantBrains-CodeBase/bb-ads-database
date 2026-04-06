"""
Cryptographic primitives for the auth system.

  - JWT RS256: sign / verify access and refresh tokens
  - Password hashing: bcrypt via passlib
  - API key: "bbads_" + base58(32 random bytes), SHA-256 hash stored in DB
"""

from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from app.core.config import get_settings

# ── Password hashing ──────────────────────────────────────────────────────────

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_context.verify(plain, hashed)


def dummy_password_verify() -> None:
    """Run a constant-time dummy bcrypt check.

    Call this when the email is not found in the DB so the response time
    is indistinguishable from a real failed login (prevents user enumeration
    via timing side-channel).
    """
    _pwd_context.dummy_verify()


# ── PEM key loading ───────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_private_key() -> str:
    path = Path(get_settings().jwt_private_key_path)
    if not path.exists():
        raise FileNotFoundError(
            f"JWT private key not found at {path}. "
            "Generate with: openssl genrsa -out keys/private.pem 2048"
        )
    return path.read_text()


@lru_cache(maxsize=1)
def _load_public_key() -> str:
    path = Path(get_settings().jwt_public_key_path)
    if not path.exists():
        raise FileNotFoundError(
            f"JWT public key not found at {path}. "
            "Generate with: openssl rsa -in keys/private.pem -pubout -out keys/public.pem"
        )
    return path.read_text()


# ── Token models ──────────────────────────────────────────────────────────────

class AccessTokenClaims(BaseModel):
    sub: str                      # user_id (str of ObjectId)
    role: str
    allowed_brands: list[str]     # list of brand_id strings
    jti: str
    iat: datetime
    exp: datetime
    type: str = "access"


class RefreshTokenClaims(BaseModel):
    sub: str                      # user_id
    jti: str                      # UUID — stored in Redis blocklist on rotation/logout
    iat: datetime
    exp: datetime
    type: str = "refresh"


# ── JWT sign / verify ─────────────────────────────────────────────────────────

def create_access_token(
    user_id: str,
    role: str,
    allowed_brands: list[str],
) -> tuple[str, str]:
    """Return (encoded_jwt, jti)."""
    settings = get_settings()
    now = datetime.now(UTC)
    jti = str(uuid.uuid4())
    claims: dict[str, Any] = {
        "sub": user_id,
        "role": role,
        "allowed_brands": allowed_brands,
        "jti": jti,
        "iat": now,
        "exp": now + timedelta(minutes=settings.jwt_access_token_expire_minutes),
        "type": "access",
    }
    token = jwt.encode(claims, _load_private_key(), algorithm="RS256")
    return token, jti


def create_refresh_token(user_id: str) -> tuple[str, str]:
    """Return (encoded_jwt, jti)."""
    settings = get_settings()
    now = datetime.now(UTC)
    jti = str(uuid.uuid4())
    claims: dict[str, Any] = {
        "sub": user_id,
        "jti": jti,
        "iat": now,
        "exp": now + timedelta(days=settings.jwt_refresh_token_expire_days),
        "type": "refresh",
    }
    token = jwt.encode(claims, _load_private_key(), algorithm="RS256")
    return token, jti


def decode_access_token(token: str) -> AccessTokenClaims:
    """Decode and validate an access token. Raises jwt.* on failure."""
    payload = jwt.decode(token, _load_public_key(), algorithms=["RS256"])
    if payload.get("type") != "access":
        raise jwt.InvalidTokenError("Not an access token")
    return AccessTokenClaims(**payload)


def decode_refresh_token(token: str) -> RefreshTokenClaims:
    """Decode and validate a refresh token. Raises jwt.* on failure."""
    payload = jwt.decode(token, _load_public_key(), algorithms=["RS256"])
    if payload.get("type") != "refresh":
        raise jwt.InvalidTokenError("Not a refresh token")
    return RefreshTokenClaims(**payload)


# ── API key generation ────────────────────────────────────────────────────────

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _base58_encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    result: list[str] = []
    while n > 0:
        n, r = divmod(n, 58)
        result.append(_BASE58_ALPHABET[r])
    for byte in data:
        if byte == 0:
            result.append(_BASE58_ALPHABET[0])
        else:
            break
    return "".join(reversed(result))


def generate_api_key() -> tuple[str, str]:
    """Return (raw_key, key_hash).

    raw_key  — shown to the user exactly once; never stored.
    key_hash — SHA-256 hex digest; stored in MongoDB.
    """
    raw_bytes = secrets.token_bytes(32)
    raw_key = f"bbads_{_base58_encode(raw_bytes)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    return raw_key, key_hash


def hash_api_key(raw_key: str) -> str:
    """Reproduce the stored hash from a raw key (for lookup)."""
    return hashlib.sha256(raw_key.encode()).hexdigest()
