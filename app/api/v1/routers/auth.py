from fastapi import APIRouter

router = APIRouter(prefix="/auth", tags=["auth"])

# Implemented in Phase 1 Week 1
# POST /auth/token        — login, returns access + refresh tokens
# POST /auth/refresh      — rotate refresh token
# POST /auth/logout       — blocklist tokens
# POST /auth/api-keys     — create / list / revoke API keys
