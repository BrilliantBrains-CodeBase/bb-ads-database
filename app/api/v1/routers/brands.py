from fastapi import APIRouter

router = APIRouter(prefix="/brands", tags=["brands"])

# Implemented in Phase 1 Week 2
# GET    /brands               — list brands for authenticated user
# POST   /brands               — create brand (also creates storage folders + ClickUp task)
# GET    /brands/{id}          — get brand detail
# PATCH  /brands/{id}          — update brand
# POST   /brands/{id}/onboard          — full onboarding flow
# GET    /brands/{id}/onboarding-status
# POST   /brands/{id}/onboard/complete
