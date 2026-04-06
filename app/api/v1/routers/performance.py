from fastapi import APIRouter

router = APIRouter(prefix="/brands", tags=["performance"])

# Implemented in Phase 1 Week 5
# GET /brands/{id}/performance/daily
# GET /brands/{id}/performance/rollup
# GET /brands/{id}/performance/summary
# GET /brands/{id}/performance/top-campaigns
# GET /brands/{id}/performance/trend
# GET /brands/{id}/performance/attribution
