from fastapi import APIRouter

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Implemented in Phase 2 / Phase 3
# POST /webhooks/clickup   — receive ClickUp task status changes, sync brand onboarding status
