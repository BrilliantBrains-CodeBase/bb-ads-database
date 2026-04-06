from fastapi import APIRouter

router = APIRouter(prefix="/brands", tags=["reports"])

# Implemented in Phase 2 Week 9
# GET  /brands/{id}/reports/scheduled
# POST /brands/{id}/reports/scheduled
# GET  /brands/{id}/reports/scheduled/{rid}
# PUT  /brands/{id}/reports/scheduled/{rid}
# DELETE /brands/{id}/reports/scheduled/{rid}
# POST /brands/{id}/reports/scheduled/{rid}/run
