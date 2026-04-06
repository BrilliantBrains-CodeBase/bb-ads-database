from fastapi import APIRouter

router = APIRouter(prefix="/brands", tags=["anomalies"])

# Implemented in Phase 2 Week 8
# GET   /brands/{id}/anomalies
# PATCH /brands/{id}/anomalies/{aid}/acknowledge
