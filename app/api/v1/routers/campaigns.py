from fastapi import APIRouter

router = APIRouter(prefix="/brands", tags=["campaigns"])

# Implemented in Phase 1 Week 2
# GET    /brands/{id}/campaigns           — list campaigns
# PATCH  /brands/{id}/campaigns/{cid}     — update campaign metadata
