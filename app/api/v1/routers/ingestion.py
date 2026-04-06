from fastapi import APIRouter

router = APIRouter(prefix="/ingest", tags=["ingestion"])

# Implemented in Phase 1 Weeks 3-4
# POST /ingest/trigger             — manual ingestion for brand x source
# POST /ingest/backfill            — backfill a date range
# POST /ingest/csv/upload          — CSV file upload
# GET  /ingest/status              — recent ingestion runs
# GET  /ingest/csv/template        — download CSV template
