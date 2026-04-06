from fastapi import APIRouter

router = APIRouter(prefix="/admin", tags=["admin"])

# Implemented in Phase 1 Week 2
# GET    /admin/users              — list users (super_admin only)
# POST   /admin/users              — create user
# PATCH  /admin/users/{id}         — update user / roles / brand access
# GET    /admin/health             — detailed health (auth required)
# GET    /admin/metrics            — Prometheus-format metrics
# POST   /admin/data-export/{bid}  — DPDP data portability (Phase 3)
# DELETE /admin/data-delete/{bid}  — DPDP right to delete (Phase 3)
