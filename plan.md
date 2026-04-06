# Agency Analytics Platform — Implementation Plan

## Context

Greenfield build of a multi-tenant ad performance platform for a single digital agency managing 20-50+ brands. The system ingests daily ad data from Google Ads, Meta Ads, Interakt (WhatsApp), and CSV uploads, stores it in MongoDB, exposes a FastAPI REST API, and layers a Claude-powered analytics suite. Currently only the spec document exists — no code, no git repo.

The spec is solid on architecture and infra but has gaps in: auth rate limiting, structured logging, error taxonomy, CORS, caching, dev experience, compliance, **brand asset storage**, and **onboarding automation**. This plan addresses those gaps within the phased timeline.

Additionally, the agency has existing client files that need migrating into a standardized folder structure, and ClickUp is the PM tool used for tracking brand onboarding workflows.

---

## Phase 0 — Project Scaffolding (Days 1-2)

### 0.1 Git + Project Root
- `git init` + `.gitignore` (Python, Docker, .env, __pycache__, .mypy_cache)
- `.env.example` with all required env vars documented
- `pyproject.toml` — project metadata, dependencies, ruff/mypy config
- `requirements.txt` (pinned) — fastapi, uvicorn, motor, pydantic, pyjwt[crypto], cryptography, passlib[bcrypt], apscheduler, httpx, python-multipart, redis, anthropic, google-ads, facebook-business, sendgrid, ruff, mypy, pytest, pytest-asyncio, mongomock-motor

### 0.2 Docker Stack
- `Dockerfile` — multi-stage (builder + slim runtime), non-root user
- `docker-compose.yml` — services: nginx, api (2 replicas), worker, mongodb (7.0), redis, mongo-express (dev profile only)
- `docker-compose.override.yml` — dev overrides (hot reload, exposed ports, mongo-express)
- `nginx/nginx.conf` — reverse proxy stub (SSL later in Phase 1 Week 6)
- `nginx/conf.d/api.conf` — upstream to api replicas, rate limiting (60 req/min)

### 0.3 App Skeleton
```
app/
  __init__.py
  main.py                          # FastAPI app factory, lifespan, CORS, middleware
  api/__init__.py
  api/v1/__init__.py
  api/v1/routers/__init__.py       # Empty router files for each module
  core/__init__.py
  core/config.py                   # Pydantic Settings (env-based)
  core/database.py                 # Motor client + get_database()
  repositories/__init__.py
  services/__init__.py
  worker/__init__.py
  middleware/__init__.py
```

### 0.4 Brand Asset Storage Structure
Each brand gets a standardized folder tree on disk (mounted Docker volume or VPS path). Created automatically on brand onboarding.

**Base path:** `BRAND_STORAGE_ROOT` env var (default: `/data/brands/`)

**Template per brand:**
```
/data/brands/{brand_slug}/
  credentials/          # Encrypted API tokens, OAuth JSONs (AES-256-GCM encrypted at rest)
  csv-uploads/          # Raw uploaded CSVs (organized by YYYY/MM/)
    2026/04/
  reports/              # Generated reports (PDF/HTML exports)
    scheduled/          # Auto-generated scheduled reports
    ad-hoc/             # Manually triggered reports
  exports/              # DPDP data exports, bulk downloads
  creatives/            # Ad creative assets (images, videos) synced from platforms (Phase 3+)
  logs/                 # Brand-specific ingestion logs (symlinked from main logs)
  config/               # Brand-specific config overrides (targets, thresholds)
    brand_config.json   # target_roas, target_cpl, budget_alert_threshold, anomaly_sensitivity
```

**Implementation:**
- `app/services/brand_storage.py`:
  - `create_brand_folders(brand_slug)` — creates full template tree, sets permissions (0o750)
  - `get_brand_path(brand_slug, subfolder)` — returns absolute path, validates brand exists
  - `cleanup_brand_folders(brand_slug)` — archives to `/data/brands/_archived/{brand_slug}_{timestamp}/`
  - CSV uploads route to `csv-uploads/YYYY/MM/{filename}_{upload_id}.csv`
  - Reports saved to `reports/scheduled/` or `reports/ad-hoc/` based on trigger type
- Called automatically from `POST /brands` (create) and `POST /brands/onboard` (onboarding flow)
- Folder creation is idempotent — re-running on existing brand fills missing subdirs without overwriting

### 0.5 MongoDB Init
- `mongo/init/01_create_indexes.js` — all indexes from spec (compound, unique, TTL, sparse)
- `mongo/migrations/` — empty dir with README explaining migration convention

### 0.6 CI Skeleton
- `.github/workflows/ci.yml` — lint (ruff + mypy), test (pytest with mongo+redis services), build Docker image
- `.github/workflows/deploy.yml` — placeholder for tagged releases

**Files created:** ~25 files. App boots with `docker-compose up` returning 200 on `/health`.

---

## Phase 1 — Foundation (Weeks 1-6)

### Week 1: Core Infrastructure

**1.1 Config & Database**
- `app/core/config.py` — Pydantic `Settings` class: MONGODB_URI, REDIS_URL, JWT keys, API secrets, all from env
- `app/core/database.py` — Motor async client, `get_database()` dependency, connection pool config
- `app/core/redis.py` — aioredis client, connection factory

**1.2 Structured Logging + Correlation IDs** *(Gap fix)*
- `app/core/logging.py` — JSON structured logger (structlog or python-json-logger)
- `app/middleware/correlation.py` — middleware that generates/extracts `X-Correlation-ID`, attaches to all log entries
- Every log line includes: timestamp, correlation_id, user_id, brand_id, level, message

**1.3 Error Taxonomy** *(Gap fix)*
- `app/core/exceptions.py` — custom exception hierarchy: `AppError`, `NotFoundError`, `ForbiddenError`, `ValidationError`, `RateLimitError`, `ExternalServiceError`
- `app/core/error_handlers.py` — FastAPI exception handlers returning consistent JSON: `{ error: { code, message, details } }`

**1.4 Auth System**
- `app/core/security.py`:
  - JWT RS256: sign/verify with PEM keys from config
  - Password hashing: bcrypt via passlib
  - API key generation: `bbads_` + 32 random bytes (base58), SHA-256 hash storage
  - Token models: AccessToken (15min), RefreshToken (7d with jti)
- `app/api/v1/routers/auth.py`:
  - `POST /auth/token` — login, returns access+refresh tokens. **Rate limited: 5 attempts/min per IP** *(Gap fix)*
  - `POST /auth/refresh` — rotate refresh token, blocklist old jti in Redis
  - `POST /auth/logout` — blocklist access+refresh jti
  - `POST /auth/api-keys` — create/list/revoke API keys
- `app/middleware/auth.py` — dependency that extracts user from JWT or API key, injects into request state

**1.5 RBAC Middleware**
- `app/core/permissions.py` — role hierarchy (super_admin > admin > analyst > viewer), permission checker decorator
- `app/middleware/brand_scope.py` — validates user has access to requested brand_id; 403 if not in `allowed_brands`

**1.6 CORS** *(Gap fix)*
- Configure in `app/main.py` — explicit allowed origins from config (not `*`), credentials support

### Week 2: Domain Models + CRUD + Brand Onboarding

**2.1 BrandScopedRepository** *(Critical file)*
- `app/repositories/base.py`:
  - Constructor takes `collection` + `brand_id` (always from JWT)
  - All methods (`find`, `find_one`, `insert`, `update`, `delete`, `aggregate`) automatically inject `brand_id`
  - **No raw collection access ever escapes this class**
  - Unit tests: verify brand_id injection on every method, verify cross-tenant query impossible

**2.2 Domain Repositories**
- `app/repositories/brands.py` — CRUD, slug uniqueness
- `app/repositories/users.py` — CRUD, email uniqueness, API key lookup
- `app/repositories/campaigns.py` — CRUD, external_id upsert
- `app/repositories/performance.py` — query by date range, campaign, source
- `app/repositories/rollups.py` — upsert by (brand, period_type, period_start, source)

**2.3 CRUD Routers**
- `app/api/v1/routers/brands.py` — `GET/POST /brands`, `GET/PATCH /brands/{id}`
  - `POST /brands` now also triggers: folder creation via `brand_storage.create_brand_folders()` + ClickUp task creation
- `app/api/v1/routers/campaigns.py` — `GET /brands/{id}/campaigns`, `PATCH /brands/{id}/campaigns/{cid}`
- `app/api/v1/routers/admin.py` — `GET/POST/PATCH /admin/users`, `GET /admin/health`, `GET /admin/metrics`
- Pydantic request/response models in `app/api/v1/schemas/` (one file per domain)

**2.4 ClickUp Integration for Onboarding**
- `app/services/clickup.py`:
  - `CLICKUP_API_TOKEN` + `CLICKUP_LIST_ID` (onboarding list) from env/config
  - `create_onboarding_task(brand)` — creates a ClickUp task with checklist:
    - [ ] Link Google Ads account
    - [ ] Link Meta Ads account
    - [ ] Link Interakt account
    - [ ] Upload initial CSV data
    - [ ] Set KPI targets (ROAS, CPL)
    - [ ] Configure budget alert thresholds
    - [ ] Verify first ingestion run
    - [ ] Review initial anomaly baselines
  - `update_task_status(task_id, status)` — move task through: "New" → "In Progress" → "Live"
  - `sync_brand_status(brand_id)` — pull ClickUp task status, update `brands.onboarding_status` in MongoDB
- `app/api/v1/routers/brands.py` additions:
  - `POST /brands/{id}/onboard` — full onboarding flow: create folders + ClickUp task + set `onboarding_status: "in_progress"`
  - `GET /brands/{id}/onboarding-status` — returns ClickUp task status + checklist progress
  - `POST /brands/{id}/onboard/complete` — mark onboarding done, activate brand for ingestion
- **ClickUp webhook receiver** (optional, Phase 2):
  - `POST /webhooks/clickup` — receives task status changes, auto-updates brand status in MongoDB

**2.5 MongoDB `brands` collection update**
- Add fields to `brands` schema:
  - `onboarding_status`: `pending | in_progress | completed | failed`
  - `clickup_task_id`: string (ClickUp task reference)
  - `storage_path`: string (absolute path to brand's folder root)
  - `onboarded_at`: datetime
  - `onboarded_by`: ObjectId (user who completed onboarding)

### Weeks 3-4: Ingestion Pipeline

**3.1 Base Ingestion Service** *(Critical file)*
- `app/services/ingestion/base.py`:
  - Template method pattern: `fetch() → transform() → upsert()`
  - Idempotent upsert via natural key `(brand_id, source, campaign_id, date)`
  - Run ID (UUID) tagged on every written record
  - Correction window: always pulls D-1 + D-0
  - Writes to `ingestion_logs` collection (running → success/partial/failed)
  - Failure isolation: each brand x source is independent

**3.2 Google Ads Connector**
- `app/services/ingestion/google_ads.py`:
  - Uses `google-ads` SDK v24+ with GAQL queries
  - OAuth2 refresh token per brand (AES-256-GCM encrypted in MongoDB)
  - Pulls: impressions, clicks, cost_micros (→ paise), conversions, conversion_value
  - Rate limit handling: batch brands with 1s delay, exponential backoff on 429
  - Maps `cost_micros` → INR paise correctly

**3.3 Meta Ads Connector**
- `app/services/ingestion/meta_ads.py`:
  - Uses `facebook-business` SDK v20+
  - System User Token (permanent) preferred; fallback to 60-day token with expiry alert
  - Pulls: impressions, clicks, spend, leads, actions (7d_click attribution)
  - Rate limit: parse `X-Business-Use-Case-Usage` header, throttle at 75%
  - Currency handling: Meta returns in account currency — convert to INR paise if needed

**3.4 CSV Upload**
- `app/services/ingestion/csv_upload.py`:
  - Pydantic validation per row
  - Column name normalization (case-insensitive, fuzzy match for common variants)
  - Date format detection (DD-MM-YYYY, YYYY-MM-DD, ISO 8601)
  - Atomic: entire file succeeds or entire file fails (rollback by `ingestion_run_id`)
  - Currency: accept INR (rupees) and convert to paise; reject mixed currencies
- `app/api/v1/routers/ingestion.py`:
  - `POST /ingest/trigger` — trigger manual ingestion for brand x source
  - `POST /ingest/backfill` — backfill date range
  - `POST /ingest/csv/upload` — file upload endpoint
  - `GET /ingest/status` — recent ingestion runs
  - `GET /ingest/csv/template` — download CSV template

**3.5 External API Mocks for Local Dev** *(Gap fix)*
- `tests/mocks/google_ads.py` — returns realistic fixture data
- `tests/mocks/meta_ads.py` — returns realistic fixture data
- `tests/conftest.py` — pytest fixtures that swap real clients for mocks via env var

**3.6 APScheduler Setup**
- `app/worker/scheduler.py` *(Critical file)*:
  - AsyncIOScheduler with jobs from spec (Section 5.3)
  - Job deduplication via Redis lock (prevent double-run if worker restarts)
  - Misfire grace time: 1 hour
- `app/worker/tasks.py` — task functions: `daily_ingestion`, `rollup_computation`, `anomaly_detection`, `scheduled_reports`, `ingestion_health_check`, `token_refresh_google`, `meta_token_expiry_check`

### Week 5: Performance API + Caching

**5.1 Performance Endpoints**
- `app/api/v1/routers/performance.py`:
  - `GET /brands/{id}/performance/daily` — raw daily data with filters (date range, source, campaign)
  - `GET /brands/{id}/performance/rollup` — pre-computed rollups
  - `GET /brands/{id}/performance/summary` — KPI summary card (spend, ROAS, CPL, CTR)
  - `GET /brands/{id}/performance/top-campaigns` — top N by metric
  - `GET /brands/{id}/performance/trend` — time series for charting
  - `GET /brands/{id}/performance/attribution` — source breakdown

**5.2 Aggregation Pipelines**
- `app/services/rollup.py` — daily/weekly/monthly rollup computation, writes to `performance_rollups`
- `app/repositories/performance.py` — MongoDB aggregation pipelines for summary, top-campaigns, trend

**5.3 Redis Caching** *(Gap fix)*
- `app/core/cache.py`:
  - Decorator `@cached(ttl=3600, key_prefix="perf")` for performance endpoints
  - Cache key: `brand_id:endpoint:params_hash`
  - Invalidation: after ingestion completes for a brand, delete `perf:{brand_id}:*`
  - TTL: 1h for summaries, 15min for daily data

### Week 6: Infrastructure Hardening

**6.1 Nginx Production Config**
- `nginx/nginx.conf` — SSL termination (Let's Encrypt), HSTS, rate limiting, streaming proxy for Claude SSE
- `nginx/conf.d/api.conf` — upstream health checks, proxy headers

**6.2 Health Check**
- `GET /health` (public, no auth) — returns `{ status, mongodb.latency_ms, redis.status, last_ingestion.hours_since }`
- Logic: `degraded` if last ingestion >26h, `down` if MongoDB/Redis unreachable

**6.3 Seed Data Script** *(Gap fix)*
- `scripts/seed_data.py` — creates test agency, 3 brands, users (one per role), sample campaigns, 30 days of performance data
- `scripts/reset_db.py` — drops all collections, re-runs init indexes, re-seeds

**6.5 Existing Client File Migration**
Migrate existing brand files (CSVs, reports, credentials, ad account exports) into the new standardized folder structure.

- `scripts/migrate_existing_files.py`:
  1. **Audit phase** — scan source directory, list all files per client, output `migration_manifest.json`:
     ```json
     { "brand_slug": "acme-corp", "files": [
       { "source": "/old/acme/google_ads_march.csv", "dest": "csv-uploads/2026/03/", "type": "csv" },
       { "source": "/old/acme/report_q1.pdf", "dest": "reports/ad-hoc/", "type": "report" }
     ]}
     ```
  2. **Dry-run mode** (`--dry-run`) — prints what would be copied, catches conflicts (name collisions, unknown file types)
  3. **Execute mode** — copies files to new structure, preserves originals (no delete), logs every move
  4. **Verify** — checksums (SHA-256) on source vs dest, report any mismatches
  5. **Cleanup** — after verification, optionally archive source dir to `_migrated_archive/`

- `scripts/audit_existing_clients.py`:
  - Lists all active clients from a source (CSV list, ClickUp, or manual input)
  - Cross-references with `brands` collection in MongoDB
  - Outputs: brands with folders vs missing, brands in DB vs not, orphan folders

- **Migration SOP** (`docs/migration_sop.md`):
  1. Run `audit_existing_clients.py` — get full client inventory
  2. Create brands in system via `POST /brands` (auto-creates folders + ClickUp tasks)
  3. Run `migrate_existing_files.py --dry-run` — review manifest
  4. Run `migrate_existing_files.py --execute` — copy files
  5. Run `migrate_existing_files.py --verify` — checksum validation
  6. Mark onboarding complete in ClickUp per brand
  7. Archive old file structure

**6.4 Integration Tests**
- `tests/integration/test_auth_flow.py` — full login → token → refresh → logout flow
- `tests/integration/test_brand_isolation.py` — brand A token cannot access brand B data
- `tests/integration/test_ingestion.py` — mock connector → verify upsert idempotency
- `tests/integration/test_performance.py` — seed data → query → verify aggregation math

---

## Phase 2 — Intelligence Layer (Weeks 7-10)

### Week 7: Claude Integration

**7.1 Claude Chat** *(Critical file)*
- `app/api/v1/routers/claude.py`:
  - `POST /claude/chat` — streaming SSE response
  - `POST /claude/recommendations` — budget recommendations
  - `POST /claude/predictions` — multi-step prediction chain
  - `GET /claude/conversations` — conversation history
  - `DELETE /claude/conversations/{id}` — delete conversation
- `app/services/claude/`:
  - `system_prompt.py` — dynamic system prompt builder (role + brand context + KPI targets + rules)
  - `tool_definitions.py` — 9 tool schemas (get_performance_summary, get_campaign_performance, etc.)
  - `tool_dispatcher.py` — routes tool calls to typed repository methods; **brand_id always from JWT**
  - `prediction_chain.py` — auto-gather context → structured prediction output
- `app/repositories/conversations.py` — CRUD for claude_conversations collection

### Week 8: Anomaly Detection + Interakt

**8.1 Anomaly Detection**
- `app/services/anomaly_detection.py`:
  - Compare 7-day rolling average vs 28-day baseline
  - Metrics: ROAS, CPL, CTR, spend
  - Severity: deviation thresholds (low: 15%, medium: 30%, high: 50%, critical: 75%)
  - Writes to `anomalies` collection with `claude_summary` (Claude generates explanation)
- `app/api/v1/routers/anomalies.py`:
  - `GET /brands/{id}/anomalies` — list with severity/date filters
  - `PATCH /brands/{id}/anomalies/{aid}/acknowledge` — mark acknowledged

**8.2 Interakt Connector**
- `app/services/ingestion/interakt.py`:
  - REST client for `/api/v1/campaign/analytics`
  - Pulls: sent, delivered, read, clicked, opted_out, leads
  - Maps to `ad_performance_raw` schema (opted_out → platform_data)

### Week 9: Scheduled Reports

**9.1 Report System**
- `app/api/v1/routers/reports.py`:
  - CRUD for `scheduled_reports`
  - `POST /brands/{id}/reports/scheduled/{rid}/run` — trigger manual run
- `app/services/report_generator.py`:
  - Builds report data from rollups + anomalies
  - Uses Claude to generate narrative summary
  - Sends via SendGrid (HTML email template)
- `app/config/indian_holidays.json` — festivals/seasons for prediction context

### Week 10: Observability

**10.1 Monitoring**
- `app/middleware/metrics.py` — request count, latency histogram, error rate (Prometheus format via `/admin/metrics`)
- Telegram alert bot for: ingestion failures, stale data, token expiry
- Uptime Robot setup on `/health`

**10.2 Backup**
- `scripts/backup.sh` — `mongodump --gzip` → `rclone sync` to Backblaze B2
- Cron: daily 04:00 IST, retention: 30d daily, 12w weekly, 12m monthly
- `scripts/verify_backup.sh` — restore to temp DB, count documents, compare

---

## Phase 3 — Scale + Resilience (Weeks 11-14)

### Week 11-12: Performance at Scale
- Parallel ingestion: `asyncio.gather` across brands (configurable concurrency limit)
- Connection pool tuning: MongoDB maxPoolSize per replica
- Portfolio endpoints: `GET /portfolio/summary`, `GET /portfolio/brands/compare`
- Brand onboarding API: `POST /brands/onboard` (creates brand + folders + ClickUp task + links ad accounts)
- ClickUp webhook receiver: `POST /webhooks/clickup` — auto-sync onboarding status
- Bulk onboarding: `POST /brands/onboard/bulk` — onboard multiple brands from CSV/ClickUp list

### Week 13: Resilience
- MongoDB replica set: primary (VPS) + secondary + arbiter
- Blue/green deployment scripts
- Load testing with Locust: 50 brands, 10 concurrent users, verify <1s API latency

### Week 14: Compliance + Polish
- `POST /admin/data-export/{brand_id}` — DPDP data portability *(Gap fix)*
- `DELETE /admin/data-delete/{brand_id}` — DPDP right to delete *(Gap fix)*
- Data retention enforcement job: auto-archive data older than policy
- API versioning strategy documented
- Deployment runbook finalized

---

## What's Missing From Your Spec (Gaps to Address)

### Address in Phase 1 (Must-have for MVP)
| Gap | Fix | Where |
|-----|-----|-------|
| No rate limiting on `/auth/token` | 5 attempts/min per IP via Redis counter | Week 1 |
| No CORS policy | Explicit allowed origins in config | Week 1 |
| No structured logging | JSON logs + correlation IDs on every request | Week 1 |
| No error taxonomy | Consistent `{ error: { code, message } }` responses | Week 1 |
| No `.env.example` | Document all required env vars | Phase 0 |
| No test seed data | `scripts/seed_data.py` with realistic test data | Week 6 |
| No API mocks for local dev | Mock clients for Google/Meta/Interakt | Weeks 3-4 |
| No Redis caching | Cache performance queries, invalidate on ingestion | Week 5 |
| No brand asset storage | Standardized folder template per brand, auto-created on onboarding | Phase 0 + Week 2 |
| No onboarding automation | ClickUp integration with checklist tasks + status sync | Week 2 |
| No existing file migration | Audit + migrate + verify scripts with dry-run + checksum | Week 6 |

### Address in Phase 2 (Important)
| Gap | Fix | Where |
|-----|-----|-------|
| No request tracing | Correlation ID middleware + OpenTelemetry-ready | Week 1 (basic), Week 10 (full) |
| No budget pacing | Budget utilization endpoint + daily pacing alerts | Week 8 |
| No holiday calendar | Static JSON config for Indian festivals/seasons | Week 9 |

### Defer to Phase 3+ (Nice-to-have)
| Gap | Notes |
|-----|-------|
| Creative performance tracking | Needs `creative_id` field + new collection; significant schema change |
| Audience segment tracking | Needs platform breakdown data; depends on API capabilities |
| A/B test tracking | Agency can use platform-native A/B tools initially |
| Cross-platform dedup | Complex attribution problem; needs UTM stitching infrastructure |
| Multi-touch attribution | Requires click-stream data not available from current sources |
| White-label reporting | UI concern; not needed for API-first MVP |
| DPDP full compliance | Data export/delete endpoints in Week 14; full DPA is a legal task |
| Competitive intelligence | External data source; out of MVP scope |

### Not Needed for This System
| Suggestion | Why Skip |
|------------|----------|
| Sharding | 50 brands x 365 days x 100 campaigns = ~1.8M docs/year = <2GB. Single node handles this for years |
| Audience overlap detection | Requires platform API access to audience definitions; not available via standard APIs |
| Invoice reconciliation | Finance team concern, not analytics platform scope |

---

## Testing Strategy

| Layer | Tool | Coverage Target |
|-------|------|-----------------|
| Unit | pytest | 80%+ overall, **100% on BrandScopedRepository and security** |
| Integration | pytest + testcontainers (MongoDB + Redis) | Auth flow, brand isolation, ingestion idempotency |
| Load | Locust (Phase 3) | 50 brands, 10 concurrent users, <1s p95 |
| Security | Manual checklist | JWT forgery, API key brute force, tenant isolation bypass |

## Verification Plan

After each phase, verify:
1. `docker-compose up` — all services healthy
2. `pytest` — all tests pass
3. `ruff check .` + `mypy .` — no lint/type errors
4. Phase 1: seed data → hit every CRUD endpoint → verify correct responses
5. Phase 1: create brand via API → verify folder tree created + ClickUp task exists + onboarding status tracked
6. Phase 1: run `migrate_existing_files.py --dry-run` on test data → verify manifest is correct → run `--execute` → verify checksums
7. Phase 2: trigger ingestion → verify rollups → ask Claude a question → verify answer cites real data
8. Phase 3: run Locust with 50 brands → verify p95 <1s, no tenant leaks
