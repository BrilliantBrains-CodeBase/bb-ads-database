**Agency Analytics Platform**

Implementation Plan

Version 1.0  ·  Confidential

| Platform | FastAPI \+ MongoDB \+ Docker |  |
| :---- | :---- | :---- |
| **Scope** | Single agency · 20–50+ brands |  |
| **Timeline** | 14 weeks (3 phases) |  |
| **Sources** | Google Ads · Meta · Interakt · CSV |  |

# **1\. Context & Confirmed Decisions**

Greenfield build of a multi-tenant ad performance platform for a single digital agency managing 20+ brands. The system ingests daily ad data from Google Ads, Meta Ads, Interakt (WhatsApp), and manual CSV uploads, stores it in MongoDB, exposes a FastAPI REST API, and layers a full Claude-powered analytics suite.

| Decision | Confirmed Choice |
| :---- | :---- |
| Multi-tenancy | Single agency \+ 20+ brands; 50+ in 6 months; brand\_id discriminator on every document |
| WhatsApp | Via Interakt (separate provider, not Click-to-WhatsApp Ads) |
| Ingestion | Daily batch at 06:00 IST; no real-time requirement |
| Currency | India-only: all amounts in INR paise (Int64); IST timezone |
| Attribution | Meta CAPI · Google Tag/GA4 · Manual CSV (CRM deferred) |
| Infrastructure | MongoDB self-hosted on Docker · FastAPI (Python) · Hetzner/DigitalOcean VPS |
| MVP Scope | API-first (no UI in MVP); 3+ month runway for full buildout |
| Claude Features | Chat with data · Scheduled reports · Budget recommendations · Anomaly alerts · Predictive recommendations |

# **2\. Architecture Decisions**

| Decision | Choice | Rationale |
| :---- | :---- | :---- |
| Framework | FastAPI \+ Motor (async) | Async-native, Pydantic validation, auto OpenAPI docs |
| Auth | JWT RS256 \+ API keys | Asymmetric JWT; API keys for Claude service account |
| Multi-tenancy | brand\_id field \+ BrandScopedRepository | Simpler than DB-per-tenant; single isolation class prevents accidental cross-tenant leaks |
| Amounts | Stored as paise (Int64) | Eliminates float precision bugs; divide by 100 for display |
| Ingestion | Pull-based cron; idempotent upsert via (brand\_id, source, campaign\_id, date) | Safe re-runs and backfill without duplicates |
| Correction window | Pull D-1 \+ D-0 on every daily run | Handles late-arriving conversions (platforms post corrections up to 24h later) |
| Aggregation | Pre-computed daily/weekly/monthly rollup docs \+ on-demand pipeline for ad-hoc | O(1) dashboard queries; aggregation pipeline only for drill-down |
| Secret management | Doppler (free tier) → env injection at container start | No secrets in code or images; rotation without redeploy |
| Claude integration | Tool-calling pattern; Claude calls typed FastAPI endpoints; never writes data | Read-only analyst; brand scope from JWT not user input |

# **3\. MongoDB Collections**

*All collections carry brand\_id: ObjectId as first field. BrandScopedRepository always injects it. Every compound index starts with brand\_id.*

## **3.1  agencies  (singleton per deployment)**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · name · slug · created\_at · settings { fiscal\_year\_start\_month, default\_timezone, default\_currency } |
| Index | { slug: 1 } unique |

## **3.2  brands**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · agency\_id · name · slug · industry · is\_active · created\_at · created\_by · settings { target\_roas, target\_cpl, budget\_alert\_threshold, anomaly\_sensitivity } |
| Indexes | { agency\_id, slug } unique   ·   { agency\_id, is\_active } |

## **3.3  users**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · agency\_id · email · hashed\_password (bcrypt) · role · allowed\_brands\[\] · is\_active · api\_keys \[{ key\_hash(SHA-256), name, scopes\[\], created\_at, last\_used }\] |
| Roles | super\_admin  |  admin  |  analyst  |  viewer |
| Indexes | { email } unique   ·   { agency\_id, role }   ·   { api\_keys.key\_hash } sparse |

## **3.4  campaigns  (master catalog across all platforms)**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · source (google\_ads|meta|interakt|manual) · external\_id · name · objective · platform\_status · our\_status (active|paused|archived) · created\_by · created\_at · start\_date · end\_date · budget\_type (daily|lifetime) · budget\_paise · labels\[\] · meta{} |
| Indexes | { brand\_id, source, external\_id } unique   ·   { brand\_id, our\_status }   ·   { brand\_id, created\_by }   ·   { brand\_id, start\_date, end\_date } |

## **3.5  ad\_performance\_raw  (hot collection — one doc per brand × source × campaign × date)**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · campaign\_id · source · date (IST midnight as UTC) · ingested\_at · ingestion\_run\_id · spend\_paise · impressions · clicks · reach · frequency · conversions · conversion\_value\_paise · leads · ctr · cpc\_paise · cpm\_paise · cpl\_paise · roas (stored at ingestion for query perf) · platform\_data{} |
| Index 1 | { brand\_id, date:-1, source }   — primary query pattern |
| Index 2 | { brand\_id, campaign\_id, date:-1 }   — drill-down |
| Index 3 | { brand\_id, date:-1 } partial   — date-range aggregations |
| Index 4 | { ingestion\_run\_id }   — tracing / rollback |

## **3.6  performance\_rollups  (pre-computed)**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · period\_type (daily|weekly|monthly) · period\_start · period\_end · source (all|…) · total\_spend\_paise · total\_impressions · total\_clicks · total\_leads · total\_conversions · avg\_roas · avg\_cpl\_paise · avg\_ctr · top\_campaigns\_by\_roas\[\] · top\_campaigns\_by\_spend\[\] · budget\_utilization · computed\_at · is\_partial |
| Indexes | { brand\_id, period\_type, period\_start, source } unique   ·   { brand\_id, period\_type, period\_start:-1 } |

## **3.7  ingestion\_logs**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · run\_id (UUID) · brand\_id · source · target\_date · status (running|success|partial|failed) · started\_at · completed\_at · records\_fetched · records\_upserted · error\_message · retry\_count · is\_backfill |
| Indexes | { brand\_id, source, target\_date }   ·   { status, started\_at:-1 }   ·   TTL 90 days |

## **3.8  anomalies**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · campaign\_id · detected\_at · date · metric (roas|cpl|ctr|spend) · current\_value · baseline\_value · deviation\_pct · direction (spike|drop) · severity (low|medium|high|critical) · acknowledged · acknowledged\_by · claude\_summary |
| Indexes | { brand\_id, detected\_at:-1 }   ·   { brand\_id, acknowledged, severity }   ·   TTL 180 days |

## **3.9  claude\_conversations**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · user\_id · created\_at · updated\_at · title · messages \[{ role, content, tool\_calls\[\], tool\_results\[\], timestamp }\] |
| Indexes | { brand\_id, user\_id, updated\_at:-1 }   ·   TTL 365 days |

## **3.10  scheduled\_reports**

| Field / Index | Detail |
| :---- | :---- |
| Fields | \_id · brand\_id · created\_by · name · schedule (weekly\_monday|monthly\_1st|…) · report\_type · recipients\[\] · last\_run · next\_run · is\_active · config{} |
| Indexes | { brand\_id, is\_active }   ·   { next\_run, is\_active }   — scheduler query |

# **4\. FastAPI Router Structure**

## **4.1  Project Layout**

/app

  /api/v1/routers/

    auth.py           \# POST /auth/token, /refresh, /logout, /api-keys

    brands.py         \# CRUD /brands

    campaigns.py      \# GET/PATCH /brands/{id}/campaigns

    performance.py    \# GET /brands/{id}/performance/daily|rollup|summary|trend|top-campaigns

    ingestion.py      \# POST /ingest/trigger|backfill|csv/upload, GET /ingest/status

    anomalies.py      \# GET/PATCH /brands/{id}/anomalies

    reports.py        \# CRUD /brands/{id}/reports/scheduled

    claude.py         \# POST /claude/chat|recommendations|predictions, GET /claude/conversations

    admin.py          \# User management \+ system health (super\_admin only)

  /core/              \# config.py · security.py · dependencies.py

  /repositories/      \# base.py (BrandScopedRepository) · campaigns · performance · rollups · anomalies

  /services/          \# ingestion/ · rollup · anomaly\_detection · report\_generator

  /worker/            \# scheduler.py (APScheduler) · tasks.py

  main.py

## **4.2  Key Endpoints**

| Router | Endpoints |
| :---- | :---- |
| Auth | POST /auth/token  ·  /auth/refresh  ·  /auth/api-keys |
| Brands | GET/POST /brands  ·  GET/PATCH /brands/{id} |
| Campaigns | GET /brands/{id}/campaigns  ·  PATCH /brands/{id}/campaigns/{cid} |
| Performance | GET /brands/{id}/performance/daily|rollup|summary|top-campaigns|trend|attribution |
| Ingestion | POST /ingest/trigger · /ingest/backfill · /ingest/csv/upload   GET /ingest/status · /ingest/csv/template |
| Anomalies | GET /brands/{id}/anomalies  ·  PATCH /{id}/acknowledge |
| Reports | CRUD /brands/{id}/reports/scheduled  ·  POST /{id}/run |
| Claude | POST /claude/chat · /claude/recommendations · /claude/predictions   GET /claude/conversations  ·  DELETE /claude/conversations/{id} |
| Admin | GET/POST/PATCH /admin/users  ·  GET /admin/health  ·  /admin/metrics |

# **5\. Ingestion Pipeline**

## **5.1  Core Principles**

* Idempotent upsert via natural key (brand\_id, source, campaign\_id, date) — safe to re-run

* Correction window — every daily run pulls D-1 AND D-0 (handles platform late-corrections)

* Run ID tracing — UUID per run on all written records; rollback \= delete by ingestion\_run\_id

* Failure isolation — each brand × source is an independent job; one failure doesn't block others

## **5.2  Connector Matrix**

| Source | SDK / Method | Key Metrics | Auth | Rate Limit |
| :---- | :---- | :---- | :---- | :---- |
| Google Ads | google-ads v24+ (GAQL) | impressions, clicks, cost\_micros, conversions, conversions\_value | OAuth2 refresh token per brand (AES-256-GCM encrypted) | 15K req/day; batch brands ×10 with 1s delay |
| Meta Ads | facebook-business v20+ | impressions, clicks, spend, leads — 7d\_click ROAS window | System User Token (permanent preferred) | Parse X-Business-Use-Case-Usage; throttle at 75% |
| Interakt (WA) | REST /api/v1/campaign/analytics | sent, delivered, read, clicked, opted\_out, leads | API Key in Authorization header (encrypted per brand) | Standard REST; no special limits documented |
| CSV Upload | Pydantic validation \+ fuzzy matching | date, campaign\_name, spend\_inr, impressions, clicks, leads, conversions, conversion\_value\_inr | Bearer JWT; analyst role minimum | Atomic: entire file succeeds or fails |

## **5.3  Scheduler Jobs  (APScheduler AsyncIOScheduler)**

| UTC Time | Job | Description |
| :---- | :---- | :---- |
| 00:30 UTC (06:00 IST) | daily\_ingestion | Pulls D-1 and D-0 for all brands |
| 01:30 UTC | rollup\_computation | Daily rollup; Monday: weekly; 1st of month: monthly |
| 02:00 UTC | anomaly\_detection | Compares 7-day rolling avg vs 28-day baseline |
| 03:00 UTC | scheduled\_reports | Sends reports where next\_run ≤ now |
| 08:00 UTC (13:30 IST) | ingestion\_health\_check | Alert if no successful run in last 26h |
| 12:00 UTC | token\_refresh\_google | Refresh Google OAuth tokens before expiry |
| 12:30 UTC | meta\_token\_expiry\_check | Alert if Meta token expires in \<7 days |

# **6\. Claude Integration Architecture**

## **6.1  Request Flow**

POST /claude/chat → Assemble system prompt (role \+ brand context \+ KPI targets \+ rules) → Claude API (claude-sonnet-4-6) with tool definitions → Tool dispatcher → typed FastAPI tool endpoints → MongoDB → Stream response back to caller → Persist conversation to claude\_conversations

## **6.2  Dynamic System Prompt Structure**

| Block | Content |
| :---- | :---- |
| ROLE | Expert digital advertising analyst for {agency\_name}; date, timezone, currency context |
| BRAND | Target ROAS, target CPL, current month budget, anomaly sensitivity |
| RULES | Always cite numbers · flag anomalies with ⚠ · use Indian number format · never fabricate |
| CAPABILITY | What Claude can and cannot do (read-only; all changes must be approved by team) |
| STYLE | Direct · tables for comparisons · max 600 words unless detailed report requested |

## **6.3  Tool Definitions  (9 tools)**

| Tool | Parameters |
| :---- | :---- |
| get\_performance\_summary | brand\_id · start\_date · end\_date · source |
| get\_campaign\_performance | brand\_id · campaign\_ids\[\] · start\_date · end\_date · sort\_by · limit |
| get\_trend\_analysis | brand\_id · metric · start\_date · end\_date · granularity · source |
| get\_anomalies | brand\_id · days\_back · severity · unacknowledged\_only |
| get\_budget\_utilization | brand\_id · month |
| compare\_periods | brand\_id · period\_a\_start · period\_a\_end · period\_b\_start · period\_b\_end |
| get\_top\_performers | brand\_id · metric · start\_date · end\_date · limit · min\_spend\_paise |
| get\_attribution\_breakdown | brand\_id · start\_date · end\_date |
| get\_brand\_list | active\_only |

**Security: brand\_id always extracted from JWT claims, never from user message input.**

## **6.4  Prediction Prompt Chain  (POST /claude/predictions)**

### **Step 1 — Auto-gather context (no user input needed)**

* get\_performance\_summary — last 90 days

* get\_trend\_analysis(roas) \+ get\_trend\_analysis(cpl) — last 60 days weekly

* get\_top\_performers(roas) — last 30 days

* get\_budget\_utilization — current month

* get\_anomalies(days\_back=30)

### **Step 2 — Structured prediction output (JSON)**

* Budget reallocation recommendations

* Next period predictions — low / mid / high scenarios

* Recommended actions (max 5, prioritised P1–P3 with data rationale)

* Risk flags

*Seasonal context injected from a static Indian festivals/holidays config file.*

# **7\. Infrastructure & Docker Compose**

## **7.1  Service Topology**

| Service | Description |
| :---- | :---- |
| nginx | Reverse proxy, SSL termination, rate limiting (60 req/min), streaming proxy for Claude |
| api | FastAPI app — 2 replicas, healthcheck on /health |
| worker | Single instance APScheduler — all cron jobs |
| mongodb | mongo:7.0 · WiredTiger cache 2GB · volume mount for persistence |
| redis | JWT blocklist · rate limit counters · job deduplication |
| certbot | Let's Encrypt SSL — run once then cron renew |
| mongo-express | profiles: \[dev\] only — never exposed in production |

## **7.2  VPS Sizing**

| Phase | Server | Spec | \~Monthly Cost |
| :---- | :---- | :---- | :---- |
| Phase 1–2 | Hetzner CX31 | 4 vCPU · 8GB RAM · 160GB NVMe | \~$25/month |
| Phase 2+ | Hetzner CX41 or Atlas M10 | 16GB RAM | Upgrade trigger: RAM \>70% or need PITR |
| Phase 3 | Mac mini M4 Pro | Primary MongoDB node \+ Hetzner secondary/arbiter replica set | Hardware investment |

# **8\. Security Model**

## **8.1  Authentication**

* JWT RS256: access 15 min TTL · refresh 7 days stored in Redis with jti for blocklisting

* API keys: bbads\_ \+ 32 random bytes (base58) · SHA-256 hash stored · raw shown once · scoped to read:analytics

* Blocklist: Redis blocklist:{jti} with TTL matching token expiry

## **8.2  RBAC**

| Role | Permissions |
| :---- | :---- |
| super\_admin | All brands · all operations · user management |
| admin | Assigned brands · read \+ write · trigger ingestion |
| analyst | Assigned brands · read-only · Claude chat · create reports |
| viewer | Assigned brands · read performance only |

## **8.3  Tenant Isolation  (Critical Pattern)**

class BrandScopedRepository:

    def \_\_init\_\_(self, collection, brand\_id):

        self.brand\_id \= brand\_id  \# always from JWT, never from user input

    async def find(self, filter, \*\*kwargs):

        return await self.collection.find({'brand\_id': self.brand\_id, \*\*filter})

**Rule: no raw collection.find() ever appears in routers. Only BrandScopedRepository.**

## **8.4  Secrets  (Doppler)**

* MONGODB\_URI  ·  JWT\_PRIVATE\_KEY\_PEM  ·  JWT\_PUBLIC\_KEY\_PEM

* ANTHROPIC\_API\_KEY  ·  GOOGLE\_ADS\_DEVELOPER\_TOKEN  ·  META\_APP\_SECRET

* ENCRYPTION\_KEY\_HEX  ·  SENDGRID\_API\_KEY

Brand API tokens encrypted at rest: AES-256-GCM via cryptography library; key in Doppler.

# **9\. Observability & Operations**

## **9.1  Health Check  (GET /health — public, no auth)**

* Returns: { status, mongodb.latency\_ms, redis.status, last\_ingestion\_run.hours\_since\_last }

* degraded if last successful ingestion \>26h ago

* down if MongoDB or Redis unreachable

## **9.2  Alert Matrix**

| Alert | Condition | Delivery |
| :---- | :---- | :---- |
| Ingestion failed | 2 consecutive failures for any brand × source | Telegram bot |
| Ingestion stale | No success in \>26h | Telegram bot |
| Meta token expiry | Expires in \<7 days | Email to admin |
| API error rate | \>5% 5xx in 5 min | Auto-notify |
| Disk usage | \>80% | Email |

*Uptime monitoring: Uptime Robot (free, 5-minute checks on /health)*

## **9.3  Backup Strategy**

* Daily at 04:00 IST: mongodump \--gzip \--archive → rclone sync to Backblaze B2

* Retention: 30 days daily · 12 weeks weekly (Monday) · 12 months monthly

* Phase 1: Self-hosted MongoDB on VPS \+ daily backup (RTO \~4h)

* Phase 2 trigger: Migrate to Atlas M10 for automated PITR backups

# **10\. CI/CD & Deployment**

## **10.1  Pipeline**

### **Push to main → GitHub Actions CI**

* Spin up MongoDB \+ Redis services

* pytest with coverage

* ruff \+ mypy linting

* Build Docker image → push to GHCR (tagged with git SHA \+ latest)

### **Tagged release → deploy.yml**

* SSH to VPS

* docker pull new image

* Rolling restart: api replicas → health check → worker

* On failure: auto-rollback to previous image

## **10.2  MongoDB Migrations**

* No ORM. Versioned scripts in mongo/migrations/v{N}\_\*.py

* Run manually on deploy (migrations are low-frequency, warrant review)

* All indexes use background=True (no read/write blocking during build)

# **11\. Phased Execution Plan**

## **Phase 1 — Foundation  (Weeks 1–6)**

**Goal: Ingestion pipeline live \+ performant API \+ tenant isolation solid**

| Agent | Tasks |
| :---- | :---- |
| backend-architect | FastAPI project structure · auth (JWT RS256 \+ API keys) · RBAC middleware · BrandScopedRepository · brands/users/campaigns/performance routers |
| database-architect | All MongoDB collections · mongo/init/01\_create\_indexes.js · schema validation |
| data-engineer | BaseIngestionService · Google Ads connector · Meta connector · CSV upload · APScheduler |
| sql-pro | KPI summary card pipeline · top campaigns pipeline · daily rollup job |
| cloud-architect | Docker Compose stack · Nginx SSL config · VPS provisioning · Doppler setup |
| security-auditor | JWT RS256 implementation · API key flow · BrandScopedRepository enforcement · audit middleware |
| deployment-engineer | GitHub Actions CI · deploy.sh \+ rollback.sh · MongoDB init scripts |

### **Phase 1 Acceptance Criteria**

* Google Ads \+ Meta data ingests daily without intervention

* Re-running ingestion for same date \= no duplicates

* Failed ingestion logged with status=failed \+ error\_message

* Brand A JWT → Brand B endpoint returns 403

* API key with read:analytics scope → POST /ingest/trigger returns 403

* All spend values stored as paise (integers, never floats)

* HTTPS enforced · /health returns 200 all-green · MongoDB data persists container restart

## **Phase 2 — Claude \+ Anomaly \+ Reports  (Weeks 7–10)**

**Goal: Full Claude analytics layer \+ automated anomaly detection \+ scheduled reports**

| Agent | Tasks |
| :---- | :---- |
| prompt-engineer | 9 Claude tool definitions · tool dispatcher · /claude/chat with streaming \+ conversation storage · dynamic system prompt · prediction prompt chain · /recommendations \+ /predictions endpoints |
| data-engineer | Interakt connector · backfill endpoint · token auto-refresh jobs |
| sql-pro | Anomaly detection pipeline · weekly/monthly rollups · attribution breakdown · period comparison |
| observability-engineer | Prometheus metrics · Telegram alert bot · Uptime Robot · ingestion health check job (08:00 UTC) |
| database-admin | Automated mongodump \+ rclone backup · weekly backup verification |
| prompt-engineer | scheduled\_reports CRUD · report generation job · SendGrid email delivery |

### **Phase 2 Acceptance Criteria**

* "What was ROAS last week vs prior week?" returns correct numbers

* Claude tool brand\_id from JWT cannot be overridden by message content

* Anomaly with 75% ROAS drop → severity=critical in MongoDB within 2h of morning run

* Monday weekly report sends by 09:00 IST

* Prediction endpoint returns JSON with budget reallocation \+ 3-scenario forecast \+ 5 actions

* Conversation history persists across sessions

## **Phase 3 — Scale \+ Resilience \+ CRM  (Weeks 11–14+)**

**Goal: 50+ brand capacity, HA path, CRM integration**

| Agent | Tasks |
| :---- | :---- |
| database-architect | Replica set (Mac mini primary \+ Hetzner secondary) · index performance audit with $indexStats |
| data-engineer | Parallel ingestion with asyncio.gather across brands · Interakt webhook receiver · data retention enforcement |
| backend-architect | Multi-brand portfolio endpoints (agency-level aggregation) · brand onboarding API |
| cloud-architect | Mac mini deployment guide · Nginx upstream HA config |
| prompt-engineer | Multi-brand Claude context · Indian seasonal calendar enrichment · prediction fine-tuning |
| deployment-engineer | Blue/green deployment · load testing with locust (50-brand ingestion window) |

# **12\. Risk Register**

| Risk | Probability | Impact | Mitigation |
| :---- | :---- | :---- | :---- |
| Meta System User token expires | **Medium** | **P1 — ingestion stops** | check\_meta\_token\_expiry job · alert 7 days before · prefer System User over 60-day token |
| Google Ads rate limit at 50 brands | **Medium** | **Partial ingestion** | Batch with 1s delay · upgrade developer token tier if needed |
| VPS disk full | **Low** | **Service down** | Alert at 80% disk · data growth \~90MB/year at 50 brands |
| Ingestion window \>6h at scale | **Low→Med** | **Stale data** | Phase 3: asyncio.gather parallel ingestion across brands |
| CSV import corrupts clean data | **Medium** | **Data quality** | Atomic per-file \+ rollback by ingestion\_run\_id delete |
| Claude returns wrong brand data | **Low** | **Tenant breach** | brand\_id from JWT always · unit test every tool's brand isolation |
| MongoDB single point of failure | **Low** | **\~4h RTO** | Daily backup to object storage · Phase 3: replica set |
| Anthropic API down during scheduled reports | **Low** | **Report delayed** | Retry 1h backoff ×3 · mark failed, log · don't block other reports |

# **13\. Critical Files**

| File | Why It's Critical |
| :---- | :---- |
| app/repositories/base.py | BrandScopedRepository — ALL tenant isolation depends on this class |
| app/worker/scheduler.py | APScheduler — operational heartbeat of the system |
| app/services/ingestion/base.py | BaseIngestionService with idempotent upsert — all 4 connectors inherit |
| app/api/v1/routers/claude.py | Claude chat: tool dispatcher, streaming, brand-scoped context injection |
| mongo/init/01\_create\_indexes.js | Index definitions — determines query performance at 50+ brands |

