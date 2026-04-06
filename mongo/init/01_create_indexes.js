/**
 * 01_create_indexes.js
 *
 * Creates all collections, JSON Schema validators, and indexes for the
 * Agency Analytics Platform.
 *
 * Runs automatically via docker-entrypoint-initdb.d on first container start.
 * Safe to re-run: createIndex() is idempotent; createCollection() with
 * validator uses `collMod` if the collection already exists.
 *
 * MongoDB 7.0 note: index builds are always optimised internally;
 * the `background` option is accepted but ignored — kept for clarity.
 *
 * Currency: all paise fields are Int64. Dates are UTC ISODate.
 * Timezone context (IST) is handled in application layer.
 */

// Switch to the application database
// `db` is already set to MONGO_INITDB_DATABASE by the entrypoint.
// Re-assign explicitly for clarity if running manually:
// use bb_ads;

print("=== Agency Analytics Platform — MongoDB Init ===");
print("Database:", db.getName());

// ─────────────────────────────────────────────────────────────────────────────
// Helper: createOrModify — creates collection with validator, or updates
// the validator if the collection already exists (idempotent).
// ─────────────────────────────────────────────────────────────────────────────
function createOrModify(name, options) {
  const existing = db.getCollectionNames().indexOf(name) !== -1;
  if (!existing) {
    db.createCollection(name, options);
    print("  created:", name);
  } else {
    db.runCommand({ collMod: name, ...options });
    print("  updated:", name);
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. agencies  (singleton — one per deployment)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("agencies", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "slug", "created_at"],
      properties: {
        name:       { bsonType: "string" },
        slug:       { bsonType: "string" },
        created_at: { bsonType: "date" },
        settings: {
          bsonType: "object",
          properties: {
            fiscal_year_start_month: { bsonType: "int", minimum: 1, maximum: 12 },
            default_timezone:        { bsonType: "string" },
            default_currency:        { bsonType: "string", enum: ["INR"] },
          },
        },
      },
    },
  },
  validationAction: "warn",   // warn, not error — allows schema evolution without migration
});

db.agencies.createIndex(
  { slug: 1 },
  { unique: true, name: "agencies_slug_unique" }
);

print("agencies: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 2. brands
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("brands", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["agency_id", "name", "slug", "is_active", "created_at"],
      properties: {
        agency_id:   { bsonType: "objectId" },
        name:        { bsonType: "string" },
        slug:        { bsonType: "string" },
        industry:    { bsonType: "string" },
        is_active:   { bsonType: "bool" },
        created_at:  { bsonType: "date" },
        created_by:  { bsonType: "objectId" },
        // Extended fields added in plan (Phase 0.4 / ClickUp integration)
        onboarding_status: {
          bsonType: "string",
          enum: ["pending", "in_progress", "completed", "blocked"],
        },
        clickup_task_id: { bsonType: "string" },
        storage_path:    { bsonType: "string" },
        onboarded_at:    { bsonType: "date" },
        onboarded_by:    { bsonType: "objectId" },
        settings: {
          bsonType: "object",
          properties: {
            target_roas:              { bsonType: ["double", "null"] },
            target_cpl:               { bsonType: ["long", "null"] },   // INR paise
            budget_alert_threshold:   { bsonType: "double" },
            anomaly_sensitivity:      { bsonType: "string", enum: ["low", "medium", "high"] },
          },
        },
      },
    },
  },
  validationAction: "warn",
});

// Every compound index starts with brand_id / agency_id per spec rule
db.brands.createIndex(
  { agency_id: 1, slug: 1 },
  { unique: true, name: "brands_agency_slug_unique" }
);
db.brands.createIndex(
  { agency_id: 1, is_active: 1 },
  { name: "brands_agency_active" }
);

print("brands: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 3. users
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["agency_id", "email", "hashed_password", "role", "is_active", "created_at"],
      properties: {
        agency_id:       { bsonType: "objectId" },
        email:           { bsonType: "string" },
        hashed_password: { bsonType: "string" },
        role: {
          bsonType: "string",
          enum: ["super_admin", "admin", "analyst", "viewer"],
        },
        allowed_brands: { bsonType: "array", items: { bsonType: "objectId" } },
        is_active:      { bsonType: "bool" },
        created_at:     { bsonType: "date" },
        api_keys: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["key_hash", "name", "created_at"],
            properties: {
              key_hash:   { bsonType: "string" },
              name:       { bsonType: "string" },
              scopes:     { bsonType: "array", items: { bsonType: "string" } },
              created_at: { bsonType: "date" },
              last_used:  { bsonType: "date" },
            },
          },
        },
      },
    },
  },
  validationAction: "warn",
});

db.users.createIndex(
  { email: 1 },
  { unique: true, name: "users_email_unique" }
);
db.users.createIndex(
  { agency_id: 1, role: 1 },
  { name: "users_agency_role" }
);
db.users.createIndex(
  { "api_keys.key_hash": 1 },
  { sparse: true, name: "users_api_key_hash_sparse" }
  // sparse: only indexes docs that have at least one API key
);

print("users: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 4. campaigns  (master catalog across all platforms)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("campaigns", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "source", "external_id", "name", "created_at"],
      properties: {
        brand_id:        { bsonType: "objectId" },
        source: {
          bsonType: "string",
          enum: ["google_ads", "meta", "interakt", "manual"],
        },
        external_id:     { bsonType: "string" },
        name:            { bsonType: "string" },
        objective:       { bsonType: "string" },
        platform_status: { bsonType: "string" },
        our_status: {
          bsonType: "string",
          enum: ["active", "paused", "archived"],
        },
        created_by:  { bsonType: "objectId" },
        created_at:  { bsonType: "date" },
        start_date:  { bsonType: "date" },
        end_date:    { bsonType: "date" },
        budget_type: { bsonType: "string", enum: ["daily", "lifetime"] },
        budget_paise:{ bsonType: "long" },  // Int64
        labels:      { bsonType: "array", items: { bsonType: "string" } },
      },
    },
  },
  validationAction: "warn",
});

db.campaigns.createIndex(
  { brand_id: 1, source: 1, external_id: 1 },
  { unique: true, name: "campaigns_brand_source_ext_unique" }
);
db.campaigns.createIndex(
  { brand_id: 1, our_status: 1 },
  { name: "campaigns_brand_status" }
);
db.campaigns.createIndex(
  { brand_id: 1, created_by: 1 },
  { name: "campaigns_brand_creator" }
);
db.campaigns.createIndex(
  { brand_id: 1, start_date: 1, end_date: 1 },
  { name: "campaigns_brand_dates" }
);

print("campaigns: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 5. ad_performance_raw  (hot collection — one doc per brand × source × campaign × date)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("ad_performance_raw", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "campaign_id", "source", "date", "ingested_at", "ingestion_run_id"],
      properties: {
        brand_id:          { bsonType: "objectId" },
        campaign_id:       { bsonType: "objectId" },
        source:            { bsonType: "string", enum: ["google_ads", "meta", "interakt", "manual"] },
        date:              { bsonType: "date" },   // IST midnight stored as UTC
        ingested_at:       { bsonType: "date" },
        ingestion_run_id:  { bsonType: "string" }, // UUID
        // All monetary fields in INR paise (Int64)
        spend_paise:            { bsonType: "long" },
        impressions:            { bsonType: "long" },
        clicks:                 { bsonType: "long" },
        reach:                  { bsonType: "long" },
        frequency:              { bsonType: "double" },
        conversions:            { bsonType: "long" },
        conversion_value_paise: { bsonType: "long" },
        leads:                  { bsonType: "long" },
        ctr:                    { bsonType: "double" },
        cpc_paise:              { bsonType: "long" },
        cpm_paise:              { bsonType: "long" },
        cpl_paise:              { bsonType: "long" },
        roas:                   { bsonType: "double" }, // stored at ingestion for fast queries
      },
    },
  },
  validationAction: "warn",
});

// Unique natural key — enables idempotent upsert
db.ad_performance_raw.createIndex(
  { brand_id: 1, source: 1, campaign_id: 1, date: 1 },
  { unique: true, name: "perf_raw_natural_key_unique" }
);
// Primary dashboard query: filter by brand + date range, group by source
db.ad_performance_raw.createIndex(
  { brand_id: 1, date: -1, source: 1 },
  { name: "perf_raw_brand_date_source" }
);
// Campaign drill-down: filter by brand + campaign, sort by date
db.ad_performance_raw.createIndex(
  { brand_id: 1, campaign_id: 1, date: -1 },
  { name: "perf_raw_brand_campaign_date" }
);
// Date-range aggregations (partial: only docs with spend > 0 to skip zero-spend rows)
db.ad_performance_raw.createIndex(
  { brand_id: 1, date: -1 },
  {
    name: "perf_raw_brand_date_partial",
    partialFilterExpression: { spend_paise: { $gt: 0 } },
  }
);
// Tracing and rollback: delete all records from a bad ingestion run
db.ad_performance_raw.createIndex(
  { ingestion_run_id: 1 },
  { name: "perf_raw_run_id" }
);

print("ad_performance_raw: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 6. performance_rollups  (pre-computed; O(1) dashboard queries)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("performance_rollups", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "period_type", "period_start", "period_end", "source", "computed_at"],
      properties: {
        brand_id:     { bsonType: "objectId" },
        period_type:  { bsonType: "string", enum: ["daily", "weekly", "monthly"] },
        period_start: { bsonType: "date" },
        period_end:   { bsonType: "date" },
        source:       { bsonType: "string" },  // "all" or specific source
        total_spend_paise:       { bsonType: "long" },
        total_impressions:       { bsonType: "long" },
        total_clicks:            { bsonType: "long" },
        total_leads:             { bsonType: "long" },
        total_conversions:       { bsonType: "long" },
        avg_roas:                { bsonType: "double" },
        avg_cpl_paise:           { bsonType: "long" },
        avg_ctr:                 { bsonType: "double" },
        top_campaigns_by_roas:   { bsonType: "array" },
        top_campaigns_by_spend:  { bsonType: "array" },
        budget_utilization:      { bsonType: "double" },
        computed_at:             { bsonType: "date" },
        is_partial:              { bsonType: "bool" },
      },
    },
  },
  validationAction: "warn",
});

db.performance_rollups.createIndex(
  { brand_id: 1, period_type: 1, period_start: 1, source: 1 },
  { unique: true, name: "rollups_period_unique" }
);
db.performance_rollups.createIndex(
  { brand_id: 1, period_type: 1, period_start: -1 },
  { name: "rollups_brand_period_date" }
);

print("performance_rollups: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 7. ingestion_logs  (TTL: 90 days)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("ingestion_logs", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["run_id", "brand_id", "source", "target_date", "status", "started_at"],
      properties: {
        run_id:      { bsonType: "string" },  // UUID
        brand_id:    { bsonType: "objectId" },
        source:      { bsonType: "string", enum: ["google_ads", "meta", "interakt", "manual", "all"] },
        target_date: { bsonType: "date" },
        status: {
          bsonType: "string",
          enum: ["running", "success", "partial", "failed"],
        },
        started_at:       { bsonType: "date" },
        completed_at:     { bsonType: "date" },
        records_fetched:  { bsonType: "long" },
        records_upserted: { bsonType: "long" },
        error_message:    { bsonType: "string" },
        retry_count:      { bsonType: "int" },
        is_backfill:      { bsonType: "bool" },
      },
    },
  },
  validationAction: "warn",
});

db.ingestion_logs.createIndex(
  { brand_id: 1, source: 1, target_date: 1 },
  { name: "ingest_logs_brand_source_date" }
);
db.ingestion_logs.createIndex(
  { status: 1, started_at: -1 },
  { name: "ingest_logs_status_time" }
);
// TTL: auto-delete logs older than 90 days
db.ingestion_logs.createIndex(
  { started_at: 1 },
  { expireAfterSeconds: 90 * 24 * 60 * 60, name: "ingest_logs_ttl_90d" }
);

print("ingestion_logs: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 8. anomalies  (TTL: 180 days)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("anomalies", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "detected_at", "date", "metric", "severity"],
      properties: {
        brand_id:       { bsonType: "objectId" },
        campaign_id:    { bsonType: "objectId" },
        detected_at:    { bsonType: "date" },
        date:           { bsonType: "date" },
        metric: {
          bsonType: "string",
          enum: ["roas", "cpl", "ctr", "spend"],
        },
        current_value:  { bsonType: "double" },
        baseline_value: { bsonType: "double" },
        deviation_pct:  { bsonType: "double" },
        direction: {
          bsonType: "string",
          enum: ["spike", "drop"],
        },
        severity: {
          bsonType: "string",
          enum: ["low", "medium", "high", "critical"],
        },
        acknowledged:    { bsonType: "bool" },
        acknowledged_by: { bsonType: "objectId" },
        claude_summary:  { bsonType: "string" },
      },
    },
  },
  validationAction: "warn",
});

db.anomalies.createIndex(
  { brand_id: 1, detected_at: -1 },
  { name: "anomalies_brand_time" }
);
db.anomalies.createIndex(
  { brand_id: 1, acknowledged: 1, severity: 1 },
  { name: "anomalies_brand_ack_severity" }
);
// TTL: auto-delete anomalies older than 180 days
db.anomalies.createIndex(
  { detected_at: 1 },
  { expireAfterSeconds: 180 * 24 * 60 * 60, name: "anomalies_ttl_180d" }
);

print("anomalies: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 9. claude_conversations  (TTL: 365 days)
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("claude_conversations", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "user_id", "created_at", "updated_at"],
      properties: {
        brand_id:   { bsonType: "objectId" },
        user_id:    { bsonType: "objectId" },
        created_at: { bsonType: "date" },
        updated_at: { bsonType: "date" },
        title:      { bsonType: "string" },
        messages: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["role", "timestamp"],
            properties: {
              role:         { bsonType: "string", enum: ["user", "assistant", "tool"] },
              content:      { bsonType: "string" },
              tool_calls:   { bsonType: "array" },
              tool_results: { bsonType: "array" },
              timestamp:    { bsonType: "date" },
            },
          },
        },
      },
    },
  },
  validationAction: "warn",
});

db.claude_conversations.createIndex(
  { brand_id: 1, user_id: 1, updated_at: -1 },
  { name: "conv_brand_user_updated" }
);
// TTL: auto-delete conversations older than 365 days
db.claude_conversations.createIndex(
  { updated_at: 1 },
  { expireAfterSeconds: 365 * 24 * 60 * 60, name: "conv_ttl_365d" }
);

print("claude_conversations: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// 10. scheduled_reports
// ═════════════════════════════════════════════════════════════════════════════
createOrModify("scheduled_reports", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["brand_id", "created_by", "name", "schedule", "report_type", "is_active"],
      properties: {
        brand_id:    { bsonType: "objectId" },
        created_by:  { bsonType: "objectId" },
        name:        { bsonType: "string" },
        schedule: {
          bsonType: "string",
          enum: ["weekly_monday", "monthly_1st", "daily", "weekly_friday"],
        },
        report_type: { bsonType: "string" },
        recipients:  { bsonType: "array", items: { bsonType: "string" } },
        last_run:    { bsonType: "date" },
        next_run:    { bsonType: "date" },
        is_active:   { bsonType: "bool" },
      },
    },
  },
  validationAction: "warn",
});

// Used when loading a brand's reports list
db.scheduled_reports.createIndex(
  { brand_id: 1, is_active: 1 },
  { name: "reports_brand_active" }
);
// Used by the scheduler worker to find due reports
db.scheduled_reports.createIndex(
  { next_run: 1, is_active: 1 },
  { name: "reports_next_run_active" }
);

print("scheduled_reports: indexes created");

// ═════════════════════════════════════════════════════════════════════════════
// Summary
// ═════════════════════════════════════════════════════════════════════════════
print("");
print("=== Init complete ===");
print("Collections: agencies, brands, users, campaigns, ad_performance_raw,");
print("             performance_rollups, ingestion_logs, anomalies,");
print("             claude_conversations, scheduled_reports");
print("Indexes created: 27 total (4 TTL, 3 unique compound, 1 sparse, 1 partial)");
