# Existing Client File Migration — Standard Operating Procedure

This SOP covers migrating files for clients who were being served *before* the BB Ads platform was deployed.  It assumes the platform is already running (Docker stack up, MongoDB seeded) and that all target brands have been onboarded via the API.

---

## Prerequisites

| Requirement | Check |
|---|---|
| Platform running (`docker-compose up`) | `curl http://localhost/health` returns `200 ok` |
| MongoDB seeded with agency/brands | `python scripts/seed_data.py --dry-run` shows brands |
| `BRAND_STORAGE_ROOT` env var set | Confirm in `.env` (default `/data/brands`) |
| Source files accessible | Mount old file share or copy to a local path |
| Python env active with dependencies | `pip install -r requirements.txt` |

---

## Step 1 — Inventory existing clients

Run the audit script to see what you have and what the platform knows about.

```bash
# Cross-reference source dirs against MongoDB + storage root
python scripts/audit_existing_clients.py \
  --source-dir /old/clients \
  --mongo \
  --output audit_report.json
```

Read the output carefully:

| Section | Action |
|---|---|
| **DB + folder** | Ready — proceed to Step 3 |
| **DB but no folder** | Run Step 2a (create folders) |
| **Orphan folders** | Verify brand was onboarded, or skip |
| **Unmapped clients** | Run Step 2b (create brand in system) |

If you have a CSV of existing clients, pass it as well:

```bash
python scripts/audit_existing_clients.py \
  --csv-file existing_clients.csv \
  --source-dir /old/clients \
  --mongo
```

CSV format:
```
name,brand_slug
Acme Corp,acme-corp
Globex,globex-corp
```
(`brand_slug` is optional — auto-generated from `name` if omitted.)

---

## Step 2a — Create brands in the system (new clients)

For every client that is *not yet in MongoDB*, create a brand via the API.  This automatically:
- Creates the standardized folder tree under `BRAND_STORAGE_ROOT/{brand_slug}/`
- Creates a ClickUp onboarding task with the standard checklist
- Sets `onboarding_status: "in_progress"`

```bash
# Example — replace token and payload with real values
curl -s -X POST http://localhost/api/v1/brands \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme Corp",
    "slug": "acme-corp",
    "industry": "ecommerce",
    "platforms": {
      "google_ads": { "customer_id": "123-456-7890" }
    }
  }'
```

For bulk onboarding, script a loop over the unmapped clients in `audit_report.json`.

---

## Step 2b — Create missing folders for brands already in DB

If a brand exists in MongoDB but has no storage folder (e.g. the folder was lost or migration skipped):

```bash
# Trigger folder creation via the onboard endpoint
curl -s -X POST http://localhost/api/v1/brands/{brand_id}/onboard \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

## Step 3 — Dry-run: review the migration plan

Scan the source directory and generate `migration_manifest.json`.  No files are written.

```bash
python scripts/migrate_existing_files.py \
  --source-dir /old/clients \
  --brand-storage-root /data/brands \
  --dry-run
```

**Review the output:**
- Every `COPY` line shows: file type, source filename, and destination path.
- Every `SKIP [CONFLICT/...]` line flags a problem to resolve before executing.

**Common conflicts and fixes:**

| Conflict | Cause | Fix |
|---|---|---|
| `UNKNOWN_TYPE` | Extension not in the classification map | Rename file to a known extension, or manually move it after migration |
| `NAME_COLLISION` | Destination file already exists | Pass `--overwrite` to replace, or rename the source file |

If you use a non-standard source structure (e.g. flat directory per brand), pass `--brand-slug`:

```bash
python scripts/migrate_existing_files.py \
  --source-dir /old/clients/acme-files \
  --brand-slug acme-corp \
  --dry-run
```

To supply an explicit name→slug mapping instead of auto-detection:

```bash
# client_map.json: { "Acme Corp": "acme-corp", "Globex Ltd": "globex-corp" }
python scripts/migrate_existing_files.py \
  --source-dir /old/clients \
  --mapping client_map.json \
  --dry-run
```

---

## Step 4 — Execute: copy files

Once the dry-run output looks correct, run the execute step.  Originals are **never deleted** — only copied.

```bash
python scripts/migrate_existing_files.py --execute
```

Progress is logged to `migration_log.jsonl` (one JSON line per file).  The manifest is updated in-place with per-file status (`copied` / `failed` / `skipped`).

To re-run after a partial failure (e.g. disk full), just re-run `--execute` — already-copied files are skipped unless `--overwrite` is passed.

---

## Step 5 — Verify: checksum validation

Recompute SHA-256 for every copied file and compare against the source hash recorded during audit.  Any mismatch is printed to stderr.

```bash
python scripts/migrate_existing_files.py --verify
```

- Exit code `0` = all checksums match.
- Exit code `1` = at least one mismatch or missing file.

**Do not proceed to Step 6 until verify exits 0.**

If mismatches are found, re-run `--execute --overwrite` to re-copy the affected files, then verify again.

---

## Step 6 — Mark onboarding complete in ClickUp

For each successfully migrated brand, update the ClickUp task to reflect that file migration is done.

Option A — via the platform API:
```bash
curl -s -X POST http://localhost/api/v1/brands/{brand_id}/onboard/complete \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

Option B — directly in ClickUp:
1. Open the brand's onboarding task.
2. Check off "Upload initial CSV data".
3. If all checklist items are done, move the task to "Live".

---

## Step 7 — Archive old file structure (cleanup)

After verification passes, archive the source directories.  This **moves** (does not delete) them to `_migrated_archive/` with a UTC timestamp suffix so they can be recovered if needed.

```bash
python scripts/migrate_existing_files.py --cleanup
```

The script will refuse to archive if any files have not been verified.  After archiving, the manifest is updated with the archive location.

To use a custom archive root:

```bash
python scripts/migrate_existing_files.py \
  --cleanup \
  --archive-root /mnt/archive/bb-migration-2026
```

---

## End-to-end command reference

```bash
# 1. Full inventory check
python scripts/audit_existing_clients.py \
  --source-dir /old/clients --mongo --output audit_report.json

# 2. Onboard any missing brands (repeat per unmapped client)
#    curl -X POST /api/v1/brands  ...

# 3. Dry-run — review manifest
python scripts/migrate_existing_files.py \
  --source-dir /old/clients --brand-storage-root /data/brands --dry-run

# 4. Execute
python scripts/migrate_existing_files.py --execute

# 5. Verify
python scripts/migrate_existing_files.py --verify

# 6. Mark complete in ClickUp / API

# 7. Archive
python scripts/migrate_existing_files.py --cleanup
```

---

## Destination folder reference

| File type | Destination |
|---|---|
| `.csv`, `.tsv` | `{brand}/csv-uploads/{YYYY}/{MM}/` (year/month from filename or mtime) |
| `.pdf`, `.html`, `.docx`, `.xlsx` | `{brand}/reports/ad-hoc/` or `/scheduled/` (if name suggests auto-generation) |
| `.json`, `.p12`, `.pem`, `.key`, `.env` | `{brand}/credentials/` |
| `.zip`, `.gz`, `.tar`, `.7z` | `{brand}/exports/` |
| `.jpg`, `.png`, `.mp4`, `.gif`, … | `{brand}/creatives/` |

All destination filenames get a short UUID suffix (`_{8hex}.ext`) to prevent collisions from files with identical names across different source paths.

---

## Rollback

If migration needs to be undone:

1. The source files are still in `_migrated_archive/{client}_{timestamp}/` — move them back.
2. Delete the copied files from `{brand_storage_root}/{brand_slug}/` using the paths in `migration_manifest.json`.
3. Reset `onboarding_status` in MongoDB if needed.

No data in the platform database is affected by file migration — only the filesystem is touched.
