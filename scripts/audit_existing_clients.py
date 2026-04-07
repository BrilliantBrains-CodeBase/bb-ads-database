#!/usr/bin/env python3
"""
audit_existing_clients.py — Cross-reference existing client files with the MongoDB brands collection.

Reports
───────
  1. Brands in DB  with storage folders  → ready for migration
  2. Brands in DB  without storage folders  → need folder creation
  3. Source directories  with no matching DB brand  → orphan folders (not yet onboarded)
  4. Brands in DB that appear in neither source nor client list  → fully missing
  5. Client list entries not matched to any DB brand  → unmapped clients

Client inventory sources (pick one)
────────────────────────────────────
  --csv-file clients.csv        Two-column CSV: "name,brand_slug"  (or just "name" to auto-slug)
  --source-dir /old/clients/    Each subdirectory = one client (same layout as migrate_existing_files.py)
  --manual "Acme Corp,Globex"   Comma-separated client names

Usage
─────
  # From a CSV file
  python scripts/audit_existing_clients.py --csv-file clients.csv --mongo

  # From a source directory (auto-detect slugs from subdirectory names)
  python scripts/audit_existing_clients.py --source-dir /old/clients --mongo

  # Just MongoDB vs storage root check
  python scripts/audit_existing_clients.py --mongo

  # Output a JSON report instead of plain text
  python scripts/audit_existing_clients.py --source-dir /old/clients --mongo --json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

MONGODB_URI        = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB         = os.getenv("MONGODB_DB_NAME", "bb_ads")
BRAND_STORAGE_ROOT = os.getenv("BRAND_STORAGE_ROOT", "/data/brands")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _load_csv(path: Path) -> list[tuple[str, str]]:
    """
    Load client inventory from CSV.

    Expected columns (header row required):
      name            — client/brand name
      brand_slug      — (optional) explicit brand slug; auto-slugified from name if absent

    Returns list of (name, slug).
    """
    results: list[tuple[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Support either "name" or first column
            name = row.get("name") or row.get("client_name") or next(iter(row.values()), "")
            name = name.strip()
            slug = (row.get("brand_slug") or row.get("slug") or "").strip() or _slugify(name)
            if name:
                results.append((name, slug))
    return results


def _source_dir_clients(source_dir: Path) -> list[tuple[str, str]]:
    """Derive (name, slug) from subdirectories of source_dir."""
    return [
        (d.name, _slugify(d.name))
        for d in sorted(source_dir.iterdir())
        if d.is_dir() and not d.name.startswith("_")
    ]


# ── MongoDB query ─────────────────────────────────────────────────────────────

async def _fetch_db_brands() -> list[dict[str, Any]]:
    """Return all active brands from MongoDB."""
    from motor.motor_asyncio import AsyncIOMotorClient
    client: Any = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    try:
        await client.admin.command("ping")
    except Exception as exc:
        print(f"ERROR: Cannot connect to MongoDB: {exc}", file=sys.stderr)
        sys.exit(1)

    db = client[MONGODB_DB]
    cursor = db["brands"].find(
        {},
        {"_id": 1, "name": 1, "slug": 1, "is_active": 1,
         "onboarding_status": 1, "storage_path": 1, "created_at": 1},
    )
    brands = await cursor.to_list(length=None)
    client.close()
    return brands


# ── Audit logic ───────────────────────────────────────────────────────────────

def _storage_slug_set(storage_root: Path) -> set[str]:
    """Return the set of brand slugs that have folders on disk."""
    if not storage_root.exists():
        return set()
    return {
        d.name for d in storage_root.iterdir()
        if d.is_dir() and not d.name.startswith("_")
    }


def _run_audit(
    client_list: list[tuple[str, str]],    # (name, slug) from external source
    db_brands: list[dict[str, Any]],
    storage_root: Path,
) -> dict[str, Any]:
    """Build a cross-reference report."""
    db_slugs: dict[str, dict[str, Any]] = {b["slug"]: b for b in db_brands}
    client_slugs: dict[str, str] = {slug: name for name, slug in client_list}
    disk_slugs: set[str] = _storage_slug_set(storage_root)

    all_slugs = set(db_slugs) | set(client_slugs) | disk_slugs

    # 1. DB brands with storage folders
    db_with_folders: list[dict[str, Any]] = []
    # 2. DB brands without storage folders
    db_without_folders: list[dict[str, Any]] = []
    # 3. Disk folders with no DB brand
    orphan_folders: list[str] = []
    # 4. DB brands not in client list and not on disk
    fully_missing: list[dict[str, Any]] = []
    # 5. Client list entries not in DB
    unmapped_clients: list[tuple[str, str]] = []

    for slug in sorted(all_slugs):
        in_db     = slug in db_slugs
        on_disk   = slug in disk_slugs
        in_client = slug in client_slugs

        if in_db and on_disk:
            db_with_folders.append({
                "slug":               slug,
                "name":               db_slugs[slug].get("name"),
                "is_active":          db_slugs[slug].get("is_active"),
                "onboarding_status":  db_slugs[slug].get("onboarding_status"),
                "in_client_list":     in_client,
            })
        elif in_db and not on_disk:
            db_without_folders.append({
                "slug":              slug,
                "name":              db_slugs[slug].get("name"),
                "is_active":         db_slugs[slug].get("is_active"),
                "onboarding_status": db_slugs[slug].get("onboarding_status"),
                "in_client_list":    in_client,
                "action":            "run create_brand_folders(slug) or POST /brands/{id}/onboard",
            })
        elif not in_db and on_disk:
            orphan_folders.append(slug)
        elif in_db and not on_disk and not in_client:
            fully_missing.append({
                "slug":   slug,
                "name":   db_slugs[slug].get("name"),
                "action": "brand exists in DB but no folder and not in client list — verify",
            })

        if in_client and not in_db:
            unmapped_clients.append((client_slugs[slug], slug))

    return {
        "generated_at":       datetime.now(UTC).isoformat(),
        "mongodb_uri":        MONGODB_URI,
        "db_name":            MONGODB_DB,
        "storage_root":       str(storage_root),
        "total_db_brands":    len(db_brands),
        "total_disk_folders": len(disk_slugs),
        "total_clients":      len(client_list),
        "db_with_folders":    db_with_folders,
        "db_without_folders": db_without_folders,
        "orphan_folders":     orphan_folders,
        "unmapped_clients":   [{"name": n, "slug": s} for n, s in unmapped_clients],
        "summary": {
            "db_with_folders":    len(db_with_folders),
            "db_without_folders": len(db_without_folders),
            "orphan_folders":     len(orphan_folders),
            "unmapped_clients":   len(unmapped_clients),
        },
    }


# ── Report printing ────────────────────────────────────────────────────────────

def _print_report(report: dict[str, Any]) -> None:
    w = 70
    print("=" * w)
    print("  BB Ads — Client Audit Report")
    print(f"  Generated : {report['generated_at']}")
    print(f"  MongoDB   : {report['db_name']}  ({report['mongodb_uri']})")
    print(f"  Storage   : {report['storage_root']}")
    print("=" * w)

    s = report["summary"]
    print(f"\n  DB brands          : {report['total_db_brands']}")
    print(f"  Disk folders       : {report['total_disk_folders']}")
    print(f"  Client list        : {report['total_clients']}")
    print()

    # ── DB + disk (good) ──────────────────────────────────────────────────────
    print(f"  ✓  DB + folder ({s['db_with_folders']}) — ready for migration:")
    if report["db_with_folders"]:
        for b in report["db_with_folders"]:
            active = "active" if b["is_active"] else "inactive"
            in_cl  = " [in client list]" if b["in_client_list"] else ""
            print(f"       {b['slug']:<30}  {active:<8}  {b['onboarding_status'] or 'n/a'}{in_cl}")
    else:
        print("       (none)")

    # ── DB but no folder ─────────────────────────────────────────────────────
    print(f"\n  ⚠  DB but no folder ({s['db_without_folders']}) — folders need creation:")
    if report["db_without_folders"]:
        for b in report["db_without_folders"]:
            print(f"       {b['slug']:<30}  → {b['action']}")
    else:
        print("       (none)")

    # ── Orphan folders ────────────────────────────────────────────────────────
    print(f"\n  ⚠  Orphan folders ({s['orphan_folders']}) — on disk but not in DB:")
    if report["orphan_folders"]:
        for slug in report["orphan_folders"]:
            print(f"       {slug}  → not yet onboarded via POST /brands")
    else:
        print("       (none)")

    # ── Unmapped clients ──────────────────────────────────────────────────────
    print(f"\n  ⚠  Unmapped clients ({s['unmapped_clients']}) — in client list but not in DB:")
    if report["unmapped_clients"]:
        for c in report["unmapped_clients"]:
            print(f"       {c['name']:<30}  (slug: {c['slug']})  → POST /brands to onboard")
    else:
        print("       (none)")

    print("\n" + "=" * w)
    print("  Next steps:")
    if s["db_without_folders"]:
        print("    1. Create missing folders via  POST /brands/{id}/onboard")
    if s["unmapped_clients"]:
        print("    2. Onboard new brands via       POST /brands  (auto-creates folders + ClickUp task)")
    if s["orphan_folders"]:
        print("    3. For orphan folders: verify the brand was onboarded, or delete if stale")
    if s["db_with_folders"]:
        print("    4. Run migrate_existing_files.py --source-dir <dir> to migrate files")
    print("=" * w)


# ── Main ───────────────────────────────────────────────────────────────────────

async def async_main(args: argparse.Namespace) -> None:
    storage_root = Path(args.brand_storage_root)

    # ── Build client list ─────────────────────────────────────────────────────
    client_list: list[tuple[str, str]] = []

    if args.csv_file:
        if not args.csv_file.exists():
            print(f"ERROR: CSV file not found: {args.csv_file}", file=sys.stderr)
            sys.exit(1)
        client_list = _load_csv(args.csv_file)
        print(f"Loaded {len(client_list)} client(s) from {args.csv_file}")

    if args.source_dir:
        if not args.source_dir.exists():
            print(f"ERROR: Source directory not found: {args.source_dir}", file=sys.stderr)
            sys.exit(1)
        from_dir = _source_dir_clients(args.source_dir)
        # Merge (no duplicates by slug)
        existing = {s for _, s in client_list}
        for name, slug in from_dir:
            if slug not in existing:
                client_list.append((name, slug))
        print(f"Detected {len(from_dir)} client dir(s) in {args.source_dir}")

    if args.manual:
        for name in (n.strip() for n in args.manual.split(",") if n.strip()):
            slug = _slugify(name)
            existing = {s for _, s in client_list}
            if slug not in existing:
                client_list.append((name, slug))

    # ── Fetch DB brands ───────────────────────────────────────────────────────
    db_brands: list[dict[str, Any]] = []
    if args.mongo:
        print(f"Connecting to MongoDB ({MONGODB_DB}) …")
        db_brands = await _fetch_db_brands()
        print(f"Found {len(db_brands)} brand(s) in DB.")

    # ── Run audit ─────────────────────────────────────────────────────────────
    report = _run_audit(client_list, db_brands, storage_root)

    # ── Output ────────────────────────────────────────────────────────────────
    if args.json:
        output = json.dumps(report, indent=2, default=str)
        if args.output:
            args.output.write_text(output, encoding="utf-8")
            print(f"Report written → {args.output}")
        else:
            print(output)
    else:
        _print_report(report)
        if args.output:
            args.output.write_text(
                json.dumps(report, indent=2, default=str), encoding="utf-8"
            )
            print(f"\n  JSON report also written → {args.output}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Cross-reference existing client files with the MongoDB brands collection.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Client inventory sources
    src = p.add_argument_group("Client inventory sources (at least one recommended)")
    src.add_argument("--csv-file", type=Path, metavar="FILE",
                     help="CSV with columns: name[,brand_slug]")
    src.add_argument("--source-dir", type=Path, metavar="DIR",
                     help="Directory whose subdirs are client folders.")
    src.add_argument("--manual", metavar="NAMES",
                     help="Comma-separated client names.")

    # DB / storage
    db = p.add_argument_group("Data sources")
    db.add_argument("--mongo", action="store_true",
                    help="Query MongoDB for brands (uses MONGODB_URI / MONGODB_DB_NAME env).")
    db.add_argument("--brand-storage-root", default=BRAND_STORAGE_ROOT, metavar="PATH",
                    help=f"Storage root to check for folders (default: {BRAND_STORAGE_ROOT}).")

    # Output
    out = p.add_argument_group("Output")
    out.add_argument("--json", action="store_true",
                     help="Output machine-readable JSON instead of human-readable text.")
    out.add_argument("--output", type=Path, metavar="FILE",
                     help="Write JSON report to this file (in addition to stdout).")

    args = p.parse_args()

    if not any([args.csv_file, args.source_dir, args.manual, args.mongo]):
        print("Tip: pass --mongo to cross-reference with the database,")
        print("     and/or --source-dir / --csv-file to include an external client list.")
        print()

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
