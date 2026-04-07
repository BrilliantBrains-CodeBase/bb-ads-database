#!/usr/bin/env python3
"""
migrate_existing_files.py — Migrate existing brand files into the standardized folder structure.

Workflow (run these steps in order)
────────────────────────────────────
  1. Audit    python scripts/migrate_existing_files.py --source-dir /old/clients
  2. Dry-run  python scripts/migrate_existing_files.py --dry-run
  3. Execute  python scripts/migrate_existing_files.py --execute
  4. Verify   python scripts/migrate_existing_files.py --verify
  5. Cleanup  python scripts/migrate_existing_files.py --cleanup

Each step reads/writes migration_manifest.json so progress is preserved.

Modes
─────
  --audit   (default)  Scan source, classify files, write migration_manifest.json.
  --dry-run            Show what would be copied; flag conflicts & unknowns. No writes.
  --execute            Copy files to destination, log every move to migration_log.jsonl.
  --verify             Recompute SHA-256 for every copied file; report mismatches.
  --cleanup            Archive source directories to _migrated_archive/ after verify.

Source directory layouts
────────────────────────
  Multi-brand (default): each subdirectory = one client
    /old/clients/
      Acme Corp/         →  brand slug "acme-corp"
      Globex Corp/       →  brand slug "globex-corp"

  Single-brand: one flat directory with --brand-slug
    /old/acme-data/      →  --brand-slug acme-corp

  Explicit mapping: --mapping client_map.json
    { "Acme Corp": "acme-corp", "Globex": "globex-corp" }

File type → destination mapping
────────────────────────────────
  .csv / .tsv                         → csv-uploads/YYYY/MM/
  .pdf / .html / .docx / .xlsx / .ppt → reports/ad-hoc/  (or /scheduled/ if name matches)
  .json / .p12 / .pem / .key / .env   → credentials/
  .zip / .gz / .tar / .7z             → exports/
  .jpg / .png / .mp4 / .gif / …       → creatives/
  (anything else)                     → flagged as UNKNOWN_TYPE conflict
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sys
import uuid
from datetime import datetime, timezone
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

import os

BRAND_STORAGE_ROOT = os.getenv("BRAND_STORAGE_ROOT", "/data/brands")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── File type classification ───────────────────────────────────────────────────

# Extension → (type_label, destination_subfolder_template)
# CSV/TSV destination uses "csv-uploads/{YYYY}/{MM}" — resolved per file.
_EXT_MAP: dict[str, tuple[str, str]] = {
    ".csv":  ("csv",        "csv-uploads/{year}/{month}"),
    ".tsv":  ("csv",        "csv-uploads/{year}/{month}"),
    # Reports — scheduled vs ad-hoc resolved later by filename heuristic
    ".pdf":  ("report",     "reports/{trigger}"),
    ".html": ("report",     "reports/{trigger}"),
    ".htm":  ("report",     "reports/{trigger}"),
    ".docx": ("report",     "reports/{trigger}"),
    ".doc":  ("report",     "reports/{trigger}"),
    ".xlsx": ("report",     "reports/{trigger}"),
    ".xls":  ("report",     "reports/{trigger}"),
    ".pptx": ("report",     "reports/{trigger}"),
    ".ppt":  ("report",     "reports/{trigger}"),
    # Credentials / API tokens
    ".json": ("credential", "credentials"),
    ".p12":  ("credential", "credentials"),
    ".pem":  ("credential", "credentials"),
    ".key":  ("credential", "credentials"),
    ".env":  ("credential", "credentials"),
    ".pfx":  ("credential", "credentials"),
    # Exports / archives
    ".zip":  ("export",     "exports"),
    ".gz":   ("export",     "exports"),
    ".tar":  ("export",     "exports"),
    ".7z":   ("export",     "exports"),
    ".bz2":  ("export",     "exports"),
    # Ad creatives
    ".jpg":  ("creative",   "creatives"),
    ".jpeg": ("creative",   "creatives"),
    ".png":  ("creative",   "creatives"),
    ".gif":  ("creative",   "creatives"),
    ".webp": ("creative",   "creatives"),
    ".svg":  ("creative",   "creatives"),
    ".mp4":  ("creative",   "creatives"),
    ".mov":  ("creative",   "creatives"),
    ".avi":  ("creative",   "creatives"),
    ".mkv":  ("creative",   "creatives"),
}

# Keywords in filenames that indicate a scheduled (auto-generated) report
_SCHEDULED_KEYWORDS = re.compile(
    r"(scheduled|weekly|monthly|daily|auto[_\-]?gen|recurring)", re.IGNORECASE
)

# Patterns to extract YYYY and MM from a filename
_YEAR_RE   = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
_MONTH_NUM = re.compile(r"(?<!\d)(0[1-9]|1[0-2])(?!\d)")
_MONTH_NAME = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
    r"|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b",
    re.IGNORECASE,
)
_MONTH_NAME_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def _slugify(name: str) -> str:
    """Convert a directory name to a brand slug: 'Acme Corp' → 'acme-corp'."""
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _infer_date(filename: str, mtime: float) -> tuple[str, str]:
    """Return (YYYY, MM) for a CSV file from its name or mtime."""
    stem = Path(filename).stem

    year_match = _YEAR_RE.search(stem)
    year = year_match.group(1) if year_match else None

    # Try numeric month first
    month_match = _MONTH_NUM.search(stem)
    month = month_match.group(1) if month_match else None

    # If no numeric month, try name
    if month is None:
        name_match = _MONTH_NAME.search(stem)
        if name_match:
            abbr = name_match.group(1)[:3].lower()
            month = _MONTH_NAME_MAP.get(abbr)

    if year and month:
        return year, month

    # Fall back to file modification time
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.strftime("%Y"), dt.strftime("%m")


def _classify(file_path: Path, mtime: float) -> tuple[str, str, dict[str, str]]:
    """
    Returns (file_type, dest_subfolder, template_vars) or ("unknown", "", {}).
    dest_subfolder may contain {year}, {month}, {trigger} placeholders.
    """
    ext = file_path.suffix.lower()
    entry = _EXT_MAP.get(ext)

    if entry is None:
        return "unknown", "", {}

    file_type, subfolder_tpl = entry
    template_vars: dict[str, str] = {}

    if file_type == "csv":
        year, month = _infer_date(file_path.name, mtime)
        template_vars = {"year": year, "month": month}

    elif file_type == "report":
        trigger = "scheduled" if _SCHEDULED_KEYWORDS.search(file_path.name) else "ad-hoc"
        template_vars = {"trigger": trigger}

    subfolder = subfolder_tpl.format(**template_vars) if template_vars else subfolder_tpl
    return file_type, subfolder, template_vars


def _safe_filename(name: str) -> str:
    """Strip unsafe characters; preserve extension."""
    p = Path(name)
    safe_stem = re.sub(r"[^\w\-\.]", "_", p.stem)[:120]
    return safe_stem + p.suffix.lower()


def _dest_filename(src: Path, file_type: str) -> str:
    """Build destination filename, adding a short uuid suffix to avoid collisions."""
    safe = _safe_filename(src.name)
    short_id = uuid.uuid4().hex[:8]
    p = Path(safe)
    return f"{p.stem}_{short_id}{p.suffix}"


# ── Manifest helpers ───────────────────────────────────────────────────────────

def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_manifest(manifest: dict[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


# ── Core phases ────────────────────────────────────────────────────────────────

def _audit_brand(
    brand_slug: str,
    source_dir: Path,
    storage_root: Path,
) -> dict[str, Any]:
    """Scan one brand's source directory and classify all files."""
    files: list[dict[str, Any]] = []
    conflicts: list[dict[str, str]] = []

    for src_path in sorted(source_dir.rglob("*")):
        if not src_path.is_file():
            continue

        mtime = src_path.stat().st_mtime
        file_type, subfolder, _ = _classify(src_path, mtime)

        if file_type == "unknown":
            conflicts.append({
                "source": str(src_path),
                "reason": "UNKNOWN_TYPE",
                "extension": src_path.suffix.lower() or "(none)",
            })
            continue

        dest_fname = _dest_filename(src_path, file_type)
        dest_rel   = f"{subfolder}/{dest_fname}"
        dest_abs   = str(storage_root / brand_slug / subfolder / dest_fname)

        # Check name collision (dest file already exists on disk)
        conflict_reason = None
        if Path(dest_abs).exists():
            conflict_reason = "NAME_COLLISION"
            conflicts.append({
                "source": str(src_path),
                "reason": conflict_reason,
                "dest": dest_abs,
            })

        sha = _sha256(src_path)
        size = src_path.stat().st_size

        files.append({
            "source":   str(src_path),
            "dest_rel": dest_rel,
            "dest_abs": dest_abs,
            "type":     file_type,
            "size_bytes": size,
            "source_sha256": sha,
            "dest_sha256":   None,
            "status":   "conflict" if conflict_reason else "pending",
            "conflict": conflict_reason,
        })

    return {
        "brand_slug":  brand_slug,
        "source_dir":  str(source_dir),
        "files":       files,
        "conflicts":   conflicts,
    }


def phase_audit(
    source_dir: Path,
    storage_root: Path,
    manifest_path: Path,
    brand_slug: str | None,
    mapping: dict[str, str],
) -> dict[str, Any]:
    """Scan source directory, build and save manifest. Returns manifest dict."""
    brands_data: list[dict[str, Any]] = []

    if brand_slug:
        # Single-brand mode
        bd = _audit_brand(brand_slug, source_dir, storage_root)
        brands_data.append(bd)
    else:
        # Multi-brand: each subdir = one client
        subdirs = sorted(d for d in source_dir.iterdir() if d.is_dir())
        if not subdirs:
            log.error("No subdirectories found in %s — use --brand-slug for a flat directory.", source_dir)
            sys.exit(1)
        for subdir in subdirs:
            slug = mapping.get(subdir.name) or _slugify(subdir.name)
            bd = _audit_brand(slug, subdir, storage_root)
            brands_data.append(bd)

    total_files = sum(len(b["files"]) for b in brands_data)
    total_conflicts = sum(len(b["conflicts"]) for b in brands_data)
    type_counts: dict[str, int] = {}
    for b in brands_data:
        for f in b["files"]:
            type_counts[f["type"]] = type_counts.get(f["type"], 0) + 1

    manifest: dict[str, Any] = {
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "source_root":      str(source_dir),
        "brand_storage_root": str(storage_root),
        "brands":           brands_data,
        "summary": {
            "total_brands":    len(brands_data),
            "total_files":     total_files,
            "total_conflicts": total_conflicts,
            "by_type":         type_counts,
        },
    }
    _save_manifest(manifest, manifest_path)
    log.info("Manifest written → %s", manifest_path)
    return manifest


def phase_dry_run(manifest: dict[str, Any]) -> None:
    """Print what would be copied; highlight conflicts."""
    print("\n── Dry-run preview ──────────────────────────────────────────────────")
    for brand in manifest["brands"]:
        slug = brand["brand_slug"]
        print(f"\n  Brand: {slug}  (source: {brand['source_dir']})")

        ok = [f for f in brand["files"] if f["status"] == "pending"]
        bad = [f for f in brand["files"] if f["status"] == "conflict"]

        for f in ok:
            size_kb = f["size_bytes"] // 1024
            print(f"    COPY  [{f['type']:10s}] {Path(f['source']).name} "
                  f"→ {f['dest_rel']}  ({size_kb} KB)")

        for c in brand["conflicts"]:
            print(f"    SKIP  [CONFLICT/{c['reason']}]  {Path(c['source']).name}")
        for f in bad:
            print(f"    SKIP  [CONFLICT/{f['conflict']}]  {Path(f['source']).name}")

    s = manifest["summary"]
    print(f"\n  Totals: {s['total_files']} files across {s['total_brands']} brand(s)")
    print(f"  By type: {s['by_type']}")
    if s["total_conflicts"]:
        print(f"  ⚠  {s['total_conflicts']} conflict(s) will be SKIPPED")
    else:
        print("  No conflicts.")


def phase_execute(
    manifest: dict[str, Any],
    manifest_path: Path,
    log_path: Path,
    overwrite: bool,
) -> None:
    """Copy files to destination, update manifest with status."""
    log_entries: list[dict[str, Any]] = []
    copied = skipped = failed = 0

    for brand in manifest["brands"]:
        for f in brand["files"]:
            if f["status"] not in ("pending", "conflict"):
                continue
            if f["status"] == "conflict" and not overwrite:
                log.debug("Skipping conflict: %s", f["source"])
                skipped += 1
                continue

            src  = Path(f["source"])
            dest = Path(f["dest_abs"])

            if dest.exists() and not overwrite:
                f["status"] = "skipped"
                skipped += 1
                log.warning("Destination exists, skipping: %s", dest)
                continue

            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(src), str(dest))   # copy2 preserves mtime/metadata

                dest_sha = _sha256(dest)
                f["dest_sha256"] = dest_sha
                f["status"] = "copied"
                copied += 1

                entry = {
                    "ts":     datetime.now(timezone.utc).isoformat(),
                    "action": "copy",
                    "brand":  brand["brand_slug"],
                    "source": f["source"],
                    "dest":   f["dest_abs"],
                    "type":   f["type"],
                    "source_sha256": f["source_sha256"],
                    "dest_sha256":   dest_sha,
                    "size_bytes": f["size_bytes"],
                }
                log_entries.append(entry)
                log.info("Copied %s → %s", src.name, dest)

            except OSError as exc:
                f["status"] = "failed"
                f["error"]  = str(exc)
                failed += 1
                log.error("Failed to copy %s: %s", src, exc)

    # Append to JSONL log
    with log_path.open("a", encoding="utf-8") as lf:
        for entry in log_entries:
            lf.write(json.dumps(entry, default=str) + "\n")

    _save_manifest(manifest, manifest_path)
    print(f"\n  Execute complete: {copied} copied, {skipped} skipped, {failed} failed")
    print(f"  Log → {log_path}")


def phase_verify(manifest: dict[str, Any], manifest_path: Path) -> bool:
    """Recompute SHA-256 for every copied file; return True if all pass."""
    ok = mismatches = missing = 0

    for brand in manifest["brands"]:
        for f in brand["files"]:
            if f["status"] != "copied":
                continue

            dest = Path(f["dest_abs"])
            if not dest.exists():
                log.error("MISSING  %s", dest)
                f["status"] = "missing"
                missing += 1
                continue

            actual_sha = _sha256(dest)
            if actual_sha == f["source_sha256"]:
                f["status"] = "verified"
                ok += 1
                log.debug("OK  %s", dest.name)
            else:
                f["status"] = "checksum_mismatch"
                f["dest_sha256_actual"] = actual_sha
                mismatches += 1
                log.error(
                    "MISMATCH  %s\n"
                    "  source sha256 : %s\n"
                    "  dest sha256   : %s",
                    dest,
                    f["source_sha256"],
                    actual_sha,
                )

    _save_manifest(manifest, manifest_path)
    print(f"\n  Verify: {ok} OK, {mismatches} mismatch(es), {missing} missing")
    if mismatches or missing:
        print("  ✗  Verification FAILED — do not run --cleanup")
        return False
    print("  ✓  All checksums match")
    return True


def phase_cleanup(
    manifest: dict[str, Any],
    manifest_path: Path,
    archive_root: Path,
) -> None:
    """Archive source directories to _migrated_archive/ after successful verify."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_root.mkdir(parents=True, exist_ok=True)

    # Check that all files are verified before archiving
    for brand in manifest["brands"]:
        unverified = [
            f for f in brand["files"]
            if f["status"] not in ("verified", "skipped", "failed", "conflict")
        ]
        if unverified:
            log.error(
                "Brand %s has %d unverified file(s). Run --verify first.",
                brand["brand_slug"], len(unverified),
            )
            sys.exit(1)

    archived_dirs: set[str] = set()
    for brand in manifest["brands"]:
        src_dir = brand["source_dir"]
        if src_dir in archived_dirs:
            continue
        src_path = Path(src_dir)
        if not src_path.exists():
            log.warning("Source dir already gone: %s", src_path)
            continue
        dest = archive_root / f"{src_path.name}_{ts}"
        shutil.move(str(src_path), str(dest))
        archived_dirs.add(src_dir)
        log.info("Archived %s → %s", src_path, dest)

    manifest["archived_at"] = datetime.now(timezone.utc).isoformat()
    manifest["archive_root"] = str(archive_root)
    _save_manifest(manifest, manifest_path)
    print(f"\n  Cleanup: {len(archived_dirs)} directory/ies archived → {archive_root}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate existing brand files into the standardized folder structure.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Mode flags (mutually exclusive — exactly one must be set)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--audit",    action="store_true", help="Scan source and write manifest (default).")
    mode.add_argument("--dry-run",  action="store_true", help="Show what would be copied without writing.")
    mode.add_argument("--execute",  action="store_true", help="Copy files to destination.")
    mode.add_argument("--verify",   action="store_true", help="Verify SHA-256 checksums of copied files.")
    mode.add_argument("--cleanup",  action="store_true", help="Archive source dirs after verify.")

    # Source / destination
    p.add_argument("--source-dir", type=Path,
                   help="Source directory (root containing per-brand subdirs, or single brand dir).")
    p.add_argument("--brand-slug",
                   help="Brand slug (for single-brand mode — source-dir points to one brand).")
    p.add_argument("--brand-storage-root", type=Path,
                   default=Path(BRAND_STORAGE_ROOT),
                   help=f"Destination root (default: {BRAND_STORAGE_ROOT}).")
    p.add_argument("--mapping", type=Path,
                   help="JSON file mapping directory names to brand slugs.")

    # Manifest / log paths
    p.add_argument("--manifest", type=Path, default=Path("migration_manifest.json"),
                   help="Manifest file path (default: migration_manifest.json).")
    p.add_argument("--log", type=Path, default=Path("migration_log.jsonl"),
                   help="JSONL log path (default: migration_log.jsonl).")
    p.add_argument("--archive-root", type=Path,
                   default=Path("_migrated_archive"),
                   help="Archive root for --cleanup (default: _migrated_archive/).")

    # Flags
    p.add_argument("--overwrite", action="store_true",
                   help="Overwrite existing destination files (execute mode only).")

    return p.parse_args()


def main() -> None:
    args = _parse_args()

    # Default mode is --audit if no mode flag was passed
    if not any([args.audit, args.dry_run, args.execute, args.verify, args.cleanup]):
        args.audit = True

    storage_root: Path = args.brand_storage_root

    # ── Load mapping ──────────────────────────────────────────────────────────
    mapping: dict[str, str] = {}
    if args.mapping and args.mapping.exists():
        mapping = json.loads(args.mapping.read_text(encoding="utf-8"))
        log.info("Loaded client→slug mapping: %d entries", len(mapping))

    # ── Modes that need the manifest but not the source dir ──────────────────
    if args.dry_run or args.verify or args.cleanup:
        manifest = _load_manifest(args.manifest)
        if not manifest:
            log.error("Manifest not found at %s — run --audit first.", args.manifest)
            sys.exit(1)

        if args.dry_run:
            phase_dry_run(manifest)
        elif args.verify:
            ok = phase_verify(manifest, args.manifest)
            sys.exit(0 if ok else 1)
        elif args.cleanup:
            phase_cleanup(manifest, args.manifest, args.archive_root)
        return

    # ── Modes that need source_dir ────────────────────────────────────────────
    if args.audit or args.execute:
        # For execute mode, load existing manifest; re-audit if not present
        if args.execute:
            manifest = _load_manifest(args.manifest)
            if not manifest:
                if not args.source_dir:
                    log.error("No manifest found and --source-dir not provided. "
                              "Run --audit first or pass --source-dir.")
                    sys.exit(1)
                log.info("No manifest found — running audit first.")
                manifest = phase_audit(
                    args.source_dir, storage_root, args.manifest,
                    args.brand_slug, mapping,
                )
            phase_execute(manifest, args.manifest, args.log, args.overwrite)
            return

        # Audit mode
        if not args.source_dir:
            log.error("--source-dir is required for --audit.")
            sys.exit(1)
        if not args.source_dir.exists():
            log.error("Source directory does not exist: %s", args.source_dir)
            sys.exit(1)

        manifest = phase_audit(
            args.source_dir, storage_root, args.manifest,
            args.brand_slug, mapping,
        )
        # Print summary
        s = manifest["summary"]
        print(f"\n  Audit complete: {s['total_files']} files, "
              f"{s['total_brands']} brand(s), "
              f"{s['total_conflicts']} conflict(s)")
        print(f"  By type: {s['by_type']}")
        print(f"  Manifest → {args.manifest}")
        if s["total_conflicts"]:
            print("\n  ⚠  Conflicts (will be skipped during execute):")
            for brand in manifest["brands"]:
                for c in brand["conflicts"]:
                    print(f"     [{brand['brand_slug']}]  {c['reason']}  "
                          f"{Path(c['source']).name}  ({c.get('extension', '')})")
        print("\n  Next steps:")
        print(f"    1. Review {args.manifest}")
        print("    2. python scripts/migrate_existing_files.py --dry-run")
        print("    3. python scripts/migrate_existing_files.py --execute")
        print("    4. python scripts/migrate_existing_files.py --verify")


if __name__ == "__main__":
    main()
