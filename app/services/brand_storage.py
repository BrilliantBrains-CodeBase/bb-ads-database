"""
Brand asset storage service.

Manages the on-disk folder tree for each brand under BRAND_STORAGE_ROOT.
All paths are derived from the brand slug — never from user-supplied strings
(no path traversal possible).

Folder template per brand:
    {root}/{brand_slug}/
        credentials/              # AES-256-GCM encrypted tokens & OAuth JSONs
        csv-uploads/{YYYY}/{MM}/  # Raw uploaded CSVs, partitioned by date
        reports/scheduled/        # Auto-generated scheduled reports
        reports/ad-hoc/           # Manually triggered reports
        exports/                  # DPDP data exports / bulk downloads
        creatives/                # Ad creative assets (Phase 3+)
        logs/                     # Brand-specific ingestion log symlinks
        config/
            brand_config.json     # KPI targets, alert thresholds
"""

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)

# ── Subfolder names ───────────────────────────────────────────────
_SUBDIRS = [
    "credentials",
    "csv-uploads",
    "reports/scheduled",
    "reports/ad-hoc",
    "exports",
    "creatives",
    "logs",
    "config",
]

_DIR_MODE = 0o750  # rwxr-x--- (owner=appuser, group=appgroup, no world access)

# Default brand_config.json written on first creation
_DEFAULT_BRAND_CONFIG: dict = {
    "target_roas": None,             # e.g. 3.5  (return on ad spend ratio)
    "target_cpl": None,              # e.g. 50000 (cost per lead in INR paise)
    "budget_alert_threshold": 0.9,   # alert when 90% of monthly budget consumed
    "anomaly_sensitivity": "medium", # low | medium | high
    "currency": "INR",
    "timezone": "Asia/Kolkata",
}

# Slug validation: lowercase alphanum + hyphens only, 3-63 chars
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$")


# ── Internal helpers ──────────────────────────────────────────────

def _storage_root() -> Path:
    return Path(get_settings().brand_storage_root)


def _validate_slug(brand_slug: str) -> None:
    """Raise ValueError if slug is unsafe / invalid."""
    if not _SLUG_RE.match(brand_slug):
        raise ValueError(
            f"Invalid brand slug '{brand_slug}'. "
            "Must be 3-63 lowercase alphanumeric characters or hyphens."
        )


def _brand_root(brand_slug: str) -> Path:
    _validate_slug(brand_slug)
    # Resolve prevents any path traversal even if slug validation is bypassed
    root = (_storage_root() / brand_slug).resolve()
    if not str(root).startswith(str(_storage_root().resolve())):
        raise ValueError(f"Path traversal detected for slug '{brand_slug}'")
    return root


# ── Public API ────────────────────────────────────────────────────

def create_brand_folders(brand_slug: str) -> Path:
    """
    Create (or fill in missing subdirs of) the standardized folder tree for a brand.

    Idempotent: safe to call multiple times; existing directories and files
    are never overwritten.

    Returns the absolute path to the brand root.
    """
    brand_root = _brand_root(brand_slug)

    # Create root
    brand_root.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)

    # Create every subdir
    for subdir in _SUBDIRS:
        path = brand_root / subdir
        path.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)

    # Write default brand_config.json only if it doesn't already exist
    config_file = brand_root / "config" / "brand_config.json"
    if not config_file.exists():
        config_file.write_text(
            json.dumps(_DEFAULT_BRAND_CONFIG, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        config_file.chmod(0o640)  # rw-r----- (no exec)

    logger.info(
        "brand_storage.created",
        brand_slug=brand_slug,
        path=str(brand_root),
    )
    return brand_root


def get_brand_path(
    brand_slug: str,
    subfolder: str | None = None,
) -> Path:
    """
    Return the absolute path to a brand's root (or a subfolder within it).
    Raises FileNotFoundError if the brand root does not exist on disk.
    """
    brand_root = _brand_root(brand_slug)
    if not brand_root.exists():
        raise FileNotFoundError(
            f"Brand storage not found for '{brand_slug}'. "
            "Run create_brand_folders() first."
        )

    if subfolder is None:
        return brand_root

    # Resolve the subfolder path and verify it stays inside brand root
    target = (brand_root / subfolder).resolve()
    if not str(target).startswith(str(brand_root.resolve())):
        raise ValueError(f"Subfolder '{subfolder}' escapes brand root.")
    return target


def get_csv_upload_path(
    brand_slug: str,
    filename: str,
    upload_id: str,
    dt: datetime | None = None,
) -> Path:
    """
    Return the full destination path for a CSV upload.
    Path pattern: csv-uploads/YYYY/MM/{stem}_{upload_id}.csv

    Creates the YYYY/MM/ directory if it doesn't exist.
    """
    if dt is None:
        dt = datetime.now(tz=timezone.utc)

    # Sanitise filename: keep stem only, strip any path components
    stem = Path(filename).stem
    # Remove characters that are unsafe in filenames
    safe_stem = re.sub(r"[^\w\-]", "_", stem)[:100]  # cap at 100 chars

    brand_root = get_brand_path(brand_slug)
    dest_dir = brand_root / "csv-uploads" / dt.strftime("%Y") / dt.strftime("%m")
    dest_dir.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)

    return dest_dir / f"{safe_stem}_{upload_id}.csv"


def get_report_path(
    brand_slug: str,
    trigger: Literal["scheduled", "ad-hoc"],
    filename: str,
) -> Path:
    """
    Return the full destination path for a generated report.
    Trigger type determines the subfolder: reports/scheduled/ or reports/ad-hoc/
    """
    brand_root = get_brand_path(brand_slug)
    safe_name = re.sub(r"[^\w\-\.]", "_", Path(filename).name)[:200]
    dest = brand_root / "reports" / trigger / safe_name
    return dest


def read_brand_config(brand_slug: str) -> dict:
    """Read and return the brand_config.json for a brand."""
    config_path = get_brand_path(brand_slug, "config/brand_config.json")
    return json.loads(config_path.read_text(encoding="utf-8"))


def write_brand_config(brand_slug: str, config: dict) -> None:
    """Overwrite brand_config.json with the supplied dict."""
    config_path = get_brand_path(brand_slug, "config/brand_config.json")
    config_path.write_text(
        json.dumps(config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("brand_storage.config_updated", brand_slug=brand_slug)


def cleanup_brand_folders(brand_slug: str) -> Path:
    """
    Archive a brand's folder tree to _archived/{brand_slug}_{timestamp}/.
    Does NOT delete the original — moves it atomically.

    Returns the archive destination path.
    """
    brand_root = _brand_root(brand_slug)
    if not brand_root.exists():
        raise FileNotFoundError(f"No storage folder found for brand '{brand_slug}'.")

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_root = _storage_root() / "_archived"
    archive_root.mkdir(mode=_DIR_MODE, parents=True, exist_ok=True)

    dest = archive_root / f"{brand_slug}_{ts}"
    shutil.move(str(brand_root), str(dest))

    logger.info(
        "brand_storage.archived",
        brand_slug=brand_slug,
        archive_path=str(dest),
    )
    return dest


def brand_exists(brand_slug: str) -> bool:
    """Return True if the brand's storage root exists on disk."""
    try:
        return _brand_root(brand_slug).exists()
    except ValueError:
        return False
