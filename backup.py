#!/usr/bin/env python3
"""
OpenStack instance backup script.

Creates a Glance snapshot of each configured server, downloads it to a local
temp directory, uploads it to S3, then applies GFS retention rules to remove
old backups from S3. One failed server does not abort the others.

Usage:
    python backup.py [--config config.yaml]
"""

import argparse
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import boto3
import openstack
import yaml
from botocore.config import Config as BotoConfig
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------------------------
# Configuration data classes
# ---------------------------------------------------------------------------

@dataclass
class RetentionConfig:
    daily: int = 7
    weekly: int = 4
    weekly_weekday: int = 0   # 0=Monday, 6=Sunday
    monthly: int = 6
    monthly_day: int = 1      # 1–28


@dataclass
class ServerConfig:
    cloud: str
    name: str
    bucket: str
    prefix: str = ""
    retention: RetentionConfig = field(default_factory=RetentionConfig)


@dataclass
class AppConfig:
    s3_endpoint: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str
    s3_profile: str
    temp_dir: Path
    global_retention: RetentionConfig
    servers: list


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _build_retention(raw: dict, defaults: RetentionConfig) -> RetentionConfig:
    return RetentionConfig(
        daily=raw.get("daily", defaults.daily),
        weekly=raw.get("weekly", defaults.weekly),
        weekly_weekday=raw.get("weekly_weekday", defaults.weekly_weekday),
        monthly=raw.get("monthly", defaults.monthly),
        monthly_day=raw.get("monthly_day", defaults.monthly_day),
    )


def load_config(path: str) -> AppConfig:
    with open(path) as fh:
        raw = yaml.safe_load(fh)

    s3_raw = raw.get("s3", {})
    s3_endpoint = os.environ.get("BACKUP_S3_ENDPOINT") or s3_raw.get("endpoint_url", "")
    s3_region = os.environ.get("BACKUP_S3_REGION") or s3_raw.get("region_name", "us-east-1")
    s3_access_key = os.environ.get("BACKUP_S3_ACCESS_KEY") or s3_raw.get("access_key", "")
    s3_secret_key = os.environ.get("BACKUP_S3_SECRET_KEY") or s3_raw.get("secret_key", "")
    s3_profile = os.environ.get("BACKUP_S3_PROFILE") or s3_raw.get("aws_profile", "")

    temp_dir = Path(raw.get("temp_dir", "/tmp/openstack-backups"))

    global_retention = _build_retention(raw.get("retention", {}), RetentionConfig())

    servers = []
    for entry in raw.get("servers", []):
        for required in ("cloud", "name", "bucket"):
            if not entry.get(required):
                raise ValueError(f"Server entry missing required field '{required}': {entry}")
        server_retention = _build_retention(
            entry.get("retention", {}), global_retention
        )
        servers.append(ServerConfig(
            cloud=entry["cloud"],
            name=entry["name"],
            bucket=entry["bucket"],
            prefix=entry.get("prefix", ""),
            retention=server_retention,
        ))

    return AppConfig(
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_profile=s3_profile,
        temp_dir=temp_dir,
        global_retention=global_retention,
        servers=servers,
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------

def build_s3_client(config: AppConfig):
    session = boto3.Session(profile_name=config.s3_profile or None)
    kwargs = {
        "region_name": config.s3_region,
        "config": BotoConfig(retries={"max_attempts": 3, "mode": "standard"}),
    }
    if config.s3_access_key and config.s3_secret_key:
        kwargs["aws_access_key_id"] = config.s3_access_key
        kwargs["aws_secret_access_key"] = config.s3_secret_key
    if config.s3_endpoint:
        kwargs["endpoint_url"] = config.s3_endpoint
    return session.client("s3", **kwargs)


# ---------------------------------------------------------------------------
# OpenStack helpers
# ---------------------------------------------------------------------------

def _find_existing_snapshot(conn, snapshot_name: str) -> Optional[str]:
    """Return image_id if a snapshot with this name already exists."""
    image = conn.image.find_image(snapshot_name)
    if image:
        return image.id
    return None


def create_snapshot(
    conn,
    server_name: str,
    snapshot_name: str,
    logger: logging.Logger,
) -> tuple:
    """
    Create (or reuse) a Glance snapshot of the server.
    Returns (image_id, disk_format).
    """
    existing_id = _find_existing_snapshot(conn, snapshot_name)
    if existing_id:
        logger.info("Snapshot '%s' already exists (id=%s), reusing", snapshot_name, existing_id)
        image = conn.image.get_image(existing_id)
        if image.status != "active":
            logger.info("Waiting for existing snapshot to become active...")
            conn.image.wait_for_image(
                existing_id,
                status="active",
                failures=["error", "killed"],
                interval=30,
                wait=7200,
            )
            image = conn.image.get_image(existing_id)
        disk_format = image.disk_format or "raw"
        return existing_id, disk_format

    server = conn.compute.find_server(server_name, ignore_missing=False)
    logger.info("Creating snapshot '%s' for server '%s' (id=%s)", snapshot_name, server_name, server.id)
    image_id = conn.compute.create_server_image(server.id, name=snapshot_name)

    logger.info("Waiting for snapshot to become active (this may take a while)...")
    conn.image.wait_for_image(
        image_id,
        status="active",
        failures=["error", "killed"],
        interval=30,
        wait=7200,
    )

    image = conn.image.get_image(image_id)
    disk_format = image.disk_format or "raw"
    logger.info("Snapshot ready: id=%s, format=%s, size=%s bytes", image_id, disk_format, image.size)
    return image_id, disk_format


def download_snapshot(
    conn,
    image_id: str,
    snapshot_name: str,
    disk_format: str,
    temp_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Stream-download a Glance image to disk.
    Returns the path to the downloaded file.
    """
    tmp_path = temp_dir / f"{snapshot_name}.tmp"
    final_path = temp_dir / f"{snapshot_name}.{disk_format}"

    image = conn.image.get_image(image_id)
    if image.size:
        logger.info("Downloading snapshot (%d MB)...", image.size // (1024 * 1024))
    else:
        logger.info("Downloading snapshot (size unknown)...")

    chunk_size = 8 * 1024 * 1024  # 8 MB
    bytes_written = 0
    with open(tmp_path, "wb") as fh:
        for chunk in conn.image.download_image(image_id, stream=True):
            if chunk:
                fh.write(chunk)
                bytes_written += len(chunk)

    tmp_path.rename(final_path)
    logger.info("Download complete: %d MB written to %s", bytes_written // (1024 * 1024), final_path)
    return final_path


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_key(server: ServerConfig, snapshot_name: str, disk_format: str) -> str:
    parts = [p for p in [server.prefix, server.name, f"{snapshot_name}.{disk_format}"] if p]
    return "/".join(parts)


def upload_to_s3(
    s3_client,
    bucket: str,
    s3_key: str,
    local_path: Path,
    logger: logging.Logger,
):
    from boto3.s3.transfer import TransferConfig
    transfer_config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,  # 100 MB
        multipart_chunksize=50 * 1024 * 1024,   # 50 MB parts
        max_concurrency=4,
    )
    logger.info("Uploading to s3://%s/%s ...", bucket, s3_key)
    s3_client.upload_file(str(local_path), bucket, s3_key, Config=transfer_config)
    logger.info("Upload complete")


def list_server_backups(s3_client, bucket: str, server: ServerConfig) -> list:
    """Return all S3 keys under this server's prefix."""
    search_prefix = "/".join(p for p in [server.prefix, server.name, ""] if p)
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


# ---------------------------------------------------------------------------
# Retention logic
# ---------------------------------------------------------------------------

# Matches filenames like: servername-2026-03-31.raw
_BACKUP_RE = re.compile(r"(?P<name>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.(?P<ext>[a-z0-9]+)$")


@dataclass
class BackupRecord:
    s3_key: str
    backup_date: date
    disk_format: str


def parse_backup_records(s3_keys: list, server_name: str) -> list:
    records = []
    for key in s3_keys:
        filename = key.split("/")[-1]
        m = _BACKUP_RE.match(filename)
        if not m:
            continue
        if m.group("name") != server_name:
            continue
        try:
            backup_date = date.fromisoformat(m.group("date"))
        except ValueError:
            continue
        records.append(BackupRecord(
            s3_key=key,
            backup_date=backup_date,
            disk_format=m.group("ext"),
        ))
    return records


def _week_monday(ref: date, weeks_ago: int) -> date:
    """Return the Monday of the calendar week that is `weeks_ago` weeks before ref's week."""
    current_monday = ref - timedelta(days=ref.weekday())
    return current_monday - timedelta(weeks=weeks_ago)


def _best_for_week(records: list, week_monday: date, preferred_weekday: int) -> Optional[BackupRecord]:
    """
    Among records within [week_monday, week_monday+6], return the one on
    `preferred_weekday` if it exists, otherwise the nearest available.
    """
    week_end = week_monday + timedelta(days=6)
    in_week = [r for r in records if week_monday <= r.backup_date <= week_end]
    if not in_week:
        return None
    preferred_date = week_monday + timedelta(days=preferred_weekday)
    return min(in_week, key=lambda r: abs((r.backup_date - preferred_date).days))


def _best_for_month(records: list, year: int, month: int, preferred_day: int) -> Optional[BackupRecord]:
    """
    Among records within the given year/month, return the one closest to
    `preferred_day`.
    """
    in_month = [r for r in records if r.backup_date.year == year and r.backup_date.month == month]
    if not in_month:
        return None
    return min(in_month, key=lambda r: abs(r.backup_date.day - preferred_day))


def compute_keys_to_keep(
    records: list,
    retention: RetentionConfig,
    today: date,
) -> set:
    keep = set()
    sorted_records = sorted(records, key=lambda r: r.backup_date, reverse=True)

    # Daily: keep N most recent
    for rec in sorted_records[: retention.daily]:
        keep.add(rec.s3_key)

    # Weekly: one per calendar week for the last N weeks
    for weeks_ago in range(retention.weekly):
        week_monday = _week_monday(today, weeks_ago)
        best = _best_for_week(sorted_records, week_monday, retention.weekly_weekday)
        if best:
            keep.add(best.s3_key)

    # Monthly: one per calendar month for the last N months
    for months_ago in range(retention.monthly):
        target = today - relativedelta(months=months_ago)
        best = _best_for_month(sorted_records, target.year, target.month, retention.monthly_day)
        if best:
            keep.add(best.s3_key)

    return keep


def apply_retention(
    s3_client,
    server: ServerConfig,
    today: date,
    logger: logging.Logger,
):
    all_keys = list_server_backups(s3_client, server.bucket, server)
    records = parse_backup_records(all_keys, server.name)

    if not records:
        logger.info("No backup records found in S3 for retention check")
        return

    keep = compute_keys_to_keep(records, server.retention, today)
    to_delete = [r for r in records if r.s3_key not in keep]

    logger.info(
        "Retention: %d backups found, keeping %d, deleting %d",
        len(records), len(keep), len(to_delete),
    )

    for rec in to_delete:
        logger.info("Deleting old backup: %s", rec.s3_key)
        s3_client.delete_object(Bucket=server.bucket, Key=rec.s3_key)


# ---------------------------------------------------------------------------
# Per-server pipeline
# ---------------------------------------------------------------------------

def _cleanup_temp(temp_dir: Path, snapshot_name: str):
    for path in temp_dir.glob(f"{snapshot_name}.*"):
        try:
            path.unlink()
        except OSError:
            pass


def backup_server(
    server: ServerConfig,
    s3_client,
    today: date,
    temp_dir: Path,
    logger: logging.Logger,
) -> bool:
    snapshot_name = f"{server.name}-{today.isoformat()}"
    image_id = None
    conn = None

    try:
        conn = openstack.connect(cloud=server.cloud)

        # 1. Create / reuse snapshot
        image_id, disk_format = create_snapshot(conn, server.name, snapshot_name, logger)

        # 2. Download to temp
        local_path = download_snapshot(conn, image_id, snapshot_name, disk_format, temp_dir, logger)

        # 3. Upload to S3
        s3_key = _s3_key(server, snapshot_name, disk_format)
        upload_to_s3(s3_client, server.bucket, s3_key, local_path, logger)

        # 4. Delete Glance snapshot (free quota)
        conn.image.delete_image(image_id)
        logger.info("Deleted Glance snapshot %s", snapshot_name)
        image_id = None  # mark as cleaned up

        # 5. Apply retention rules
        apply_retention(s3_client, server, today, logger)

        logger.info("Backup completed successfully")
        return True

    except openstack.exceptions.ResourceNotFound:
        logger.error("Server '%s' not found in cloud '%s'", server.name, server.cloud)
        return False
    except Exception:
        logger.exception("Backup failed")
        # If S3 upload failed, leave the Glance snapshot for manual retry
        if image_id:
            logger.warning(
                "Glance snapshot '%s' (id=%s) was NOT deleted due to earlier failure "
                "— it will be reused on the next run",
                snapshot_name, image_id,
            )
        return False
    finally:
        _cleanup_temp(temp_dir, snapshot_name)
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OpenStack instance backup tool")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    setup_logging()
    log = logging.getLogger("backup")

    try:
        config = load_config(args.config)
    except Exception as exc:
        logging.critical("Failed to load config '%s': %s", args.config, exc)
        sys.exit(1)

    config.temp_dir.mkdir(parents=True, exist_ok=True)
    s3_client = build_s3_client(config)
    today = date.today()

    log.info("Starting backup run for %d server(s) — date %s", len(config.servers), today)

    results = {}
    for server in config.servers:
        server_log = logging.getLogger(f"backup.{server.name}")
        server_log.info("--- Starting backup for %s (cloud: %s) ---", server.name, server.cloud)
        results[server.name] = backup_server(server, s3_client, today, config.temp_dir, server_log)

    failed = [name for name, ok in results.items() if not ok]
    succeeded = [name for name, ok in results.items() if ok]

    log.info("Backup run complete: %d succeeded, %d failed", len(succeeded), len(failed))
    if succeeded:
        log.info("Succeeded: %s", ", ".join(succeeded))
    if failed:
        log.error("Failed: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()
