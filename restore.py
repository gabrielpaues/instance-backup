#!/usr/bin/env python3
"""
OpenStack instance restore script.

Downloads a snapshot from S3 and uploads it to OpenStack Glance so a new
instance can be booted from it.

Modes:
  Default     — restores the latest snapshot for the given server
  --snapshot  — restores a specific snapshot by date (YYYY-MM-DD) or full filename
  --interactive / -i  — lists available snapshots and prompts for selection

Usage:
    python restore.py --server web-server-01
    python restore.py --server web-server-01 --snapshot 2026-03-28
    python restore.py --server web-server-01 --interactive
    python restore.py --server web-server-01 --cloud mycloud --config /etc/backup/config.yaml
"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import boto3
import openstack
import yaml
from botocore.config import Config as BotoConfig

# ---------------------------------------------------------------------------
# Configuration (shared subset with backup.py)
# ---------------------------------------------------------------------------

@dataclass
class RetentionConfig:
    daily: int = 7
    weekly: int = 4
    weekly_weekday: int = 0
    monthly: int = 6
    monthly_day: int = 1


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
    servers: list


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

    global_retention = _build_retention(raw.get("retention", {}), RetentionConfig())
    servers = []
    for entry in raw.get("servers", []):
        servers.append(ServerConfig(
            cloud=entry["cloud"],
            name=entry["name"],
            bucket=entry["bucket"],
            prefix=entry.get("prefix", ""),
            retention=_build_retention(entry.get("retention", {}), global_retention),
        ))

    return AppConfig(
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_profile=s3_profile,
        temp_dir=Path(raw.get("temp_dir", "/tmp/openstack-backups")),
        servers=servers,
    )


def find_server_config(config: AppConfig, server_name: str) -> Optional[ServerConfig]:
    for s in config.servers:
        if s.name == server_name:
            return s
    return None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# S3 helpers
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


_BACKUP_RE = re.compile(r"(?P<name>.+)-(?P<date>\d{4}-\d{2}-\d{2})\.(?P<ext>[a-z0-9]+)$")


@dataclass
class SnapshotEntry:
    s3_key: str
    filename: str
    backup_date: date
    disk_format: str
    size_bytes: int = 0


def _metadata_s3_key(server: ServerConfig) -> str:
    parts = [p for p in [server.prefix, server.name, f"{server.name}-metadata.json"] if p]
    return "/".join(parts)


def load_server_metadata(s3_client, server: ServerConfig) -> dict:
    """Load the server metadata JSON written by backup.py, or return an empty dict."""
    try:
        key = _metadata_s3_key(server)
        resp = s3_client.get_object(Bucket=server.bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception:
        return {}


def _build_server_create_cmd(image_id: str, metadata: dict) -> str:
    parts = [f"openstack server create --image {image_id}"]
    parts.append(f"--flavor {metadata['flavor']}" if metadata.get("flavor") else "--flavor <flavor>")
    if metadata.get("key_name"):
        parts.append(f"--key-name {metadata['key_name']}")
    for net in metadata.get("networks", []):
        parts.append(f"--network {net}")
    if not metadata.get("networks"):
        parts.append("--network <network>")
    parts.append("<new-server-name>")
    return " ".join(parts)


def list_snapshots(s3_client, server: ServerConfig) -> list:
    search_prefix = "/".join(p for p in [server.prefix, server.name, ""] if p)
    paginator = s3_client.get_paginator("list_objects_v2")
    entries = []
    for page in paginator.paginate(Bucket=server.bucket, Prefix=search_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            m = _BACKUP_RE.match(filename)
            if not m or m.group("name") != server.name:
                continue
            try:
                backup_date = date.fromisoformat(m.group("date"))
            except ValueError:
                continue
            entries.append(SnapshotEntry(
                s3_key=key,
                filename=filename,
                backup_date=backup_date,
                disk_format=m.group("ext"),
                size_bytes=obj.get("Size", 0),
            ))
    return sorted(entries, key=lambda e: e.backup_date, reverse=True)


def _fmt_size(size_bytes: int) -> str:
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / 1024**3:.1f} GB"
    if size_bytes >= 1024 ** 2:
        return f"{size_bytes / 1024**2:.1f} MB"
    return f"{size_bytes / 1024:.1f} KB"


# ---------------------------------------------------------------------------
# Snapshot selection
# ---------------------------------------------------------------------------

def select_latest(snapshots: list) -> SnapshotEntry:
    if not snapshots:
        raise SystemExit("No snapshots found in S3 for this server.")
    return snapshots[0]


def select_by_spec(snapshots: list, spec: str) -> SnapshotEntry:
    """
    Match a snapshot by exact filename, full S3 key, or date (YYYY-MM-DD).
    """
    # Try date match first
    date_match = re.fullmatch(r"\d{4}-\d{2}-\d{2}", spec)
    if date_match:
        try:
            target = date.fromisoformat(spec)
        except ValueError:
            raise SystemExit(f"Invalid date '{spec}'. Expected YYYY-MM-DD.")
        matches = [s for s in snapshots if s.backup_date == target]
        if not matches:
            raise SystemExit(f"No snapshot found for date {spec}.")
        return matches[0]

    # Try filename or key match
    matches = [s for s in snapshots if s.filename == spec or s.s3_key == spec]
    if not matches:
        raise SystemExit(f"No snapshot found matching '{spec}'.")
    return matches[0]


def select_interactive(snapshots: list) -> SnapshotEntry:
    if not snapshots:
        raise SystemExit("No snapshots found in S3 for this server.")

    print("\nAvailable snapshots (newest first):\n")
    print(f"  {'#':<4} {'Date':<12} {'Format':<8} {'Size':<10} Filename")
    print(f"  {'-'*4} {'-'*12} {'-'*8} {'-'*10} {'-'*40}")
    for i, snap in enumerate(snapshots, 1):
        size_str = _fmt_size(snap.size_bytes) if snap.size_bytes else "unknown"
        print(f"  {i:<4} {snap.backup_date.isoformat():<12} {snap.disk_format:<8} {size_str:<10} {snap.filename}")

    print()
    while True:
        try:
            raw = input(f"Select snapshot [1-{len(snapshots)}] (default: 1, latest): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            raise SystemExit("Aborted.")

        if raw == "":
            return snapshots[0]
        try:
            idx = int(raw)
        except ValueError:
            print(f"  Enter a number between 1 and {len(snapshots)}.")
            continue
        if 1 <= idx <= len(snapshots):
            return snapshots[idx - 1]
        print(f"  Enter a number between 1 and {len(snapshots)}.")


# ---------------------------------------------------------------------------
# Download from S3
# ---------------------------------------------------------------------------

def download_from_s3(
    s3_client,
    bucket: str,
    s3_key: str,
    dest_path: Path,
    size_bytes: int,
    log: logging.Logger,
) -> None:
    tmp_path = dest_path.parent / (dest_path.name + ".tmp")
    size_str = _fmt_size(size_bytes) if size_bytes else "unknown size"
    log.info("Downloading s3://%s/%s (%s)...", bucket, s3_key, size_str)

    from boto3.s3.transfer import TransferConfig
    transfer_config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,
        multipart_chunksize=50 * 1024 * 1024,
        max_concurrency=4,
    )
    s3_client.download_file(bucket, s3_key, str(tmp_path), Config=transfer_config)
    tmp_path.rename(dest_path)
    log.info("Download complete: %s", dest_path)


# ---------------------------------------------------------------------------
# Upload to Glance
# ---------------------------------------------------------------------------

def upload_to_glance(
    conn,
    local_path: Path,
    image_name: str,
    disk_format: str,
    log: logging.Logger,
) -> str:
    """
    Create a Glance image from a local file.
    Returns the new image ID.
    """
    log.info("Creating Glance image '%s' (format: %s) and uploading data...", image_name, disk_format)
    image = conn.image.create_image(
        name=image_name,
        disk_format=disk_format,
        container_format="bare",
        visibility="private",
        filename=str(local_path),
    )
    log.info("Upload complete (id=%s), waiting for image to become active...", image.id)
    import time
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        img = conn.image.get_image(image.id)
        if img.status == "active":
            break
        if img.status in ("error", "killed"):
            raise RuntimeError(f"Image {image.id} entered failed state: {img.status}")
        time.sleep(15)
    else:
        raise RuntimeError(f"Image {image.id} did not become active within timeout")
    log.info("Image ready: id=%s name='%s'", image.id, image_name)
    return image.id


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Restore an OpenStack instance snapshot from S3 into Glance.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Restore latest backup of web-server-01
  %(prog)s --server web-server-01

  # Restore a specific date
  %(prog)s --server web-server-01 --snapshot 2026-03-28

  # Choose interactively from a list
  %(prog)s --server web-server-01 --interactive

  # Override the cloud (useful when server is not in config.yaml)
  %(prog)s --server web-server-01 --cloud mycloud --bucket my-backups
        """,
    )
    parser.add_argument("--server", required=True, help="Server name to restore")
    parser.add_argument(
        "--snapshot",
        metavar="DATE_OR_FILE",
        help="Specific snapshot to restore: a date (YYYY-MM-DD) or exact filename",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="List available snapshots and prompt for selection",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: config.yaml)")
    parser.add_argument("--cloud", help="Override OpenStack cloud name from config")
    parser.add_argument("--bucket", help="Override S3 bucket name from config")
    parser.add_argument("--prefix", help="Override S3 prefix from config")
    parser.add_argument("--temp-dir", help="Override temp directory from config")
    args = parser.parse_args()

    if args.snapshot and args.interactive:
        parser.error("--snapshot and --interactive are mutually exclusive.")

    setup_logging()
    log = logging.getLogger("restore")

    try:
        config = load_config(args.config)
    except Exception as exc:
        log.critical("Failed to load config '%s': %s", args.config, exc)
        sys.exit(1)

    # Build effective server config — merge CLI overrides on top of config file
    server_cfg = find_server_config(config, args.server)
    if server_cfg is None:
        if not (args.cloud and args.bucket):
            log.error(
                "Server '%s' not found in config. "
                "Use --cloud and --bucket to specify it explicitly.",
                args.server,
            )
            sys.exit(1)
        server_cfg = ServerConfig(
            cloud=args.cloud,
            name=args.server,
            bucket=args.bucket,
            prefix=args.prefix or "",
        )
    else:
        if args.cloud:
            server_cfg.cloud = args.cloud
        if args.bucket:
            server_cfg.bucket = args.bucket
        if args.prefix is not None:
            server_cfg.prefix = args.prefix

    temp_dir = Path(args.temp_dir) if args.temp_dir else config.temp_dir
    temp_dir.mkdir(parents=True, exist_ok=True)

    s3_client = build_s3_client(config)
    metadata = load_server_metadata(s3_client, server_cfg)

    # List available snapshots
    log.info("Listing snapshots for '%s' in s3://%s ...", args.server, server_cfg.bucket)
    snapshots = list_snapshots(s3_client, server_cfg)
    if not snapshots:
        log.error("No snapshots found for server '%s'.", args.server)
        sys.exit(1)
    log.info("Found %d snapshot(s)", len(snapshots))

    # Select snapshot
    if args.interactive:
        chosen = select_interactive(snapshots)
    elif args.snapshot:
        chosen = select_by_spec(snapshots, args.snapshot)
    else:
        chosen = select_latest(snapshots)
        log.info("Using latest snapshot: %s", chosen.filename)

    # Build the Glance image name from the snapshot's backup date (deterministic,
    # so restoring the same snapshot twice produces the same name and can be detected)
    base_name = chosen.filename.rsplit(".", 1)[0]  # strip extension
    glance_image_name = f"{base_name}-restored"

    log.info("Will restore '%s' as Glance image '%s'", chosen.filename, glance_image_name)

    # Check whether this snapshot has already been restored
    try:
        conn = openstack.connect(cloud=server_cfg.cloud)
        existing = conn.image.find_image(glance_image_name)
        if existing:
            log.error(
                "Snapshot '%s' has already been restored as Glance image '%s' (id=%s). "
                "It is ready to be used in OpenStack — no action taken.",
                chosen.filename, glance_image_name, existing.id,
            )
            print()
            print("This snapshot is already restored and ready to start in OpenStack.")
            print(f"  Image name : {glance_image_name}")
            print(f"  Image ID   : {existing.id}")
            print(f"  Cloud      : {server_cfg.cloud}")
            print()
            print("To launch a new instance from this image:")
            print(f"  {_build_server_create_cmd(existing.id, metadata)}")
            print()
            conn.close()
            sys.exit(0)
        conn.close()
    except Exception:
        log.exception("Failed to check Glance for existing image")
        sys.exit(1)

    # Download from S3
    local_path = temp_dir / chosen.filename
    try:
        download_from_s3(s3_client, server_cfg.bucket, chosen.s3_key, local_path, chosen.size_bytes, log)
    except Exception:
        log.exception("Failed to download snapshot from S3")
        sys.exit(1)

    # Upload to Glance
    try:
        conn = openstack.connect(cloud=server_cfg.cloud)
        image_id = upload_to_glance(conn, local_path, glance_image_name, chosen.disk_format, log)
        conn.close()
    except Exception:
        log.exception("Failed to upload image to Glance")
        sys.exit(1)
    finally:
        if local_path.exists():
            local_path.unlink()

    print()
    print("Restore complete.")
    print(f"  Image name : {glance_image_name}")
    print(f"  Image ID   : {image_id}")
    print(f"  Cloud      : {server_cfg.cloud}")
    print()
    print("To launch a new instance from this image:")
    print(f"  {_build_server_create_cmd(image_id, metadata)}")
    print()


if __name__ == "__main__":
    main()
