# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**instance-backup** is a Python utility that automates daily backups of OpenStack instances to S3 with GFS (Grandfather-Father-Son) retention. Two scripts handle the full lifecycle:

- `backup.py` — snapshot OpenStack servers → download → upload to S3 → apply retention
- `restore.py` — list S3 backups → download → upload to Glance for instance launch

## Setup

```bash
pip install -r requirements.txt
cp config.yaml.example config.yaml
# Edit config.yaml with OpenStack cloud name, S3 settings, and server list
```

## Running

```bash
# Backup all configured servers
python backup.py --config config.yaml

# Restore latest backup for a server
python restore.py --server <server-name>

# Restore interactively (choose from list)
python restore.py --server <server-name> --interactive

# Restore a specific date
python restore.py --server <server-name> --snapshot 2024-01-15
```

No test suite or linter is configured.

## Architecture

Both scripts share the same configuration model (`AppConfig`, `ServerConfig`, `RetentionConfig` dataclasses) and S3 client setup. Each script is self-contained — there are no shared modules.

### backup.py pipeline (per server, run in parallel)

1. Create Glance snapshot (or reuse one already named for today's date)
2. Download snapshot to temp dir in 8MB chunks
3. Upload to S3 under `{prefix}/{server-name}/{server-name}-{YYYY-MM-DD}.{ext}` (multipart for >100MB)
4. Delete the Glance snapshot to free quota
5. Apply GFS retention — delete S3 keys not in the computed keep-set
6. Cleanup temp files (in `finally`)

If S3 upload fails, the Glance snapshot is intentionally preserved for retry. One server's failure does not abort others. Exit code 1 if any server failed.

### restore.py pipeline

1. List and parse S3 objects under `{prefix}/{server-name}/`
2. Select snapshot (latest / by date-or-filename / interactive table)
3. Download from S3 to temp dir
4. Upload to Glance and wait for `active` state
5. Print `openstack server create` command

### Retention logic (`compute_keys_to_keep` in backup.py)

S3 keys follow the pattern `{name}-{YYYY-MM-DD}.{ext}` (parsed by `_BACKUP_RE`). The GFS algorithm:

- **Daily**: keep the N most recent backups by date
- **Weekly**: for each of the last N weeks, keep one backup (preferring the configured `weekly_day`, e.g. `sunday`)
- **Monthly**: for each of the last N months, keep one backup (preferring the configured `monthly_day`, e.g. `1`)

Categories are non-exclusive — a single backup can satisfy multiple tiers.

### Configuration hierarchy

1. Global `retention:` block in YAML
2. Per-server `retention:` override in YAML
3. CLI flags (restore.py only: `--cloud`, `--bucket`, `--prefix`, `--temp-dir`)

S3 credentials are resolved in order: `BACKUP_S3_*` env vars → `config.yaml` explicit keys → named AWS profile → default boto3 chain.
