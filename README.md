# instance-backup

A daily backup solution for OpenStack instances. Creates Glance snapshots, downloads them locally, uploads to S3, and enforces a Grandfather-Father-Son (GFS) retention policy.

## How it works

A cron job runs `backup.py` once per day. For each configured server it:

1. Connects to OpenStack using credentials from `clouds.yaml`
2. Creates a Glance snapshot named `{server}-{YYYY-MM-DD}`
3. Downloads the snapshot to a local temp directory
4. Uploads it to the configured S3 bucket (multipart for large images)
5. Deletes the Glance snapshot to free quota
6. Applies GFS retention rules and removes expired backups from S3

One server failing does not abort the others. The script exits with a non-zero status if any server failed, so cron's error reporting (e.g. `MAILTO`) fires appropriately.

## Setup

```bash
pip install -r requirements.txt
cp config.yaml.example config.yaml
# Edit config.yaml with your servers, S3 settings, and retention policy
# Ensure clouds.yaml is in place (see config.yaml.example for format)
python backup.py --config config.yaml
```

## Configuration

**`clouds.yaml`** — standard OpenStack credential file. Default search paths:
- `./clouds.yaml`
- `~/.config/openstack/clouds.yaml`
- `/etc/openstack/clouds.yaml`

**`config.yaml`** — backup configuration:

| Section | Description |
|---|---|
| `s3` | Endpoint, region, and credentials (also readable from env vars) |
| `temp_dir` | Local directory for temporary snapshot downloads |
| `retention` | Global GFS retention defaults |
| `servers` | List of servers to back up, each with `cloud`, `name`, `bucket`, optional `prefix` and per-server `retention` overrides |

S3 credentials can be set via environment variables instead of the config file:
`BACKUP_S3_ENDPOINT`, `BACKUP_S3_REGION`, `BACKUP_S3_ACCESS_KEY`, `BACKUP_S3_SECRET_KEY`

## Retention policy (GFS)

Three independent rules — a backup satisfying any one is kept. A single S3 object can satisfy multiple rules without duplication.

| Rule | Setting | Behaviour |
|---|---|---|
| **Daily** | `daily: 7` | Keep the N most recent backups |
| **Weekly** | `weekly: 4`, `weekly_weekday: 0` | Keep one per calendar week for the last N weeks, preferring the configured weekday |
| **Monthly** | `monthly: 6`, `monthly_day: 1` | Keep one per calendar month for the last N months, preferring the configured day |

Only files matching the pattern `{server}-{YYYY-MM-DD}.{ext}` are considered for deletion. Any other files in the same S3 prefix are left untouched.

## Cron setup

```
0 2 * * * /path/to/venv/bin/python /path/to/backup.py --config /etc/openstack-backup/config.yaml >> /var/log/openstack-backup.log 2>&1
```

## Restoring a snapshot

`restore.py` downloads a snapshot from S3 and uploads it to Glance so a new instance can be booted from it. The restored image is named `{original-snapshot-name}-restored-{timestamp}` to avoid collisions.

**Restore latest backup (default):**
```bash
python restore.py --server web-server-01
```

**Restore a specific date:**
```bash
python restore.py --server web-server-01 --snapshot 2026-03-28
```

**Interactive mode — lists all available snapshots and prompts for selection:**
```bash
python restore.py --server web-server-01 --interactive
```

```
Available snapshots (newest first):

  #    Date         Format   Size       Filename
  ---- ------------ -------- ---------- ----------------------------------------
  1    2026-03-31   qcow2    12.4 GB    web-server-01-2026-03-31.qcow2
  2    2026-03-30   qcow2    12.3 GB    web-server-01-2026-03-30.qcow2
  3    2026-03-23   qcow2    12.1 GB    web-server-01-2026-03-23.qcow2

Select snapshot [1-3] (default: 1, latest):
```

After a successful restore the script prints the Glance image ID and an `openstack server create` command you can run to boot a new instance:

```
Restore complete.
  Image name : web-server-01-2026-03-31-restored-20260331T142500
  Image ID   : a1b2c3d4-...
  Cloud      : production

To launch a new instance from this image:
  openstack server create --image a1b2c3d4-... --flavor <flavor> --network <network> <new-server-name>
```

**Options:**

| Flag | Description |
|---|---|
| `--server` | Server name to restore (required) |
| `--snapshot` | Date (`YYYY-MM-DD`) or exact filename to restore |
| `--interactive`, `-i` | Prompt for snapshot selection |
| `--config` | Path to `config.yaml` (default: `config.yaml`) |
| `--cloud` | Override OpenStack cloud from config |
| `--bucket` | Override S3 bucket from config |
| `--temp-dir` | Override temp directory from config |

If the server is not in `config.yaml`, use `--cloud` and `--bucket` together to specify it on the command line.
