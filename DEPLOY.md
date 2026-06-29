# Production Deployment Guide

This guide covers deploying the `misp-taxii-connector-service` stack to a production Linux server (tested on RHEL/CentOS 7+ and Ubuntu 20.04+). It assumes the server is reachable at `10.80.120.8` for OpenTAXII and `10.80.120.12` for MISP — adjust accordingly for your environment.

## Architecture overview

Three logical pieces run on the server:

| Component | Runs as | Talks to | Schedule |
|---|---|---|---|
| `otx2taxii` | systemd timer or cron | OTX API (out), OpenTAXII (in) | Every 1 hour |
| OpenTAXII | Docker Compose | `otx2taxii` (in), `taxii2misp` (in), Postgres (backing) | Always up |
| `taxii2misp` | systemd timer or cron | OpenTAXII (out), MISP (out) | Every 1 hour |

OpenTAXII is the only long-running service. Both connector services are one-shot jobs that run periodically.

## Pre-flight checklist

Before you start:

- [ ] Server has Docker and Docker Compose v2 installed (`docker compose version` works)
- [ ] Server has Git installed
- [ ] You have an OTX API key from https://otx.alienvault.com/settings
- [ ] You have a MISP auth key from your MISP instance (Administration → List Users → Edit)
- [ ] You have OpenTAXII credentials (default is `admin` / `admin` if you haven't changed them)
- [ ] Outbound HTTPS works from the server to `otx.alienvault.com`, your MISP, and your OpenTAXII
- [ ] Inbound HTTPS to the server's port 9000 (OpenTAXII) works from your browser / curl client

## Step 1 — Install OpenTAXII stack

OpenTAXII runs as a Docker Compose stack with two Postgres backends (data + auth). It is **not** part of this repository — clone it separately or copy from your dev machine.

```bash
# On the server
mkdir -p /opt/misp-taxii-connector
cd /opt/misp-taxii-connector

# Clone this repo (replace URL with your fork if needed)
git clone https://github.com/izzatbey/misp-taxii-connector-service.git .

# Clone the OpenTAXII stack into a sibling directory
cd /opt
git clone <your-opentaxii-repo-url> opentaxii-stack
cd opentaxii-stack

# Generate self-signed TLS certs (or use your own)
mkdir -p certs
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout certs/key.pem \
  -out certs/cert.pem \
  -days 365 \
  -subj "/CN=10.80.120.8"

# Start OpenTAXII
docker compose up -d
```

Verify it's up:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
# Should see: db, authdb, opentaxii (all Up)
```

Test the discovery endpoint:
```bash
curl -k -u admin:admin -H "Accept: application/taxii+json;version=2.1" \
  https://10.80.120.8:9000/taxii2/ | jq
```

You should get a JSON response with a list of API roots.

## Step 2 — Create the OpenTAXII collection

If you haven't already, create the collection that the connectors will write to / read from. Use the OpenTAXII admin CLI or the API.

Example via API (assuming default API root exists):
```bash
# First, list the API roots
curl -k -u admin:admin https://10.80.120.8:9000/taxii2/ | jq '.api_roots[0].url'
# Use that URL to create a collection
# (See OpenTAXII docs for the exact API — this varies by version)
```

Note the **API root ID** and **Collection ID** — you'll need them later.

## Step 3 — Configure the otx2taxii service

```bash
cd /opt/misp-taxii-connector-service/otx2taxii

# Copy the env template
cp .env.example .env
nano .env   # fill in real values
```

Required `.env` values:
```env
OTX_API_KEY=<your-otx-api-key>
TAXII_URL=https://10.80.120.8:9000/taxii2/
USERNAME=admin
PASSWORD=admin
MISP_URL=https://10.80.120.12/
MISP_API_KEY=<your-misp-api-key>
REDIS_HOST=localhost
OTX_AUTHOR_FILTER=None    # or a specific author name to limit processing
MAX_BUNDLES_TO_PUSH=None  # or a number to cap per run
MAX_WORKERS=8             # 8 is safe for Postgres-backed OpenTAXII
```

### Optional: mount the OTX cache as a volume

The OTXv2 SDK stores a local cache of all pulses in `/root/.otx_cache_data/` inside the container. On a fresh container, the first run has to re-fetch everything from OTX (slow if you have many subscriptions). To skip this on subsequent deploys, add a volume to `docker-compose.yaml`:

```yaml
otx2taxii:
    volumes:
      - ./output:/app/output
      - ./logs:/app/logs
      - ./otx_cache_data:/root/.otx_cache_data   # add this
```

## Step 4 — Build and test otx2taxii

```bash
cd /opt/misp-taxii-connector-service/otx2taxii
docker compose build --no-cache
docker compose up
```

Watch the logs. A successful run ends with:
```
One-shot cycle complete. Exiting with status 0.
```

Verify OpenTAXII has the data:
```bash
curl -k -u admin:admin \
  -H "Accept: application/taxii+json;version=2.1" \
  "https://10.80.120.8:9000/taxii2/<api-root-id>/collections/<collection-id>/objects/" \
  | jq '.objects | length'
```

Should return a number > 0.

## Step 5 — Schedule otx2taxii to run periodically

This service is one-shot. Use systemd timer (recommended) or cron to run it on a schedule.

### Option A: systemd timer (recommended)

Create `/etc/systemd/system/otx2taxii.service`:
```ini
[Unit]
Description=OTX to OpenTAXII sync
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
WorkingDirectory=/opt/misp-taxii-connector-service/otx2taxii
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
RemainAfterExit=no
TimeoutStartSec=3600

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/otx2taxii.timer`:
```ini
[Unit]
Description=Run otx2taxii hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now otx2taxii.timer

# Check status
sudo systemctl list-timers otx2taxii*
sudo journalctl -u otx2taxii.service -n 50
```

### Option B: cron

Edit the crontab:
```bash
crontab -e
```

Add:
```cron
0 * * * * cd /opt/misp-taxii-connector-service/otx2taxii && /usr/bin/docker compose up >> /var/log/otx2taxii.log 2>&1
```

This runs every hour on the hour.

## Step 6 — Configure and run taxii2misp

Same pattern as otx2taxii:
```bash
cd /opt/misp-taxii-connector-service/taxii2misp
cp .env.example .env
nano .env   # fill in real values
```

See `taxii2misp/.env.example` for all options.

## Operational runbooks

### Check the latest run

```bash
# systemd
sudo journalctl -u otx2taxii.service -n 100 --no-pager

# cron
tail -n 100 /var/log/otx2taxii.log
```

### Manually trigger a run

```bash
# systemd
sudo systemctl start otx2taxii.service

# cron / direct
cd /opt/misp-taxii-connector-service/otx2taxii
docker compose up
```

### Clear the dedup cache (force re-push)

If you want to force re-pushing all STIX objects (e.g., after a schema change):
```bash
docker exec -it redis redis-cli FLUSHDB
```

⚠️ This wipes everything in Redis DB 0 (taxii_ids cache, STIX UUID cache, OTX timestamp cache). On the next run, all objects will be re-pushed.

### Check what's in the OpenTAXII collection

```bash
# Count of objects
curl -k -u admin:admin \
  -H "Accept: application/taxii+json;version=2.1" \
  "https://10.80.120.8:9000/taxii2/<api-root-id>/collections/<collection-id>/objects/" \
  | jq '.objects | length'

# Inspect a specific object
curl -k -u admin:admin \
  -H "Accept: application/taxii+json;version=2.1" \
  "https://10.80.120.8:9000/taxii2/<api-root-id>/collections/<collection-id>/objects/<object-id>/" \
  | jq .
```

### Restart everything

```bash
# OpenTAXII
cd /opt/opentaxii-stack
docker compose restart

# otx2taxii (stops the timer, runs once)
sudo systemctl start otx2taxii.service
```

## Security notes

- **`otx2taxii/.env` and `taxii2misp/.env` contain real secrets.** They are gitignored. **Never commit them.**
- The bundled `LICENSE` is Apache 2.0 — keep it.
- The default OpenTAXII credentials are `admin` / `admin`. **Change these in production** by editing the `OPENTAXII_USER` and `OPENTAXII_PASS` env vars in the OpenTAXII `docker-compose.yml`, then update the matching `USERNAME` and `PASSWORD` in `otx2taxii/.env` and `taxii2misp/.env`.
- The bundled self-signed TLS certs (`cert.pem` / `key.pem`) are for testing. **Use real certs (e.g., Let's Encrypt) in production.**

## Troubleshooting

### "OTX API exhausted retries" / 504 errors

AlienVault OTX has occasional upstream 5xx errors. The service handles this by:
1. Raising `OTXAPIUnavailable` 
2. Sleeping 5 minutes
3. Exiting with status 1
4. systemd / cron retries on the next scheduled run

If this is frequent, check https://status.alienvault.com for outages.

### "Connection aborted" / "Remote end closed connection"

Usually means the bundle is too large for OpenTAXII to process in one request, OR the Gunicorn worker timed out. Mitigations:
- The new code (since v0.2) retries on this error with 2s/4s/8s backoff.
- If it still fails, increase the Gunicorn timeout in the OpenTAXII Dockerfile (`--timeout=900`) or split large pulses (workaround: filter to authors with smaller pulses via `OTX_AUTHOR_FILTER`).

### Collection stays empty after a run

1. Check the otx2taxii log for `[✔] Bundle pushed successfully. Status: complete` — if missing, the push failed.
2. Check the OpenTAXII log: `docker logs opentaxii` for any errors.
3. Test write directly: see the manual POST test in the project's runbook.
4. Verify the user `admin` has write permission on the collection (TAXII 2.1 separates read and write ACLs).

### Container is in a restart loop

```bash
docker ps -a --filter "name=otx2taxii"
# Look at the most recent exit code

docker logs --tail 100 otx2taxii-scheduler
```

Common causes: bad env vars, OTX API outage (we now back off 5 minutes), or network unreachability.

## Updates / redeployment

```bash
cd /opt/misp-taxii-connector-service
git pull
cd otx2taxii
docker compose build --no-cache
docker compose down
docker compose up
```

Repeat for `taxii2misp` if it also changed.
