# misp-taxii-connector-service

Two-service stack for ingesting AlienVault OTX threat pulses into MISP via OpenTAXII.

```
┌──────────────────┐    STIX 2.1     ┌──────────────┐    STIX 2.1     ┌──────┐
│ AlienVault OTX   │  ──────────────▶│  OpenTAXII   │  ──────────────▶│ MISP │
│ (subscribed      │   push bundle   │  (Postgres)  │   poll + push   │      │
│  pulses)         │                 │              │                 │      │
└──────────────────┘                 └──────────────┘                 └──────┘
        ▲                                     ▲                            ▲
        │                                     │                            │
        │ poll                                │ HTTP poll                  │ push
        │                                     │                            │
   ┌────┴─────┐                          ┌────┴────┐                  ┌────┴────┐
   │ otx2taxii│ ───── writes ──────────▶ │ OpenTAXII│ ◀─── pulls ─── │ taxii2misp│
   │          │                          │ server  │                 │          │
   └──────────┘                          └─────────┘                 └──────────┘
```

## Components

- **`otx2taxii/`** — Polls AlienVault OTX for subscribed pulses, converts them to STIX 2.1 bundles, and pushes them to an OpenTAXII collection. Runs once per invocation (no built-in scheduler — designed to be run by cron, systemd, or a wrapper).
- **`taxii2misp/`** — Polls the OpenTAXII collection, converts STIX 2.1 objects to MISP attributes, and creates/updates MISP events.

## Features

- **Per-pulse parallel processing** — `otx2taxii` uses a thread pool (default 6 workers) to process and push multiple pulses concurrently.
- **Author filtering** — Limit processing to specific OTX authors via `OTX_AUTHOR_FILTER` env var.
- **Smart caching** — Redis-backed STIX ID cache prevents duplicate writes to OpenTAXII.
- **Pre-validation** — Filter out STIX objects that already exist in the collection before pushing.
- **Retry on transient errors** — TAXII push retries on `RemoteDisconnected` and similar transient network errors with exponential backoff.
- **Clean exit semantics** — Both services exit 0 on success, non-zero on failure, suitable for cron / systemd / K8s jobs.

## Quick start (local development)

```bash
# 1. Copy env templates
cp otx2taxii/.env.example otx2taxii/.env
cp taxii2misp/.env.example taxii2misp/.env
# Edit each .env with your real OTX/MISP/TAXII credentials

# 2. Start otx2taxii (one-shot run)
cd otx2taxii
docker compose build --no-cache
docker compose up

# 3. Start taxii2misp (one-shot run)
cd ../taxii2misp
docker compose build --no-cache
docker compose up
```

## Production deployment

See **[DEPLOY.md](DEPLOY.md)** for the full production deployment guide, including:
- Pre-flight checklist
- Server setup steps
- Scheduling with cron / systemd
- Operational runbooks (logs, restarts, troubleshooting)
- Security notes

## Configuration

Both services are configured via environment variables. See:
- `otx2taxii/.env.example` for the otx2taxii config template
- `taxii2misp/.env.example` for the taxii2misp config template

Both files list every available option with comments.

## Architecture notes

- **OpenTAXII is the data bus** between the two services. It is designed as a producer/consumer queue (TAXII 2.1 collections) and handles auth, persistence, and pagination.
- **Redis is the dedup cache** for STIX object IDs. It is shared by both services (separate DBs by default: `otx2taxii` uses DB 0, `taxii2misp` uses DB 1).
- **PostgreSQL is the OpenTAXII backing store.** The included `docker-compose.yml` for OpenTAXII uses two separate Postgres instances (one for data, one for auth).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
