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

---

## OTX→TAXII Performance & Architecture

The `otx2taxii` service has gone through several rounds of
optimisation. This section documents the **current architecture** and
how to roll back if anything misbehaves.

### Two-process (outbox) architecture — the default

The service now runs **two cooperating processes** inside one
container, supervised by `supervisor.sh`:

```
   ingest.py  ──►  stix_outbox/pending/*.json  ──►  main.py
   (OTX)        (chunk JSON files on disk)        (TAXII)
   ~50 MB                                          ~50 MB
   peak RAM                                        peak RAM
```

Why this exists: the original single-process design had to
simultaneously hold the OTXv2 SDK + the on-disk OTX cache walk +
the full STIX bundle in memory, which peaked at **~14 GB RAM** on
workloads dominated by w0rmsign-style "Server Scanning YYYY-MM-DD"
pulses (~547 indicators each, 1423+ pulses).

Splitting the work means **each process holds one task at a time**:

- **`ingest.py`** (the writer) — connects to OTX, walks subscribed
  pulses one at a time, fetches indicators for the current pulse
  only, builds STIX chunks of at most `MAX_INDICATORS_PER_PULSE`
  indicators each, writes one JSON file per chunk to
  `stix_outbox/pending/`, then releases the memory and moves on.
- **`main.py`** (the pusher) — scans `stix_outbox/pending/`, reads
  one chunk JSON, pushes to TAXII, moves the file to
  `stix_outbox/processed/` on success, leaves it in `pending/` on
  failure for the next cycle to retry.

Each process keeps its own scheduler loop and signal handling.

### Pulse chunking

Pulses with more than `MAX_INDICATORS_PER_PULSE` indicators (default
200) are split into multiple chunks. Each chunk becomes its own
STIX bundle with a unique Grouping SDO and a name like:

```
Server Scanning 2026-06-15 - 1/3
Server Scanning 2026-06-15 - 2/3
Server Scanning 2026-06-15 - 3/3
```

A 547-indicator pulse produces 3 chunks (200, 200, 147). Indicator
IDs are deterministic across chunks (derived from `pulse_id +
indicator_type + pattern`), so re-runs never duplicate.

### Outbox directory layout

```
stix_outbox/
├── pending/        # writer puts chunks here; reader drains it
│   ├── pulse-abc__1__3__200.json
│   ├── pulse-abc__2__3__200.json
│   └── pulse-abc__3__3__147.json
└── processed/      # reader moves successful pushes here; auto-cleaned after OUTBOX_RETENTION_DAYS
```

Each chunk filename embeds `<pulse_id>__<chunk_idx>__<chunk_total>__<indicator_count>`
so files sort deterministically and can be sanity-checked without
re-parsing the JSON.

### Configuration knobs

| Env var | Default | Purpose |
|---|---|---|
| `ENABLE_OUTBOX_MODE` | `true` | When `false`, falls back to the legacy single-process OTX→TAXII path |
| `STIX_OUTBOX_DIR` | `/app/stix_outbox` | Where chunks are written |
| `INGEST_INTERVAL_SECONDS` | `3600` | How often ingest.py re-runs in `--loop` mode |
| `OTX_BACKOFF_SECONDS` | `300` | Back-off when OTX is unavailable |
| `OUTBOX_RETENTION_DAYS` | `7` | Auto-delete processed/ files older than this (0 = keep forever) |
| `MAX_INDICATORS_PER_PULSE` | `200` | Pulse chunking threshold (0 = no chunking) |
| `MAX_WORKERS` | `1` | Thread-pool size in main.py for parallel chunk pushes |
| `MAX_BUNDLES_TO_PUSH` | `None` | Soft cap on chunks pushed per cycle (None = unlimited) |
| `SCHEDULER_INTERVAL_SECONDS` | `3600` | main.py cycle interval |

### Rolling back to the legacy path

If the new architecture causes issues:

1. Set `ENABLE_OUTBOX_MODE=false` in `.env`.
2. In `Dockerfile`, change `CMD ["./supervisor.sh"]` back to
   `CMD ["python", "-u", "main.py"]`.
3. Rebuild and redeploy.

The legacy `process_otx_to_taxii` path is still fully functional
inside `main.py`; it's just no longer the default.

### Smoke test

`otx2taxii/smoke_test_outbox.py` exercises the outbox contract with
mocked OTX and TAXII clients:

```bash
python otx2taxii/smoke_test_outbox.py
```

It validates 7 scenarios:
1. Filename generation
2. Disk listing + ordering
3. Successful push moves file pending → processed
4. Failed push keeps file in pending for retry
5. Corrupt JSON is moved to processed (skipped)
6. End-to-end cycle: write 3 chunks, push all 3, verify move
7. Empty pending dir is a no-op

Expected output: `=== 7 passed, 0 failed, 7 total ===`.

### Resource budget (expected after the two-process split)

| Process | Peak RAM | Notes |
|---|---|---|
| `ingest.py` | ~50–100 MB | One pulse + its indicators at a time, plus OTXv2 cache walk (~120 MB for the cached pulse list inside `getall()`) |
| `main.py` | ~50 MB | One chunk JSON file at a time (~2–5 MB per chunk) |

Combined container peak: **~150–250 MB** instead of the previous
14 GB. If you still see high RAM, set `OTX_CACHE_CLEAR_ON_START=true`
once to wipe the bloated on-disk OTX cache, then back to `false`.

### What to watch in logs

- `[ingest] Wrote pulse-abc__1__3__200.json (1/3 for 'Server Scanning 2026-06-15')`
- `[main] Pushing chunk 1/3 (200 indicators) to TAXII...`
- `[main] Successfully pushed chunk 1/3.`
- `[main/outbox] Cycle complete: 4269 pushed, 4269 processed, 0 failed.`

If you see `Cycle complete: N pushed, M failed` with `M > 0`, the
chunks are still in `pending/` and will retry on the next cycle.
Check the TAXII server logs / credentials first.
