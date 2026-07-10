# SESSION.md — taxii2misp + taxii-mv-proxy migration

> **Purpose:** This is a single, self-contained session brief for the OpenCode /
> agentic assistant that will continue this work. Read it top-to-bottom before
> running any commands. It captures the entire conversation history, the bugs
> found, the fixes applied, and the **current open question** (empty database).
>
> Nothing depends on memory outside this file. The full chain of reasoning —
> from "30-minute connector stalls" through "we built an MV-backed proxy that
> worked at 314ms" through "the DB schema is currently empty" — is recorded here.

---

## 1. Environment snapshot

| Component | Where it lives | Specs / state |
|---|---|---|
| Wazuh server (host) | `10.80.150.113` | AlmaLinux; **host `/dev/shm` is 7.7GB with 0% used** (irrelevant). `/data/docker` 389GB, 304GB free. |
| MISP | `https://10.80.150.113/` (MISP_PORT=50000 mapped to 80) | MISP 2.5.42 |
| OpenTAXII | docker container `examples-opentaxii-1` (`docker compose -f /data/opentaxii/examples/docker-compose-local.yml`) | image: `examples-opentaxii` (the same project builds it from `Dockerfile` with `command: ... gunicorn ... --timeout=600`) |
| Postgres `db-1` | `examples-db-1` | `postgres:13`; **CURRENTLY EMPTY — no tables at all**. Volume ID `62ef8a272fbbbc62a0344a44f0fa49647d9c5238469994ad5f296ea5e2b4108c` mounted at `/var/lib/postgresql/data` — data state to be verified (see Section 9). |
| Postgres `authdb-1` | `examples-authdb-1` | separate Postgres for auth_api. Not in scope. |
| **MV-backed TAXII proxy** | `/data/misp-taxii-connector-service/taxii-mv-proxy/` (this is the new piece — see Section 6) | FastAPI on port **9001** inside the container, host port **9001:9001**. Connected via docker network `examples_default`. |
| taxii2misp-connector | `/data/misp-taxii-connector-service/taxii2misp/` | refactored to streaming v2 (see Section 7). |
| Redis (`redis-taxii2misp`) | default port 6379, db=1 | stores dedup hashes and per-collection cursor checkpoints |
| MISP proxy OTX → OpenTAXII | `/data/misp-taxii-connector-service/otx2taxii/` | pushes OTX pulses into OpenTAXII |

### Container name shortlist (aliases you'll see in `docker ps`)

| Alias | Real container |
|---|---|
| `opentaxii-1` | `examples-opentaxii-1` |
| `db` | `examples-db-1` |
| `db-1` | `examples-db-1` |
| `authdb-1` | `examples-authdb-1` |
| `taxii2misp-connector` | `taxii2misp-taxii2misp-connector` |

The `examples_` prefix comes from the compose project name. DNS inside the user-defined
docker network resolves these names without the prefix; from the host you must use
the full `examples-XXX-1` form.

### Key file paths

```
repo root:                          /data/misp-taxii-connector-service/
proxy:                              taxii-mv-proxy/proxy.py
                                     taxii-mv-proxy/.env
                                     taxii-mv-proxy/docker-compose.yml
                                     taxii-mv-proxy/Dockerfile
                                     taxii-mv-proxy/requirements.txt
connector:                          taxii2misp/main.py
                                     taxii2misp/clients/taxii_client.py
                                     taxii2misp/.env
                                     taxii2misp/docker-compose.yml
                                     taxii2misp/clients/stix_processor.py
OpenTAXII:                          /data/opentaxii/examples/docker-compose-local.yml
                                     /data/opentaxii/examples/data-configuration*.yml
                                     /data/opentaxii/Dockerfile  (builds gunicorn entrypoint)
otx2taxii:                          /data/misp-taxii-connector-service/otx2taxii/docker-compose.yaml
```

> Working directory is whichever one you ran a `cd` into at the start of the
> session. The assistant on the **wazuh server** must run commands in
> `/data/opentaxii/examples`, `/data/misp-taxii-connector-service/taxii-mv-proxy`,
> or `/data/misp-taxii-connector-service/taxii2misp` as appropriate.

---

## 2. The original problem

`taxii2misp-connector` was stalling for **30+ minutes** per run. Symptoms:

- Periodic `Failed to parse STIX object: ... 'c' ...` warnings coming from the
  Python STIX 2.1 parser — these are real but **were never the cause of the stall**;
  see Section 7 for why.
- Repeated `Full-collection fetch failed: ... RemoteDisconnected` errors from
  `opentaxii.v21.as_pages` against the OpenTAXII server.
- Gunicorn workers being killed every ~6 minutes with `WORKER TIMEOUT (pid:N)
  Worker (pid:N) was sent SIGKILL! Perhaps out of memory?` because the default
  `--timeout 30` was too short for slow queries.
- Postgres eating 31% of host RAM while doing a giant
  `SELECT id::text, max(version) GROUP BY id` over 8M rows per page.
- The connector's `_fetch_all_stix_objects` was buffering the **entire collection
  in memory** then slicing — easily OOM at scale.

Initial goal: stop the 30-minute stalls without rewriting everything.

---

## 3. Bug #1 — `RemoteDisconnected` was a misread exception symbol

The connector had a `try/except` tuple including `requests.exceptions.RemoteDisconnected`.
On the runtime installed in the connector image, **`requests.exceptions.RemoteDisconnected`
does not exist as a top-level attribute**. It's an alias of `ConnectionError`.
The `except` clause itself raised `AttributeError`, which we masked:

```
WARNING - Full-collection fetch failed: module 'requests.exceptions' has no attribute 'RemoteDisconnected'
```

so every fetch silently returned `[]`, every cycle processed zero objects, and
every `Marker Check (Stale)` waited 5s.

### Fix in `taxii2misp/clients/taxii_client.py`

Replace the brittle except-tuple with:

```python
_remote_disconnected_exc = None
def _resolve_remote_disconnected():
    global _remote_disconnected_exc
    if _remote_disconnected_exc is not None:
        return _remote_disconnected_exc
    try:
        from requests.exceptions import RemoteDisconnected as _rd
        _remote_disconnected_exc = _rd
    except ImportError:
        from requests.exceptions import ConnectionError as _ce
        _remote_disconnected_exc = _ce
    return _remote_disconnected_exc

_transport_errors = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.Timeout,
    OSError,
    ConnectionResetError,
    _resolve_remote_disconnected(),   # resolved lazily inside raise handler
)
```

Result: 30-minute stall narrowed to "1 minute timeout per fetch" — still slow
but at least fails fast and retries instead of pretending success.

---

## 4. Bug #2 — Postgres open-taxii query path takes 2 minutes per page

Even when OpenTAXII returned data, the underlying query was pathological. Log
fragments (db-1 container logs) showed:

```sql
SELECT pk, id, collection_id, type, spec_version,
       date_added, version, serialized_data
FROM   opentaxii_stixobject
WHERE  collection_id = 'f14ba215-3f4c-4106-9979-2c818f6d9e76'::UUID
  AND  opentaxii_stixobject.pk IN (
    SELECT opentaxii_stixobject.pk
    FROM   opentaxii_stixobject
    JOIN   ( SELECT id, max(version) AS max_version
             FROM   opentaxii_stixobject
             WHERE  collection_id = 'f14ba215-...'::UUID
             GROUP BY id ) AS anon_1
       ON  opentaxii_stixobject.id = anon_1.id
      AND  opentaxii_stixobject.version = anon_1.max_version
  )
ORDER BY date_added, opentaxii_stixobject.id
LIMIT 1000
```

The inner `max(version) GROUP BY id` over **8.2M rows** is a sequential aggregate
and an on-disk sort (`Disk Sort: 2.3GB`). It re-runs on every page request, so
gunicorn workers time out (note: --timeout=600 is now in the OpenTAXII Dockerfile
to mitigate the SIGKILLs, but it doesn't fix the perf).

### Fix — Materialized View backed proxy

Rather than fix the SQL in OpenTAXII upstream, we **front it with a
Postgres-views-the-table-directly read proxy** that bypasses OpenTAXII's SQL and
serves the data via FastAPI.

#### Step 1 — Materialized View in Postgres

```sql
CREATE MATERIALIZED VIEW opentaxii_stixobject_latest
AS
  SELECT DISTINCT ON (collection_id, id)
         pk, id, collection_id, type, spec_version,
         version, date_added, serialized_data
  FROM   opentaxii_stixobject
  ORDER BY collection_id, id, version DESC;
```

This deduplicates multiple versions per `(collection_id, id)` in a single
scan, using the `(collection_id, id, version DESC)` UNIQUE constraint as the
underlying ordering.

#### Step 2 — Indexes

```sql
-- covers /manifest/?next=... cursor walks
CREATE UNIQUE INDEX idx_latest_col_date_id
  ON opentaxii_stixobject_latest (collection_id, date_added, id, pk);

-- covers /manifest/?id=... and /objects/?id=... lookups by pk
CREATE UNIQUE INDEX idx_latest_pk
  ON opentaxii_stixobject_latest (pk);

-- covers collection listing
CREATE INDEX IF NOT EXISTS idx_latest_collection_id
  ON opentaxii_stixobject_latest (collection_id, id);

ANALYZE opentaxii_stixobject_latest;
```

Verified EXPLAIN on a `LIMIT 1000` page-row scan: 314 ms (was 1m56s).

#### Step 3 — Background refresh

The MV goes stale as new objects land in OpenTAXII. Schedule:

```cron
*/15 * * * *  docker exec examples-db-1 psql -U security -d opentaxii -c \
              "REFRESH MATERIALIZED VIEW CONCURRENTLY opentaxii_stixobject_latest;" \
              >> /var/log/mv-refresh.log 2>&1
```

The proxy points at the MV, so even though the MV is up to 15 min behind the base
table, that's the worst-case data freshness we expose. The connector deduplicates
by Redis-keyed id so duplicates from MV ↔ base table during the refresh window
are harmless.

#### Step 4 — Operational gotcha observed during deployment

`REFRESH MATERIALIZED VIEW CONCURRENTLY` requires the unique index I created
in Step 2. Without `idx_latest_pk` (UNIQUE), the refresh fails with
`cannot REFRESH MATERIALIZED VIEW CONCURRENTLY`. The fix shipped in the same
deploy.

---

## 5. Bug #3 — Host `/dev/shm` was actually fine; container `/dev/shm` was 64MB

After everything was stable and working end-to-end (proxy: 314ms per page; connector:
Page N yielded; MISP events published), one specific page (page 373) started
failing with a hard 500:

```
psycopg2.errors.DiskFull: could not resize shared memory segment
"/PostgreSQL.1600358559" to 50438144 bytes: No space left on device
```

Diagnosed (NOT a host disk problem):

```bash
docker exec examples-db-1 df -h /dev/shm
# shm  64M  1.8M  63M  3% /dev/shm   ← container cap, default Docker
```

vs.

```bash
df -h /dev/shm
# tmpfs  7.7G  0  7.7G  0% /dev/shm  ← host has plenty
```

The cause: `db-1` was started with `command: postgres -c shared_buffers=4GB -c work_mem=256MB`,
and a hash aggregate hit the docker-default 64 MB `/dev/shm` cap before the
host's tmpfs was even relevant. The error name `DiskFull` is misleading — it's
an allocation failure inside a tmpfs capacity.

### Fix — `shm_size: '2gb'` in db-1 (already applied)

`/data/opentaxii/examples/docker-compose-local.yml`, under the `db:` service:

```yaml
  db:
    image: postgres:13
    shm_size: '2gb'                      # ← added
    environment:
      POSTGRES_USER: security
      POSTGRES_PASSWORD: xxq2IUjfaQtchg
      POSTGRES_DB: opentaxii
    command: postgres -c shared_buffers=4GB -c work_mem=256MB
```

Verify in the wazuh host compose: above is what we have today.

**Belt-and-suspenders fallback** (in case shm_size ever removed):

```bash
docker exec examples-db-1 psql -U security -d opentaxii -c \
  "ALTER SYSTEM SET work_mem = '32MB'; \
   SELECT pg_reload_conf();"
```

32 MB aggregate size fits comfortably in the default 64 MB cap. Tradeoff: per-page
latency climbs by ~100 ms but stays under 2s; total walk time grows ~3 minutes.

---

## 6. taxii-mv-proxy — the FastAPI service that bypasses OpenTAXII

Built as a sibling project: `/data/misp-taxii-connector-service/taxii-mv-proxy/`.

### Files in this directory

- `proxy.py` — FastAPI app (~570 lines). Endpoints:
  - `GET /taxii2/` — discovery, served from MV row `opentaxii_collection`
  - `GET /healthz` — liveness, runs `SELECT count(*) FROM opentaxii_stixobject_latest`
  - `GET /taxii2/{api_root}/` — API root metadata (served from MV)
  - `GET /taxii2/{api_root}/collections/` — collection list (from MV)
  - `GET /taxii2/{api_root}/collections/{cid}/` — single collection metadata
  - `GET /taxii2/{api_root}/collections/{cid}/manifest/?limit=N&next=...` — MV-backed,
    cursor format `<date_added>|<stix_id>|<pk_uuid>`
  - `GET /taxii2/{api_root}/collections/{cid}/manifest/{obj_id}/` — single-object manifest
  - `GET /taxii2/{api_root}/collections/{cid}/objects/?id=a,b,c&limit=N&next=...` —
    MV-backed, returns full STIX objects from `serialized_data`
  - `GET /taxii2/{api_root}/collections/{cid}/objects/{obj_id}/` — single object lookup
- `Dockerfile` — python:3.11-slim + libpq-dev + uvicorn. CMD forces `--workers 1`
  (psycopg2 single connection per process).
- `docker-compose.yml` — uses `examples_default` external network. **No
  `env_file:` directive;** env is read from a `.env` file inside the compose
  project directory via Python's `python-dotenv`.
- `requirements.txt` — FastAPI, uvicorn[standard], psycopg2-binary, pydantic,
  python-dotenv, requests.
- `.env` (overrides `.env.example`):
  ```
  POSTGRES_HOST=db
  POSTGRES_PORT=5432
  POSTGRES_DB=opentaxii
  POSTGRES_USER=security
  POSTGRES_PASSWORD=xxq2IUjfaQtchg
  DEFAULT_COLLECTION_ID=f14ba215-3f4c-4106-9979-2c818f6d9e76
  UPSTREAM_TAXII_URL=http://opentaxii:9000/taxii2/
  PROXY_HOST=0.0.0.0
  PROXY_PORT=9001
  PROXY_PUBLIC_URL=http://10.80.150.113:9001/taxii2/
  DEFAULT_PAGE_SIZE=200
  MAX_PAGE_SIZE=1000
  LOG_LEVEL=INFO
  ```
  Note `PROXY_PUBLIC_URL=http://` not `https://` (no TLS termination in proxy yet).

### FastAPI route dispatch contract

Mount prefix is `/taxii2`. The dynamic handler is registered as:

```python
@app.api_route("/taxii2/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
async def root_dynamic(full_path: str, request: Request):
    full_path = "taxii2/" + full_path.lstrip("/")   # strip+reattach fix
    # ...regex match against:
    #   taxii2/([^/]+)/collections/([^/]+)/manifest/?
    #   taxii2/([^/]+)/collections/([^/]+)/manifest/([^/]+)/?
    #   taxii2/([^/]+)/collections/([^/]+)/objects/?
    #   taxii2/([^/]+)/collections/([^/]+)/objects/([^/]+)/?
    # if no match → upstream_pass-through(req)
```

**Important gotcha**: FastAPI's `{full_path:path}` strips the prefix where you
mount the route. Mounting at `/taxii2/{full_path:path}` means we get only
`{api_root}/collections/...` etc. — without the `taxii2/` prefix. The
explicit `full_path = "taxii2/" + full_path.lstrip("/")` reattach is required
for the regex patterns to match. **No change has been made to that reattach
line in any subsequent patch.**

### Cursor format

`<date_added>|<stix_id>|<pk_uuid>` — three fields separated by `|`. URL-encoded as
`%7C`. The `pk_uuid` is a uuid (NOT a STIX id) and gets `::uuid` cast when
passed back into the `(date_added, id, pk) > (%s, %s, %s::uuid)` row-tuple
comparison in the cursor predicate. **This is critical and has been the source
of multiple false-starts in this session.** See Bug #8 below.

### `ps: 9001` on the host

Inside the docker `examples_default` user-defined network, the proxy's hostname
is the service name in compose (`taxii-mv-proxy`) and its port is `9001`. From
the **host**, the proxy is reached at `http://localhost:9001` (because the host
port mapping `9001:9001` is set). From **MISP/taxii2misp on the host**, the
connector was changed to use `http://10.80.150.113:9001/taxii2/` (DISCOVERY_URL
in `taxii2misp/.env`).

---

## 7. taxii2misp-connector refactor — streaming v2

`taxii2misp/clients/taxii_client.py::get_all_objects_with_resource_management`
was a **buffer-the-whole-collection-then-slice** generator. At 8M rows this
OOMs the connector and stalls on memory pressure. It was rewritten as
**streaming v2**: the connector pulls `/manifest/` pages, accumulates up to
`object_fetch_chunk` STIX ids, then bulk-fetches `/objects/?id=...` for those,
emits a Python `batch_objects` list, waits for the caller (`main.py`) to finish
processing, then advances the cursor.

### Key invariants

- `object_fetch_chunk` is read from `settings.TAXII_CHUNK_SIZE` (env
  `TAXII_CHUNK_SIZE`) via `main.py`'s `TAXIIClient(chunk_size=...)` →
  `self.chunk_size`. The streaming v2 caps it at `min(..., 500)` to stay under
  URL-length limits. Default is 500. The chunk size *is* env-driven and was
  not hardcoded at the time of the last commit.
- Cursor checkpoint is in Redis: key
  `taxii_global_progress:{collection.id}` value `{last_pk_index, last_date_added, last_stix_id, total_processed, timestamp}`.
- Completion key: `taxii_completion:{collection.id}`, with TTL 86400, so the next
  run knows whether to resume-from-checkpoint or start fresh.
- Restarting the connector picks up from the checkpoint automatically.
- `consecutive_failures` cap is `page_retries + 1` (so `page_retries=3` →
  4). After 4 consecutive transport failures the connector logs `Giving up
  after 4 consecutive manifest failures — bailing out of run; checkpoint
  preserved` and the cycle sleeps for `30 seconds` before retrying.

### Connector logs that mean "it's working":

```
INFO clients.taxii_client - 📡 Streaming TAXII collection via /manifest/ → /objects/
       (manifest_batch_ids=500, object_fetch_chunk=500, batch_size=10000, resume=...)
INFO clients.taxii_client -   Page 1: yielded +500 objects (total_yielded=500)
INFO __main__ - 📦 Processing batch N with 500 objects
INFO clients.taxii_client - Created memory store with 500 objects
INFO __main__ - 📊 Progress: ... objects processed across ... batches
```

### And `events published to MISP` after we restored grouping aggregation

The flow in `taxii_client.py` requires `extract_grouping_objects` to return a
non-empty `grouping_ids` list for `process_batch_groupings` to emit MISP
events. The original source-data has no grouping SDOs, so we shipped two
defensive layers:

1. (Earlier — now removed) **Synthetic grouping**: proxy would append one
   `grouping--{uuid5}` SDO per `/objects/` response. Was reverted because the
   synthetic indicator/grouping caused MISP-side ingestion crashes — see
   Section 8 "Rollback".
2. (Kept, but with safer semantics) **Connector grouping-id extraction
   happens BEFORE the parse() call** so a `parse()` exception can't drop the
   grouping id:

```python
# In extract_grouping_objects():
for obj in stix_objects:
    # Step 1: collect grouping ids WITHOUT parsing
    if isinstance(obj, dict):
        if obj.get("type") == "grouping":
            grouping_ids.append(obj.get("id"))
    elif hasattr(obj, "type"):
        if obj.type == "grouping":
            grouping_ids.append(obj.id)
    # Step 2: try to parse; on failure skip (do NOT inject raw dicts into
    # MemoryStore — that was the second crash source)
    try:
        if isinstance(obj, dict):
            parsed_objects.append(parse(obj, allow_custom=True))
        elif hasattr(obj, "type"):
            parsed_objects.append(obj)
    except Exception as parse_error:
        logger.warning(f"Failed to parse STIX object: {parse_error}")
        continue
```

---

## 8. Rollback — synthetic indicator/grouping was reverted

On 2026-07-10, the user reported the connector was generating events **but
crashing**. The cause was the proxy's earlier "synthetic indicator + synthetic
grouping" logic:

- The synthetic grouping's `object_refs` referenced indicators that were also
  a result of upstream data that lacked a proper `type` field. The `python-stix2`
  parser, when parsing the synthetic grouping SDO, ran into `UnboundLocalError:
  cannot access local variable 'c'` — known bug in stix2 v2.1's `_valid_id`
  parsing path. This crashed the connector before any MISP event publish.
- The previous "raw dict fallback" we added in
  `extract_grouping_objects` mixed dicts into the `MemoryStore`, which then
  crashed in the MISP-publisher path that expected real STIX SDOs.

### What I removed (in the local repo and pushed to wazuh host)

Both blocks are gone from `taxii-mv-proxy/proxy.py`:

```python
# REMOVED — was at lines ~383-409 (synthetic STIX indicator wrap)
if isinstance(obj, dict) and "type" not in obj:
    pattern = obj.get("pattern") or ""
    obj = {"type": "indicator", "id": ..., ...}

# REMOVED — was at lines ~423-470 (synthetic grouping synthesis)
if objs and not any((isinstance(o, dict) and o.get("type") == "grouping") for o in objs):
    ref_ids = sorted(o.get("id") for o in objs ...)
    grouping_id = f"grouping--{uuid.uuid5(...)}"
    objs.append({...})
```

Also removed in `taxii2misp/clients/taxii_client.py`:

```python
# REMOVED — was a "raw dict fallback" that mixed unparsed dicts into the
# MemoryStore when stix2 parse() failed. Caused second-stage crashes.
if isinstance(obj, dict) and obj.get("pattern"):
    parsed_objects.append(obj)   # ← removed
```

### What kept happening after rollback

After removing the synthetic-grouping + raw-dict-fallback, the connector
proceeded farther into the walk. It recovered 2,998,920 objects (visible in
resume-state messages). At that point it hit page 373 where the proxy then
failed with `psycopg2.errors.DiskFull` (Section 5). Same data point, same
parser problem — the work-around went about as deep as it could before
running into the real /dev/shm bug.

---

## 9. Bug #4 — Database is now empty (current open question)

While debugging the `/dev/shm` problem, the assistant told the user to:

1. Add `shm_size: '2gb'` to db-1 ✅ (verified in compose)
2. Restart db-1 with `docker compose ... down db && up -d db`

The restart may have caused `db-1` to come up **without its volume mounted or
initialized**. After the restart:

```sql
opentaxii=# SELECT count(*) FROM opentaxii_stixobject;
ERROR: relation "opentaxii_stixobject" does not exist
opentaxii=# \dt
Did not find any relations.
```

### Investigation commands (run on wazuh host, paste output back to assistant)

```bash
# 1. Confirm db-1 has a volume at all
docker inspect examples-db-1 | jq '.[0].Mounts[] | {Type, Name, Source, Destination}'
# Already gave: anonymous volume 62ef8a272fbbbc62... mounted at /var/lib/postgresql/data

# 2. Look INSIDE that volume on the host
VOL=/data/docker/volumes/62ef8a272fbbbc62a0344a44f0fa49647d9c5238469994ad5f296ea5e2b4108c/_data

du -sh "$VOL"
ls -la "$VOL"
ls -la "$VOL/base" 2>/dev/null | head -20

# 3. Most important — find out if otx2taxii pipeline (which populates the DB
# from OTX) is running and pushing data, OR if we need to bootstrap schema first
docker ps --filter "name=otx" --format "{{.Names}} {{.Image}} {{.Status}}"

# Check the opentaxii container logs for any "Created tables" or "schema bootstrap"
docker logs examples-opentaxii-1 --tail 60 2>&1 | tail -30

# Check for opentaxii's auto-init scripts (typically in /docker-entrypoint-initdb.d)
docker exec examples-opentaxii-1 find / -name "*.sql" -type f 2>/dev/null | head -10
docker exec examples-opentaxii-1 find / -name "*.yaml" -type f 2>/dev/null | head -10

# Maybe an init script exists in the mounted input dir:
ls -la /data/opentaxii/examples/
ls -la /data/opentaxii/examples/data-configuration*.yml 2>/dev/null

# Also check what otx2taxii uses to push:
ls -la /data/misp-taxii-connector-service/otx2taxii/
head -30 /data/misp-taxii-connector-service/otx2taxii/docker-compose.yaml 2>/dev/null
```

### Possible outcomes

#### Outcome A — volume has data, db-1 just need to re-bootstrap schema

If `ls $VOL/base/` shows PG internal files (PG_VERSION, files like 1234, 5678, etc.):

```bash
# The db-1 image's entrypoint-initdb.d is empty (or not mounted), so PG
# started without bootstrapping schema. Need to manually run
# OpenTAXII's schema bootstrap. The OpenTAXII image includes
# `opentaxii-manage.py` for this.
docker exec examples-opentaxii-1 opentaxii-manage create_tables
docker exec examples-opentaxii-1 opentaxii-manage create_accounts \
    --username admin --password admin
```

(see OpenTAXII upstream for the right invocation; paths may differ.)

If that still leaves tables missing, recreate them by hand:

```sql
-- Pseudo-DDL; consult OpenTAXII source for exact column types.
CREATE TABLE opentaxii_collection (
  id UUID PRIMARY KEY,
  name VARCHAR(255),
  ...
);
CREATE TABLE opentaxii_stixobject (
  pk UUID PRIMARY KEY,
  id VARCHAR(255),
  collection_id UUID REFERENCES opentaxii_collection(id),
  type VARCHAR(255),
  spec_version VARCHAR(255),
  date_added TIMESTAMP,
  version TIMESTAMP,
  serialized_data JSONB
);
CREATE INDEX ON opentaxii_stixobject (collection_id, id, version DESC);

CREATE UNIQUE INDEX opentaxii_stixobject_collection_id_id_version_key
  ON opentaxii_stixobject (collection_id, id, version);

-- Then re-create the MV and indexes (see Section 4):
CREATE MATERIALIZED VIEW opentaxii_stixobject_latest AS
  SELECT DISTINCT ON (collection_id, id) pk, id, collection_id, type, spec_version, version, date_added, serialized_data
  FROM   opentaxii_stixobject
  ORDER BY collection_id, id, version DESC;
-- ... and the three indexes from Section 4.
```

#### Outcome B — the volume is intact but the tables are missing because db-1 was remounted on a fresh empty volume

This happens if at some point someone ran `docker compose ... down -v` (note the `-v`).
The volume's source dir on the host (`/data/docker/volumes/62ef8a272fbbbc62.../`) is empty.
We must re-create schema and re-push data from upstream.

1. Bootstrap schema (Outcome A path)
2. Restart `otx2taxii` so it pushes everything back into OpenTAXII
3. Recreate MV + indexes (Section 4)

#### Outcome C — the volume's tables ARE there but db-1 was started without mounting it

Happens if `--volume` argument or compose's `volumes:` got dropped. Re-add the
mount in compose (or use `docker run --volume <vol>:/var/lib/postgresql/data`)
and restart.

### Currently running connector behavior

Even with the schema missing, the **proxy reports 200 OK** for /manifest/ (it
just returns `[]`), the **connector logs "Found 0 grouping objects"** and
"No groupings found in batch N", and the page count just climbs without any
MISP events. That's why the `0 objects` metric is climbing in the logs but no
events are flowing. **The proxy didn't break — the upstream DB has no schema.**

---

## 10. Repro step-by-step (clean install, post-restoration)

When ready to verify end-to-end after fixes land:

```bash
# 1. From /data/opentaxii/examples, after DB schema is back:
docker compose -f docker-compose-local.yml ps
# Expect: db, db-1, opentaxii-1, authdb all up

# 2. Apply MV + indexes (Section 4)
docker exec examples-db-1 psql -U security -d opentaxii -c < /tmp/mv-recreate.sql

# 3. Start the proxy (Section 6)
cd /data/misp-taxii-connector-service/taxii-mv-proxy
docker compose up -d --build
docker compose logs -f | tee /tmp/proxy.log

# 4. Smoke-test the proxy directly
curl -k http://localhost:9001/healthz
# Should show {"db_ok":true,"upstream_ok":<whatever>,...}

curl -k 'http://localhost:9001/taxii2/f14ba215-3f4c-4106-9979-2c818f6d9e76/collections/f14ba215-3f4c-4106-9979-2c818f6d9e76/manifest/?limit=10'
# Should return 200 OK with {"objects":[...], "more":<bool>}

# 5. Confirm connector URL is correct
grep ^DISCOVERY_URL /data/misp-taxii-connector-service/taxii2misp/.env
# Expected: DISCOVERY_URL=http://10.80.150.113:9001/taxii2/  (http, not https)

# 6. Restart connector
cd /data/misp-taxii-connector-service/taxii2misp
docker compose restart taxii2misp-connector

# 7. Tail logs
docker compose logs -f taxii2misp-connector | grep -E 'INFO|WARN'

# 8. Look for: "Page N: yielded +200 objects" then "Found 1 grouping objects"
# (the latter requires source-data to have STIX groupings; OTX data may not)
# Then: "events published to MISP" when things line up
```

---

## 11. Known gotchas and historical pitfalls

### ps:9001 vs /healthz

When the proxy is responding with HTTP 502 for `healthz`, check whether
`upstream_ok` is `false` (red) or `true` (green) in the response. `db_ok` should
always be `true` if Postgres is reachable. If `upstream_ok` is false, the
proxy can no longer reach opentaxii:9000 internally, but our hot path
(`/manifest/`, `/objects/`) doesn't need it.

### The `_transport_errors` tuple — leave the `_resolve_remote_disconnected()`
shim in place

If you simplify back to a literal `requests.exceptions.RemoteDisconnected`,
the bug returns. Keep the lazy `_resolve_remote_disconnected()` and the
`RuntimeError` fallback for safety.

### The full_path reattach in `root_dynamic`

Do **NOT** remove the line:

```python
full_path = "taxii2/" + full_path.lstrip("/")
```

Without it, the four regex patterns in `root_dynamic` (which all start with
`taxii2/...`) never match and the request falls through to upstream passthrough,
which will 502 because opentaxii is sick.

### psycopg2 RealDictCursor + aliased columns

`_fetch_manifest_page` uses `RealDictCursor`. Two things must match:

1. The `rebuilt` constant must include the column you read next (e.g.
   `pk::text AS pk_uuid` if you read `r["pk_uuid"]`).
2. The Python consumer must read the right dict key.

In a previous edit, the SELECT had `pk::text AS pk` but the consumer read
`r["pk"]`, causing `KeyError: 'pk'`. We **renamed** the alias to `pk_uuid` and
the consumer to `r["pk_uuid"]` to dodge any chance of a column-name collision.
The SELECT and the cursor builder both use that name.

### Cursor decode / sentinel guard

`_fetch_objects_page` must NOT emit `AND (date_added, id, pk) > (...)` when
`next_cursor is None`. Earlier code passed empty-string sentinels `("", "")` for
the date_added params when no cursor was supplied, which Postgres rejected with
`InvalidDatetimeFormat`. The current code emits the cursor clause only when a
real cursor is present.

### Empty-string sentinel on date_added

If you ever see `psycopg2.errors.InvalidDatetimeFormat: invalid input syntax
for type timestamp: ""`, that's the sentinel bug returning. The fix is the
conditional cursor_clause described above.

### Streaming cursors must use the `pk_uuid` alias for the third param

The third pipe-segment of the cursor is `pk_uuid`, NOT `pk`. If you copy the
cursor-decoder back to `r.get("pk")`, you get `KeyError: 'pk'`. Always use
`last.get("pk", last.get("pk_uuid", ""))` for safety.

---

## 12. Outstanding open questions for the next assistant

1. **Is `/data/docker/volumes/62ef8a272fbbbc62a0344a44f0fa49647d9c5238469994ad5f296ea5e2b4108c/_data/base/` populated with Postgres files?** — Run the
   investigation commands in Section 9. Most important check.

2. **Is the otx2taxii → OpenTAXII pipeline still pushing data?** — Run
   `docker ps --filter "name=otx"`. If yes, the schema may exist but the
   bulk of data may not yet be there. After confirming schema is fine, just
   let the pipeline run.

3. **After schema+data is back, restore the MV** — Section 4 sql.

4. **Verify that `proxy.py` line ~398 (`full_path = "taxii2/" + full_path.lstrip("/")`) is still in the wazuh host's deployed file.** — `grep "full_path = " /data/misp-taxii-connector-service/taxii-mv-proxy/proxy.py` from the
   wazuh host.

5. **Confirm connector `DISCOVERY_URL`** in
   `/data/misp-taxii-connector-service/taxii2misp/.env` is `http://10.80.150.113:9001/taxii2/`
   (no `https`).

6. **If MISP events aren't flowing after all fixes**, check whether the user
   data has STIX grouping SDOs at all. If not, the connector's
   `extract_grouping_objects` returns empty and the publish step is a no-op.

---

## 13. Quick-reference: the full list of patches shipped

Order doesn't matter; all are in the local repo and (mostly) on the wazuh host.

| # | File | What |
|---|---|---|
| 1 | `taxii2misp/clients/taxii_client.py` | `_resolve_remote_disconnected()` shim + `_transport_errors` tuple |
| 2 | `taxii2misp/clients/taxii_client.py` | streaming v2 in `get_all_objects_with_resource_management` |
| 3 | `taxii2misp/clients/taxii_client.py` | ps-exception-tolerant `extract_grouping_objects`: extract grouping id BEFORE parse; no raw-dict fallback |
| 4 | `taxii2misp/clients/taxii_client.py` | `batch_size → self.chunk_size` (env-driven); cap at 500 |
| 5 | `taxii-mv-proxy/proxy.py` | entire FastAPI MV-backed service (570 lines) |
| 6 | `taxii-mv-proxy/proxy.py` | fastapi-discovery + api-root + collections endpoints served from Postgres MV |
| 7 | `taxii-mv-proxy/proxy.py` | manifest/objects handlers with cursor-based pagination, `pk::text AS pk_uuid` |
| 8 | `taxii-mv-proxy/.env` | `PROXY_PUBLIC_URL=http://...:9001/taxii2/` (no `https`) |
| 9 | `taxii-mv-proxy/docker-compose.yml` | joins `examples_default` external network; maps host port 9001:9001 |
| 10 | Database `opentaxii_stixobject_latest` MV + 3 indexes (Section 4) |
| 11 | `db-1 compose` `shm_size: '2gb'` (Section 5) |

**Removed:** synthetic indicator wrapping in proxy, synthetic grouping
synthesis in proxy, raw-dict fallback in connector (`extract_grouping_objects`).

---

## 14. If the assistant reading this needs to do additional debugging

The auth flow is basic HTTP basic with username `admin` / password `admin` (see
MISP Django default in the OpenTAXII data-configuration; tests confirmed
this works). TLS is **not** configured anywhere — the connector uses
`VERIFY_SSL=false` and the proxy uses `PROXY_PUBLIC_URL=http://...`. If you want
TLS, mount `/certs/` into both opentaxii-1 and taxii-mv-proxy, run uvicorn
with `--ssl-certfile /certs/cert.pem --ssl-keyfile /certs/key.pem`, and update
`DISCOVERY_URL` to `https://...:9001/...`.

---

## 15. OpenCode-specific instructions for this session

This file is intended to be loaded by an agent running **inside the wazuh
host**. To use it:

```bash
# on wazuh host
cd /data/misp-taxii-connector-service
# Open the assistant with this repo as context
opencode .
# In the prompt, paste a short request like:
# "Read SESSION.md and diagnose the empty database"
```

The agent should:
1. Read this entire file.
2. Run the Section 9 investigation commands.
3. Report findings on data survival, schema status, otx2taxii pipeline.
4. Propose the exact restore sequence.
5. After restoration succeeds, replay Section 10 verification.

Don't retry code patches if they're already in the file — those are settled.

---

End of SESSION.md.
