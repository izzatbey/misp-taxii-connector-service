# taxii-mv-proxy

A read-only TAXII 2.1 proxy that sits in front of an OpenTAXII
deployment and serves `/manifest/` and `/objects/` endpoints
directly from a Postgres materialized view (`opentaxii_stixobject_latest`)
instead of OpenTAXII's slow `DISTINCT ON max(version) GROUP BY id`
path. Cuts p95 `/manifest/?limit=10` from **116 seconds → 314 ms**
on an 8.2M-row collection.

## Why

The default OpenTAXII `Get Objects` SQL query has an inner
`max(version) GROUP BY id` subquery that has to scan every row in
the collection for **every page request**. At 100k–10M row scale
this hits Postgres's disk-based sort, exceeds Gunicorn's `--timeout`,
and triggers worker SIGKILLs — which the connector interprets as
"server disconnected, retry".

Building the materialized view once and serving `READ` traffic
through this proxy removes that bottleneck completely. Writes
(POST /objects add) still go to OpenTAXII directly.

## Endpoints implemented

| Method | Path | Behaviour |
|---|---|---|
| GET | `/taxii2/` | passthrough to upstream |
| GET | `/taxii2/{api_root}/` | passthrough |
| GET | `/taxii2/{api_root}/collections/` | passthrough |
| GET | `/taxii2/{api_root}/collections/{id}/` | passthrough |
| GET | `/taxii2/{api_root}/collections/{id}/manifest/` | MV-backed |
| GET | `/taxii2/{api_root}/collections/{id}/manifest/{obj}/` | MV-backed |
| GET | `/taxii2/{api_root}/collections/{id}/objects/` | MV-backed |
| GET | `/taxii2/{api_root}/collections/{id}/objects/{obj}/` | MV-backed |
| GET | `/healthz` | liveness — exercises DB + upstream |

## Setup

1. Build the MV (one-time, ~5–10 min on 8M rows):

```sql
CREATE MATERIALIZED VIEW opentaxii_stixobject_latest AS
  SELECT DISTINCT ON (collection_id, id)
         pk, id, collection_id, type, spec_version,
         date_added, version, serialized_data
  FROM opentaxii_stixobject
  ORDER BY collection_id, id, version DESC;

CREATE UNIQUE INDEX idx_latest_col_date_id_pk
  ON opentaxii_stixobject_latest (collection_id, date_added, id, pk);
CREATE INDEX idx_latest_pk
  ON opentaxii_stixobject_latest (pk);
```

2. Schedule `REFRESH MATERIALIZED VIEW CONCURRENTLY opentaxii_stixobject_latest` every 5–15 min.

3. Run the proxy:

```bash
cp .env.example .env   # adjust as needed
docker compose up -d
```

4. Smoke-test:

```bash
curl -k 'https://10.80.150.113:9000/taxii2/<api_root>/collections/<coll>/manifest/?limit=10'
```

5. Point the `taxii2misp-connector`'s `DISCOVERY_URL` at the proxy's
   TAXII URL (or replace the host running OpenTAXII's port 9000 with
   this proxy — they're the same URL by default).

## Operational notes

- The proxy uses **one** psycopg2 connection per process. For
  parallelism run multiple replicas behind a load balancer.
- The proxy is unauthenticated by design. Run it on a private
  network in front of OpenTAXII.
- Pagination uses `(date_added, id, pk)` cursors with the
  `?next=<date_added>|<id>|<pk>` convention. We ignore
  OpenTAXII's expected cursor encoding; the connector library
  follows our cursors just like any other TAXII 2.1 server.
- The MV is the source of truth for reads. If you add new objects
  to OpenTAXII outside the proxy (e.g. via `otx2taxii`), wait for
  the next `REFRESH` (≤15 min) before they appear here.
- Refresh policy: `REFRESH MATERIALIZED VIEW CONCURRENTLY` with
  the unique index above; safe with readers; runs in 2–5 minutes
  on 8M rows.

## What I did NOT implement

- POST /objects add — open this path directly on OpenTAXII.
- Auth — assumed closed network.
- Per-tenant access control — single collection per proxy. For
  multi-tenant deploy one proxy per collection (or add bearer
  auth here).
