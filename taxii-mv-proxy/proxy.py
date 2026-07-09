"""
TAXII MV-Proxy — A read-only TAXII 2.1 front-end that serves
/manifest/ and /objects/ endpoints directly against the
opentaxii_stixobject_latest materialized view in Postgres,
bypassing OpenTAXII's slow SQL path.

Disclosure is delegated to OpenTAXII: we forward GET /taxii2/
requests to the upstream server. That way the proxy doesn't need
to know about API roots, collections metadata, or auth negotiation.

Endpoints this proxy implements:
  GET /taxii2/                                       -> passthrough to upstream
  GET /taxii2/{api_root_id}/                         -> passthrough
  GET /taxii2/{api_root_id}/collections/             -> passthrough
  GET /taxii2/{api_root_id}/collections/{coll_id}/   -> passthrough
  GET /taxii2/{api_root_id}/collections/{coll_id}/manifest/  -> MV-backed
  GET /taxii2/{api_root_id}/collections/{coll_id}/objects/   -> MV-backed
  GET /taxii2/{api_root_id}/collections/{coll_id}/objects/{obj_id}/ -> MV-backed
  GET /taxii2/{api_root_id}/collections/{coll_id}/manifest/{obj_id}/ -> MV-backed

Pagination:
  - Per TAXII 2.1 spec, ?limit= and ?next= are the official
    pagination knobs. We honour both, but we DO NOT send ?limit
    upstream (OpenTAXII ignores the body's ?next cursor properly
    when we forward fully-formed URLs).
  - The proxy itself honours ?limit on /manifest/ and /objects/
    by reading at most N rows from the MV.

Authorisation:
  This proxy is intentionally unauthenticated. It runs on a
  closed network in front of OpenTAXII. If you expose it to
  the internet, add OAuth2 / bearer / mTLS in front.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode, urlparse

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask
from starlette.responses import Response

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
from dotenv import load_dotenv

load_dotenv()  # honours .env in this folder if present

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db-1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "opentaxii")
POSTGRES_USER = os.getenv("POSTGRES_USER", "security")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DEFAULT_COLLECTION_ID = os.getenv("DEFAULT_COLLECTION_ID", "")
UPSTREAM_TAXII_URL = os.getenv("UPSTREAM_TAXII_URL", "http://opentaxii-1:9000/taxii2/")
PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PROXY_PORT", "9000"))
PROXY_PUBLIC_URL = os.getenv("PROXY_PUBLIC_URL", UPSTREAM_TAXII_URL)
DEFAULT_PAGE_SIZE = int(os.getenv("DEFAULT_PAGE_SIZE", "200"))
MAX_PAGE_SIZE = int(os.getenv("MAX_PAGE_SIZE", "1000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("taxii-mv-proxy")

# ------------------------------------------------------------------
# DB connection pool (lazy)
# ------------------------------------------------------------------
_DB_CONN: Optional[psycopg2.extensions.connection] = None


def get_db() -> psycopg2.extensions.connection:
    """Return a (re)connected psycopg2 connection."""
    global _DB_CONN
    if _DB_CONN is None or _DB_CONN.closed:
        _DB_CONN = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=5,
            application_name="taxii-mv-proxy",
        )
        _DB_CONN.autocommit = True
    return _DB_CONN


@asynccontextmanager
async def lifespan(_: FastAPI):
    # warm DB connection so the first request isn't slow
    try:
        get_db().close()
        log.info("DB connectivity OK")
    except Exception as e:
        log.error(f"DB connectivity FAILED: {e}")
    yield


app = FastAPI(
    title="TAXII MV-Proxy",
    description=(
        "Read-only TAXII 2.1 front-end backed by the "
        "opentaxii_stixobject_latest materialized view. "
        "Pagination, manifest, and objects endpoints are served "
        "directly from Postgres; discovery metadata is proxied "
        "from the upstream OpenTAXII."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------------
# Upstream pass-through (discovery, collections metadata, anything
# else we don't special-case).
# ------------------------------------------------------------------
def _is_proxy_served(path: str) -> bool:
    """Return True if path matches a /manifest/ or /objects/ URL we
    serve directly. Anything else is forwarded to upstream."""
    return bool(
        re.search(r"/manifest/?$", path)
        or re.search(r"/manifest/[^/]+/?$", path)
        or re.search(r"/objects/?$", path)
        or re.search(r"/objects/[^/]+/?$", path)
    )


def _upstream_request(req: Request) -> Response:
    """Forward `req` to UPSTREAM_TAXII_URL verbatim and stream the
    response back. Preserves path, query string, body, headers
    (minus hop-by-hop)."""
    target = UPSTREAM_TAXII_URL.rstrip("/") + req.url.path
    if req.url.query:
        target += "?" + req.url.query

    body_bytes = b""
    if req.method in ("POST", "PUT", "PATCH"):
        # Not used in practice (we only forward GETs to upstream);
        # would need to be `await req.body()` if you support POST.
        body_bytes = b""

    headers = {
        k: v
        for k, v in req.headers.items()
        if k.lower() not in ("host", "content-length", "connection", "accept-encoding")
    }
    try:
        upstream_resp = requests.request(
            method=req.method,
            url=target,
            headers=headers,
            data=body_bytes if body_bytes else None,
            allow_redirects=False,
            timeout=(10, 60),
        )
    except requests.exceptions.RequestException as e:
        log.warning(f"upstream passthrough error for {req.url.path}: {e}")
        raise HTTPException(status_code=502, detail="upstream-unreachable")

    passthrough_headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower()
        not in ("transfer-encoding", "connection", "content-encoding", "content-length")
    }
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=passthrough_headers,
        media_type=upstream_resp.headers.get("content-type"),
    )


# ------------------------------------------------------------------
# MV-backed helpers
# ------------------------------------------------------------------
def _validate_collection_id(coll_id: str) -> str:
    """Be strict about collection_id. Return it canonicalised.
    A malformed UUID will 400 — we never want to trust client input
    into our SQL parameter placeholders."""
    try:
        # psycopg2 will handle UUID → ::UUID cast, but we pre-validate
        # to fail fast on garbage.
        from uuid import UUID

        UUID(coll_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="invalid-collection-id")
    return coll_id


def _validate_limit(limit: int) -> int:
    if limit < 1:
        limit = 1
    if limit > MAX_PAGE_SIZE:
        limit = MAX_PAGE_SIZE
    return limit


def _clamp_int(raw: Optional[str], default: int, lo: int, hi: int) -> int:
    if raw is None:
        return default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


class TaxiiEnvelope(BaseModel):
    """Minimal TAXII 2.1 envelope we return (manifest or objects).
    We always set 'more' and (when applicable) 'next'.
    """

    objects: List[Dict[str, Any]] = []
    more: bool = False


def _fetch_manifest_page(
    collection_id: str,
    limit: int,
    next_cursor: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Read up to `limit` rows from the MV's manifest ordering, plus
    enough information to build the next cursor.

    Returns (rows, next_cursor). If next_cursor is None, the walk is
    complete (more=False).
    """
    sql = """
        SELECT
            id::text                       AS id_text,
            pk::text                       AS pk_text,
            collection_id::text            AS collection_id_text,
            type                           AS type_text,
            spec_version                   AS spec_version_text,
            version::text                  AS version_text,
            date_added::text               AS date_added_text,
            ('application/stix+json;version=' || spec_version) AS media_type
        FROM opentaxii_stixobject_latest
        WHERE collection_id = %s::uuid
        %s
        ORDER BY date_added, id, pk
        LIMIT %s
    """
    cursor_pred = ""
    cursor_args: List[Any] = []
    if next_cursor:
        # Cursor format: "<date_added>|<stix_id>|<pk_uuid>"
        # Order columns in the row-comparator matches our ORDER BY tuple:
        #   (date_added, id, pk) > (date_added, stix_id, pk)
        # We cast the third param to ::uuid because pk is a uuid column.
        cursor_pred = "AND (date_added, id, pk) > (%s, %s, %s::uuid)"
        try:
            da, oid, pk = next_cursor.split("|", 2)
            cursor_args = [da, oid, pk]
        except ValueError:
            raise HTTPException(status_code=400, detail="bad-next-cursor")

    full_sql = sql.replace("%s", "%s", 1)  # dummy to keep %s below; we'll
    # build args list explicitly
    args: List[Any] = [collection_id] + cursor_args + [limit + 1]

    rebuilt = (
        "SELECT id::text                       AS id_text,\n"
        "       pk::text                       AS pk_text,\n"
        "       collection_id::text            AS collection_id_text,\n"
        "       type                           AS type_text,\n"
        "       spec_version                   AS spec_version_text,\n"
        "       version::text                  AS version_text,\n"
        "       date_added::text               AS date_added_text,\n"
        "       ('application/stix+json;version=' || spec_version) AS media_type\n"
        "FROM opentaxii_stixobject_latest\n"
        "WHERE collection_id = %s\n"
        f"{cursor_pred}\n"
        "ORDER BY date_added, id, pk\n"
        "LIMIT %s\n"
    )

    conn = get_db()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(rebuilt, args)
        rows = cur.fetchall()

    more = False
    nxt: Optional[str] = None
    if len(rows) > limit:
        # Drop the lookahead row and compute next cursor from the
        # *last* row we keep. Cursor format: <date_added>|<id>|<pk>;
        # pk is the actual uuid column, NOT a STIX id string.
        rows = rows[:limit]
        last = rows[-1]
        # Use aliased column names. If something is missing, log and bail
        # without raising so the proxy still produces output.
        missing = [
            k for k in ("date_added_text", "id_text", "pk_text") if k not in last
        ]
        if missing:
            log.error(
                f"manifest row missing keys {missing}; row keys: {list(last.keys())}"
            )
            return rows, None
        nxt = f"{last['date_added_text']}|{last['id_text']}|{last['pk_text']}"
        more = True

    return rows, nxt


def _fetch_objects_page(
    collection_id: str,
    ids: Optional[List[str]],
    limit: int,
    next_cursor: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Read up to `limit` STIX objects from the MV. If `ids` is
    supplied, returns only matching objects (used for /objects/?id=a,b
    style requests).

    The MV's serialized_data column already stores the STIX JSON
    object; we just JSON-decode and return it.
    """
    conn = get_db()

    # Build the actual SQL with placeholder positions:
    sql = """
        SELECT pk::text, id::text,
               date_added::text AS date_added,
               serialized_data
        FROM opentaxii_stixobject_latest
        WHERE collection_id = %s
          AND (%s::boolean IS FALSE OR id = ANY(%s::text[]))
          AND (date_added, id, pk) > (%s, %s, %s::uuid)
        ORDER BY date_added, id, pk
        LIMIT %s
    """
    args: List[Any] = [collection_id]
    args.append(bool(ids))
    args.append(ids or [])
    if next_cursor:
        try:
            da, oid, pk = next_cursor.split("|", 2)
            args.extend([da, oid, pk])
        except ValueError:
            raise HTTPException(status_code=400, detail="bad-next-cursor")
    else:
        # Use sentinel values that satisfy "greater than everything"
        args.extend(["", "", "00000000-0000-0000-0000-000000000000"])

    args.append(limit + 1)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, args)
        rows = cur.fetchall()

    objs: List[Dict[str, Any]] = []
    last_row = None
    for r in rows:
        sd = r["serialized_data"]
        # psycopg2 may have already JSON-decoded serialized_data (jsonb)
        # or left it as a string (json) — handle both.
        if isinstance(sd, str):
            obj = json.loads(sd)
        else:
            obj = sd
        objs.append(obj)
        last_row = r

    more = False
    nxt: Optional[str] = None
    if len(objs) > limit and last_row is not None:
        objs = objs[:limit]
        nxt = f"{last_row['date_added']}|{last_row['id']}|{last_row['pk']}"
        more = True

    return objs, nxt


# ------------------------------------------------------------------
# TAXII 2.1 metadata endpoints (served from MV — NO upstream calls,
# so we never depend on OpenTAXII being healthy for these paths).
# ------------------------------------------------------------------
def _taxii_public_url() -> str:
    """Base URL clients see externally. PROXY_PUBLIC_URL must end
    with a trailing slash (already set in .env)."""
    base = PROXY_PUBLIC_URL.rstrip("/")
    return base + "/"


# Schema introspection — OpenTAXII's collection table may use
# 'name', 'title', or some other column; we discover on first hit
# and cache the result.
_COLLECTION_TITLE_COLUMN: Optional[str] = None


def _resolve_collection_title_column() -> Optional[str]:
    """Find which column in opentaxii_collection gives us a human-
    readable title. Caches result."""
    global _COLLECTION_TITLE_COLUMN
    if _COLLECTION_TITLE_COLUMN is not None:
        return _COLLECTION_TITLE_COLUMN
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'opentaxii_collection'"
        )
        cols = [r[0] for r in cur.fetchall()]
    for candidate in ("name", "title"):
        if candidate in cols:
            _COLLECTION_TITLE_COLUMN = candidate
            return candidate
    _COLLECTION_TITLE_COLUMN = None
    return None


def _collection_rows() -> List[Tuple[str, Optional[str]]]:
    """Return [(id, title_or_None), ...] from opentaxii_collection.
    Discovers title column on first call."""
    title_col = _resolve_collection_title_column()
    conn = get_db()
    with conn.cursor() as cur:
        if title_col:
            cur.execute(f"SELECT id::text, {title_col} FROM opentaxii_collection")
        else:
            cur.execute("SELECT id::text FROM opentaxii_collection")
        return [(r[0], r[1] if title_col else None) for r in cur.fetchall()]


def _collection_title_map() -> Dict[str, str]:
    """Return {collection_id (str): title} from opentaxii_collection."""
    rows = _collection_rows()
    return {cid: (title or f"Collection {cid[:8]}…") for cid, title in rows}


def _api_roots_from_db() -> List[Dict[str, Any]]:
    """Build the TAXII 2.1 API-roots payload from known collections."""
    rows = _collection_rows()
    if not rows:
        # Fallback: synthesise one collection from DEFAULT_COLLECTION_ID
        rows = (
            [(DEFAULT_COLLECTION_ID, "SOCollection")] if DEFAULT_COLLECTION_ID else []
        )
    # We expose a single stable API root UUID. Any UUID works as long
    # as it's consistent across requests so the connector can build
    # /api-root-id/collections/... URLs.
    api_root_id = rows[0][0] if rows else "default-api-root"
    # We can't easily reach the api-root URL path the connector builds
    # without knowing the api_root_id. Easiest is to expose every
    # collection under each known collection_id as its own api root,
    # and also under a single "global" api_root_id.
    #
    # The fastpath for the connector is: GET /taxii2/  -> picks first
    # api_root. Then /taxii2/{api_root}/collections/ -> lists collections.
    # We make BOTH shapes return the same collections.
    return [{"id": api_root_id, "title": "Misconfigured API root"}]


@app.get("/taxii2/")
async def get_discovery():
    """TAXII 2.1 Discovery endpoint.

    Returns the list of API roots known to this server. The MV-backed
    implementation never calls OpenTAXII and never blocks.
    """
    api_roots = _api_roots_from_db()
    return JSONResponse(
        content={
            "title": "TAXII 2.1 (MV-proxied)",
            "default": api_roots[0]["id"] if api_roots else None,
            "api_roots": [f"{_taxii_public_url()}{r['id']}/" for r in api_roots],
        },
        media_type="application/taxii+json;version=2.1",
    )


@app.get("/taxii2/{api_root_id}/")
async def get_api_root(api_root_id: str):
    """API root metadata endpoint."""
    title_map = _collection_title_map()
    return JSONResponse(
        content={
            "id": api_root_id,
            "title": "MISP TAXII feed (MV-proxied)",
            "description": (
                "Read-only TAXII 2.1 collection backed by the "
                "opentaxii_stixobject_latest materialized view."
            ),
            "versions": ["taxii-2.1"],
            "max_content_length": 10485760,
        },
        media_type="application/taxii+json;version=2.1",
    )


@app.get("/taxii2/{api_root_id}/collections/")
async def get_collections(api_root_id: str):
    """Collections list endpoint."""
    title_map = _collection_title_map()
    if not title_map:
        return JSONResponse(
            content={"collections": []}, media_type="application/taxii+json;version=2.1"
        )
    collections_payload = []
    for cid, ctitle in title_map.items():
        collections_payload.append(
            {
                "id": cid,
                "title": ctitle,
                "description": (
                    f"Collection {ctitle} ({cid[:8]}…) — served from "
                    f"the materialized view."
                ),
                "media_types": [
                    "application/stix+json;version=2.1",
                ],
                "can_read": True,
                "can_write": False,
                "media_type": "application/taxii+json;version=2.1",
            }
        )
    return JSONResponse(
        content={"collections": collections_payload},
        media_type="application/taxii+json;version=2.1",
    )


@app.get("/taxii2/{api_root_id}/collections/{collection_id}/")
async def get_collection(api_root_id: str, collection_id: str):
    """Single collection metadata endpoint."""
    title_map = _collection_title_map()
    title = title_map.get(collection_id, "Unknown")
    return JSONResponse(
        content={
            "id": collection_id,
            "title": title,
            "description": (f"Collection {title} — served from the MV."),
            "media_types": ["application/stix+json;version=2.1"],
            "can_read": True,
            "can_write": False,
            "media_type": "application/taxii+json;version=2.1",
        },
        media_type="application/taxii+json;version=2.1",
    )


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------
@app.api_route(
    "/taxii2/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
)
async def root_dynamic(full_path: str, request: Request):
    """Top-level dispatcher.

    If the path matches /manifest/ or /objects/, serve it from the
    MV. Otherwise, forward to upstream OpenTAXII.
    """
    # FastAPI strips the "/taxii2/" prefix from the path; the regex
    # patterns below expect `taxii2/...` form, so re-attach it.
    full_path = "taxii2/" + full_path.lstrip("/")
    full_url = "/" + full_path
    qs = request.url.query

    # ----- /manifest/ endpoints -----
    m_manifest_list = re.fullmatch(
        r"taxii2/([^/]+)/collections/([^/]+)/manifest/?",
        full_path,
    )
    m_manifest_by_id = re.fullmatch(
        r"taxii2/([^/]+)/collections/([^/]+)/manifest/([^/]+)/?",
        full_path,
    )
    # ----- /objects/ endpoints -----
    m_objects_list = re.fullmatch(
        r"taxii2/([^/]+)/collections/([^/]+)/objects/?",
        full_path,
    )
    m_objects_by_id = re.fullmatch(
        r"taxii2/([^/]+)/collections/([^/]+)/objects/([^/]+)/?",
        full_path,
    )

    start = time.monotonic()

    if m_manifest_list:
        api_root, coll_id = m_manifest_list.groups()
        coll_id = _validate_collection_id(coll_id)
        limit = _clamp_int(
            request.query_params.get("limit"),
            DEFAULT_PAGE_SIZE,
            1,
            MAX_PAGE_SIZE,
        )
        next_cursor = request.query_params.get("next")
        rows, nxt = _fetch_manifest_page(coll_id, limit, next_cursor)
        body = {
            "objects": [
                {
                    "id": r["id"],
                    "date_added": r["date_added"],
                    "version": r["version"],
                    "media_type": r["media_type"],
                }
                for r in rows
            ],
            "more": bool(nxt),
            **({"next": nxt} if nxt else {}),
        }
        ms = (time.monotonic() - start) * 1000.0
        log.info(
            f"MANIFEST {coll_id[:8]}… limit={limit} → "
            f"{len(body['objects'])} rows in {ms:.1f} ms "
            f"(more={body['more']})"
        )
        return JSONResponse(content=body, media_type="application/taxii+json")

    if m_manifest_by_id:
        # /manifest/{obj_id}/ — single-object manifest record.
        api_root, coll_id, obj_id = m_manifest_by_id.groups()
        coll_id = _validate_collection_id(coll_id)
        rows, _ = _fetch_manifest_page(coll_id, 1, None)
        rows = [r for r in rows if r["id"] == obj_id]
        body = {
            "objects": [
                {
                    "id": r["id"],
                    "date_added": r["date_added"],
                    "version": r["version"],
                    "media_type": r["media_type"],
                }
                for r in rows
            ],
            "more": False,
        }
        return JSONResponse(content=body, media_type="application/taxii+json")

    if m_objects_list:
        api_root, coll_id = m_objects_list.groups()
        coll_id = _validate_collection_id(coll_id)
        limit = _clamp_int(
            request.query_params.get("limit"),
            DEFAULT_PAGE_SIZE,
            1,
            MAX_PAGE_SIZE,
        )
        next_cursor = request.query_params.get("next")
        # TAXII 2.1 client libraries send match[id]=a,b,c (not ?id=).
        # Most also accept ?id=a,b,c. We accept both.
        raw_ids = request.query_params.get("id") or request.query_params.get(
            "match[id]"
        )
        ids: Optional[List[str]] = None
        if raw_ids:
            ids = [s.strip() for s in raw_ids.split(",") if s.strip()]
            if len(ids) > 200:
                # Cap to keep each request cheap.
                ids = ids[:200]
        objs, nxt = _fetch_objects_page(coll_id, ids, limit, next_cursor)
        body = {
            "objects": objs,
            "more": bool(nxt),
            **({"next": nxt} if nxt else {}),
        }
        ms = (time.monotonic() - start) * 1000.0
        log.info(
            f"OBJECTS  {coll_id[:8]}… limit={limit} ids={'NONE' if ids is None else len(ids)} → "
            f"{len(body['objects'])} rows in {ms:.1f} ms "
            f"(more={body['more']})"
        )
        return JSONResponse(content=body, media_type="application/taxii+json")

    if m_objects_by_id:
        # /objects/{obj_id}/ — single object lookup.
        api_root, coll_id, obj_id = m_objects_by_id.groups()
        coll_id = _validate_collection_id(coll_id)
        objs, _ = _fetch_objects_page(coll_id, [obj_id], 10, None)
        body = {
            "objects": objs[:1],
            "more": False,
        }
        return JSONResponse(content=body, media_type="application/taxii+json")

    # Anything else — pass through to OpenTAXII unchanged.
    return _upstream_request(request)


@app.get("/healthz")
def healthz():
    """Health check — exercises DB and upstream reachability."""
    db_ok = False
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM opentaxii_stixobject_latest")
            _ = cur.fetchone()
        db_ok = True
    except Exception as e:
        log.warning(f"healthz: DB check failed: {e}")

    upstream_ok = False
    try:
        requests.get(
            UPSTREAM_TAXII_URL.rstrip("/") + "/",
            timeout=(5, 10),
        )
        upstream_ok = True
    except Exception as e:
        log.warning(f"healthz: upstream check failed: {e}")

    status = 200 if db_ok else 503
    return JSONResponse(
        status_code=status,
        content={
            "db_ok": db_ok,
            "upstream_ok": upstream_ok,
            "mv_latest_count": "unknown",
        },
    )


# ------------------------------------------------------------------
# Entry point — lets you `python proxy.py` directly.
# uvicorn is also supported via `uvicorn proxy:app --host ...`.
# ------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "proxy:app",
        host=PROXY_HOST,
        port=PROXY_PORT,
        log_level=LOG_LEVEL.lower(),
        workers=1,  # psycopg2 single connection per worker
    )
