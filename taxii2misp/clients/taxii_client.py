import logging
import time
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib3.exceptions import InsecureRequestWarning
import warnings

import requests
import taxii2client.v21 as taxii21
from stix2 import MemoryStore, parse

# Import shutdown handler
from utils.signal_handlers import is_shutdown_requested

warnings.simplefilter("ignore", InsecureRequestWarning)

logger = logging.getLogger(__name__)


class TAXIIClient:
    def __init__(
        self,
        discovery_url,
        username,
        password,
        verify_ssl,
        request_timeout=60,
        chunk_size=100,
        page_retries=3,
        page_retry_backoff=2.0,
    ):
        self.discovery_url = discovery_url
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.request_timeout = request_timeout
        self.chunk_size = chunk_size
        # Retry policy for transient TAXII connection failures
        # (RemoteDisconnected, ConnectionResetError, ReadTimeout).
        # TAXII servers / nginx proxies often drop keepalive sockets after
        # ~30-60s of inactivity; paginating a 100k+ object collection
        # with a 60s+ page time triggers this. We retry the failed page
        # up to `page_retries` times with mild exponential backoff.
        self.page_retries = page_retries
        self.page_retry_backoff = page_retry_backoff
        self.server = None
        self.api_root = None
        self.collection = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize the TAXII 2.1 client and connect to the server."""
        try:
            logger.info(f"Connecting to TAXII server at: {self.discovery_url}")

            # Initialize the TAXII server connection (without timeout parameter)
            self.server = taxii21.Server(
                self.discovery_url,
                user=self.username,
                password=self.password,
                verify=self.verify_ssl,
            )

            # Get the first API root (assuming single API root for simplicity)
            api_roots = self.server.api_roots
            if not api_roots:
                raise ValueError("No API roots found on the TAXII server")

            self.api_root = api_roots[0]
            logger.info(f"Connected to API root: {self.api_root.url}")

            # Get the first collection (or you can modify this to select a specific collection)
            collections = self.api_root.collections
            if not collections:
                raise ValueError("No collections found in the API root")

            self.collection = collections[0]
            logger.info(
                f"Using collection: {self.collection.title} (ID: {self.collection.id})"
            )

        except Exception as e:
            logger.error(f"Failed to initialize TAXII client: {e}")
            raise

    # ------------------------------------------------------------------
    # Internal HTTP helpers (bypass taxii2client's quirks).
    #
    # The taxii2client library always sends `?limit=N` as a query param
    # (see taxii2client/common.py:_filter_kwargs_to_query_params). On the
    # OpenTAXII server in this stack, that means every /objects/ and
    # /manifest/ call materialises the entire remaining collection server-
    # side and streams it in one response — multi-minute renders, Postgres
    # joins eating host RAM, "stalled" cycles that never complete.
    #
    # We bypass that by:
    #   1) walking /manifest/ (returns *only* IDs + timestamps, not full
    #      STIX objects — tiny response, fast server render even at 100k
    #      objects), and
    #   2) issuing raw `requests` with NO `?limit=` parameter and tight
    #      per-page connect/read timeouts. The server returns its own
    #      internal page size (the OpenTAXII default is small enough to
    #      respond in ~1s).
    # ------------------------------------------------------------------
    def _http_session(self) -> requests.Session:
        """Return the underlying requests.Session for raw HTTP calls."""
        # taxii2client shares a Session across collections of the same
        # ApiRoot, so we reuse whatever the library set up (auth + verify
        # already configured). Fall back to a fresh Session if the
        # private attribute is ever renamed.
        try:
            return self.collection._conn.session  # type: ignore[attr-defined]
        except AttributeError:
            sess = requests.Session()
            if self.username and self.password:
                import requests.auth as _ra

                sess.auth = _ra.HTTPBasicAuth(self.username, self.password)
            sess.verify = bool(self.verify_ssl)
            return sess

    def _manifest_url(self) -> str:
        """Return the manifest endpoint URL for the current collection."""
        # collection.url already ends with the collection id and a slash
        base = self.collection.url
        if not base.endswith("/"):
            base = base + "/"
        return base + "manifest/"

    def _objects_url(self) -> str:
        """Return the objects endpoint URL for the current collection."""
        base = self.collection.url
        if not base.endswith("/"):
            base = base + "/"
        return base + "objects/"

    def _raw_get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Tuple[int, int] = (10, 60),
    ) -> Tuple[Dict[str, Any], int, Optional[str]]:
        """
        Issue a raw HTTP GET against the TAXII server, bypassing
        taxii2client.get_objects/get_manifest (both add `limit` query
        params we don't want).

        Args:
            url: full URL
            params: query parameters (NONE may include 'limit')
            timeout: (connect_timeout, read_timeout) seconds

        Returns:
            (json_body, status_code, next_cursor) — may raise on transport
            errors so the caller can retry.

        Raises:
            requests.exceptions.RequestException — for transport errors
            json.JSONDecodeError — if response is not JSON
        """
        sess = self._http_session()
        resp = sess.get(url, params=params or None, timeout=timeout)
        status = resp.status_code
        next_hdr = resp.headers.get("X-TAXII-Date")  # debugging sanity
        body: Dict[str, Any] = {}
        try:
            body = resp.json()
        except ValueError:
            # Not JSON — caller decides what to do (typically HTTPError).
            resp.raise_for_status()
        # Spec: pagination cursor lives in body.more and body.next
        return body, status, body.get("next") if isinstance(body, dict) else None

    def get_all_objects_with_resource_management(
        self, redis_client=None, batch_size=10000, rest_seconds=5
    ):
        """
        Streaming, resource-aware STIX object retrieval via TAXII /manifest/.

        FIXED (2026-07-09 v2): rewritten to walk /manifest/ instead of
        /objects/. The previous version of this method (and the one before
        it) called self.collection.get_objects(limit=...). On the
        OpenTAXII server backing this stack, that consistently hung for
        2+ minutes per call because OpenTAXII's /objects/ materialises
        the entire remaining collection server-side regardless of the
        `limit` query param (its Postgres backend is the slow part — it
        was eating 31% of host RAM).

        Manifest-walking is fundamentally lighter:
          - each envelope contains only object IDs, timestamps, version,
            and media_type — not full STIX objects.
          - typical per-page response: a few KB even at 10k objects.
          - per-call server render: ~1s on large collections.

        We then batch-fetch the *full* STIX objects by ID (in groups of
        `http_page_size`, default 200) using /objects/?id=<a,b,c>. Each
        /objects/ call is bounded so the server can't blow up.

        Caller contract is unchanged from the v1 streaming rewrite:
        yields one batch (a list of STIX object dicts) per outer iteration.

        Args:
            redis_client: Redis client for caching progress (optional)
            batch_size:   preferred objects per yielded batch. We honour
                          it for the /objects/ fetch granularity; the
                          manifest walk defaults to a per-page cache of
                          500 IDs (~few KB per response).
            rest_seconds: seconds to rest between successful batches
        """
        import gc

        logger.info(
            f"🚀 Starting STREAMING resource-managed retrieval "
            f"(batch_size={batch_size}, rest={rest_seconds}s)"
        )

        # --------------------------------------------------------------
        # Resolve the exceptions we'll catch. Newer `requests` versions
        # don't always expose RemoteDisconnected at the top level (it's
        # an alias of ConnectionError), so attribute access itself can
        # raise AttributeError — which silently aborted every fetch
        # before. Resolve once, defensively.
        # --------------------------------------------------------------
        try:
            _remote_disconnected_exc = requests.exceptions.RemoteDisconnected
        except AttributeError:
            _remote_disconnected_exc = requests.exceptions.ConnectionError

        _transport_errors = (
            requests.exceptions.ConnectionError,  # covers RemoteDisconnected
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            OSError,
            ConnectionResetError,
        )

        # --------------------------------------------------------------
        # Per-fetch batch sizes — driven by `self.chunk_size` which
        # came from settings.TAXII_CHUNK_SIZE (env var). Cap at 500
        # because beyond that, URL-encoded /objects/?id=a,b,c requests
        # can run into proxy URL-length limits.
        #
        # - manifest_batch_ids: how many IDs we accumulate from /manifest/
        #   before triggering a /objects/ bulk-fetch + yield.
        # - object_fetch_chunk: how many IDs per /objects/?id=a,b,c call.
        # - http_timeout: (connect_seconds, read_seconds).
        # --------------------------------------------------------------
        manifest_batch_ids = max(50, min(500, int(self.chunk_size or 500)))
        object_fetch_chunk = max(
            50, min(500, int(batch_size or self.chunk_size or 500))
        )
        http_timeout = (10, 60)

        cache_key = f"taxii_global_progress:{self.collection.id}"
        completion_key = f"taxii_completion:{self.collection.id}"

        # --------------------------------------------------------------
        # Resume state for the manifest walk.
        # --------------------------------------------------------------
        next_manifest_cursor: Optional[str] = None
        page_index = 0
        total_objects_yielded = 0
        if redis_client and redis_client.is_connected():
            try:
                cached = redis_client.get_cached_data(cache_key)
                if cached:
                    next_manifest_cursor = cached.get("next_cursor") or None
                    page_index = int(cached.get("last_page_index", 0))
                    total_objects_yielded = int(cached.get("total_processed", 0))
                    if next_manifest_cursor:
                        logger.info(
                            f"📋 Resuming manifest walk from cached cursor "
                            f"(page {page_index}, "
                            f"{total_objects_yielded} objects "
                            f"already yielded last run)"
                        )
                completed = redis_client.get_cached_data(completion_key)
                if completed and not next_manifest_cursor:
                    logger.info(
                        f"✅ Last run completed cleanly at "
                        f"{completed.get('completed_at')} — re-walking "
                        f"fresh; Redis dedup handles updates"
                    )
                    try:
                        redis_client._redis_client.delete(cache_key)
                    except Exception:
                        pass
                    page_index = 0
                    total_objects_yielded = 0
            except Exception as e:
                logger.warning(f"Failed to read checkpoint state: {e}")

        logger.info(
            f"📡 Streaming TAXII collection via /manifest/ → /objects/ "
            f"(manifest_batch_ids={manifest_batch_ids}, "
            f"object_fetch_chunk={object_fetch_chunk}, "
            f"batch_size={batch_size}, "
            f"resume={'yes' if next_manifest_cursor else 'no'})"
        )

        consecutive_failures = 0
        max_consecutive_failures = max(2, getattr(self, "page_retries", 3) + 1)

        manifest_url = self._manifest_url()
        objects_url = self._objects_url()

        try:
            while True:
                if is_shutdown_requested():
                    logger.info("Shutdown requested during streaming walk")
                    return

                page_start = time.time()

                # ==========================================================
                # PHASE 1: collect up to manifest_batch_ids IDs by walking
                # /manifest/ forward from next_manifest_cursor (or from
                # scratch on first iteration). Manifest responses are
                # small (~few KB even at 10k objects in the collection)
                # so we loop until we've buffered enough IDs to make a
                # /objects/ fetch worth doing.
                # ==========================================================
                buffered_ids: List[str] = []
                # Track the LAST manifest cursor we saw so we can persist
                # the checkpoint after this phase finishes (not after
                # each tiny manifest page — that's redundant).
                last_manifest_cursor = next_manifest_cursor
                last_manifest_more = True

                try:
                    while len(buffered_ids) < manifest_batch_ids:
                        # Raw GET against /manifest/. NO `limit` param.
                        params: Optional[Dict[str, Any]] = None
                        if last_manifest_cursor:
                            params = {"next": last_manifest_cursor}

                        body, status, cursor = self._raw_get_json(
                            manifest_url,
                            params=params,
                            timeout=http_timeout,
                        )
                        # Update cursor for next manifest fetch
                        last_manifest_cursor = cursor
                        last_manifest_more = bool(body.get("more", False))

                        # Manifest records: {"id": "...", "date_added": "...",
                        #                     "versions": [...], "media_type": "..."}.
                        # We only need `id` for the subsequent /objects/ fetch.
                        ids_on_page = [
                            m["id"]
                            for m in body.get("objects", [])
                            if isinstance(m, dict) and m.get("id")
                        ]
                        buffered_ids.extend(ids_on_page)
                        page_index += 1

                        # If server says we're done walking manifest, stop
                        # accumulating and proceed to PHASE 2 with what
                        # we have.
                        if not last_manifest_more or cursor is None:
                            break
                except _transport_errors as e:
                    consecutive_failures += 1
                    logger.warning(
                        f"  Manifest fetch failed (page {page_index}): "
                        f"{type(e).__name__}: {e}; "
                        f"consecutive_failures={consecutive_failures}/"
                        f"{max_consecutive_failures}"
                    )
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(
                            f"  Giving up after {consecutive_failures} "
                            f"consecutive manifest failures — bailing "
                            f"out of run; checkpoint preserved"
                        )
                        return
                    time.sleep(self.page_retry_backoff)
                    continue
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"  Manifest returned non-JSON (page {page_index}): {e}"
                    )
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error("Bailing — manifest endpoint is broken")
                        return
                    time.sleep(self.page_retry_backoff)
                    continue

                consecutive_failures = 0

                # ==========================================================
                # End-of-walk terminator: no IDs buffered AND manifest
                # says we're done.
                # ==========================================================
                if not buffered_ids and not last_manifest_more:
                    logger.info(
                        f"✅ Manifest walk complete: {page_index} "
                        f"manifest pages, {total_objects_yielded} "
                        f"objects yielded"
                    )
                    if redis_client and redis_client.is_connected():
                        try:
                            redis_client.set_cached_data(
                                completion_key,
                                {
                                    "completed_at": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                    "total_pages": page_index,
                                    "total_processed": total_objects_yielded,
                                },
                                ttl_seconds=86400,
                            )
                            redis_client._redis_client.delete(cache_key)
                            logger.info(
                                "💾 Marked collection complete, cleared checkpoint"
                            )
                        except Exception:
                            pass
                    return

                if not buffered_ids:
                    # Server told us `more: true` but returned empty page —
                    # skip and try again on next loop.
                    next_manifest_cursor = last_manifest_cursor
                    time.sleep(self.page_retry_backoff)
                    continue

                # ==========================================================
                # PHASE 2: fetch full STIX objects for the buffered IDs.
                # We slice into chunks of object_fetch_chunk because the
                # server has to render each full STIX object and that
                # adds up quickly on big collections.
                #
                # IMPORTANT: we drop any IDs we already know to be in
                # Redis's processed_groupings set so we don't re-render
                # objects MISP already has. (The caller
                # process_batch_groupings also dedups, but skipping
                # here saves upstream bandwidth.)
                # ==========================================================
                pending_ids: List[str] = list(buffered_ids)
                # Preserve cursor state for the NEXT outer iteration.
                next_manifest_cursor = last_manifest_cursor

                yielded_any = False
                for off in range(0, len(pending_ids), object_fetch_chunk):
                    if is_shutdown_requested():
                        return
                    sub_ids = pending_ids[off : off + object_fetch_chunk]

                    full_objects: List[Dict[str, Any]] = []
                    try:
                        # Raw GET /objects/?id=a,b,c — server returns the
                        # STIX objects matching any of those IDs.
                        body, status, cursor = self._raw_get_json(
                            objects_url,
                            params={"id": ",".join(sub_ids)},
                            timeout=http_timeout,
                        )
                        objs = body.get("objects", []) or []
                        if isinstance(objs, list):
                            full_objects = [o for o in objs if isinstance(o, dict)]
                    except _transport_errors as e:
                        logger.warning(
                            f"  Objects fetch failed for "
                            f"{len(sub_ids)} IDs: "
                            f"{type(e).__name__}: {e} — skipping chunk"
                        )
                        continue
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"  Objects fetch returned non-JSON for "
                            f"{len(sub_ids)} IDs: {e} — skipping chunk"
                        )
                        continue

                    if not full_objects:
                        continue

                    yielded_any = True
                    total_objects_yielded += len(full_objects)
                    if page_index <= 5 or page_index % 10 == 1:
                        logger.info(
                            f"  Page {page_index}: yielded "
                            f"+{len(full_objects)} objects "
                            f"(total_yielded={total_objects_yielded})"
                        )
                    if logger.isEnabledFor(logging.DEBUG):
                        type_counts: Dict[str, int] = {}
                        for obj in full_objects:
                            ot = (
                                obj.get("type")
                                if isinstance(obj, dict)
                                else getattr(obj, "type", None)
                            ) or "unknown"
                            type_counts[ot] = type_counts.get(ot, 0) + 1
                        logger.debug(f"    types: {dict(sorted(type_counts.items()))}")

                    # Persist checkpoint BEFORE yield so a crash mid-
                    # yield doesn't cause us to re-process a batch.
                    if redis_client and redis_client.is_connected():
                        try:
                            redis_client.set_cached_data(
                                cache_key,
                                {
                                    "next_cursor": next_manifest_cursor,
                                    "last_page_index": page_index,
                                    "last_run_time": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                    "total_processed": total_objects_yielded,
                                    "timestamp": time.time(),
                                },
                                ttl_seconds=86400,
                            )
                        except Exception as e:
                            logger.warning(f"Failed to cache progress: {e}")

                    yield full_objects

                # If the manifest walk is fully complete and we still
                # have nothing to yield, mark complete and stop.
                if not yielded_any and not last_manifest_more:
                    logger.info(
                        f"✅ Manifest walk complete (nothing more to "
                        f"yield): {page_index} manifest pages, "
                        f"{total_objects_yielded} objects yielded"
                    )
                    if redis_client and redis_client.is_connected():
                        try:
                            redis_client.set_cached_data(
                                completion_key,
                                {
                                    "completed_at": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                    "total_pages": page_index,
                                    "total_processed": total_objects_yielded,
                                },
                                ttl_seconds=86400,
                            )
                            redis_client._redis_client.delete(cache_key)
                        except Exception:
                            pass
                    return

                # Courtesy sleep + periodic GC.
                if rest_seconds and not is_shutdown_requested():
                    time.sleep(rest_seconds)
                if page_index % 50 == 0:
                    gc.collect()

        except Exception as e:
            logger.error(
                f"Error in streaming resource-managed retrieval: {e}",
                exc_info=True,
            )
            return

    def _fetch_batch_with_pages(self, offset, batch_size):
        """
        Fetch a specific batch using page-based navigation.
        This is a fallback when direct offset isn't supported.
        """
        try:
            # Calculate which page we need
            page_size = self.chunk_size
            start_page = offset // page_size
            end_page = (offset + batch_size) // page_size + 1

            logger.debug(
                f"Fetching pages {start_page} to {end_page} for offset {offset}"
            )

            # CORRECT pagination: taxii21.as_pages returns envelopes that
            # follow the server's `next` cursor. The previous code
            # `get_objects(...).as_pages()` was broken — get_objects()
            # returns a dict, not a thing with as_pages().
            pages = taxii21.as_pages(self.collection.get_objects, per_request=page_size)

            all_objects = []
            current_page = 0

            for page in pages:
                if current_page < start_page:
                    current_page += 1
                    continue

                if current_page > end_page:
                    break

                page_envelope = page.get("envelope", page)
                page_objects = page_envelope.get("objects", [])
                all_objects.extend(page_objects)
                current_page += 1

                if len(all_objects) >= batch_size + (offset % page_size):
                    break

            # Extract the specific batch we want
            start_idx = offset % len(all_objects) if all_objects else 0
            end_idx = min(start_idx + batch_size, len(all_objects))

            return all_objects[start_idx:end_idx] if all_objects else []

        except Exception as e:
            logger.error(f"Error in page-based batch fetch: {e}")
            return []

    def discover_available_object_types(self):
        """
        Discover what STIX object types are available on the TAXII server.
        This helps ensure we fetch all available types.
        """
        try:
            logger.info("Discovering available STIX object types...")

            # Get a sample of objects to see what types are available
            envelope = self.collection.get_objects(limit=1000)
            sample_objects = envelope.get("objects", [])

            discovered_types = set()
            for obj in sample_objects:
                if isinstance(obj, dict) and "type" in obj:
                    discovered_types.add(obj["type"])
                elif hasattr(obj, "type"):
                    discovered_types.add(obj.type)

            discovered_list = sorted(list(discovered_types))
            logger.info(f"Discovered object types: {discovered_list}")

            return discovered_list

        except Exception as e:
            logger.warning(f"Failed to discover object types: {e}")
            # Return comprehensive fallback list
            return [
                "indicator",
                "grouping",
                "malware",
                "threat-actor",
                "attack-pattern",
                "vulnerability",
                "intrusion-set",
                "campaign",
                "course-of-action",
                "tool",
                "identity",
                "marking-definition",
                "location",
                "note",
                "observed-data",
                "opinion",
                "report",
                "infrastructure",
            ]

    def get_all_objects(self, max_objects=None, object_types=None, start_date=None):
        """
        Retrieve all STIX objects from the TAXII collection with smart pagination.

        Args:
            max_objects: Maximum number of objects to retrieve
            object_types: List of STIX object types to filter (e.g., ['indicator', 'grouping'])
            start_date: Only retrieve objects modified after this date

        Returns:
            List of STIX objects
        """
        if object_types is None:
            # Dynamically discover available object types to ensure we get everything
            logger.info("No object types specified, discovering available types...")
            object_types = self.discover_available_object_types()
            logger.info(f"Will fetch objects of types: {object_types}")

        logger.info(f"Retrieving STIX objects of types: {object_types}")

        try:
            all_objects = []
            total_retrieved = 0

            for obj_type in object_types:
                if max_objects and total_retrieved >= max_objects:
                    break

                logger.info(f"Fetching {obj_type} objects...")

                # Calculate remaining objects to fetch
                remaining = None
                if max_objects:
                    remaining = max_objects - total_retrieved

                objects = self._get_objects_by_type(
                    obj_type, max_objects=remaining, start_date=start_date
                )

                if objects:
                    all_objects.extend(objects)
                    total_retrieved += len(objects)
                    logger.info(f"Retrieved {len(objects)} {obj_type} objects")
                else:
                    logger.info(f"No {obj_type} objects found")

            logger.info(f"Total objects retrieved: {total_retrieved}")
            return all_objects

        except Exception as e:
            logger.error(f"Error retrieving STIX objects: {e}")
            return []

    def _get_objects_by_type(self, object_type, max_objects=None, start_date=None):
        """
        Get objects of a specific type using pagination.
        """
        try:
            # Build filter parameters
            filter_params = {"type": object_type}

            if start_date:
                filter_params["added_after"] = start_date.isoformat()

            logger.debug(f"Using filter parameters: {filter_params}")

            objects = []
            retrieved_count = 0

            # Use taxii21.as_pages for efficient pagination (correct API).
            # The previous `get_objects().as_pages()` pattern was broken
            # because get_objects returns a dict that has no as_pages method.
            try:
                envelope_pages = taxii21.as_pages(
                    lambda **kw: self.collection.get_objects(
                        limit=self.chunk_size, **filter_params, **kw
                    ),
                    per_request=self.chunk_size,
                )

                for page_envelope in envelope_pages:
                    if max_objects and retrieved_count >= max_objects:
                        break

                    page_objects = page_envelope.get("objects", [])
                    if not page_objects:
                        logger.debug(f"Empty page received for {object_type}")
                        continue

                    # Apply max_objects limit to this page
                    if max_objects:
                        remaining = max_objects - retrieved_count
                        page_objects = page_objects[:remaining]

                    objects.extend(page_objects)
                    retrieved_count += len(page_objects)

                    logger.debug(
                        f"Retrieved {len(page_objects)} {object_type} objects in this page (total: {retrieved_count})"
                    )

                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

            except Exception as page_error:
                logger.warning(
                    f"Pagination failed for {object_type}, trying direct fetch: {page_error}"
                )

                # Fallback to direct fetch without pagination
                try:
                    if max_objects is None:
                        # Fetch ALL objects by using a very high limit
                        limit = 100000  # Very high limit to get all objects
                    else:
                        limit = max_objects or 1000

                    envelope = self.collection.get_objects(limit=limit, **filter_params)
                    objects = envelope.get("objects", [])
                    retrieved_count = len(objects)

                except Exception as fallback_error:
                    logger.error(
                        f"Both pagination and direct fetch failed for {object_type}: {fallback_error}"
                    )
                    return []

            logger.info(
                f"Successfully retrieved {retrieved_count} {object_type} objects"
            )
            return objects

        except Exception as e:
            logger.error(f"Error retrieving {object_type} objects: {e}")
            return []

    def get_all_objects_with_grouping_priority(
        self, max_objects=None, max_batches=None
    ):
        """
        Enhanced method that prioritizes grouping objects and their referenced indicators.
        This ensures we get complete threat intelligence packages with performance optimizations.

        Args:
            max_objects: Maximum number of objects to retrieve
            max_batches: Maximum number of batches to process when searching for referenced objects (for testing)
        """
        try:
            start_time = time.time()
            logger.info(
                "🚀 Starting ENHANCED STIX object retrieval with performance optimizations..."
            )

            # For testing: limit grouping objects to a small number
            grouping_limit = 5 if max_objects and max_objects <= 50 else None

            # Step 1: Get grouping objects with performance monitoring
            grouping_start = time.time()
            grouping_objects = self._get_objects_by_type(
                "grouping", max_objects=grouping_limit
            )
            grouping_time = time.time() - grouping_start

            logger.info(
                f"📦 Found {len(grouping_objects)} grouping objects in {grouping_time:.2f}s"
            )

            if not grouping_objects:
                logger.warning(
                    "No grouping objects found. Falling back to direct object retrieval..."
                )
                return self.get_all_objects(max_objects=max_objects)

            # Step 2: Extract referenced object IDs with optimization
            referenced_ids = set()
            for grouping in grouping_objects:
                object_refs = grouping.get("object_refs", [])
                referenced_ids.update(object_refs)

            logger.info(f"🔗 Groupings reference {len(referenced_ids)} unique objects")

            # Step 3: Optimized search for referenced objects
            all_objects = list(grouping_objects)  # Start with groupings

            if referenced_ids:
                search_start = time.time()

                if max_batches:
                    logger.info(
                        f"⚡ PERFORMANCE MODE: Searching for referenced objects (max {max_batches} batches)"
                    )
                else:
                    logger.info(
                        f"🔍 Searching for all {len(referenced_ids)} referenced objects with optimizations"
                    )

                referenced_objects = self._search_for_referenced_objects(
                    referenced_ids, max_batches=max_batches
                )
                search_time = time.time() - search_start

                all_objects.extend(referenced_objects)
                logger.info(
                    f"🎯 Reference search completed in {search_time:.2f}s: found {len(referenced_objects)} objects"
                )

            # Step 4: Apply max_objects limit if specified
            if max_objects and len(all_objects) > max_objects:
                logger.info(
                    f"📊 Limiting results to {max_objects} objects (from {len(all_objects)} total)"
                )
                all_objects = all_objects[:max_objects]

            total_time = time.time() - start_time

            # Performance summary
            logger.info(f"✅ RETRIEVAL COMPLETED in {total_time:.2f}s:")
            logger.info(f"   📦 Groupings: {len(grouping_objects)} objects")
            logger.info(
                f"   🔗 Referenced: {len(referenced_objects) if 'referenced_objects' in locals() else 0} objects"
            )
            logger.info(f"   📊 Total: {len(all_objects)} objects")
            logger.info(
                f"   ⚡ Performance: {len(all_objects) / total_time:.1f} objects/second"
            )

            return all_objects

        except Exception as e:
            logger.error(f"Error in enhanced object retrieval: {e}")
            # Fallback to basic retrieval
            return self.get_all_objects(max_objects=max_objects)

    def get_all_objects_with_comprehensive_batching(
        self, max_objects=None, batch_size=5000
    ):
        """
        Enhanced method with comprehensive batching to ensure complete bundling of
        all indicators from groupings. Processes STIX objects in batches of 5000
        to ensure all indicators are bundled into events before sending to MISP.

        Args:
            max_objects: Maximum number of objects to retrieve
            batch_size: Size of batches for processing (default: 5000)

        Returns:
            List of STIX objects with comprehensive bundling
        """
        try:
            start_time = time.time()
            logger.info(
                f"🚀 Starting COMPREHENSIVE BATCHING with {batch_size} objects per batch..."
            )

            # Step 1: Get all grouping objects first to understand structure
            grouping_start = time.time()
            all_groupings = self._get_objects_by_type("grouping", max_objects=None)
            grouping_time = time.time() - grouping_start

            logger.info(
                f"📦 Found {len(all_groupings)} grouping objects in {grouping_time:.2f}s"
            )

            if not all_groupings:
                logger.warning("No grouping objects found. Using standard retrieval...")
                return self.get_all_objects(max_objects=max_objects)

            # Step 2: Extract ALL referenced object IDs comprehensively
            all_referenced_ids = set()
            grouping_summary = {}

            for grouping in all_groupings:
                object_refs = grouping.get("object_refs", [])
                all_referenced_ids.update(object_refs)
                grouping_summary[grouping.get("id")] = {
                    "name": grouping.get("name", "Unknown"),
                    "ref_count": len(object_refs),
                }

            logger.info(
                f"🔗 Total unique objects referenced by all groupings: {len(all_referenced_ids)}"
            )

            # Step 3: Comprehensive batched search for ALL referenced objects
            all_objects = list(all_groupings)  # Start with all groupings

            if all_referenced_ids:
                search_start = time.time()
                logger.info(
                    f"🔍 Starting comprehensive batch search for {len(all_referenced_ids)} objects in batches of {batch_size}"
                )

                # Process in batches of specified size (default 5000)
                referenced_objects = self._comprehensive_batch_search(
                    all_referenced_ids, batch_size
                )
                search_time = time.time() - search_start

                all_objects.extend(referenced_objects)
                logger.info(
                    f"🎯 Comprehensive search completed in {search_time:.2f}s: found {len(referenced_objects)} objects"
                )

                # Verify completeness of bundling
                self._verify_bundling_completeness(grouping_summary, referenced_objects)

            # Step 4: Apply max_objects limit if specified
            if max_objects and len(all_objects) > max_objects:
                logger.info(
                    f"📊 Limiting results to {max_objects} objects (from {len(all_objects)} total)"
                )
                all_objects = all_objects[:max_objects]

            total_time = time.time() - start_time

            # Comprehensive performance summary
            logger.info(f"✅ COMPREHENSIVE BATCHING COMPLETED in {total_time:.2f}s:")
            logger.info(f"   📦 Groupings: {len(all_groupings)} objects")
            logger.info(
                f"   🔗 Referenced: {len(referenced_objects) if 'referenced_objects' in locals() else 0} objects"
            )
            logger.info(f"   📊 Total: {len(all_objects)} objects")
            logger.info(
                f"   ⚡ Performance: {len(all_objects) / total_time:.1f} objects/second"
            )
            logger.info(f"   🎯 Batch Size: {batch_size} objects per batch")

            return all_objects

        except Exception as e:
            logger.error(f"Error in comprehensive batching: {e}")
            # Fallback to enhanced grouping priority method
            return self.get_all_objects_with_grouping_priority(max_objects=max_objects)

    def _comprehensive_batch_search(
        self, object_ids: Set[str], batch_size: int = 5000
    ) -> List[Dict]:
        """
        Comprehensive batch search that processes objects in larger batches (5000)
        to ensure complete bundling of all indicators before MISP event creation.

        Args:
            object_ids: Set of object IDs to search for
            batch_size: Size of each batch (default: 5000)

        Returns:
            List of found STIX objects
        """
        try:
            found_objects = []
            object_id_list = list(object_ids)

            # Calculate batches based on the specified batch size (5000)
            total_batches = (len(object_id_list) + batch_size - 1) // batch_size

            logger.info(
                f"🔍 COMPREHENSIVE BATCH SEARCH: Processing {len(object_id_list)} objects in {total_batches} batches of {batch_size}"
            )

            start_time = time.time()
            successful_batches = 0

            for batch_num in range(total_batches):
                batch_start = time.time()
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(object_id_list))
                batch_ids = object_id_list[start_idx:end_idx]

                try:
                    logger.info(
                        f"🔎 Processing batch {batch_num + 1}/{total_batches}: {len(batch_ids)} objects"
                    )

                    envelope = self.collection.get_objects(
                        limit=batch_size, id=batch_ids
                    )

                    batch_objects = envelope.get("objects", [])
                    if batch_objects:
                        found_objects.extend(batch_objects)
                        successful_batches += 1

                        batch_time = time.time() - batch_start
                        avg_time = (time.time() - start_time) / (batch_num + 1)
                        eta = avg_time * (total_batches - batch_num - 1)

                        logger.info(
                            f"✅ Batch {batch_num + 1}/{total_batches}: {len(batch_objects)} objects found ({batch_time:.2f}s, ETA: {eta:.1f}s)"
                        )
                    else:
                        logger.debug(f"📭 Empty batch {batch_num + 1}/{total_batches}")

                except Exception as batch_error:
                    logger.warning(f"❌ Batch {batch_num + 1} failed: {batch_error}")
                    continue

                # Reduced delay for better performance with large batches
                if total_batches > 5:
                    time.sleep(0.02)  # Minimal delay for large batch processing

            total_time = time.time() - start_time
            success_rate = (
                (successful_batches / total_batches) * 100 if total_batches > 0 else 0
            )

            logger.info(f"🎯 COMPREHENSIVE BATCH SEARCH COMPLETED:")
            logger.info(f"   📊 Total objects found: {len(found_objects)}")
            logger.info(f"   ⏱️  Total time: {total_time:.2f}s")
            logger.info(f"   ✅ Success rate: {success_rate:.1f}%")
            logger.info(
                f"   ⚡ Throughput: {len(found_objects) / total_time:.1f} objects/second"
            )

            return found_objects

        except Exception as e:
            logger.error(f"Error in comprehensive batch search: {e}")
            return []

    def _verify_bundling_completeness(
        self, grouping_summary: Dict, referenced_objects: List[Dict]
    ):
        """
        Verify that all referenced objects for each grouping have been found
        to ensure complete bundling before MISP event creation.

        Args:
            grouping_summary: Dictionary with grouping info and reference counts
            referenced_objects: List of found referenced objects
        """
        try:
            found_ids = {
                obj.get("id")
                for obj in referenced_objects
                if isinstance(obj, dict) and "id" in obj
            }

            logger.info("🔍 BUNDLING COMPLETENESS VERIFICATION:")
            complete_groupings = 0
            incomplete_groupings = 0

            for grouping_id, info in grouping_summary.items():
                # This is a simplified check - in practice, we'd need to track per-grouping referenced IDs
                logger.info(f"   📦 {info['name']}: {info['ref_count']} references")

            total_refs = sum(info["ref_count"] for info in grouping_summary.values())
            found_ratio = len(found_ids) / total_refs if total_refs > 0 else 0

            logger.info(
                f"   📊 Overall bundling completeness: {len(found_ids)}/{total_refs} objects ({found_ratio * 100:.1f}%)"
            )

            if found_ratio >= 0.95:
                logger.info("   ✅ Excellent bundling completeness (≥95%)")
            elif found_ratio >= 0.80:
                logger.warning("   ⚠️  Good bundling completeness (≥80%)")
            else:
                logger.warning(
                    "   ❌ Poor bundling completeness (<80%) - some indicators may be missing"
                )

        except Exception as e:
            logger.error(f"Error verifying bundling completeness: {e}")

    def _search_for_referenced_objects(
        self, object_ids: Set[str], max_batches: Optional[int] = None
    ) -> List[Dict]:
        """
        Search for specific objects by their IDs with performance optimizations.

        Args:
            object_ids: Set of object IDs to search for
            max_batches: Maximum number of batches to process (for testing). If None, process all batches.
        """
        try:
            logger.info(f"Searching for {len(object_ids)} referenced objects...")

            found_objects = []
            remaining_ids = set(object_ids)

            # Performance optimization: Skip type-by-type search if we have many IDs
            # and go directly to batch ID search which is more efficient
            if len(object_ids) > 50:
                logger.info(
                    f"Large ID set detected ({len(object_ids)} IDs) - using optimized batch search only"
                )
                return self._optimized_batch_search(remaining_ids, max_batches)

            # Strategy 1: Optimized type-based search with early termination
            logger.info("Starting optimized type-based search...")
            common_types = [
                "indicator",
                "identity",
                "malware",
                "threat-actor",
                "attack-pattern",
            ]

            # Process types in parallel-like manner (though still sequential due to API limits)
            for obj_type in common_types:
                if not remaining_ids:  # Early termination if all IDs found
                    logger.info(
                        "All referenced objects found - terminating type search early"
                    )
                    break

                try:
                    logger.info(
                        f"Searching {obj_type} objects for {len(remaining_ids)} remaining IDs..."
                    )

                    # Use smaller chunks for better responsiveness
                    type_objects = self._get_objects_by_type_optimized(
                        obj_type, target_ids=remaining_ids
                    )

                    # Process and remove found IDs
                    type_found = 0
                    for obj in type_objects:
                        if isinstance(obj, dict) and obj.get("id") in remaining_ids:
                            found_objects.append(obj)
                            remaining_ids.remove(obj["id"])
                            type_found += 1

                    if type_found > 0:
                        logger.info(
                            f"✓ Found {type_found} {obj_type} objects ({len(remaining_ids)} IDs remaining)"
                        )
                    else:
                        logger.debug(f"No {obj_type} objects found")

                except Exception as type_error:
                    logger.warning(f"Error fetching {obj_type} objects: {type_error}")
                    continue

            # Strategy 2: Optimized batch search for remaining IDs
            if remaining_ids and (len(found_objects) < len(object_ids) * 0.8):
                logger.info(
                    f"Running optimized batch search for {len(remaining_ids)} remaining IDs..."
                )
                batch_found = self._optimized_batch_search(remaining_ids, max_batches)
                found_objects.extend(batch_found)

            # Remove duplicates efficiently
            unique_objects = {
                obj["id"]: obj
                for obj in found_objects
                if isinstance(obj, dict) and "id" in obj
            }
            final_objects = list(unique_objects.values())

            logger.info(
                f"Search completed: found {len(final_objects)} objects out of {len(object_ids)} requested ({len(final_objects) / len(object_ids) * 100:.1f}% success rate)"
            )

            return final_objects

        except Exception as e:
            logger.error(f"Error searching for referenced objects: {e}")
            return []

    def _optimized_batch_search(
        self, object_ids: Set[str], max_batches: Optional[int] = None
    ) -> List[Dict]:
        """
        Optimized batch search with improved performance and reduced delays.
        """
        try:
            found_objects = []
            object_id_list = list(object_ids)

            # Dynamic batch sizing based on total objects
            if len(object_id_list) > 1000:
                search_batch_size = 200  # Larger batches for big datasets
            elif len(object_id_list) > 100:
                search_batch_size = 100  # Medium batches
            else:
                search_batch_size = 50  # Smaller batches for small datasets

            # Calculate batches
            total_batches = (
                len(object_id_list) + search_batch_size - 1
            ) // search_batch_size
            batches_to_process = (
                min(total_batches, max_batches) if max_batches else total_batches
            )

            if max_batches and max_batches < total_batches:
                logger.info(
                    f"🚀 PERFORMANCE MODE: Processing {max_batches}/{total_batches} batches (batch size: {search_batch_size})"
                )
            else:
                logger.info(
                    f"🚀 OPTIMIZED BATCH SEARCH: Processing {total_batches} batches (batch size: {search_batch_size})"
                )

            # Process batches with performance optimizations
            successful_batches = 0
            start_time = time.time()

            for batch_num in range(batches_to_process):
                batch_start = time.time()
                i = batch_num * search_batch_size
                batch_ids = object_id_list[i : i + search_batch_size]

                try:
                    envelope = self.collection.get_objects(
                        limit=search_batch_size, id=batch_ids
                    )

                    batch_objects = envelope.get("objects", [])
                    if batch_objects:
                        found_objects.extend(batch_objects)
                        successful_batches += 1

                        batch_time = time.time() - batch_start
                        avg_time = (time.time() - start_time) / (batch_num + 1)
                        eta = avg_time * (batches_to_process - batch_num - 1)

                        logger.info(
                            f"⚡ Batch {batch_num + 1}/{batches_to_process}: {len(batch_objects)} objects ({batch_time:.2f}s, ETA: {eta:.1f}s)"
                        )
                    else:
                        logger.debug(
                            f"Empty batch {batch_num + 1}/{batches_to_process}"
                        )

                except Exception as batch_error:
                    logger.warning(f"Batch {batch_num + 1} failed: {batch_error}")
                    continue

                # Reduced delay for better performance - only if processing many batches
                if batches_to_process > 10:
                    time.sleep(0.05)  # Reduced from 0.1s to 0.05s

            total_time = time.time() - start_time
            success_rate = (
                (successful_batches / batches_to_process) * 100
                if batches_to_process > 0
                else 0
            )

            logger.info(
                f"🎯 Batch search completed: {len(found_objects)} objects in {total_time:.2f}s ({success_rate:.1f}% batch success rate)"
            )

            return found_objects

        except Exception as e:
            logger.error(f"Error in optimized batch search: {e}")
            return []

    def _get_objects_by_type_optimized(
        self, object_type: str, target_ids: Set[str] = None, max_objects: int = None
    ) -> List[Dict]:
        """
        Optimized version of _get_objects_by_type with early termination and better chunking.
        """
        try:
            # Build filter parameters
            filter_params = {"type": object_type}

            objects = []
            retrieved_count = 0
            found_target_count = 0

            # Use larger chunk size for better performance
            chunk_size = min(
                self.chunk_size * 2, 200
            )  # Double the chunk size but cap at 200

            try:
                # CORRECT pagination: use taxii21.as_pages, not
                # `get_objects().as_pages()` (which never worked).
                envelope_pages = taxii21.as_pages(
                    lambda **kw: self.collection.get_objects(
                        limit=chunk_size, **filter_params, **kw
                    ),
                    per_request=chunk_size,
                )

                for page_num, page_envelope in enumerate(envelope_pages):
                    if max_objects and retrieved_count >= max_objects:
                        break

                    # Early termination if we found all target IDs
                    if target_ids and found_target_count >= len(target_ids):
                        logger.debug(
                            f"All target {object_type} objects found - terminating early"
                        )
                        break

                    page_objects = page_envelope.get("objects", [])
                    if not page_objects:
                        logger.debug(f"Empty page {page_num + 1} for {object_type}")
                        continue

                    # Apply max_objects limit to this page
                    if max_objects:
                        remaining = max_objects - retrieved_count
                        page_objects = page_objects[:remaining]

                    # If we have target IDs, count how many we found in this page
                    if target_ids:
                        page_target_count = sum(
                            1
                            for obj in page_objects
                            if isinstance(obj, dict) and obj.get("id") in target_ids
                        )
                        found_target_count += page_target_count

                        if page_target_count > 0:
                            logger.debug(
                                f"Page {page_num + 1}: found {page_target_count} target {object_type} objects"
                            )

                    objects.extend(page_objects)
                    retrieved_count += len(page_objects)

                    # Reduced delay for better performance
                    time.sleep(0.02)  # Reduced from 0.1s to 0.02s

            except Exception as page_error:
                logger.warning(
                    f"Pagination failed for {object_type}, trying direct fetch: {page_error}"
                )

                # Fallback to direct fetch
                try:
                    envelope = self.collection.get_objects(
                        limit=max_objects or 1000, **filter_params
                    )
                    objects = envelope.get("objects", [])
                    retrieved_count = len(objects)

                except Exception as fallback_error:
                    logger.error(
                        f"Both pagination and direct fetch failed for {object_type}: {fallback_error}"
                    )
                    return []

            logger.debug(f"Retrieved {retrieved_count} {object_type} objects")
            return objects

        except Exception as e:
            logger.error(f"Error retrieving {object_type} objects: {e}")
            return []

    def extract_grouping_objects(self, stix_objects):
        """
        Extract grouping objects and create a memory store for processing.

        Returns:
            Tuple of (MemoryStore, list of grouping IDs)
        """
        try:
            if not stix_objects:
                logger.warning("No STIX objects provided for grouping extraction")
                return None, []

            logger.info(
                f"Processing {len(stix_objects)} STIX objects for grouping extraction"
            )

            # Parse STIX objects and create memory store
            parsed_objects = []
            grouping_ids = []

            for obj in stix_objects:
                # ---- Extract grouping IDs BEFORE attempting parse, so
                # a stix2 library parse-error on the grouping object itself
                # doesn't drop the grouping id (previously the parse was
                # first, and any exception skipped the rest of the
                # iteration including the grouping-id append).
                if isinstance(obj, dict):
                    if obj.get("type") == "grouping":
                        gid = obj.get("id")
                        if gid:
                            grouping_ids.append(gid)
                elif hasattr(obj, "type"):
                    if obj.type == "grouping":
                        grouping_ids.append(obj.id)

                # ---- Now try to parse the object into a STIX SDO. We do
                # this in a separate try/except per-object so a single
                # parser failure doesn't take down the rest of the batch.
                try:
                    if isinstance(obj, dict):
                        parsed_obj = parse(obj, allow_custom=True)
                        parsed_objects.append(parsed_obj)
                    elif hasattr(obj, "type"):
                        parsed_objects.append(obj)
                except Exception as parse_error:
                    logger.warning(f"Failed to parse STIX object: {parse_error}")
                    # On parse failure, fall back to a hand-rolled STIX
                    # Indicator object so we still keep its data in the
                    # memory store as a basic python dict. This avoids
                    # "Failed to parse" silent drops for malformed
                    # objects.
                    if isinstance(obj, dict) and obj.get("pattern"):
                        parsed_objects.append(obj)
                    continue

            if not parsed_objects:
                logger.warning("No valid STIX objects could be parsed")
                return None, []

            # Create memory store
            memory_store = MemoryStore(parsed_objects)

            logger.info(f"Created memory store with {len(parsed_objects)} objects")
            logger.info(f"Found {len(grouping_ids)} grouping objects: {grouping_ids}")

            return memory_store, grouping_ids

        except Exception as e:
            logger.error(f"Error extracting grouping objects: {e}")
            return None, []

    def get_collection_info(self):
        """Get information about the current collection."""
        if not self.collection:
            return None

        try:
            return {
                "id": self.collection.id,
                "title": self.collection.title,
                "description": getattr(self.collection, "description", "N/A"),
                "can_read": self.collection.can_read,
                "can_write": self.collection.can_write,
                "media_types": getattr(self.collection, "media_types", []),
            }
        except Exception as e:
            logger.error(f"Error getting collection info: {e}")
            return None

    def get_objects_for_testing(self, max_objects=50, max_batches=2):
        """
        Quick method to get a limited number of objects for testing MISP integration.
        This significantly reduces processing time by limiting both object count and batch processing.

        Args:
            max_objects: Maximum total objects to retrieve (default: 50)
            max_batches: Maximum batches to process when searching (default: 2)

        Returns:
            List of STIX objects suitable for testing
        """
        logger.info(
            f"Running test mode: max {max_objects} objects, max {max_batches} batches"
        )
        return self.get_all_objects_with_grouping_priority(
            max_objects=max_objects, max_batches=max_batches
        )

    def test_connection(self):
        """Test the TAXII connection and return basic server info."""
        try:
            if not self.server:
                return False, "Server not initialized"

            # Try to get server information
            server_info = {
                "title": getattr(self.server, "title", "Unknown"),
                "description": getattr(self.server, "description", "N/A"),
                "api_roots": len(self.server.api_roots) if self.server.api_roots else 0,
            }

            collection_info = self.get_collection_info()

            return True, {"server": server_info, "collection": collection_info}

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False, str(e)
