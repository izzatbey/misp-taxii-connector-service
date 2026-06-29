import redis
import logging
import requests
import os
import json
import http.client
import time
from urllib3.exceptions import ProtocolError
from taxii2client.exceptions import AccessError
from taxii2client.v21 import Server, Collection, Status as TAXIIStatus, as_pages

from clients.redis_utility import RedisClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TAXII_IDS_CACHE_KEY_PREFIX = "taxii_ids:"
TAXII_METADATA_CACHE_KEY_PREFIX = "taxii_metadata:"


class TAXIIClient:
    """
    A client for interacting with a TAXII server.
    """

    def __init__(
        self,
        taxii_url: str,
        username: str,
        password: str,
        verify_ssl: bool = False,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: str | None = None,
        cache_ttl_seconds: int = 3600,
    ):
        self.taxii_url = taxii_url
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self._server = None
        self._api_root = None
        self._collection = None
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_push_retries = 3

        self.redis_client_instance = RedisClient(
            redis_host, redis_port, redis_db, redis_password
        )
        self.redis_client = self.redis_client_instance.get_client()

    def connect_to_server(self) -> Server:
        """
        Establishes TAXII Server Connection.
        """
        if self._server:
            return self._server

        logger.info(f"Connecting to TAXII Server at {self.taxii_url}...")
        try:
            self._server = Server(
                self.taxii_url,
                user=self.username,
                password=self.password,
                verify=self.verify_ssl,
            )
            logger.info("Successfully connected to TAXII Server.")
            return self._server
        except AccessError as ae:
            logger.critical(
                f"Access Denied to TAXII Server: {ae}. Check credentials. Exiting."
            )
            raise
        except requests.exceptions.RequestException as req_e:
            logger.critical(f"Error connecting to TAXII server: {req_e}. Exiting.")
            raise
        except Exception as e:
            logger.critical(
                f"Unexpected error during TAXII server connection: {e}. Exiting."
            )
            raise

    def get_default_collection(self) -> Collection:
        """
        Retrieves the default collection from the TAXII server.
        """
        if self._collection:
            return self._collection

        server = self.connect_to_server()

        self._api_root = server.api_roots[0]
        logger.info(f"Using API Root: {self._api_root.title}")

        logger.info("Retrieving collection...")
        self._collection = self._api_root.collections[0]
        logger.info(
            f"Selected TAXII Collection: {self._collection.title} (ID: {self._collection.id})"
        )
        return self._collection

    def _fetch_all_stix_ids_from_taxii(
        self, max_objects_for_test: int | None = None
    ) -> set[str]:
        """
        Internal method to fetch all STIX Object IDs directly from the TAXII server.
        """
        logger.info(
            f"TAXIIClient._fetch_all_stix_ids_from_taxii received max_objects_for_test: {max_objects_for_test}"
        )

        fetched_ids = set()
        if not self._collection:
            logger.warning(
                "TAXII collection not initialized for direct fetch. Cannot retrieve existing IDs."
            )
            return fetched_ids

        logger.info(
            f"Fetching ALL STIX Object IDs directly from TAXII Collection '{self._collection.title}'..."
        )
        try:
            LIMIT_PER_REQUEST = 500
            page_counter = 0

            for envelope in as_pages(
                self._collection.get_objects,
                per_request=LIMIT_PER_REQUEST,
            ):
                page_counter += 1
                current_objects = []

                if isinstance(envelope, TAXIIStatus):
                    current_objects = envelope.objects
                elif isinstance(envelope, dict) and "objects" in envelope:
                    current_objects = envelope.get("objects", [])
                elif envelope == {}:
                    logger.info(
                        "TAXII collection returned an empty dictionary page. Assuming no more objects."
                    )
                    break
                else:
                    logger.warning(
                        f"TAXII collection.get_objects() via as_pages returned an unexpected response type on page {page_counter}: {type(envelope)}. "
                        "Assuming no more existing objects."
                    )
                    logger.debug(f"Full unexpected response: {envelope}")
                    break

                if not current_objects:
                    logger.info(
                        f"Page {page_counter} returned no objects. Assuming end of collection."
                    )
                    break

                for obj in current_objects:
                    fetched_ids.add(obj["id"])

                logger.info(
                    f"[*] Page {page_counter}: Added {len(current_objects)} objects. Total collected so far: {len(fetched_ids)}"
                )

                if (
                    max_objects_for_test is not None
                    and len(fetched_ids) >= max_objects_for_test
                ):
                    logger.info(
                        f"Reached test limit of {max_objects_for_test} objects. Stopping fetch."
                    )
                    break

        except AccessError as ae:
            logger.error(
                f"[✘] Access Denied when getting existing IDs from TAXII. Error: {ae}"
            )
        except requests.exceptions.RequestException as req_e:
            logger.error(
                f"[✘] HTTP/Network Error getting existing IDs from TAXII: {req_e}"
            )
        except Exception as e:
            logger.error(
                f"[✘] Failed to get existing IDs directly from TAXII: {e}",
                exc_info=True,
            )

        logger.info(
            f"[✔] Successfully retrieved {len(fetched_ids)} existing STIX Object IDs directly from TAXII."
        )
        return fetched_ids

    def get_existing_stix_ids(
        self, max_objects_for_test: int | None = None
    ) -> set[str]:
        """
        Retrieves a set of existing STIX Object IDs from the connected TAXII collection,
        using Redis cache if available and not expired.
        """
        existing_ids = set()
        if not self._collection:
            logger.warning(
                "TAXII collection not initialized. Cannot retrieve existing IDs."
            )
            return existing_ids

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        if self.redis_client:
            try:
                # Check Redis cache first
                cached_ids = self.redis_client.smembers(cache_key)
                if cached_ids:
                    logger.info(
                        f"Retrieved {len(cached_ids)} STIX IDs from Redis cache (key: {cache_key})."
                    )
                    # If we retrieved from cache, we should still respect the max_objects_for_test
                    # for the *return value*, even if the cache contains more.
                    # However, for 'existing_ids' that are later used for de-duplication,
                    # returning the full set is usually desired. The 'max_objects_for_test'
                    # is primarily for limiting the *fetch* operation, not necessarily
                    # limiting the *return* from cache. Given your current logs, it implies
                    # the cache was empty, so this branch wasn't taken.
                    return cached_ids
                else:
                    logger.info(
                        f"No STIX IDs found in Redis cache or cache expired for key: {cache_key}. Fetching from TAXII."
                    )
            except Exception as e:
                logger.error(
                    f"Error accessing Redis cache for TAXII IDs: {e}. Fetching from TAXII instead.",
                    exc_info=True,
                )
                # Fallback to direct fetch if Redis fails
        else:
            logger.warning(
                "Redis client not available. Fetching all STIX IDs directly from TAXII (no caching)."
            )

        # --- NEW DIAGNOSTIC LOG (ADDED HERE) ---
        logger.info(
            f"TAXIIClient.get_existing_stix_ids is about to call _fetch_all_stix_ids_from_taxii with max_objects_for_test={max_objects_for_test}"
        )
        # --- END NEW DIAGNOSTIC LOG ---

        # If not in cache or Redis not available, fetch directly from TAXII
        existing_ids = self._fetch_all_stix_ids_from_taxii(
            max_objects_for_test=max_objects_for_test
        )

        # Store in Redis if connection is available and data was fetched
        # We only cache if a full fetch was performed (i.e., max_objects_for_test was None)
        if self.redis_client and existing_ids and max_objects_for_test is None:
            try:
                self.redis_client.sadd(cache_key, *existing_ids)
                self.redis_client.expire(cache_key, self.cache_ttl_seconds)
                logger.info(
                    f"Stored {len(existing_ids)} STIX IDs in Redis cache (key: {cache_key}) with TTL: {self.cache_ttl_seconds}s."
                )
            except Exception as e:
                logger.error(
                    f"Failed to store STIX IDs in Redis cache: {e}", exc_info=True
                )
        elif self.redis_client and existing_ids and max_objects_for_test is not None:
            # This branch correctly skips caching when a test limit is applied
            logger.info(
                f"Skipping cache for TAXII IDs because max_objects_for_test was set ({max_objects_for_test})."
            )

        return existing_ids

    def add_stix_ids_to_cache(self, new_ids: set[str]) -> None:
        """
        Add new STIX IDs to the Redis cache without fetching all existing IDs.
        This is useful when we know new IDs have been added to TAXII.
        """
        if not self.redis_client or not self._collection:
            return

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        try:
            if new_ids:
                # Add new IDs to the existing set
                self.redis_client.sadd(cache_key, *new_ids)
                # Refresh TTL
                self.redis_client.expire(cache_key, self.cache_ttl_seconds)
                logger.info(
                    f"Added {len(new_ids)} new STIX IDs to Redis cache (key: {cache_key})."
                )
        except Exception as e:
            logger.error(
                f"Failed to add new STIX IDs to Redis cache: {e}", exc_info=True
            )

    def is_stix_id_cached(self, stix_id: str) -> bool:
        """
        Check if a specific STIX ID exists in the Redis cache.
        This is more efficient than fetching all IDs for single ID checks.
        """
        if not self.redis_client or not self._collection:
            return False

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        try:
            return self.redis_client.sismember(cache_key, stix_id)
        except Exception as e:
            logger.error(f"Failed to check STIX ID in Redis cache: {e}", exc_info=True)
            return False

    def check_stix_ids_existence(self, stix_ids: set[str]) -> tuple[set[str], set[str]]:
        """
        Check which STIX IDs already exist in the cache and which are new.

        Args:
            stix_ids: Set of STIX IDs to check

        Returns:
            tuple: (existing_ids, new_ids)
        """
        if not self.redis_client or not self._collection:
            return set(), stix_ids

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        try:
            # Check if cache exists and is not empty
            if not self.redis_client.exists(cache_key):
                logger.info("Redis cache is empty. All IDs considered new.")
                return set(), stix_ids

            existing_ids = set()
            new_ids = set()

            # Use pipeline for efficient batch operations
            pipeline = self.redis_client.pipeline()
            for stix_id in stix_ids:
                pipeline.sismember(cache_key, stix_id)

            results = pipeline.execute()

            for stix_id, exists in zip(stix_ids, results):
                if exists:
                    existing_ids.add(stix_id)
                else:
                    new_ids.add(stix_id)

            logger.info(
                f"ID existence check: {len(existing_ids)} existing, {len(new_ids)} new"
            )
            return existing_ids, new_ids

        except Exception as e:
            logger.error(
                f"Failed to check STIX IDs existence in Redis cache: {e}", exc_info=True
            )
            return set(), stix_ids

    def get_cache_statistics(self) -> dict:
        """
        Get statistics about the Redis cache for monitoring purposes.
        """
        if not self.redis_client or not self._collection:
            return {"cache_available": False}

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        try:
            stats = {
                "cache_available": True,
                "cache_exists": self.redis_client.exists(cache_key),
                "total_cached_ids": self.redis_client.scard(cache_key),
                "cache_ttl": self.redis_client.ttl(cache_key),
                "cache_key": cache_key,
            }
            return stats
        except Exception as e:
            logger.error(f"Failed to get cache statistics: {e}", exc_info=True)
            return {"cache_available": False, "error": str(e)}

    def refresh_cache_from_taxii(self, force: bool = False) -> bool:
        """
        Refresh the Redis cache with fresh data from TAXII server.

        Args:
            force: If True, refresh even if cache is not expired

        Returns:
            bool: True if cache was refreshed, False otherwise
        """
        if not self.redis_client or not self._collection:
            return False

        cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

        try:
            # Check if refresh is needed
            if not force and self.redis_client.exists(cache_key):
                ttl = self.redis_client.ttl(cache_key)
                if ttl > 0:
                    logger.info(
                        f"Cache is still valid for {ttl} seconds. Skipping refresh."
                    )
                    return False

            logger.info("Refreshing Redis cache with fresh data from TAXII...")

            # Fetch fresh data from TAXII
            fresh_ids = self._fetch_all_stix_ids_from_taxii(max_objects_for_test=None)

            if fresh_ids:
                # Replace the entire cache with fresh data
                self.redis_client.delete(cache_key)
                self.redis_client.sadd(cache_key, *fresh_ids)
                self.redis_client.expire(cache_key, self.cache_ttl_seconds)
                logger.info(f"Cache refreshed with {len(fresh_ids)} STIX IDs.")
                return True
            else:
                logger.warning("No fresh data retrieved from TAXII for cache refresh.")
                return False

        except Exception as e:
            logger.error(f"Failed to refresh cache from TAXII: {e}", exc_info=True)
            return False

    def get_first_page_of_stix_objects(self, page_limit: int = 50) -> list[dict]:
        """
        Retrieves the first page of STIX Objects from the connected TAXII collection.

        Args:
            page_limit (int): The maximum number of objects to retrieve on the first page.

        Returns:
            list[dict]: A list of STIX objects (as dictionaries).
        """
        if not self._collection:
            logger.warning("TAXII collection not initialized. Cannot retrieve objects.")
            return []

        logger.info(
            f"Retrieving first page of STIX Objects from TAXII Collection '{self._collection.title}' (Limit: {page_limit})..."
        )
        try:
            get_objects_params = {"limit": page_limit}
            objects_response = self._collection.get_objects(**get_objects_params)

            if isinstance(objects_response, TAXIIStatus):
                return objects_response.objects
            elif isinstance(objects_response, dict) and "objects" in objects_response:
                return objects_response.get("objects", [])
            elif objects_response == {}:
                logger.info(
                    "TAXII collection returned an empty dictionary for the first page."
                )
                return []
            else:
                logger.warning(
                    f"TAXII collection.get_objects() returned an unexpected response type: {type(objects_response)}. "
                    "Returning empty list."
                )
                logger.debug(f"Full unexpected response: {objects_response}")
                return []

        except AccessError as ae:
            logger.error(
                f"[✘] Access Denied when getting first page objects. Error: {ae}"
            )
        except requests.exceptions.RequestException as req_e:
            logger.error(
                f"[✘] HTTP/Network Error getting first page objects from TAXII: {req_e}"
            )
        except Exception as e:
            logger.error(f"[✘] Failed to get first page objects: {e}", exc_info=True)
        return []

    def _push_with_retry(
        self, bundle, content_type, accept, max_retries: int | None = None
    ):
        """
        Pushes a STIX bundle to the TAXII collection with retry-on-transient-failure logic.

        Retries on transient network errors (RemoteDisconnected, ConnectionError,
        ChunkedEncodingError, ProtocolError, 5xx HTTP responses) with exponential
        backoff (2s, 4s, 8s). Does NOT retry on auth errors (AccessError) or
        timeouts (likely payload-size issue).

        Args:
            bundle: The STIX Bundle object serialized as a dictionary.
            content_type: TAXII content type header value.
            accept: TAXII accept header value.
            max_retries: Maximum number of retry attempts. Defaults to
                ``self.max_push_retries`` (instance attribute, defaults to 3).

        Returns:
            The ``push_response`` object returned by ``add_objects()``.

        Raises:
            AccessError: Auth error — propagates immediately (no retry).
            requests.exceptions.Timeout: Payload too large — propagates immediately.
            requests.exceptions.RequestException: If non-transient or all retries
                exhausted.
        """
        if max_retries is None:
            max_retries = self.max_push_retries

        transient_network_errors = (
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            ProtocolError,
            http.client.RemoteDisconnected,
        )

        last_exception = None
        for attempt in range(max_retries + 1):  # initial attempt + retries
            try:
                push_response = self._collection.add_objects(
                    bundle,
                    accept=accept,
                    content_type=content_type,
                )
                if attempt > 0:
                    logger.info(
                        f"[✔] Bundle pushed successfully on attempt {attempt + 1} after {attempt} retries."
                    )
                return push_response

            except AccessError:
                # Auth issue — retrying won't help, propagate immediately
                raise

            except requests.exceptions.Timeout:
                # Payload is probably too big — retrying won't help, propagate immediately
                raise

            except transient_network_errors as e:
                last_exception = e
                error_type = type(e).__name__
                if attempt < max_retries:
                    # Spec: 2s, 4s, 8s exponential backoff
                    backoff = 2 ** (attempt + 1)
                    logger.warning(
                        f"Push attempt {attempt + 1}/{max_retries} failed with {error_type}: {e}. Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    logger.warning(
                        f"Push attempt {attempt + 1}/{max_retries} failed with {error_type}: {e}. No more retries."
                    )

            except requests.exceptions.RequestException as e:
                # Non-transient network errors are not retried; transient ones with
                # 5xx responses are retried below.
                last_exception = e
                response = getattr(e, "response", None)
                status_code = getattr(response, "status_code", None)
                if (
                    status_code is not None
                    and status_code >= 500
                    and attempt < max_retries
                ):
                    error_type = type(e).__name__
                    backoff = 2 ** (attempt + 1)
                    logger.warning(
                        f"Push attempt {attempt + 1}/{max_retries} failed with {error_type} "
                        f"(HTTP {status_code}): {e}. Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    # Non-retriable RequestException — propagate
                    raise

        # All retries exhausted — re-raise the last transient exception
        if last_exception is not None:
            raise last_exception
        # Should not reach here, but be safe
        raise requests.exceptions.RequestException(
            "Push failed after all retries with no captured exception."
        )

    def add_stix_bundle(self, bundle: dict) -> bool:
        """
        Pushes a STIX Bundle (as a dictionary) to the TAXII collection.

        Args:
            bundle (dict): The STIX Bundle object serialized as a dictionary.

        Returns:
            bool: True if the bundle was successfully pushed, False otherwise.
        """
        if not self._collection:
            logger.error("TAXII collection not initialized. Cannot add STIX bundle.")
            return False

        num_objects = len(bundle.get("objects", []))
        logger.info(
            f"Attempting to push STIX Bundle with {num_objects} objects to TAXII..."
        )
        try:
            push_response = self._push_with_retry(
                bundle,
                accept="application/taxii+json;version=2.1",
                content_type="application/taxii+json;version=2.1",
            )

            if isinstance(push_response, TAXIIStatus):
                logger.info(
                    f"[✔] Bundle pushed successfully. Status: {push_response.status}"
                )

                # Update Redis cache with successfully pushed objects
                if self.redis_client and self._collection:
                    cache_key = f"{TAXII_IDS_CACHE_KEY_PREFIX}{self._collection.id}"

                    # Extract IDs of successfully pushed objects
                    successful_ids = set()
                    bundle_objects = bundle.get("objects", [])

                    if (
                        hasattr(push_response, "success_count")
                        and push_response.success_count > 0
                    ):
                        # If we have success count, assume all objects were successful unless we have failure details
                        if hasattr(push_response, "failed") and push_response.failed:
                            # Get IDs of failed objects
                            failed_ids = {
                                f_obj.get("id")
                                for f_obj in push_response.failed
                                if f_obj.get("id")
                            }
                            # Add IDs of non-failed objects to successful set
                            successful_ids = {
                                obj.get("id")
                                for obj in bundle_objects
                                if obj.get("id") and obj.get("id") not in failed_ids
                            }
                        else:
                            # No failures reported, all objects successful
                            successful_ids = {
                                obj.get("id") for obj in bundle_objects if obj.get("id")
                            }

                    # Update cache with successful IDs
                    if successful_ids:
                        self.add_stix_ids_to_cache(successful_ids)
                        logger.info(
                            f"Added {len(successful_ids)} new STIX IDs to Redis cache."
                        )

                # Log detailed response information
                if hasattr(push_response, "success_count"):
                    logger.info(
                        f"Successfully sent: {push_response.success_count} objects."
                    )
                if hasattr(push_response, "failed_count"):
                    logger.warning(
                        f"Failed to send: {push_response.failed_count} objects."
                    )
                if hasattr(push_response, "failed") and push_response.failed:
                    for f_obj in push_response.failed:
                        logger.error(
                            f"Object ID Failed: {f_obj.get('id', 'N/A')} - Message: {f_obj.get('message', 'No message')}"
                        )
                return True
            else:
                logger.error(
                    f"[✘] Unexpected response type from TAXII server: {type(push_response)}. Response: {push_response}"
                )
                return False
        except AccessError:
            logger.error(
                "[✘] Access Denied when pushing bundle to TAXII server. Check credentials."
            )
            return False
        except requests.exceptions.RequestException as req_e:
            logger.error(f"[✘] Network or HTTP Error when pushing bundle: {req_e}")
            return False
        except Exception as e:
            logger.error(
                f"[✘] Failed to push Bundle to TAXII Server: {e}", exc_info=True
            )
            return False

    def filter_new_stix_objects(self, stix_objects: list[dict]) -> list[dict]:
        """
        Filter out STIX objects that already exist in the TAXII server (via Redis cache).
        This is more efficient than checking each object individually.

        Args:
            stix_objects: List of STIX objects to filter

        Returns:
            list[dict]: List of new STIX objects that don't exist in TAXII
        """
        if not stix_objects:
            return []

        # Extract IDs from the objects
        object_ids = {obj.get("id") for obj in stix_objects if obj.get("id")}

        if not object_ids:
            logger.warning("No valid IDs found in STIX objects for filtering.")
            return stix_objects

        # Check which IDs already exist
        existing_ids, new_ids = self.check_stix_ids_existence(object_ids)

        # Special handling for groupings (campaigns, reports, etc.)
        # If a grouping references objects that have been updated, we need to update the grouping too
        updated_groupings = set()
        groupings = [
            obj
            for obj in stix_objects
            if obj.get("type") in ("grouping", "report", "campaign")
        ]

        for group in groupings:
            group_id = group.get("id")
            if group_id in existing_ids:
                # This is an existing grouping - check if we need to update it
                if "object_refs" in group:
                    # If any referenced object is in new_ids, we need to update this grouping
                    for ref in group.get("object_refs", []):
                        if ref in new_ids:
                            logger.info(
                                f"Grouping {group_id} references updated object {ref} - will update grouping"
                            )
                            updated_groupings.add(group_id)
                            # Remove from existing_ids so it's included in the filtered result
                            if group_id in existing_ids:
                                existing_ids.remove(group_id)
                            break

        # Filter objects to only include those with new IDs or updated groupings
        new_objects = [
            obj
            for obj in stix_objects
            if obj.get("id") in new_ids
            or (
                obj.get("type") in ("grouping", "report", "campaign")
                and obj.get("id") in updated_groupings
            )
        ]

        logger.info(
            f"Filtered {len(stix_objects)} objects: {len(new_objects)} new/updated, {len(existing_ids)} duplicates"
        )
        if updated_groupings:
            logger.info(
                f"Included {len(updated_groupings)} existing groupings that reference updated objects"
            )

        return new_objects

    def pre_validate_bundle_objects(self, bundle: dict) -> dict:
        """
        Pre-validate a STIX bundle by removing objects that already exist in TAXII.
        This reduces the load on the TAXII server and improves performance.

        Args:
            bundle: STIX bundle dictionary

        Returns:
            dict: Filtered bundle with only new objects
        """
        if not bundle or "objects" not in bundle:
            return bundle

        original_count = len(bundle["objects"])
        new_objects = self.filter_new_stix_objects(bundle["objects"])

        # Create a new bundle with filtered objects
        filtered_bundle = bundle.copy()
        filtered_bundle["objects"] = new_objects

        logger.info(
            f"Bundle pre-validation: {original_count} original objects, {len(new_objects)} new/updated objects"
        )

        return filtered_bundle

    def get_missing_stix_ids(self, required_ids: set[str]) -> set[str]:
        """
        Get a set of STIX IDs that are missing from the TAXII server.

        Args:
            required_ids: Set of STIX IDs to check

        Returns:
            set[str]: Set of missing STIX IDs
        """
        if not required_ids:
            return set()

        existing_ids, missing_ids = self.check_stix_ids_existence(required_ids)

        logger.info(
            f"Missing ID check: {len(missing_ids)} missing out of {len(required_ids)} total"
        )

        return missing_ids
