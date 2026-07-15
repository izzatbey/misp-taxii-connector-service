# The contents of the file: /misp-taxii-connector/misp-taxii-connector/taxii2misp/main.py

#!/usr/bin/env python3

import logging
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
import time
import json
import sys
import pathlib
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from misp_stix_converter import misp_stix_converter
from pymisp import PyMISP, MISPEvent
from stix2 import MemoryStore, parse
import taxii2client.v21

# Import local modules
from config.settings import Config
from clients.taxii_client import TAXIIClient
from clients.misp_client import MISPClient
from clients.redis_client import MISPEventRedisClient
from services.stix_processor import STIXProcessor
from services.event_quality_filter import EventQualityFilter
from services.duplicate_checker import DuplicateChecker
from services.enhanced_duplicate_checker import EnhancedDuplicateChecker
from utils.signal_handlers import setup_signal_handlers, is_shutdown_requested

warnings.simplefilter("ignore", InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_taxii_to_misp(
    config,
    taxii_client,
    misp_client,
    stix_processor,
    redis_client=None,
    duplicate_checker=None,
    quality_filter=None,
    misp_client_factory=None,
):
    logger.info("Starting TAXII to MISP synchronization process...")

    # Initialize quality filter if not provided (TODO 2 integration)
    if quality_filter is None:
        quality_filter = EventQualityFilter()
        logger.info("🛡️  Initialized event quality filter for TODO 2 requirements")

    try:
        # Test connection first
        connection_ok, connection_info = taxii_client.test_connection()
        if not connection_ok:
            logger.error(f"TAXII connection test failed: {connection_info}")
            return 0

        logger.info(
            f"TAXII connection verified. Collection: {connection_info.get('collection', {}).get('title', 'Unknown')}"
        )

        # Use resource-managed approach to prevent system crashes
        logger.info("🚀 Starting resource-managed STIX object retrieval...")

        # Configure batch processing parameters
        batch_size = (
            config.TAXII_BATCH_SIZE if hasattr(config, "TAXII_BATCH_SIZE") else 10000
        )
        rest_seconds = (
            config.TAXII_REST_SECONDS if hasattr(config, "TAXII_REST_SECONDS") else 5
        )

        logger.info(
            f"📋 Batch configuration: {batch_size} objects per batch, {rest_seconds}s rest between batches"
        )

        # Process objects in manageable batches
        total_objects = 0
        events_pushed_count = 0

        # Use the resource-managed generator to process batches
        for batch_num, batch_objects in enumerate(
            taxii_client.get_all_objects_with_resource_management(
                redis_client=redis_client,
                batch_size=batch_size,
                rest_seconds=rest_seconds,
            ),
            1,
        ):
            if is_shutdown_requested():
                logger.info("Shutdown requested during batch processing")
                break

            logger.info(
                f"📦 Processing batch {batch_num} with {len(batch_objects)} objects..."
            )

            # Create memory store for this batch
            memory_store, grouping_ids = taxii_client.extract_grouping_objects(
                batch_objects
            )

            if memory_store and grouping_ids:
                logger.info(f"Found {len(grouping_ids)} groupings in batch {batch_num}")

                # Process groupings in this batch immediately to free memory
                events_in_batch = process_batch_groupings(
                    memory_store,
                    grouping_ids,
                    stix_processor,
                    misp_client,
                    redis_client,
                    duplicate_checker,
                    quality_filter,
                    config,
                    misp_client_factory=misp_client_factory,
                )

                events_pushed_count += events_in_batch
                logger.info(
                    f"✅ Batch {batch_num}: {events_in_batch} events published to MISP"
                )
            else:
                logger.info(f"No groupings found in batch {batch_num}")

            total_objects += len(batch_objects)

            # Clear batch from memory to prevent accumulation
            del batch_objects
            if "memory_store" in locals():
                del memory_store

            # Force garbage collection after each batch
            import gc

            gc.collect()

            logger.info(
                f"📊 Progress: {total_objects} objects processed across {batch_num} batches"
            )

        logger.info(
            f"🎯 Resource-managed processing completed: {total_objects} total objects, {events_pushed_count} events published"
        )

        return events_pushed_count

    except Exception as e:
        logger.error(f"Error in TAXII to MISP synchronization: {e}", exc_info=True)
        return 0


def process_batch_groupings(
    memory_store,
    grouping_ids,
    stix_processor,
    misp_client,
    redis_client,
    duplicate_checker,
    quality_filter,
    config,
    misp_client_factory=None,
):
    """
    Process groupings from a single batch to MISP events.
    This prevents memory buildup by processing each batch immediately.

    Performance optimisations baked in (2026-07-13):
      * Trust the batch-level Redis filter as the authoritative dedup
        gate — eliminates a per-event Redis hit-check that could never
        fire (the batch filter already removed processed groupings).
      * Drop the MISP search_index per-event duplicate check; rely on
        Redis for normal dedup. When MISP_PARALLEL_DUP_CHECK=true the
        check is still done (defence in depth).
      * When MISP_PARALLEL_WORKERS > 1, fan out add_event+publish_event
        across a ThreadPoolExecutor. Each worker builds its own
        MISPClient (PyMISP sessions are not thread-safe) using
        misp_client_factory. The default config (4 workers) gives
        ~4× speedup on the per-event phase.
      * Reduced per-event log spam from ~9 lines to 2 (one before the
        MISP call, one after success/failure).
    """
    events_pushed_count = 0

    # Filter out already processed groupings using Redis (authoritative)
    if redis_client and redis_client.is_connected():
        logger.debug("Filtering out already processed groupings using Redis cache...")
        original_count = len(grouping_ids)
        grouping_ids = redis_client.filter_unprocessed_groupings(grouping_ids)
        filtered_count = original_count - len(grouping_ids)
        if filtered_count > 0:
            logger.info(
                f"Filtered {filtered_count} already-processed groupings; "
                f"{len(grouping_ids)} to process."
            )
    else:
        logger.warning("Redis not available, cannot filter already processed groupings")

    # Convert groupings to (grouping_id, misp_event) tuples first, so the
    # parallel pool can iterate over the work units without re-running the
    # STIX->MISP conversion per worker (that's the expensive part and must
    # be done once, not per worker).
    work_units: list[tuple[str, Any]] = []
    for grouping_id in grouping_ids:
        if is_shutdown_requested():
            break
        try:
            misp_events, _ = stix_processor.process_grouping(
                memory_store, grouping_id, distribution=config.MISP_DISTRIBUTION_LEVEL
            )
            if not misp_events:
                logger.debug(f"No MISP events produced for grouping '{grouping_id}'")
                continue
            filtered_events, skipped_events = quality_filter.filter_events(misp_events)
            if skipped_events:
                logger.debug(
                    f"Quality filter dropped {len(skipped_events)} events for '{grouping_id}'"
                )
            if not filtered_events:
                logger.debug(f"All events filtered out for '{grouping_id}'; skipping.")
                continue
            for ev in filtered_events:
                work_units.append((grouping_id, ev))
        except Exception as e:
            logger.error(
                f"Error processing grouping '{grouping_id}': {e}", exc_info=True
            )

    if not work_units:
        return 0

    logger.info(
        f"Publishing {len(work_units)} MISP events "
        f"(workers={config.MISP_PARALLEL_WORKERS}, "
        f"dup_check={config.MISP_PARALLEL_DUP_CHECK})..."
    )

    workers = max(1, int(getattr(config, "MISP_PARALLEL_WORKERS", 1)))
    dup_check_enabled = bool(getattr(config, "MISP_PARALLEL_DUP_CHECK", False))

    def _publish_one(unit: tuple[str, Any]) -> dict:
        """Worker function: publish one (grouping_id, event) to MISP.

        Each call uses its own MISPClient (built lazily on the first
        call inside the worker thread). Returns a small dict that the
        outer aggregator reduces into events_pushed_count.
        """
        grouping_id, event = unit
        event_name = event.info if hasattr(event, "info") else str(event)
        result = {
            "grouping_id": grouping_id,
            "event_name": event_name,
            "status": "failed",
            "event_id": None,
            "event_uuid": None,
            "error": None,
        }

        # Per-thread MISP client. PyMISP's underlying requests.Session
        # is not safe to share across threads, so we build a fresh
        # client (cheap) on first use in this thread.
        thread_misp_client = misp_client
        if workers > 1 and misp_client_factory is not None:
            try:
                thread_misp_client = misp_client_factory()
            except Exception as e:
                logger.warning(
                    f"Failed to build per-thread MISP client; falling back "
                    f"to shared client. err={e}"
                )
                thread_misp_client = misp_client

        # Optional defence-in-depth MISP search_index check
        if dup_check_enabled and duplicate_checker is not None:
            try:
                is_dup, _ = duplicate_checker.simple_misp_duplicate_check(event_name)
                if is_dup:
                    result["status"] = "duplicate"
                    return result
            except Exception as e:
                logger.debug(f"Per-event dup check failed (continuing): {e}")

        try:
            logger.debug(f"MISP: adding event '{event_name}' (grouping {grouping_id})")
            misp_response = thread_misp_client.add_event(event, publish=True)
            if not (
                misp_response
                and "Event" in misp_response
                and "id" in misp_response["Event"]
            ):
                result["error"] = f"add_event returned: {misp_response}"
                logger.warning(
                    f"add_event failed for '{event_name}' (grouping {grouping_id}): {misp_response}"
                )
                return result

            event_id = misp_response["Event"]["id"]
            event_uuid = misp_response["Event"]["uuid"]
            result["event_id"] = event_id
            result["event_uuid"] = event_uuid

            # Explicit publish is now a no-op (add_event pre-published),
            # but we still call it for defence-in-depth in case
            # MISP_ALERT_ON_PUBLISH=true and we need to trigger alerts.
            if config.MISP_ALERT_ON_PUBLISH:
                try:
                    thread_misp_client.publish_event(
                        event_id, alert=config.MISP_ALERT_ON_PUBLISH
                    )
                except Exception as e:
                    logger.debug(
                        f"publish_event follow-up failed (already published): {e}"
                    )

            # Mark in duplicate checker (best effort; usually None when Redis is up)
            if duplicate_checker is not None:
                try:
                    grouping_obj = memory_store.get(grouping_id)
                    grouping_dict = None
                    if grouping_obj is not None:
                        if hasattr(grouping_obj, "_inner"):
                            grouping_dict = grouping_obj._inner
                        elif hasattr(grouping_obj, "serialize"):
                            grouping_dict = grouping_obj.serialize()
                        else:
                            grouping_dict = (
                                dict(grouping_obj)
                                if hasattr(grouping_obj, "__dict__")
                                else {}
                            )
                    duplicate_checker.mark_event_created(
                        event_info=event_name,
                        event_uuid=event_uuid,
                        misp_event_id=str(event_id),
                        grouping_id=grouping_id,
                        grouping_obj=grouping_dict or {},
                    )
                except Exception as e:
                    logger.debug(f"duplicate_checker.mark_event_created failed: {e}")

            # Mark grouping as processed in Redis
            if redis_client and redis_client.is_connected():
                try:
                    redis_client.mark_grouping_as_processed(
                        grouping_id=grouping_id,
                        grouping_obj=grouping_dict or {},
                        misp_event_id=str(event_id),
                        misp_event_uuid=event_uuid,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to mark grouping {grouping_id} as processed in Redis: {e}"
                    )

            result["status"] = "pushed"
            logger.info(
                f"MISP event created+published: '{event_name}' "
                f"id={event_id} uuid={event_uuid} (grouping {grouping_id})"
            )
            return result

        except Exception as e:
            result["error"] = str(e)
            logger.error(
                f"Error adding/publishing event '{event_name}' "
                f"(grouping {grouping_id}): {e}",
                exc_info=True,
            )
            return result

    # Run the workers — serial if workers==1, otherwise ThreadPoolExecutor.
    if workers <= 1:
        results = [_publish_one(u) for u in work_units]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_publish_one, u): u for u in work_units}
            for fut in as_completed(futures):
                try:
                    results.append(fut.result())
                except Exception as e:
                    unit = futures[fut]
                    logger.error(
                        f"Worker raised for grouping {unit[0]}: {e}", exc_info=True
                    )
                    results.append(
                        {
                            "grouping_id": unit[0],
                            "event_name": getattr(unit[1], "info", str(unit[1])),
                            "status": "failed",
                            "event_id": None,
                            "event_uuid": None,
                            "error": str(e),
                        }
                    )

    # Aggregate
    pushed = sum(1 for r in results if r["status"] == "pushed")
    dupes = sum(1 for r in results if r["status"] == "duplicate")
    failed = sum(1 for r in results if r["status"] == "failed")
    events_pushed_count = pushed
    logger.info(
        f"MISP publish summary: pushed={pushed} duplicates={dupes} failed={failed} "
        f"(of {len(work_units)})"
    )
    return events_pushed_count


def main():
    load_dotenv()

    setup_signal_handlers()

    try:
        config = Config()
        logger.info("Configuration loaded successfully.")

        logger.info(f"TAXII Discovery URL: {config.DISCOVERY_URL}")
        logger.info(f"MISP URL: {config.MISP_URL}")
        logger.info(f"SSL Verification: {config.VERIFY_SSL}")
        logger.info(f"MISP Distribution Level: {config.MISP_DISTRIBUTION_LEVEL}")
        logger.info(f"MISP Request Timeout: {config.MISP_REQUEST_TIMEOUT} seconds")
        logger.info(f"Scheduler Interval: {config.SCHEDULER_INTERVAL_SECONDS} seconds")
        logger.info(
            f"Max Events Per Run: {'Unlimited' if config.MAX_EVENTS_PER_RUN is None else config.MAX_EVENTS_PER_RUN}"
        )
        logger.info(f"Comprehensive Processing: {config.COMPREHENSIVE_PROCESSING}")
        logger.info(f"TAXII Chunk Size: {getattr(config, 'TAXII_CHUNK_SIZE', 1000)}")
        logger.info(
            f"TAXII Request Timeout: {getattr(config, 'TAXII_REQUEST_TIMEOUT', 300)} seconds"
        )

        taxii_client = TAXIIClient(
            discovery_url=config.DISCOVERY_URL,
            username=config.USERNAME,
            password=config.PASSWORD,
            verify_ssl=config.VERIFY_SSL,
            request_timeout=getattr(config, "TAXII_REQUEST_TIMEOUT", 300),
            chunk_size=getattr(config, "TAXII_CHUNK_SIZE", 1000),
            page_retries=getattr(config, "TAXII_PAGE_RETRIES", 3),
            page_retry_backoff=getattr(config, "TAXII_PAGE_RETRY_BACKOFF_SECONDS", 2.0),
        )
        logger.info("TAXII Client initialized successfully.")

        # Try to initialize MISP client with fallback
        try:
            misp_client = MISPClient(
                misp_url=config.MISP_URL,
                misp_api_key=config.MISP_API_KEY,
                verify_ssl=config.VERIFY_SSL,
                distribution=config.MISP_DISTRIBUTION_LEVEL,
                threat_level=config.MISP_THREAT_LEVEL,
                analysis=config.MISP_ANALYSIS_LEVEL,
                request_timeout=config.MISP_REQUEST_TIMEOUT,
            )
            logger.info("MISP Client initialized successfully.")

        except Exception as misp_error:
            logger.warning(f"Standard MISP client initialization failed: {misp_error}")
            logger.info("Trying alternative MISP client initialization...")

            try:
                misp_client = MISPClient.create_simple_client(
                    misp_url=config.MISP_URL,
                    misp_api_key=config.MISP_API_KEY,
                    verify_ssl=config.VERIFY_SSL,
                    distribution=config.MISP_DISTRIBUTION_LEVEL,
                    threat_level=config.MISP_THREAT_LEVEL,
                    analysis=config.MISP_ANALYSIS_LEVEL,
                    request_timeout=config.MISP_REQUEST_TIMEOUT,
                )
                logger.info("Alternative MISP Client initialized successfully.")

            except Exception as alt_error:
                logger.error(
                    f"Both MISP client initialization methods failed: {alt_error}"
                )
                raise

        # ------------------------------------------------------------------
        # Per-thread MISP client factory (used when MISP_PARALLEL_WORKERS > 1)
        # ------------------------------------------------------------------
        # PyMISP's underlying requests.Session is NOT thread-safe, so each
        # worker thread must own its own MISPClient. The factory captures
        # the config and produces a fresh client on demand. We use
        # create_simple_client (bypasses PyMISP version checks) so all
        # workers are homogeneous regardless of which init path the
        # primary client took.
        def _build_misp_client() -> MISPClient:
            return MISPClient.create_simple_client(
                misp_url=config.MISP_URL,
                misp_api_key=config.MISP_API_KEY,
                verify_ssl=config.VERIFY_SSL,
                distribution=config.MISP_DISTRIBUTION_LEVEL,
                threat_level=config.MISP_THREAT_LEVEL,
                analysis=config.MISP_ANALYSIS_LEVEL,
                request_timeout=config.MISP_REQUEST_TIMEOUT,
            )

        misp_client_factory = _build_misp_client
        logger.info(
            f"MISP parallel workers: {config.MISP_PARALLEL_WORKERS} "
            f"(dup_check={config.MISP_PARALLEL_DUP_CHECK})"
        )

        stix_processor = STIXProcessor(temp_dir=config.TEMP_DIR)
        logger.info("STIX Processor initialized successfully.")

        # Initialize Redis client for event tracking if enabled
        redis_client = None
        duplicate_checker = None
        if config.REDIS_ENABLE_TRACKING:
            try:
                redis_client = MISPEventRedisClient(
                    host=config.REDIS_HOST,
                    port=config.REDIS_PORT,
                    db=config.REDIS_DB,
                    password=config.REDIS_PASSWORD,
                    ttl_seconds=config.REDIS_TTL_SECONDS,
                )

                if redis_client.is_connected():
                    logger.info(
                        "Redis client initialized successfully for event tracking."
                    )

                    # Initialize enhanced duplicate checker with Redis and MISP clients
                    duplicate_checker = EnhancedDuplicateChecker(
                        redis_client, misp_client
                    )
                    logger.info("Enhanced duplicate checker initialized successfully.")

                    # Log current processing stats
                    stats = redis_client.get_processing_stats()
                    if stats:
                        logger.info(f"Previous processing stats: {stats}")
                else:
                    logger.warning(
                        "Redis client initialized but not connected. Event tracking disabled."
                    )
                    redis_client = None

            except Exception as e:
                logger.warning(
                    f"Failed to initialize Redis client: {e}. Event tracking disabled."
                )
                redis_client = None
        else:
            logger.info("Redis event tracking disabled by configuration.")

        # Initialize event quality filter for TODO 2 requirements
        quality_filter = EventQualityFilter()
        logger.info(
            "🛡️  Event Quality Filter initialized - will prevent comment-only events from being published"
        )

        while not is_shutdown_requested():
            try:
                process_taxii_to_misp(
                    config,
                    taxii_client,
                    misp_client,
                    stix_processor,
                    redis_client,
                    duplicate_checker,
                    quality_filter,
                    misp_client_factory=misp_client_factory,
                )

                if not is_shutdown_requested():
                    logger.info(
                        f"Sleeping for {config.SCHEDULER_INTERVAL_SECONDS} seconds before next synchronization cycle..."
                    )

                    sleep_interval = min(10, config.SCHEDULER_INTERVAL_SECONDS)
                    remaining_sleep = config.SCHEDULER_INTERVAL_SECONDS

                    while remaining_sleep > 0 and not is_shutdown_requested():
                        time.sleep(min(sleep_interval, remaining_sleep))
                        remaining_sleep -= sleep_interval

            except Exception as e:
                logger.error(f"Error in main processing loop: {e}", exc_info=True)
                logger.info("Waiting 10 seconds before retrying due to error...")
                time.sleep(10)

        logger.info("Shutdown requested. Exiting...")

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
