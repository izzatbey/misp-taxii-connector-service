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
):
    """
    Process groupings from a single batch to MISP events.
    This prevents memory buildup by processing each batch immediately.
    """
    events_pushed_count = 0

    # Filter out already processed groupings using Redis
    if redis_client and redis_client.is_connected():
        logger.info("Filtering out already processed groupings using Redis cache...")
        original_count = len(grouping_ids)
        grouping_ids = redis_client.filter_unprocessed_groupings(grouping_ids)
        filtered_count = original_count - len(grouping_ids)

        if filtered_count > 0:
            logger.info(
                f"Filtered out {filtered_count} already processed groupings, "
                f"{len(grouping_ids)} remaining to process"
            )
        else:
            logger.info("No previously processed groupings found, processing all")
    else:
        logger.warning("Redis not available, cannot filter already processed groupings")

    # Process each grouping in the batch
    for grouping_id in grouping_ids:
        if is_shutdown_requested():
            logger.info("Shutdown requested. Stopping processing.")
            break

        try:
            misp_events, _ = stix_processor.process_grouping(
                memory_store, grouping_id, distribution=config.MISP_DISTRIBUTION_LEVEL
            )

            if not misp_events:
                logger.warning(f"No MISP events produced for grouping '{grouping_id}'")
                continue

            # Apply quality filtering before publishing events
            filtered_events, skipped_events = quality_filter.filter_events(misp_events)

            if skipped_events:
                logger.info(
                    f"🛡️  Quality filter skipped {len(skipped_events)} low-quality events for grouping '{grouping_id}'"
                )

            if not filtered_events:
                logger.info(
                    f"🚫 All events filtered out for grouping '{grouping_id}' - skipping MISP publication"
                )
                continue

            logger.info(
                f"✅ Quality filter passed {len(filtered_events)}/{len(misp_events)} events for grouping '{grouping_id}'"
            )

            for event in filtered_events:
                try:
                    # Check for duplicates before adding to MISP
                    skip_event = False

                    if duplicate_checker:
                        # Use the simple but effective MISP duplicate check first
                        logger.info(
                            f"🔍 SIMPLE DUPLICATE CHECK: Checking event '{event.info if hasattr(event, 'info') else str(event)}'"
                        )
                        is_duplicate, duplicate_data = (
                            duplicate_checker.simple_misp_duplicate_check(
                                event.info if hasattr(event, "info") else str(event)
                            )
                        )

                        if is_duplicate:
                            logger.info(
                                f"🚫 SIMPLE DUPLICATE DETECTED: Skipping event '{event.info if hasattr(event, 'info') else str(event)}'"
                            )
                            logger.info(
                                f"   Existing event: ID={duplicate_data.get('id')}, Info='{duplicate_data.get('info')}'"
                            )
                            skip_event = True
                        else:
                            # Fallback to comprehensive duplicate check
                            logger.info(
                                f"🔍 COMPREHENSIVE DUPLICATE CHECK: Checking event '{event.info if hasattr(event, 'info') else str(event)}'"
                            )
                            event_exists, existing_info = (
                                duplicate_checker.check_event_exists(
                                    event_info=event.info
                                    if hasattr(event, "info")
                                    else str(event),
                                    grouping_id=grouping_id,
                                )
                            )

                            if event_exists:
                                logger.info(
                                    f"🚫 COMPREHENSIVE DUPLICATE DETECTED: Skipping event '{event.info if hasattr(event, 'info') else str(event)}': {existing_info}"
                                )
                                skip_event = True
                            else:
                                logger.info(
                                    f"✅ NO DUPLICATES FOUND: Event '{event.info if hasattr(event, 'info') else str(event)}' is unique - proceeding"
                                )
                    else:
                        # Fallback duplicate detection when enhanced checker is not available
                        event_name = (
                            event.info if hasattr(event, "info") else str(event)
                        )
                        logger.info(
                            f"🔍 FALLBACK DUPLICATE CHECK: Checking MISP database for event '{event_name}'"
                        )

                        try:
                            # Simple MISP database check by event name
                            search_results = misp_client.misp.search_index(
                                eventinfo=event_name.strip(), limit=10, pythonify=True
                            )

                            if search_results:
                                for result in search_results:
                                    try:
                                        # Handle different response formats
                                        if isinstance(result, dict):
                                            result_obj = result.get("Event", result)
                                        elif hasattr(result, "info"):
                                            result_obj = {
                                                "info": result.info,
                                                "id": getattr(result, "id", None),
                                            }
                                        else:
                                            continue

                                        if isinstance(result_obj, dict):
                                            existing_name = result_obj.get(
                                                "info", ""
                                            ).strip()
                                            # Check for exact match (case-insensitive)
                                            if (
                                                existing_name.lower()
                                                == event_name.lower()
                                            ):
                                                logger.info(
                                                    f"🚫 FALLBACK DUPLICATE DETECTED: Skipping duplicate event '{event_name}' (found: {existing_name})"
                                                )
                                                skip_event = True
                                                break  # Break out of the search results loop
                                    except Exception as e:
                                        logger.warning(
                                            f"Error processing search result: {e}"
                                        )
                                        continue

                            if not skip_event:
                                logger.info(
                                    f"✅ FALLBACK DUPLICATE CHECK: No duplicate found for '{event_name}' - proceeding"
                                )

                        except Exception as e:
                            logger.warning(
                                f"⚠️  FALLBACK DUPLICATE CHECK FAILED: {e}. Proceeding with event creation."
                            )

                    # Skip this event if duplicate was found
                    if skip_event:
                        continue

                    # Add event to MISP
                    logger.info(
                        f"📤 MISP CLIENT: Adding event '{event.info if hasattr(event, 'info') else str(event)}'"
                    )
                    misp_response = misp_client.add_event(event)

                    if (
                        misp_response
                        and "Event" in misp_response
                        and "id" in misp_response["Event"]
                    ):
                        event_id = misp_response["Event"]["id"]
                        event_uuid = misp_response["Event"]["uuid"]

                        logger.info(
                            f"Event added successfully to MISP: ID {event_id} (UUID {event_uuid})"
                        )

                        # Mark event as created in duplicate checker
                        if duplicate_checker:
                            grouping_obj = memory_store.get(grouping_id)
                            grouping_dict = None
                            if grouping_obj:
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
                                event_info=event.info
                                if hasattr(event, "info")
                                else str(event),
                                event_uuid=event_uuid,
                                misp_event_id=str(event_id),
                                grouping_id=grouping_id,
                                grouping_obj=grouping_dict,
                            )

                        logger.info(f"Publishing MISP Event with ID: {event_id}")
                        misp_client.publish_event(
                            event_id, alert=config.MISP_ALERT_ON_PUBLISH
                        )

                        # Mark grouping as processed in Redis
                        if redis_client and redis_client.is_connected():
                            success = redis_client.mark_grouping_as_processed(
                                grouping_id=grouping_id,
                                grouping_obj=grouping_dict or {},
                                misp_event_id=str(event_id),
                                misp_event_uuid=event_uuid,
                            )

                            if success:
                                logger.info(
                                    f"Marked grouping {grouping_id} as processed in Redis"
                                )
                            else:
                                logger.warning(
                                    f"Failed to mark grouping {grouping_id} as processed in Redis"
                                )

                        events_pushed_count += 1
                    else:
                        logger.warning(
                            f"Failed to add grouping '{grouping_id}' to MISP. Response: {misp_response}"
                        )

                except Exception as e:
                    logger.error(
                        f"Error adding/publishing event for grouping '{grouping_id}': {e}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                f"Error processing grouping '{grouping_id}': {e}", exc_info=True
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
