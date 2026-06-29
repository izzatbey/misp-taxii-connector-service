# main.py

import logging
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
import json
import requests
import time
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from config.settings import Config
from clients.otx_client import OTXClient
from clients.taxii_client import TAXIIClient
from services.pulse_processor import PulseProcessor

warnings.simplefilter("ignore", InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logging.info(f"Received signal {signum}. Requesting graceful shutdown...")
    shutdown_requested = True


def _process_single_pulse(
    pulse_detail: dict,
    config,
    otx_client,
    taxii_client,
    pulse_processor,
    existing_stix_ids_snapshot: set[str],
) -> tuple[bool, bool, str, str]:
    """
    Process a single OTX pulse: fetch indicators, build a STIX bundle, optionally
    pre-validate it, and push it to TAXII.

    Runs inside a worker thread. `existing_stix_ids_snapshot` is a private copy
    owned by this worker — it must NOT be mutated or returned to the caller
    (returning it would reintroduce the cross-thread set corruption that the
    per-worker `.copy()` in `process_otx_to_taxii` is designed to prevent).

    Returns (push_success, processed_success, pulse_id, pulse_name).
    """
    pulse_id = pulse_detail.get("id")
    pulse_name = pulse_detail.get("name", "N/A")

    logging.info(
        f"\n--- [Pulse {pulse_id}] Attempting to process Pulse: '{pulse_name}' (ID: {pulse_id}) ---"
    )

    push_success = False
    processed_success = False

    try:
        # Check if the pulse indicators have changed - if not, skip processing
        pulse_indicators = otx_client.get_pulse_indicators(pulse_id)

        # Check if this pulse has been processed before and if its indicators have changed
        if config.ENABLE_CACHE_PREVALIDATION and not otx_client.check_pulse_changed(
            pulse_id, pulse_indicators
        ):
            logging.info(
                f"[Pulse {pulse_id}] Pulse '{pulse_name}' has not changed since last processing. Skipping."
            )
            return push_success, processed_success, str(pulse_id), pulse_name

        stix_bundle = pulse_processor.process_pulse_data(
            pulse_detail, pulse_indicators, existing_stix_ids_snapshot
        )

        if stix_bundle:
            processed_success = True

            bundle_as_dict = json.loads(stix_bundle.serialize())

            # Pre-validate bundle to remove duplicates (if enabled)
            if config.ENABLE_CACHE_PREVALIDATION:
                original_count = len(bundle_as_dict.get("objects", []))
                bundle_as_dict = taxii_client.pre_validate_bundle_objects(
                    bundle_as_dict
                )
                validated_count = len(bundle_as_dict.get("objects", []))

                if validated_count == 0:
                    logging.info(
                        f"[Pulse {pulse_id}] All objects in bundle for pulse '{pulse_name}' are duplicates. Skipping push."
                    )
                    return push_success, processed_success, str(pulse_id), pulse_name
                elif validated_count < original_count:
                    logging.info(
                        f"[Pulse {pulse_id}] Bundle pre-validation for pulse '{pulse_name}': {original_count} → {validated_count} objects (removed {original_count - validated_count} duplicates)"
                    )

            if config.ENABLE_CACHE_PREVALIDATION:
                for obj in bundle_as_dict.get("objects", []):
                    if "id" in obj:
                        otx_client.cache_stix_uuid(obj["id"], pulse_id)

            logging.info(
                f"[Pulse {pulse_id}] Attempting to push STIX Bundle for pulse '{pulse_name}' to TAXII..."
            )
            if taxii_client.add_stix_bundle(bundle_as_dict):
                push_success = True

                # Store pulse in cache for future change detection
                pulse_detail["indicators"] = (
                    pulse_indicators  # Make sure indicators are included
                )
                otx_client._cache_pulse(pulse_detail)

                logging.info(
                    f"[Pulse {pulse_id}] Successfully pushed bundle for Pulse '{pulse_name}'."
                )
            else:
                logging.error(
                    f"[Pulse {pulse_id}] Failed to push STIX Bundle for pulse '{pulse_name}' to TAXII."
                )
        else:
            logging.info(
                f"[Pulse {pulse_id}] No new STIX Bundle generated for pulse '{pulse_name}' (likely a duplicate Grouping or no indicators). Skipping push."
            )

    except Exception as e:
        logging.error(
            f"[Pulse {pulse_id}] Error processing pulse '{pulse_name}' (ID: {pulse_id}): {e}",
            exc_info=True,
        )

    return push_success, processed_success, str(pulse_id), pulse_name


def process_otx_to_taxii(
    config, otx_client, taxii_client, pulse_processor, existing_stix_ids
):
    logging.info("Starting OTX to TAXII synchronization process...")

    # For production use, ensure no artificial limits are enforced
    if config.OTX_TEST_PULSE_LIMIT is None:
        logging.info("Running in production mode - no pulse limit applied")
    else:
        logging.info(f"Running with test pulse limit: {config.OTX_TEST_PULSE_LIMIT}")

    # Pass the OTX_TEST_PULSE_LIMIT from config to the OTX client
    pulses_from_otx_generator = otx_client.get_all_subscribed_pulses(
        max_pulses=config.OTX_TEST_PULSE_LIMIT
    )
    pulses_from_otx = list(pulses_from_otx_generator)

    if not pulses_from_otx:
        logging.info(
            "No new or updated pulses retrieved from OTX for the current time window. Nothing to process or push."
        )
    else:
        logging.info(
            f"Found {len(pulses_from_otx)} new/updated pulses from OTX to potentially process and push."
        )

        processed_pulses_count = 0
        pushed_bundles_count = 0

        output_dir = "./output/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # ------------------------------------------------------------------
        # Parallel per-pulse processing.
        #
        # Each worker receives its OWN copy of `existing_stix_ids` via
        # `.copy()` on submission. This prevents the threads from corrupting
        # the shared de-dup set (set mutations from concurrent threads are
        # not safe and would intermittently lose or duplicate IDs).
        #
        # MAX_BUNDLES_TO_PUSH semantics: we still cap total successful pushes,
        # but the cap is now "stop accepting NEW work after N successes".
        # Choice: `threading.Lock` + an int counter — cleaner than a shared
        # list of completed futures because the early-stop decision is a
        # single atomic check (`counter >= limit`) inside the submitter's
        # `as_completed` loop, with no need to mutate a list under a lock
        # for every completion. Workers themselves are uninstrumented.
        # ------------------------------------------------------------------
        max_push_limit = config.MAX_BUNDLES_TO_PUSH
        push_counter_lock = threading.Lock()
        pushed_so_far = 0

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            # Map future -> (pulse_id, pulse_name) so we can log cancellation context.
            futures = {}
            for pulse_detail in pulses_from_otx:
                # Honour the cap by refusing to submit any new work once we've
                # already reached the limit. Already-running workers finish.
                if max_push_limit is not None:
                    with push_counter_lock:
                        if pushed_so_far >= max_push_limit:
                            logging.info(
                                f"Reached the maximum push limit of {max_push_limit} bundles. "
                                "Not submitting further work; already-running workers will complete."
                            )
                            break

                pulse_id = pulse_detail.get("id")
                pulse_name = pulse_detail.get("name", "N/A")

                # Per-worker private copy of the de-dup set.
                snapshot = existing_stix_ids.copy()

                future = executor.submit(
                    _process_single_pulse,
                    pulse_detail,
                    config,
                    otx_client,
                    taxii_client,
                    pulse_processor,
                    snapshot,
                )
                futures[future] = (str(pulse_id), pulse_name)

            # Drain completed futures, update the shared counter, aggregate results.
            results = []
            for future in as_completed(futures):
                pid, pname = futures[future]
                try:
                    push_success, processed_success, _pid, _pname = future.result()
                except Exception as e:
                    # _process_single_pulse swallows its own exceptions and returns
                    # (False, False, ...). This is a true belt-and-suspenders guard
                    # for any unexpected error raised outside the inner try/except.
                    logging.error(
                        f"Unhandled exception from worker for pulse '{pname}' (ID: {pid}): {e}",
                        exc_info=True,
                    )
                    push_success, processed_success = False, False

                if push_success:
                    with push_counter_lock:
                        pushed_so_far += 1
                results.append((push_success, processed_success))

        # Aggregate after the pool is fully shut down.
        processed_pulses_count = sum(1 for _, p in results if p)
        pushed_bundles_count = sum(1 for s, _ in results if s)

        logging.info(f"\n--- OTX to TAXII Synchronization Summary ---")
        logging.info(
            f"Total OTX Pulses retrieved and considered: {len(pulses_from_otx)}"
        )
        logging.info(
            f"Total OTX Pulses processed into new STIX Bundles: {processed_pulses_count}"
        )
        logging.info(
            f"Total STIX Bundles successfully pushed to TAXII: {pushed_bundles_count}"
        )

        if pushed_bundles_count == 0:
            logging.info(
                "Note: zero bundles pushed (likely all duplicates, all pre-validated, or no new pulses from OTX)."
            )


def main():
    load_dotenv()

    try:
        config = Config()
        logging.info("Configuration loaded.")

        if not config.VERIFY_SSL:
            logging.warning("SSL verification is disabled as per configuration.")

        logging.info(f"OTX API Key (partial): {config.OTX_API_KEY[:4]}XXX...")
        logging.info(f"TAXII URL: {config.TAXII_URL}")
        logging.info(f"TAXII Username: {config.USERNAME}")
        logging.info(f"TAXII SSL Verification: {config.VERIFY_SSL}")
        logging.info(f"STIX Namespace: {config.CUSTOM_STIX_NAMESPACE}")
        logging.info(
            f"Redis Host: {config.REDIS_HOST}:{config.REDIS_PORT}, DB: {config.REDIS_DB}"
        )
        logging.info(
            f"Redis Cache TTL (for TAXII IDs): {config.CACHE_TTL_SECONDS} seconds"
        )
        logging.info(f"Max Bundles to Push: {config.MAX_BUNDLES_TO_PUSH}")
        logging.info(f"Scheduler Interval: {config.SCHEDULER_INTERVAL_SECONDS} seconds")
        logging.info("Main function is now ready to initialize clients.")

        logging.info(
            f"Configured to push a maximum of {config.MAX_BUNDLES_TO_PUSH} new STIX bundles to TAXII per run."
        )

        taxii_client = TAXIIClient(
            taxii_url=config.TAXII_URL,
            username=config.USERNAME,
            password=config.PASSWORD,
            verify_ssl=config.VERIFY_SSL,
            redis_host=config.REDIS_HOST,
            redis_port=config.REDIS_PORT,
            redis_db=config.REDIS_DB,
            redis_password=config.REDIS_PASSWORD,
            cache_ttl_seconds=config.CACHE_TTL_SECONDS,
        )
        logging.info("TAXII Client initialized successfully.")

        collection = taxii_client.get_default_collection()
        logging.info(f"Default Collection: {collection.title} (ID: {collection.id})")

        existing_stix_ids = taxii_client.get_existing_stix_ids()
        logging.info(
            f"Retrieved {len(existing_stix_ids)} existing STIX IDs (from cache or TAXII server) for de-duplication."
        )

        otx_client = OTXClient(
            api_key=config.OTX_API_KEY,
            redis_host=config.REDIS_HOST,
            redis_port=config.REDIS_PORT,
            redis_db=config.REDIS_DB,
            redis_password=config.REDIS_PASSWORD,
        )
        logging.info("OTX Client initialized successfully.")

        pulse_processor = PulseProcessor(
            custom_stix_namespace=config.CUSTOM_STIX_NAMESPACE
        )
        logging.info("PulseProcessor initialized successfully.")

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            process_otx_to_taxii(
                config, otx_client, taxii_client, pulse_processor, existing_stix_ids
            )
            logging.info("One-shot cycle complete. Exiting with status 0.")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error in main processing loop: {e}", exc_info=True)
            logging.error("One-shot cycle failed. Exiting with status 1.")
            sys.exit(1)

    except ValueError as ve:
        logging.critical(
            f"Configuration Error: {ve}. Please check your .env file and ensure all required variables are set correctly."
        )
        exit(1)
    except requests.exceptions.RequestException as req_e:
        logging.error(
            f"Network or HTTP error during initialization or processing: {req_e}"
        )
        exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
