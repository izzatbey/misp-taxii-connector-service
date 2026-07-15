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
from clients.otx_client import OTXClient, OTXAPIUnavailable
from clients.taxii_client import TAXIIClient
from services.pulse_processor import PulseProcessor
from services.pulse_decay import PulseDecayFilter

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


def _interruptible_sleep(total_seconds: int) -> None:
    """
    Sleep `total_seconds` in small chunks so SIGINT/SIGTERM is honoured
    within at most 1 second. Without this, a long `time.sleep(...)`
    between scheduler cycles would block the shutdown signal handler
    from being processed promptly.
    """
    remaining = max(0, int(total_seconds))
    while remaining > 0 and not shutdown_requested:
        chunk = min(1, remaining)
        time.sleep(chunk)
        remaining -= chunk


def _stix_bundle_to_dict(bundle) -> dict:
    """
    Convert a stix2.Bundle to a plain dict for TAXII push.

    We use the proven JSON-round-trip path here. stix2's `obj._inner`
    contains STIXdatetime / STIXObjectProperty values that
    simplejson.dumps cannot encode directly — the taxii2-client library
    has its own encoders but they expect the round-trip output. So
    the round-trip is the safe and reliable approach; the streaming
    iterator (iter_subscribed_pulses) is what gives the actual RAM win.
    """
    return json.loads(bundle.serialize())


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

        stix_bundles = pulse_processor.process_pulse_data(
            pulse_detail, pulse_indicators, existing_stix_ids_snapshot
        )

        if stix_bundles:
            processed_success = True
            total_chunks = len(stix_bundles)

            if total_chunks > 1:
                logging.info(
                    f"[Pulse {pulse_id}] Pulse '{pulse_name}' produced "
                    f"{total_chunks} chunk bundles — pushing each one separately."
                )

            # Track success across all chunks so we can decide whether to
            # consider this pulse "fully pushed" or not.
            chunks_pushed_ok = 0

            for chunk_idx, stix_bundle in enumerate(stix_bundles, start=1):
                chunk_label = (
                    f"chunk {chunk_idx}/{total_chunks}"
                    if total_chunks > 1
                    else "bundle"
                )

                # Convert STIX Bundle → dict for TAXII push. The actual RAM
                # win comes from streaming pulses (see iter_subscribed_pulses);
                # the round-trip here is the only safe path because stix2's
                # _inner dict contains non-JSON-serialisable STIXdatetime.
                bundle_as_dict = _stix_bundle_to_dict(stix_bundle)

                # Pre-validate bundle to remove duplicates (if enabled)
                if config.ENABLE_CACHE_PREVALIDATION:
                    original_count = len(bundle_as_dict.get("objects", []))
                    bundle_as_dict = taxii_client.pre_validate_bundle_objects(
                        bundle_as_dict
                    )
                    validated_count = len(bundle_as_dict.get("objects", []))

                    if validated_count == 0:
                        logging.info(
                            f"[Pulse {pulse_id}] {chunk_label}: all objects are duplicates. Skipping push."
                        )
                        continue
                    elif validated_count < original_count:
                        logging.info(
                            f"[Pulse {pulse_id}] {chunk_label} pre-validation: "
                            f"{original_count} → {validated_count} objects "
                            f"(removed {original_count - validated_count} duplicates)"
                        )

                if config.ENABLE_CACHE_PREVALIDATION:
                    for obj in bundle_as_dict.get("objects", []):
                        if "id" in obj:
                            otx_client.cache_stix_uuid(obj["id"], pulse_id)

                logging.info(
                    f"[Pulse {pulse_id}] Attempting to push {chunk_label} "
                    f"for pulse '{pulse_name}' to TAXII..."
                )
                if taxii_client.add_stix_bundle(bundle_as_dict):
                    chunks_pushed_ok += 1
                    logging.info(
                        f"[Pulse {pulse_id}] Successfully pushed {chunk_label} "
                        f"({chunks_pushed_ok}/{total_chunks} so far)."
                    )
                else:
                    logging.error(
                        f"[Pulse {pulse_id}] Failed to push {chunk_label} "
                        f"for pulse '{pulse_name}' to TAXII."
                    )

            # The pulse is considered "fully pushed" only if every chunk
            # succeeded. If any chunk failed, we re-attempt next cycle
            # (the indicator cache check will skip already-pushed chunks
            # because their grouping IDs will already be in TAXII).
            if chunks_pushed_ok == total_chunks and total_chunks > 0:
                push_success = True

                # Store pulse in cache for future change detection (only
                # when ALL chunks pushed — otherwise we'll retry next cycle
                # and the change-detection will still trigger).
                pulse_detail["indicators"] = pulse_indicators
                otx_client._cache_pulse(pulse_detail)

                logging.info(
                    f"[Pulse {pulse_id}] All {total_chunks} chunk(s) pushed "
                    f"successfully for Pulse '{pulse_name}'."
                )
            else:
                logging.warning(
                    f"[Pulse {pulse_id}] Only {chunks_pushed_ok}/{total_chunks} "
                    f"chunks pushed for Pulse '{pulse_name}'. Will retry "
                    "remaining chunks on next cycle."
                )
        else:
            logging.info(
                f"[Pulse {pulse_id}] No new STIX Bundle generated for pulse '{pulse_name}' (likely all chunks already exist in TAXII or no indicators). Skipping push."
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

    # Recency / decay filter: skip stale pulses before submitting work to the
    # executor. This avoids wasted OTX indicator fetches + STIX builds + TAXII
    # pushes for threat intel that is no longer relevant. Built once per cycle;
    # the counters are reported in the end-of-cycle summary.
    decay_filter = PulseDecayFilter.from_config(config)
    if decay_filter.enabled and decay_filter.max_age_days > 0:
        logging.info(
            f"Decay filter ACTIVE: skipping pulses whose created AND modified "
            f"are older than {decay_filter.max_age_days} days."
        )
    else:
        logging.info("Decay filter DISABLED (all pulses processed).")

    # For production use, ensure no artificial limits are enforced
    if config.OTX_TEST_PULSE_LIMIT is None:
        logging.info("Running in production mode - no pulse limit applied")
    else:
        logging.info(f"Running with test pulse limit: {config.OTX_TEST_PULSE_LIMIT}")

    # Use the streaming iterator so we don't materialise all 1500+ pulses
    # in RAM before submitting work. The previous `list(generator)` kept
    # every pulse dict alive until the cycle finished — for w0rmsign
    # subscriptions with thousands of pulses that was the dominant RAM
    # spike on small VMs.
    pulses_from_otx_generator = otx_client.iter_subscribed_pulses(
        max_pulses=config.OTX_TEST_PULSE_LIMIT,
        author_name=config.OTX_AUTHOR_FILTER,
    )

    # Peek the first item for log/early-exit; otherwise pass the generator
    # straight into the executor submit loop.
    try:
        first_pulse = next(pulses_from_otx_generator)
    except StopIteration:
        logging.info(
            "No new or updated pulses retrieved from OTX for the current time window. Nothing to process or push."
        )
        return

    # Re-chain the generator so the executor consumes ALL pulses.
    def _all_pulses():
        yield first_pulse
        yield from pulses_from_otx_generator

    pulses_from_otx = _all_pulses()
    logging.info(
        "Streaming pulses from OTX (first pulse seen; count will be reported at end of cycle)."
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

            # --- Decay / recency gate ---------------------------------
            # Skip stale pulses before submitting to the executor. This is
            # the core of the decaying system: stale pulses never spawn a
            # worker, so no OTX indicator fetch / STIX build / TAXII push
            # happens for them. should_process() logs the skip reason.
            if not decay_filter.should_process(pulse_detail):
                continue

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
    # Note: pulses_from_otx is a generator now so we can't len() it.
    # The results list gives us the count of processed pulses.
    logging.info(f"Total OTX Pulses submitted to executor: {len(results)}")
    logging.info(
        f"Total OTX Pulses processed into new STIX Bundles: {processed_pulses_count}"
    )
    logging.info(
        f"Total STIX Bundles successfully pushed to TAXII: {pushed_bundles_count}"
    )
    logging.info(decay_filter.summary())

    if pushed_bundles_count == 0:
        logging.info(
            "Note: zero bundles pushed (likely all duplicates, all pre-validated, or no new pulses from OTX)."
        )


# =============================================================================
# Outbox-mode path (the new two-process architecture).
#
# main.py in outbox mode does NOT touch OTX. It only:
#   1. Scans <STIX_OUTBOX_DIR>/pending/ for chunk JSON files written by ingest.py
#   2. Pushes each chunk to TAXII
#   3. On success: moves file from pending/ → processed/
#   4. On failure: leaves file in pending/ for next-cycle retry
#
# This keeps RAM bounded to ONE chunk at a time (~2-5 MB) instead of all
# pulses in memory. The OTX ingest process (ingest.py) handles the heavy
# lifting separately.
# =============================================================================


def _list_outbox_chunks(pending_dir: str) -> list[str]:
    """
    Return sorted list of chunk JSON paths in the pending dir.

    Sorted by (pulse_id, chunk_idx) so chunks of the same pulse are pushed
    in order. This isn't strictly required by TAXII (the server will accept
    them in any order), but it makes logs nicer and avoids a "chunk 3 of 3"
    arriving before "chunk 1 of 3" if files were written out of order.
    """
    if not os.path.isdir(pending_dir):
        return []
    paths = [
        os.path.join(pending_dir, f)
        for f in os.listdir(pending_dir)
        if f.endswith(".json") and not f.endswith(".tmp")
    ]
    # Sort by filename (which embeds pulse_id__chunk_idx__chunk_total).
    paths.sort()
    return paths


def _cleanup_processed_outbox(processed_dir: str, retention_days: int) -> None:
    """
    Delete files from <processed_dir> older than retention_days.

    Default 7 days. Set OUTBOX_RETENTION_DAYS=0 to disable cleanup entirely.
    """
    if retention_days <= 0:
        return
    cutoff = time.time() - (retention_days * 86400)
    deleted = 0
    try:
        for fname in os.listdir(processed_dir):
            fpath = os.path.join(processed_dir, fname)
            try:
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
                    deleted += 1
            except OSError as e:
                logging.warning(f"Could not delete old outbox file {fpath}: {e}")
    except OSError as e:
        logging.warning(f"Could not list processed outbox dir {processed_dir}: {e}")
    if deleted:
        logging.info(
            f"Cleaned up {deleted} outbox file(s) older than {retention_days} days."
        )


def _push_one_outbox_chunk(
    config,
    taxii_client,
    otx_client,
    pending_dir: str,
    processed_dir: str,
    chunk_path: str,
) -> tuple[bool, bool, str, int, int]:
    """
    Read one chunk JSON file, push to TAXII, move to processed/ on success.

    Returns (push_success, processed_success, pulse_id, chunk_idx, chunk_total).
    """
    push_success = False
    processed_success = False
    pulse_id = "?"
    chunk_idx = 0
    chunk_total = 0

    try:
        with open(chunk_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        pulse_id = str(payload.get("pulse_id", "?"))
        chunk_idx = int(payload.get("chunk_idx", 0))
        chunk_total = int(payload.get("chunk_total", 0))
        indicator_count = int(payload.get("indicator_count", 0))
        bundle_dict = payload.get("stix_bundle", {})

        if not bundle_dict or "objects" not in bundle_dict:
            logging.warning(
                f"[main] Chunk {os.path.basename(chunk_path)} has empty/invalid bundle. "
                "Moving to processed/ to skip."
            )
            # Treat as processed so we don't loop on it forever.
            try:
                os.replace(
                    chunk_path,
                    os.path.join(processed_dir, os.path.basename(chunk_path)),
                )
            except OSError:
                pass
            return False, True, pulse_id, chunk_idx, chunk_total

        processed_success = True  # We have a valid bundle to push.

        chunk_label = (
            f"chunk {chunk_idx}/{chunk_total}" if chunk_total > 1 else "bundle"
        )
        logging.info(
            f"[Pulse {pulse_id}] Pushing {chunk_label} "
            f"({indicator_count} indicators) to TAXII..."
        )

        # Pre-validate against TAXII (if enabled) to skip all-duplicate chunks.
        bundle_to_push = bundle_dict
        if config.ENABLE_CACHE_PREVALIDATION:
            original_count = len(bundle_dict.get("objects", []))
            bundle_to_push = taxii_client.pre_validate_bundle_objects(bundle_dict)
            validated_count = len(bundle_to_push.get("objects", []))
            if validated_count == 0:
                logging.info(
                    f"[Pulse {pulse_id}] {chunk_label}: all objects are duplicates. "
                    "Moving to processed/ without pushing."
                )
                os.replace(
                    chunk_path,
                    os.path.join(processed_dir, os.path.basename(chunk_path)),
                )
                # Count as "pushed" (we're done with it).
                return True, True, pulse_id, chunk_idx, chunk_total
            elif validated_count < original_count:
                logging.info(
                    f"[Pulse {pulse_id}] {chunk_label} pre-validation: "
                    f"{original_count} → {validated_count} objects "
                    f"(removed {original_count - validated_count} duplicates)"
                )

        if config.ENABLE_CACHE_PREVALIDATION:
            for obj in bundle_to_push.get("objects", []):
                if "id" in obj:
                    otx_client.cache_stix_uuid(obj["id"], pulse_id)

        if taxii_client.add_stix_bundle(bundle_to_push):
            push_success = True
            logging.info(f"[Pulse {pulse_id}] Successfully pushed {chunk_label}.")
            # Move the file from pending/ to processed/ so we don't retry.
            try:
                os.replace(
                    chunk_path,
                    os.path.join(processed_dir, os.path.basename(chunk_path)),
                )
            except OSError as e:
                logging.error(
                    f"[Pulse {pulse_id}] Pushed OK but could not move "
                    f"{chunk_path} -> processed/: {e}. Will retry next cycle."
                )
                # Return success=False so the next cycle retries — but we
                # already pushed, so the TAXII server will dedup.
                push_success = False
        else:
            logging.error(
                f"[Pulse {pulse_id}] Failed to push {chunk_label}. Will retry next cycle."
            )

    except json.JSONDecodeError as e:
        logging.error(
            f"[main] Could not parse chunk file {chunk_path}: {e}. "
            "Moving to processed/ to skip (corrupt file)."
        )
        try:
            os.replace(
                chunk_path, os.path.join(processed_dir, os.path.basename(chunk_path))
            )
        except OSError:
            pass
    except Exception as e:
        logging.error(
            f"[main] Error processing chunk {chunk_path}: {e}",
            exc_info=True,
        )

    return push_success, processed_success, pulse_id, chunk_idx, chunk_total


def process_outbox_to_taxii(config, taxii_client, otx_client) -> None:
    """
    Outbox-mode cycle: scan pending/ and push each chunk to TAXII.

    Each chunk is pushed by a worker thread (via the existing thread pool
    pattern). Capped by MAX_BUNDLES_TO_PUSH (None = unlimited).
    """
    pending_dir = os.path.join(config.STIX_OUTBOX_DIR, "pending")
    processed_dir = os.path.join(config.STIX_OUTBOX_DIR, "processed")
    os.makedirs(pending_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    # Periodic cleanup of processed/ dir.
    _cleanup_processed_outbox(processed_dir, config.OUTBOX_RETENTION_DAYS)

    chunk_paths = _list_outbox_chunks(pending_dir)
    if not chunk_paths:
        logging.info(
            f"[main/outbox] No chunks pending in {pending_dir}. Nothing to push."
        )
        return

    logging.info(
        f"[main/outbox] Found {len(chunk_paths)} chunk(s) pending. "
        f"Pushing with {config.MAX_WORKERS} worker(s)..."
    )

    max_push_limit = config.MAX_BUNDLES_TO_PUSH
    push_counter_lock = threading.Lock()
    pushed_so_far = 0

    results = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {}
        for chunk_path in chunk_paths:
            if max_push_limit is not None:
                with push_counter_lock:
                    if pushed_so_far >= max_push_limit:
                        logging.info(
                            f"Reached MAX_BUNDLES_TO_PUSH={max_push_limit}. "
                            "Remaining chunks will be pushed on the next cycle."
                        )
                        break

            future = executor.submit(
                _push_one_outbox_chunk,
                config,
                taxii_client,
                otx_client,
                pending_dir,
                processed_dir,
                chunk_path,
            )
            futures[future] = chunk_path

        for future in as_completed(futures):
            try:
                push_ok, proc_ok, pid, cidx, ctot = future.result()
            except Exception as e:
                logging.error(
                    f"[main/outbox] Unhandled exception in chunk worker: {e}",
                    exc_info=True,
                )
                push_ok, proc_ok = False, False
            if push_ok:
                with push_counter_lock:
                    pushed_so_far += 1
            results.append((push_ok, proc_ok))

    processed_count = sum(1 for _, p in results if p)
    pushed_count = sum(1 for s, _ in results if s)
    logging.info(
        f"[main/outbox] Cycle complete: {pushed_count} pushed, "
        f"{processed_count} processed, {len(results) - processed_count} failed."
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
        logging.info(
            f"Resource throttling: MAX_WORKERS={config.MAX_WORKERS}, "
            f"OTX_MAX_CONCURRENT_REQUESTS={config.OTX_MAX_CONCURRENT_REQUESTS}, "
            f"OTX_REQUEST_DELAY_SECONDS={config.OTX_REQUEST_DELAY_SECONDS}, "
            f"OTX_LIST_PAGE_DELAY_SECONDS={config.OTX_LIST_PAGE_DELAY_SECONDS}, "
            f"OTX_CACHE_CLEAR_ON_START={config.OTX_CACHE_CLEAR_ON_START}, "
            f"OTX_MAX_LIST_PAGES={config.OTX_MAX_LIST_PAGES}, "
            f"MAX_INDICATORS_PER_PULSE={config.MAX_INDICATORS_PER_PULSE}"
        )
        logging.info("Main function is now ready to initialize clients.")

        logging.info(
            f"Configured to push a maximum of {config.MAX_BUNDLES_TO_PUSH} new STIX bundles to TAXII per run. "
            f"(Each pulse may produce multiple chunks when it has more than "
            f"MAX_INDICATORS_PER_PULSE={config.MAX_INDICATORS_PER_PULSE} indicators.)"
        )

        # ------------------------------------------------------------------
        # Outbox mode log line — helps operators see which path is active.
        # ------------------------------------------------------------------
        if config.ENABLE_OUTBOX_MODE:
            logging.info(
                f"[main] OUTBOX MODE IS ON. Reading chunks from "
                f"{os.path.join(config.STIX_OUTBOX_DIR, 'pending')}. "
                f"OTX ingestion happens in a separate process (ingest.py)."
            )
        else:
            logging.info(
                "[main] Outbox mode is OFF. Running the legacy single-process "
                "OTX → TAXII path. RAM usage may be high."
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

        # OTX client is needed in BOTH modes:
        # - In outbox mode: only for `cache_stix_uuid()` during pre-validation.
        # - In legacy mode: full OTX fetching + indicator pulling.
        # The constructor is cheap (just creates a small SDK wrapper); the
        # RAM-heavy parts (OTXv2Cached disk walk) only happen on first
        # .update() call.
        otx_client = OTXClient(
            api_key=config.OTX_API_KEY,
            allowed_authors=config.OTX_AUTHOR_FILTER,
            redis_host=config.REDIS_HOST,
            redis_port=config.REDIS_PORT,
            redis_db=config.REDIS_DB,
            redis_password=config.REDIS_PASSWORD,
        )
        logging.info("OTX Client initialized successfully.")

        pulse_processor = None
        if not config.ENABLE_OUTBOX_MODE:
            # Only needed in legacy mode for STIX bundle construction.
            pulse_processor = PulseProcessor(
                custom_stix_namespace=config.CUSTOM_STIX_NAMESPACE
            )
            logging.info("PulseProcessor initialized successfully.")
        else:
            logging.info(
                "[main] Outbox mode: PulseProcessor NOT initialised in this process "
                "(ingest.py owns it)."
            )

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # ------------------------------------------------------------------
        # Scheduler loop. Two paths depending on outbox mode.
        #
        # Both modes use the same in-process loop pattern: stay alive
        # between cycles (don't sys.exit so Docker doesn't restart us
        # in a tight loop), honour SIGINT/SIGTERM via the
        # shutdown_requested flag, sleep an interruptible amount between
        # cycles.
        # ------------------------------------------------------------------
        cycle = 0
        while not shutdown_requested:
            cycle += 1
            logging.info(f"========== Scheduler cycle #{cycle} starting ==========")
            try:
                if config.ENABLE_OUTBOX_MODE:
                    # Outbox path: scan pending/ and push each chunk.
                    process_outbox_to_taxii(config, taxii_client, otx_client)
                else:
                    # Legacy path: fetch from OTX and push inline.
                    existing_stix_ids = taxii_client.get_existing_stix_ids()
                    process_otx_to_taxii(
                        config,
                        otx_client,
                        taxii_client,
                        pulse_processor,
                        existing_stix_ids,
                    )
                logging.info(f"Cycle #{cycle} complete.")
            except OTXAPIUnavailable as e:
                # Only legacy mode can hit OTX directly, but keep the guard.
                logging.error(
                    f"OTX API unavailable in cycle #{cycle}: {e}. "
                    f"Backing off for {config.OTX_BACKOFF_SECONDS}s before next attempt."
                )
                _interruptible_sleep(config.OTX_BACKOFF_SECONDS)
                continue
            except Exception as e:
                logging.error(
                    f"Error in scheduler cycle #{cycle}: {e}",
                    exc_info=True,
                )
                # Don't exit on transient errors — sleep then retry.
                _interruptible_sleep(min(60, config.SCHEDULER_INTERVAL_SECONDS))
                continue

            # Sleep between cycles (interruptible so SIGTERM exits promptly).
            logging.info(
                f"Sleeping {config.SCHEDULER_INTERVAL_SECONDS}s before next cycle "
                "(SIGINT/SIGTERM to exit immediately)."
            )
            _interruptible_sleep(config.SCHEDULER_INTERVAL_SECONDS)

        logging.info("Shutdown requested. Exiting with status 0.")
        sys.exit(0)

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
