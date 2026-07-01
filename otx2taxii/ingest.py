"""
ingest.py — Out-of-process OTX ingestion.

This module is the FIRST half of the new two-process architecture:

    ingest.py  (this file)  →  /stix_outbox/pending/*.json  →  main.py (pusher)

Responsibilities:
    - Connect to OTX (via the same OTXv2 SDK as before)
    - Walk subscribed pulses, filter by whitelisted authors
    - For each pulse, fetch indicators, build STIX chunks
    - Write each chunk as a separate JSON file in /stix_outbox/pending/

This module DOES NOT touch TAXII. It is intentionally TAXII-ignorant so
the RAM profile stays low (just one pulse + its indicators at a time).

On exit, the calling shell (Dockerfile CMD) decides when to run this
again — typically once per hour.

Run modes:
    python ingest.py                 # one-shot, exit when done
    python ingest.py --loop          # internal loop, sleep between cycles
                                     # (use this for single-process deployment)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from clients.otx_client import OTXClient, OTXAPIUnavailable
from config.settings import Config
from services.pulse_processor import PulseProcessor

# ---------------------------------------------------------------------------
# Globals + signal handling for graceful shutdown.
# ---------------------------------------------------------------------------
shutdown_requested = False


def signal_handler(signum, frame):
    global shutdown_requested
    logging.info(f"[ingest] Received signal {signum}. Requesting graceful shutdown...")
    shutdown_requested = True


def _interruptible_sleep(total_seconds: int) -> None:
    remaining = max(0, int(total_seconds))
    while remaining > 0 and not shutdown_requested:
        time.sleep(min(1, remaining))
        remaining -= 1


# ---------------------------------------------------------------------------
# Outbox helpers.
# ---------------------------------------------------------------------------
def _ensure_outbox_dirs(outbox_dir: str) -> tuple[str, str]:
    """
    Make sure <outbox>/pending and <outbox>/processed exist. Returns
    (pending_dir, processed_dir).
    """
    pending = os.path.join(outbox_dir, "pending")
    processed = os.path.join(outbox_dir, "processed")
    os.makedirs(pending, exist_ok=True)
    os.makedirs(processed, exist_ok=True)
    return pending, processed


def _safe_chunk_filename(
    pulse_id: str, chunk_idx: int, chunk_total: int, indicator_count: int
) -> str:
    """
    Filename: <pulse_id>__<idx>__<total>__<indicators>.json

    - Double-underscore separators survive the wildest filename munging.
    - `chunk_total` lets the reader re-derive ordering without parsing JSON.
    - `indicator_count` lets us sanity-check without re-loading the file.
    """
    # Strip path-unsafe characters from the pulse_id just in case.
    safe_pulse_id = "".join(
        c if c.isalnum() or c in "-_" else "_" for c in str(pulse_id)
    )
    return f"{safe_pulse_id}__{chunk_idx}__{chunk_total}__{indicator_count}.json"


def _write_chunk(
    pending_dir: str,
    pulse_id: str,
    pulse_name: str,
    chunk_idx: int,
    chunk_total: int,
    bundle_dict: dict,
) -> str:
    """
    Atomically write a chunk's STIX bundle as JSON to the outbox.

    Returns the path written. Uses a temp file + rename to avoid
    leaving half-written JSON if the process dies mid-write.
    """
    indicator_count = len(bundle_dict.get("objects", []))
    filename = _safe_chunk_filename(pulse_id, chunk_idx, chunk_total, indicator_count)
    final_path = os.path.join(pending_dir, filename)
    tmp_path = final_path + ".tmp"

    payload = {
        "pulse_id": str(pulse_id),
        "pulse_name": str(pulse_name),
        "chunk_idx": int(chunk_idx),
        "chunk_total": int(chunk_total),
        "indicator_count": int(indicator_count),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "stix_bundle": bundle_dict,
    }

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    os.replace(tmp_path, final_path)

    return final_path


# ---------------------------------------------------------------------------
# Core ingestion loop.
# ---------------------------------------------------------------------------
def run_one_ingest_cycle(
    config: Config,
    otx_client: OTXClient,
    pulse_processor: PulseProcessor,
    pending_dir: str,
) -> dict:
    """
    Pull all whitelisted-author pulses from OTX, chunk them, write
    chunk files to <pending_dir>. Returns a small stats dict.

    This function uses the streaming `iter_subscribed_pulses` so we
    never hold the full pulse list in RAM. Each pulse is processed
    individually and released.
    """
    stats = {
        "pulses_seen": 0,
        "bundles_generated": 0,
        "chunks_written": 0,
        "chunks_failed": 0,
        "chunks_skipped_already_in_taxii": 0,
    }

    existing_stix_ids: set[str] = set()  # We don't dedup against TAXII here;
    # main.py will do that. We DO need to skip indicator IDs that
    # we've already written out this cycle (to avoid duplicate work
    # if the OTX SDK returns the same pulse twice via different
    # author filters).
    written_grouping_ids: set[str] = set()

    logging.info("[ingest] Starting one ingest cycle (OTX → outbox)")

    for pulse_detail in otx_client.iter_subscribed_pulses(
        max_pulses=config.OTX_TEST_PULSE_LIMIT,
        author_name=config.OTX_AUTHOR_FILTER,
    ):
        if shutdown_requested:
            logging.info("[ingest] Shutdown requested mid-cycle, stopping.")
            break

        stats["pulses_seen"] += 1
        pulse_id = str(pulse_detail.get("id"))
        pulse_name = pulse_detail.get("name", "Unknown Pulse")

        try:
            pulse_indicators = otx_client.get_pulse_indicators(pulse_id)
        except Exception as e:
            logging.error(
                f"[ingest] Failed to fetch indicators for pulse '{pulse_name}' "
                f"(ID: {pulse_id}): {e}",
                exc_info=True,
            )
            stats["chunks_failed"] += 1
            continue

        # Build chunks. process_pulse_data returns list[Bundle]; one per chunk.
        # We pass `existing_stix_ids=set()` here because dedup against TAXII
        # is main.py's job. We DO need a local dedup set so this cycle doesn't
        # write the same chunk twice if the SDK yields duplicates.
        # (Use a local set that grows as we go.)
        bundles = pulse_processor.process_pulse_data(
            pulse_detail, pulse_indicators, existing_stix_ids=set()
        )

        if not bundles:
            # No new chunks — grouping already existed in TAXII, or no indicators.
            stats["chunks_skipped_already_in_taxii"] += 1
            continue

        stats["bundles_generated"] += len(bundles)

        for chunk_idx, bundle in enumerate(bundles, start=1):
            chunk_total = len(bundles)

            # Convert bundle → dict for JSON serialization.
            bundle_dict = json.loads(bundle.serialize())

            # Optional: skip if we already wrote this exact grouping ID this cycle.
            grouping_id = None
            for obj in bundle_dict.get("objects", []):
                if obj.get("type") == "grouping":
                    grouping_id = obj.get("id")
                    break
            if grouping_id and grouping_id in written_grouping_ids:
                logging.debug(
                    f"[ingest] Skipping duplicate grouping {grouping_id} for "
                    f"pulse '{pulse_name}' chunk {chunk_idx}/{chunk_total}."
                )
                continue

            try:
                written_path = _write_chunk(
                    pending_dir=pending_dir,
                    pulse_id=pulse_id,
                    pulse_name=pulse_name,
                    chunk_idx=chunk_idx,
                    chunk_total=chunk_total,
                    bundle_dict=bundle_dict,
                )
                if grouping_id:
                    written_grouping_ids.add(grouping_id)
                stats["chunks_written"] += 1
                logging.info(
                    f"[ingest] Wrote {os.path.basename(written_path)} "
                    f"({chunk_idx}/{chunk_total} for '{pulse_name}')"
                )
            except Exception as e:
                logging.error(
                    f"[ingest] Failed to write chunk for pulse '{pulse_name}' "
                    f"({chunk_idx}/{chunk_total}): {e}",
                    exc_info=True,
                )
                stats["chunks_failed"] += 1

    logging.info(
        f"[ingest] Cycle stats: {stats['pulses_seen']} pulses seen, "
        f"{stats['bundles_generated']} bundles generated, "
        f"{stats['chunks_written']} chunks written, "
        f"{stats['chunks_failed']} failed, "
        f"{stats['chunks_skipped_already_in_taxii']} skipped (no new content)."
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="OTX to outbox ingestion")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously, sleeping INGEST_INTERVAL_SECONDS between cycles.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single cycle and exit (default).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Wipe <STIX_OUTBOX_DIR>/pending and <STIX_OUTBOX_DIR>/processed, then exit. "
            "Use this if the outbox has gotten into a bad state (corrupt chunks piling up, "
            "or after a major TAXII endpoint migration where you want to re-push from scratch)."
        ),
    )
    args = parser.parse_args()

    if args.reset:
        # Need a minimal config to know where the outbox is. Don't init OTX.
        load_dotenv()
        config = Config()
        pending_dir, processed_dir = _ensure_outbox_dirs(config.STIX_OUTBOX_DIR)

        for label, d in [("pending", pending_dir), ("processed", processed_dir)]:
            if os.path.isdir(d):
                count = 0
                for fname in os.listdir(d):
                    fpath = os.path.join(d, fname)
                    if os.path.isfile(fpath) and not fname.endswith(".tmp"):
                        try:
                            os.remove(fpath)
                            count += 1
                        except OSError as e:
                            logging.warning(f"Could not delete {fpath}: {e}")
                logging.info(f"[ingest] --reset: deleted {count} file(s) from {d}")
        logging.info("[ingest] --reset: outbox wiped. Exiting.")
        sys.exit(0)

    load_dotenv()
    config = Config()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.info("[ingest] Starting OTX ingestion process.")
    logging.info(
        f"[ingest] Outbox dir: {config.STIX_OUTBOX_DIR}, "
        f"max_indicators_per_pulse={config.MAX_INDICATORS_PER_PULSE}, "
        f"otx_max_list_pages={config.OTX_MAX_LIST_PAGES}"
    )

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    pending_dir, _ = _ensure_outbox_dirs(config.STIX_OUTBOX_DIR)

    # Build OTX client + pulse processor. These hold the only real state
    # in this process; main.py holds its own separate ones.
    otx_client = OTXClient(
        api_key=config.OTX_API_KEY,
        allowed_authors=config.OTX_AUTHOR_FILTER,
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_db=config.REDIS_DB,
        redis_password=config.REDIS_PASSWORD,
    )
    pulse_processor = PulseProcessor(custom_stix_namespace=config.CUSTOM_STIX_NAMESPACE)

    cycle = 0
    while not shutdown_requested:
        cycle += 1
        logging.info(f"[ingest] ======== Cycle #{cycle} starting ========")
        try:
            run_one_ingest_cycle(config, otx_client, pulse_processor, pending_dir)
        except OTXAPIUnavailable as e:
            logging.error(
                f"[ingest] OTX API unavailable: {e}. Backing off "
                f"{config.OTX_BACKOFF_SECONDS}s before next cycle."
            )
            _interruptible_sleep(config.OTX_BACKOFF_SECONDS)
            continue
        except Exception as e:
            logging.error(
                f"[ingest] Unhandled error in cycle #{cycle}: {e}",
                exc_info=True,
            )
            # Don't exit on transient errors.
            _interruptible_sleep(60)
            continue

        if not args.loop:
            logging.info("[ingest] One-shot cycle complete. Exiting with status 0.")
            sys.exit(0)

        logging.info(
            f"[ingest] Sleeping {config.INGEST_INTERVAL_SECONDS}s before next ingest cycle."
        )
        _interruptible_sleep(config.INGEST_INTERVAL_SECONDS)

    logging.info("[ingest] Shutdown requested. Exiting.")
    sys.exit(0)


if __name__ == "__main__":
    main()
