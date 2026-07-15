import os
import uuid
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration class to manage settings for the OTX to TAXII connector.
    Loads environment variables and provides access to configuration values.
    """

    def __init__(self) -> None:
        load_dotenv()

        self.OTX_API_KEY: str = self._get_env_variable("OTX_API_KEY")
        self.TAXII_URL: str = self._get_env_variable("TAXII_URL")
        self.USERNAME: str = self._get_env_variable("USERNAME")
        self.PASSWORD: str = self._get_env_variable("PASSWORD")
        self.VERIFY_SSL: bool = self._parse_boolean(
            self._get_env_variable("VERIFY_SSL")
        )
        self.CUSTOM_STIX_NAMESPACE: uuid.UUID = uuid.UUID(
            "a67b2d4f-1e9c-4f81-8b0d-7c2a3e5f1b0a"
        )
        self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
        self.REDIS_DB = int(os.getenv("REDIS_DB", 0))
        self.REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")  # Optional
        self.CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 3600))
        self.CACHE_REFRESH_THRESHOLD = int(os.getenv("CACHE_REFRESH_THRESHOLD", 300))
        self.ENABLE_CACHE_PREVALIDATION = self._parse_boolean(
            os.getenv("ENABLE_CACHE_PREVALIDATION", "true")
        )
        self.TAXII_TEST_OBJECT_LIMIT = (
            self._get_optional_int_env("TAXII_TEST_OBJECT_LIMIT") or None
        )
        self.OTX_TEST_PULSE_LIMIT = (
            self._get_optional_int_env("OTX_TEST_PULSE_LIMIT") or None
        )
        self.MAX_BUNDLES_TO_PUSH = (
            self._get_optional_int_env("MAX_BUNDLES_TO_PUSH") or None
        )
        self.SCHEDULER_INTERVAL_SECONDS = (
            self._get_optional_int_env("SCHEDULER_INTERVAL_SECONDS") or 3600
        )
        # Maximum number of worker threads used to process OTX pulses concurrently
        # inside process_otx_to_taxii(). Increase for faster throughput on large
        # pulse sets; decrease if you hit TAXII rate-limits or Redis contention.
        # Default lowered from 6 to 2 to keep CPU/RAM usage low on small VMs.
        self.MAX_WORKERS = int(os.getenv("MAX_WORKERS", "2"))

        # ------------------------------------------------------------------
        # Resource-throttling knobs (added to keep CPU/RAM usage bounded on
        # small VMs). All values are tunable via .env without code changes.
        # ------------------------------------------------------------------

        # Maximum number of CONCURRENT outbound HTTPS requests to the OTX API
        # at any given moment. Independent of MAX_WORKERS — workers may run
        # STIX build / Redis / TAXII work concurrently but must take this
        # semaphore before each OTX HTTP call. Default 1 = fully serialised.
        # Increase to 2–3 if OTX rate-limits are not a concern.
        self.OTX_MAX_CONCURRENT_REQUESTS = int(
            os.getenv("OTX_MAX_CONCURRENT_REQUESTS", "1")
        )

        # Sleep added after every outbound OTX HTTP call (seconds). Stacks
        # on top of the semaphore to give the upstream API breathing room
        # and to flatten CPU spikes during bursty list-page fetches.
        # Default 0.5s = at most ~2 OTX calls/sec.
        self.OTX_REQUEST_DELAY_SECONDS = float(
            os.getenv("OTX_REQUEST_DELAY_SECONDS", "0.5")
        )

        # Sleep between sequential OTX list-page fetches during
        # OTXv2Cached.update() (seconds). Page walks for the subscribed
        # feed can be long (50+ pages); this throttle keeps memory and CPU
        # low while still letting the cache warm up.
        self.OTX_LIST_PAGE_DELAY_SECONDS = float(
            os.getenv("OTX_LIST_PAGE_DELAY_SECONDS", "1.0")
        )

        # Optional: clear the bloated on-disk OTX cache (~/.otx_cache_data)
        # at the start of every run. Useful if the cache has grown huge
        # over time and `update()` is slow because it walks thousands of
        # JSON files. Default false; set true once to reset.
        self.OTX_CACHE_CLEAR_ON_START = self._parse_boolean(
            os.getenv("OTX_CACHE_CLEAR_ON_START", "false")
        )

        # Optional cap on the number of OTX list pages to walk during a
        # single run. Default 0 = unlimited. Useful if OTX keeps returning
        # very deep pages. Set to e.g. 30 to cap to ~30 pages.
        self.OTX_MAX_LIST_PAGES = int(os.getenv("OTX_MAX_LIST_PAGES", "0"))

        # Optional cap on indicators processed per single pulse.
        # w0rmsign-style "server scanning" pulses can contain 500+
        # indicators which dominates RAM and CPU during STIX bundle
        # construction. Default 200 (was unlimited). Set to 0 to disable.
        # Pulses with more indicators than this get a warning + truncated.
        self.MAX_INDICATORS_PER_PULSE = int(
            os.getenv("MAX_INDICATORS_PER_PULSE", "200")
        )

        # ------------------------------------------------------------------
        # Pulse decay / recency filter (the "decaying system").
        #
        # A pulse is processed only if its `created` OR `modified` timestamp
        # falls within the last PULSE_MAX_AGE_DAYS days. This prevents stale
        # threat intel (created long ago AND untouched recently) from being
        # re-fetched and re-pushed every cycle, keeping OTX->TAXII volume
        # bounded.
        #
        # Set PULSE_DECAY_ENABLED=false to disable the filter entirely (no-op,
        # all pulses processed). Set PULSE_MAX_AGE_DAYS=0 to disable too.
        # Set PULSE_DECAY_STRICT_MISSING=true to skip pulses with no parseable
        # timestamps (default false = process them, to avoid silent data loss).
        # ------------------------------------------------------------------
        self.PULSE_MAX_AGE_DAYS = int(os.getenv("PULSE_MAX_AGE_DAYS", "90"))
        self.PULSE_DECAY_ENABLED = self._parse_boolean(
            os.getenv("PULSE_DECAY_ENABLED", "true")
        )
        self.PULSE_DECAY_STRICT_MISSING = self._parse_boolean(
            os.getenv("PULSE_DECAY_STRICT_MISSING", "false")
        )

        # OTX SDK retry / timeout overrides.
        # OTXv2 mounts an HTTPAdapter with total=5 retries on
        # 429/500/502/503/504 + backoff_factor=1, which amplifies one
        # slow/hung OTX call into ~31 seconds of internal back-off before
        # RetryError. On persistent 5xx it gives "too many 504 error responses".
        # Override here to fail fast on OTX 5xx so our own
        # OTX_REQUEST_DELAY_SECONDS back-off handles the rate-limit instead.
        self.OTX_MAX_SDK_RETRIES = max(1, int(os.getenv("OTX_MAX_SDK_RETRIES", "1")))
        self.OTX_CONNECT_TIMEOUT = float(os.getenv("OTX_CONNECT_TIMEOUT", "10.0"))
        self.OTX_READ_TIMEOUT = float(os.getenv("OTX_READ_TIMEOUT", "60.0"))

        # ------------------------------------------------------------------
        # Outbox / two-process architecture knobs.
        #
        # When ENABLE_OUTBOX_MODE=true, ingest.py writes STIX chunk
        # bundles as JSON files into <STIX_OUTBOX_DIR>/pending/, and
        # main.py reads them and pushes them to TAXII. This decouples
        # OTX ingestion (RAM-heavy due to on-disk cache walks) from
        # TAXII pushing, allowing each process to stay under ~100 MB
        # peak RAM instead of the previous 14 GB.
        #
        # When ENABLE_OUTBOX_MODE=false, the old single-process path
        # is used (kept for backwards compatibility / quick fallback).
        # ------------------------------------------------------------------
        self.ENABLE_OUTBOX_MODE = self._parse_boolean(
            os.getenv("ENABLE_OUTBOX_MODE", "true")
        )
        self.STIX_OUTBOX_DIR = os.getenv(
            "STIX_OUTBOX_DIR", os.path.join(os.getcwd(), "stix_outbox")
        )
        # How often ingest.py re-runs when --loop is used.
        self.INGEST_INTERVAL_SECONDS = int(os.getenv("INGEST_INTERVAL_SECONDS", "3600"))
        # How long to back off if OTX is unavailable.
        self.OTX_BACKOFF_SECONDS = int(os.getenv("OTX_BACKOFF_SECONDS", "300"))
        # How long to keep processed chunks on disk before auto-deleting
        # them. 0 = keep forever. Default 7 days.
        self.OUTBOX_RETENTION_DAYS = int(os.getenv("OUTBOX_RETENTION_DAYS", "7"))

        # OTX Author Filter Configuration
        # If set to a comma-separated list of author names, only pulses from
        # those authors will be processed. Matching is case-insensitive and
        # whitespace is trimmed.
        # If set to None/empty/'none'/'all'/'null', pulses from ALL subscribed
        # authors will be processed.
        #
        # Example (whitelist of multiple authors):
        #   OTX_AUTHOR_FILTER=AlienVault,MalwarePatrol,Conrat45,SeventySix
        raw_filter = os.getenv("OTX_AUTHOR_FILTER", None)
        if raw_filter is None or raw_filter.strip().lower() in (
            "none",
            "null",
            "",
            "all",
        ):
            self.OTX_AUTHOR_FILTER = None
        else:
            # Parse comma-separated list, trim whitespace, drop empty entries.
            # Always store as a list (possibly empty -> treated as None below).
            parsed = [a.strip() for a in raw_filter.split(",") if a.strip()]
            self.OTX_AUTHOR_FILTER = parsed if parsed else None

        logger.info(
            f"Config loaded: TAXII_TEST_OBJECT_LIMIT={self.TAXII_TEST_OBJECT_LIMIT}, OTX_TEST_PULSE_LIMIT={self.OTX_TEST_PULSE_LIMIT}"
        )
        if self.OTX_AUTHOR_FILTER is None:
            logger.info("OTX Author Filter: ALL subscribed authors")
        else:
            logger.info(
                f"OTX Author Filter: whitelist of {len(self.OTX_AUTHOR_FILTER)} author(s): {self.OTX_AUTHOR_FILTER}"
            )

    def _get_env_variable(self, key: str) -> str:
        """
        Helper method to retrieve environment variables with a fallback.
        Raises an error if the variable is not set.
        """
        value = os.getenv(key)
        if value is None:
            raise ValueError(f"Environment variable '{key}' is not set.")
        return value

    def _get_optional_int_env(self, key: str) -> int | None:
        value = os.getenv(key)
        if value:
            if value.lower() in ("none", "null", ""):
                return None
            try:
                return int(value)
            except ValueError:
                raise ValueError(
                    f"Environment variable '{key}' must be an integer if set."
                )
        return None

    def _parse_boolean(self, value: str) -> bool:
        return value.lower() in ("true", "1", "t", "y", "yes")
