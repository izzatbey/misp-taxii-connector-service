import logging
from OTXv2 import OTXv2, OTXv2Cached
from OTXv2 import RetryError as OTXv2RetryError
import os
from dotenv import load_dotenv
import datetime
import pytz
import requests
import json
import shutil
import threading
import time

from clients.redis_utility import RedisClient

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Module-level throttle for outbound OTX HTTPS calls.
# ----------------------------------------------------------------------
# A single semaphore shared across all OTXClient instances so that
# concurrent workers cannot collectively hammer the OTX API. The delay
# is applied per-request to flatten CPU spikes during bursty fetches.
# These are populated lazily on first use from env vars so we don't
# need a constructor arg change.
_otx_request_semaphore: threading.Semaphore | None = None
_otx_request_semaphore_lock = threading.Lock()
_otx_request_delay_seconds: float = 0.0
_otx_list_page_delay_seconds: float = 0.0
_otx_max_list_pages: int = 0


def _get_otx_request_semaphore() -> threading.Semaphore:
    """Lazily build (and cache) the module-level OTX request semaphore."""
    global _otx_request_semaphore, _otx_request_delay_seconds
    global _otx_list_page_delay_seconds, _otx_max_list_pages
    if _otx_request_semaphore is None:
        with _otx_request_semaphore_lock:
            if _otx_request_semaphore is None:
                try:
                    max_concurrent = max(
                        1, int(os.getenv("OTX_MAX_CONCURRENT_REQUESTS", "1"))
                    )
                except ValueError:
                    max_concurrent = 1
                try:
                    _otx_request_delay_seconds = max(
                        0.0, float(os.getenv("OTX_REQUEST_DELAY_SECONDS", "0.5"))
                    )
                except ValueError:
                    _otx_request_delay_seconds = 0.5
                try:
                    _otx_list_page_delay_seconds = max(
                        0.0, float(os.getenv("OTX_LIST_PAGE_DELAY_SECONDS", "1.0"))
                    )
                except ValueError:
                    _otx_list_page_delay_seconds = 1.0
                try:
                    _otx_max_list_pages = max(
                        0, int(os.getenv("OTX_MAX_LIST_PAGES", "0"))
                    )
                except ValueError:
                    _otx_max_list_pages = 0
                _otx_request_semaphore = threading.Semaphore(max_concurrent)
                logger.info(
                    "OTX request throttle initialised: max_concurrent=%d, "
                    "request_delay=%.2fs, list_page_delay=%.2fs, max_list_pages=%d",
                    max_concurrent,
                    _otx_request_delay_seconds,
                    _otx_list_page_delay_seconds,
                    _otx_max_list_pages,
                )
    return _otx_request_semaphore


def _sleep_after_request() -> None:
    """Sleep OTX_REQUEST_DELAY_SECONDS after a single OTX HTTPS call."""
    # Make sure the throttling knobs are initialised first.
    _get_otx_request_semaphore()
    if _otx_request_delay_seconds > 0:
        time.sleep(_otx_request_delay_seconds)


def _sleep_between_list_pages() -> None:
    """Sleep OTX_LIST_PAGE_DELAY_SECONDS between OTX list-page fetches."""
    _get_otx_request_semaphore()
    if _otx_list_page_delay_seconds > 0:
        time.sleep(_otx_list_page_delay_seconds)


class FilteredOTXv2Cached(OTXv2Cached):
    """
    Subclass of OTXv2Cached that filters out non-whitelisted authors at
    write time. The OTX API returns all subscribed authors' pulses; we
    skip persistence for any author not in the whitelist.

    This does NOT make the initial fetch faster — the SDK still iterates
    every pulse in memory. It only prevents non-whitelisted pulses from
    being written to the local on-disk cache.
    """

    def __init__(
        self,
        api_key,
        allowed_authors=None,
        cache_dir=None,
        max_age=None,
        *args,
        **kwargs,
    ):
        # Normalize to lowercase set for case-insensitive matching
        self._allowed_authors = (
            {a.lower() for a in allowed_authors} if allowed_authors else None
        )
        super().__init__(api_key, cache_dir=cache_dir, max_age=max_age, *args, **kwargs)

        # Override the OTXv2 SDK's urllib3 retry policy so one slow OTX 5xx
        # doesn't amplify into ~31 seconds of internal back-off.
        # OTXv2 mounts an HTTPAdapter with total=5 retries on 429/500/502/503/504.
        # We reduce to 1 retry + set explicit timeouts. The existing
        # OTX_REQUEST_DELAY_SECONDS / OTX_BACKOFF_SECONDS in main.py handle back-off.
        try:
            import os as _os

            _max_retries = max(1, int(_os.getenv("OTX_MAX_SDK_RETRIES", "1")))
            _connect_timeout = float(_os.getenv("OTX_CONNECT_TIMEOUT", "10.0"))
            _read_timeout = float(_os.getenv("OTX_READ_TIMEOUT", "60.0"))
        except (ValueError, TypeError):
            _max_retries, _connect_timeout, _read_timeout = 1, 10.0, 60.0

        # Persist these as instance attributes so the throttle wrapper
        # (which intercepts self.otx.get / self.otx.session().get AFTER
        # this point) can apply them as a real `timeout=` kwarg.
        #
        # Why this matters: the SDK calls self.session().get(url, ...) with
        # no timeout kwarg. Setting self.session.timeout is a no-op on
        # requests.Session — there is no such attribute. The only way to
        # actually cap a hung OTX call is to inject `timeout=` into the
        # outbound request itself. We do this in the throttle wrapper
        # below.
        self._otx_connect_timeout = _connect_timeout
        self._otx_read_timeout = _read_timeout

        self._override_sdk_session(
            max_retries=_max_retries,
            connect_timeout=_connect_timeout,
            read_timeout=_read_timeout,
        )

    def _override_sdk_session(
        self,
        max_retries: int = 1,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
    ) -> None:
        """
        Override the OTXv2 SDK's urllib3 HTTPAdapter retry + timeout policy.

        OTXv2 by default mounts an HTTPAdapter with total=5 retries on
        429/500/502/503/504 + backoff_factor=1, which can amplify one slow OTX
        5xx into ~31 seconds of internal back-off before RetryError.
        On persistent 5xx, urllib3 gives up with "too many 504 error responses".

        We replace the adapter with one that has max_retries=1 (fail fast on
        5xx — let our own OTX_REQUEST_DELAY_SECONDS + OTX_BACKOFF_SECONDS
        handle back-off).

        IMPORTANT: setting self.session.timeout is a NO-OP on
        requests.Session — that attribute doesn't exist. We instead capture
        the timeout values on the instance and inject them as a real
        `timeout=` kwarg into every outbound call inside the throttle
        wrapper (see _install_request_throttle + _patch_session_get).
        """
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            import logging as _log

            retry_cfg = Retry(
                total=max_retries,
                connect=0,
                read=0,
                status_forcelist=[],
                raise_on_status=False,
            )
            adapter = HTTPAdapter(
                max_retries=retry_cfg,
                pool_connections=4,
                pool_maxsize=4,
            )
            # Force the session to exist first so we can mount our adapter.
            # NOTE: `self.session()` is a method — we MUST call it with parens
            # to get the underlying requests.Session instance. Without parens
            # we'd have a bound method object, which has no `.mount`.
            _sess = self.session()
            _sess.mount("https://", adapter)
            _sess.mount("http://", adapter)
            _log.getLogger("clients.otx_client").info(
                f"OTX SDK HTTPAdapter overridden: retries={max_retries}, "
                f"connect={connect_timeout}s, read={read_timeout}s "
                "(timeouts injected per-call in throttle wrapper)."
            )
        except Exception as e:
            _log.getLogger("clients.otx_client").warning(
                f"Could not override OTX SDK session (may already be cached): {e}"
            )

    def save_pulse(self, p):
        """Override to skip writing pulses from non-whitelisted authors."""
        if self._allowed_authors is not None:
            author = (p.get("author_name") or "").lower()
            if author not in self._allowed_authors:
                # Silently skip — do not log per-pulse (would spam the log on
                # large subscription lists)
                return None
        return super().save_pulse(p)


class OTXAPIUnavailable(Exception):
    """Raised when the OTX API is unreachable or returning server errors.
    main.py should treat this as fatal (exit non-zero) so Docker restart
    policy can back off instead of hammering the API.
    """


LAST_FETCH_TIMESTAMP_KEY = "otx_last_fetch_timestamp"
PULSE_CACHE_KEY_PREFIX = "otx_pulse:"
PULSE_LIST_KEY = "otx_pulse_list"
STIX_UUID_CACHE_PREFIX = "stix_uuid:"
STIX_UUID_SET_KEY = "stix_uuid_set"
PULSE_STIX_MAP_PREFIX = "pulse_stix_map:"
PULSE_EXPIRY = 86400
DEFAULT_PULSE_PAGE_SIZE = 500


class OTXClient:
    """
    Client for interacting with the OTX API.
    This class is responsible for fetching pulses and indicators from OTX,
    with support for time-windowed incremental updates using Redis.
    """

    def __init__(
        self,
        api_key: str,
        allowed_authors: list[str] | None = None,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: str | None = None,
    ):
        if not api_key:
            raise ValueError("OTX API Key is required.")

        otx_cache_dir = os.getenv(
            "OTX_CACHE_DIR", os.path.expanduser("~/.otx_cache_data")
        )

        # Optional: blow away the on-disk OTX cache once if requested.
        # Useful when the cache has grown to thousands of files and
        # `update()` is slow because it walks + re-parses every JSON file.
        clear_cache_on_start = os.getenv(
            "OTX_CACHE_CLEAR_ON_START", "false"
        ).lower() in (
            "true",
            "1",
            "t",
            "y",
            "yes",
        )
        if clear_cache_on_start and os.path.isdir(otx_cache_dir):
            try:
                shutil.rmtree(otx_cache_dir)
                logger.warning(
                    f"OTX_CACHE_CLEAR_ON_START=true — removed cache directory: {otx_cache_dir}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to clear OTX cache directory {otx_cache_dir}: {e}",
                    exc_info=True,
                )

        self.otx = FilteredOTXv2Cached(
            api_key, allowed_authors=allowed_authors, cache_dir=otx_cache_dir
        )

        # Monkey-patch the SDK's pagination walker so that every page fetch
        # is gated by the OTX request semaphore + a delay between pages.
        # This keeps bulk `update()` + `getall()` walks from hammering
        # OTX or pegging the CPU when there are 50+ pages to walk.
        try:
            max_list_pages = max(0, int(os.getenv("OTX_MAX_LIST_PAGES", "0")))
        except ValueError:
            max_list_pages = 0
        self._install_request_throttle(max_list_pages=max_list_pages)

        logger.info(
            f"OTX Client initialized with FilteredOTXv2Cached. Cache directory: {otx_cache_dir}"
        )
        if allowed_authors:
            logger.info(
                f"OTX cache write filter active: only saving pulses from {len(allowed_authors)} whitelisted author(s)"
            )
        else:
            logger.info(
                "OTX cache write filter not active: all subscribed authors will be cached"
            )

        self.redis_client_instance = RedisClient(
            redis_host, redis_port, redis_db, redis_password
        )
        self.redis_client = self.redis_client_instance.get_client()

    def _get_last_fetched_timestamp(self) -> datetime.datetime | None:
        """Retrieves the last fetched timestamp from Redis (for application tracking)."""
        if self.redis_client:
            try:
                timestamp_str = self.redis_client.get(LAST_FETCH_TIMESTAMP_KEY)
                if timestamp_str:
                    dt = datetime.datetime.fromisoformat(timestamp_str).replace(
                        tzinfo=pytz.UTC
                    )
                    logger.info(
                        f"Retrieved application's last fetched timestamp from Redis: {dt}"
                    )
                    return dt
            except Exception as e:
                logger.error(
                    f"Error retrieving application's last fetched timestamp from Redis: {e}",
                    exc_info=True,
                )
        return None

    def _set_last_fetched_timestamp(self, timestamp: datetime.datetime):
        """Stores the given timestamp in Redis (for application tracking)."""
        if self.redis_client:
            try:
                self.redis_client.set(LAST_FETCH_TIMESTAMP_KEY, timestamp.isoformat())
                logger.info(
                    f"Stored application's new last fetched timestamp in Redis: {timestamp}"
                )
            except Exception as e:
                logger.error(
                    f"Error storing application's last fetched timestamp in Redis: {e}",
                    exc_info=True,
                )

    def iter_subscribed_pulses(
        self,
        author_name: str | list[str] | None = None,
        max_pulses: int | None = None,
    ):
        """
        Streaming variant of get_all_subscribed_pulses.

        Same logic as get_all_subscribed_pulses, but yields each pulse as
        it's loaded from the on-disk OTX cache instead of materialising
        the entire list in RAM. The `last_fetched_timestamp` write at the
        end still happens; we just track it as we go.

        Use this from main.py when you have hundreds-to-thousands of
        pulses per cycle — materialising the full list before processing
        wastes peak RAM on the call site.
        """
        all_fetched_pulses = []

        if isinstance(author_name, str):
            author_name = [author_name]
        author_whitelist = {a.lower() for a in author_name} if author_name else None

        if author_whitelist is None:
            logger.info(
                "No author specified - will retrieve pulses from ALL subscribed authors"
            )
        else:
            logger.info(
                f"Will filter pulses to {len(author_whitelist)} whitelisted author(s): {sorted(author_whitelist)}"
            )

        if max_pulses is None:
            logger.info("No pulse limit set - will retrieve all available pulses")
        else:
            logger.info(f"Will limit to {max_pulses} pulses total")

        logger.info(
            "Starting OTXv2Cached update to fetch new/modified pulses into local cache..."
        )
        try:
            self.otx.update()
            logger.info(
                "OTXv2Cached update completed. Data should now be in local cache."
            )

            processed_pulses_count = 0

            def _yield_and_track(pulse):
                """Yield a pulse while tracking the latest-modified timestamp."""
                nonlocal processed_pulses_count, latest_modified_time_in_batch
                processed_pulses_count += 1
                if processed_pulses_count % 100 == 0:
                    logger.info(f"Processed {processed_pulses_count} pulses so far...")
                modified_str = pulse.get("modified")
                if modified_str:
                    try:
                        current_dt = datetime.datetime.fromisoformat(
                            modified_str
                        ).replace(tzinfo=pytz.UTC)
                        # latest_modified_time_in_batch captured by closure below
                    except ValueError:
                        pass
                return pulse

            latest_modified_time_in_batch = None

            def _track_modified(pulse):
                """Update latest-modified-timestamp tracking (closure)."""
                nonlocal latest_modified_time_in_batch
                modified_str = pulse.get("modified")
                if modified_str:
                    try:
                        current_dt = datetime.datetime.fromisoformat(
                            modified_str
                        ).replace(tzinfo=pytz.UTC)
                        if (
                            latest_modified_time_in_batch is None
                            or current_dt > latest_modified_time_in_batch
                        ):
                            latest_modified_time_in_batch = current_dt
                    except ValueError:
                        logger.warning(
                            f"Could not parse 'modified' timestamp '{modified_str}' for pulse {pulse.get('id')}."
                        )

            if author_whitelist is None:
                logger.info("Retrieving pulses from ALL subscribed authors")
                for pulse in self.otx.getall(iter=True, limit=max_pulses):
                    if max_pulses is not None and processed_pulses_count >= max_pulses:
                        break
                    _track_modified(pulse)
                    processed_pulses_count += 1
                    if processed_pulses_count % 100 == 0:
                        logger.info(
                            f"Heartbeat: {processed_pulses_count} pulses iterated so far..."
                        )
                    yield pulse
            else:
                logger.info(
                    f"Filtering pulses by whitelisted authors (will call OTX API {len(author_whitelist)} times)"
                )
                # --------------------------------------------------------------
                # Per-author circuit breaker
                # --------------------------------------------------------------
                # If a particular OTX pulse endpoint repeatedly hangs or 5xxs
                # (e.g. a single corrupt pulse ID), we don't want to stall the
                # whole cycle. Track consecutive getall() failures per author;
                # after `OTX_CONSECUTIVE_FAILURE_LIMIT` (default 5), skip the
                # rest of that author's pulses for this cycle.
                #
                # The heartbeat every 50 pulses keeps the cycle visible even
                # when progress is slow.
                # --------------------------------------------------------------
                try:
                    failure_limit = max(
                        1, int(os.getenv("OTX_CONSECUTIVE_FAILURE_LIMIT", "5"))
                    )
                except ValueError:
                    failure_limit = 5

                for author in sorted(author_whitelist):
                    logger.info(f"  -> Fetching pulses for author: '{author}'")
                    consecutive_failures = 0
                    author_pulses_seen = 0
                    try:
                        for pulse in self.otx.getall(
                            iter=True, author_name=author, limit=max_pulses
                        ):
                            if (
                                max_pulses is not None
                                and processed_pulses_count >= max_pulses
                            ):
                                break
                            pulse_author = (pulse.get("author_name") or "").lower()
                            if pulse_author != author:
                                continue
                            _track_modified(pulse)
                            processed_pulses_count += 1
                            author_pulses_seen += 1
                            if author_pulses_seen % 50 == 0:
                                logger.info(
                                    f"Heartbeat: '{author}' author at {author_pulses_seen} pulses; total {processed_pulses_count} so far..."
                                )
                            yield pulse
                    except (
                        requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.RequestException,
                        OTXv2RetryError,
                    ) as e:
                        logger.warning(
                            f"getall() for author '{author}' failed: {type(e).__name__}: {e}. "
                            f"Skipping remaining pulses for this author in this cycle."
                        )
                        continue

                    if max_pulses is not None and processed_pulses_count >= max_pulses:
                        break

                    logger.info(
                        f"  -> '{author}': yielded {author_pulses_seen} pulse(s) this cycle."
                    )

            # Persist the latest modified timestamp after we finish yielding
            if latest_modified_time_in_batch:
                self._set_last_fetched_timestamp(
                    latest_modified_time_in_batch + datetime.timedelta(seconds=1)
                )
            else:
                logger.info(
                    "No pulses with valid 'modified' timestamps were found to update the application's last fetch time."
                )

        except requests.exceptions.Timeout as e:
            logger.error(
                f"OTX API update timed out: {e}.",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"OTX API timeout: {e}") from e
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to OTX API during update: {e}.",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"OTX API connection error: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.error(
                f"An HTTP error occurred during OTXv2Cached update: {e}", exc_info=True
            )
            raise OTXAPIUnavailable(f"OTX API HTTP error: {e}") from e
        except OTXv2RetryError as e:
            logger.error(
                f"OTX SDK exhausted retries (likely upstream 5xx): {e}", exc_info=True
            )
            raise OTXAPIUnavailable(f"OTX API exhausted retries: {e}") from e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during OTXv2Cached update: {e}",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"Unexpected OTX error: {e}") from e

    def get_all_subscribed_pulses(
        self,
        author_name: str | list[str] | None = None,
        max_pulses: int | None = None,
    ) -> list[dict]:
        """
        Retrieves all pulses that the authenticated user is subscribed to from OTX.
        Utilizes OTXv2Cached's internal update mechanism for fetching and caching.
        If max_pulses is provided, limits the number of pulses fetched for testing purposes.
        If max_pulses is None, retrieves all available pulses without limitation.

        Args:
            author_name: Filter pulses by author. Accepts:
                        - None: retrieves pulses from ALL subscribed authors.
                        - str: a single author name (e.g., "AlienVault").
                        - list[str]: a whitelist of author names; matching is
                          case-insensitive. The function will call OTX once per
                          author and merge the results.
            max_pulses: Maximum number of pulses to retrieve (None for unlimited)
        """
        all_fetched_pulses = []

        # Normalize author_name to one of:
        #   None            -> all subscribed authors
        #   list[str]       -> whitelist of authors (case-insensitive matching)
        if isinstance(author_name, str):
            author_name = [author_name]
        author_whitelist = {a.lower() for a in author_name} if author_name else None

        if author_whitelist is None:
            logger.info(
                "No author specified - will retrieve pulses from ALL subscribed authors"
            )
        else:
            logger.info(
                f"Will filter pulses to {len(author_whitelist)} whitelisted author(s): {sorted(author_whitelist)}"
            )

        if max_pulses is None:
            logger.info("No pulse limit set - will retrieve all available pulses")
        else:
            logger.info(f"Will limit to {max_pulses} pulses total")

        logger.info(
            "Starting OTXv2Cached update to fetch new/modified pulses into local cache..."
        )
        try:
            self.otx.update()
            logger.info(
                "OTXv2Cached update completed. Data should now be in local cache."
            )

            processed_pulses_count = 0

            if author_whitelist is None:
                # No filter — get pulses from all subscribed authors
                logger.info("Retrieving pulses from ALL subscribed authors")
                for pulse in self.otx.getall(iter=True, limit=max_pulses):
                    if max_pulses is not None and processed_pulses_count >= max_pulses:
                        logger.info(
                            f"Reached test limit of {max_pulses} pulses from cache. Stopping iteration."
                        )
                        break

                    all_fetched_pulses.append(pulse)
                    processed_pulses_count += 1

                    # Log every 100 pulses for progress tracking
                    if processed_pulses_count % 100 == 0:
                        logger.info(
                            f"Processed {processed_pulses_count} pulses so far (from all authors)..."
                        )
            else:
                # Whitelist filter — call getall() once per author and merge results.
                # The OTXv2 SDK's author_name parameter only supports a single author,
                # so we make N calls (one per whitelisted author) and combine the results.
                logger.info(
                    f"Filtering pulses by whitelisted authors (will call OTX API {len(author_whitelist)} times)"
                )
                # Preserve user-provided order for predictable logging
                for author in sorted(author_whitelist):
                    logger.info(f"  -> Fetching pulses for author: '{author}'")
                    for pulse in self.otx.getall(
                        iter=True, author_name=author, limit=max_pulses
                    ):
                        if (
                            max_pulses is not None
                            and processed_pulses_count >= max_pulses
                        ):
                            logger.info(
                                f"Reached test limit of {max_pulses} pulses. Stopping iteration."
                            )
                            break
                        # Defensive: case-insensitive re-check (SDK matching may differ)
                        pulse_author = (pulse.get("author_name") or "").lower()
                        if pulse_author != author:
                            logger.debug(
                                f"Skipping pulse {pulse.get('id')} from author '{pulse_author}' (not {author})"
                            )
                            continue
                        all_fetched_pulses.append(pulse)
                        processed_pulses_count += 1
                        # Log every 100 pulses for progress tracking
                        if processed_pulses_count % 100 == 0:
                            logger.info(
                                f"Processed {processed_pulses_count} whitelisted-author pulses so far..."
                            )
                    if max_pulses is not None and processed_pulses_count >= max_pulses:
                        break

            if all_fetched_pulses:
                if author_whitelist is None:
                    logger.info(
                        f"Successfully retrieved {len(all_fetched_pulses)} pulses from OTXv2Cached's local cache (from all subscribed authors)."
                    )

                    # Log author distribution
                    author_counts = {}
                    for pulse in all_fetched_pulses:
                        author = pulse.get("author_name", "Unknown")
                        author_counts[author] = author_counts.get(author, 0) + 1

                    logger.info("Pulse distribution by author:")
                    for author, count in sorted(
                        author_counts.items(), key=lambda x: x[1], reverse=True
                    ):
                        logger.info(f"  - {author}: {count} pulses")
                else:
                    logger.info(
                        f"Successfully retrieved {len(all_fetched_pulses)} pulses from whitelisted authors: {sorted(author_whitelist)}"
                    )

                    # Log author distribution (only for whitelisted authors actually seen)
                    author_counts = {}
                    for pulse in all_fetched_pulses:
                        author = pulse.get("author_name", "Unknown")
                        author_counts[author] = author_counts.get(author, 0) + 1

                    logger.info("Pulse distribution by author (whitelisted):")
                    for author, count in sorted(
                        author_counts.items(), key=lambda x: x[1], reverse=True
                    ):
                        logger.info(f"  - {author}: {count} pulses")

                latest_modified_time_in_batch = None
                for pulse in all_fetched_pulses:
                    modified_str = pulse.get("modified")
                    if modified_str:
                        try:
                            current_pulse_modified_dt = datetime.datetime.fromisoformat(
                                modified_str
                            ).replace(tzinfo=pytz.UTC)
                            if (
                                latest_modified_time_in_batch is None
                                or current_pulse_modified_dt
                                > latest_modified_time_in_batch
                            ):
                                latest_modified_time_in_batch = (
                                    current_pulse_modified_dt
                                )
                        except ValueError:
                            logger.warning(
                                f"Could not parse 'modified' timestamp '{modified_str}' for pulse {pulse.get('id')}. Skipping for timestamp update."
                            )

                if latest_modified_time_in_batch:
                    self._set_last_fetched_timestamp(
                        latest_modified_time_in_batch + datetime.timedelta(seconds=1)
                    )
                else:
                    logger.info(
                        "No pulses with valid 'modified' timestamps were found to update the application's last fetch time."
                    )
            else:
                if author_whitelist is None:
                    logger.info(
                        "No new or updated pulses found from any subscribed author in OTXv2Cached's local cache after update."
                    )
                else:
                    logger.info(
                        f"No new or updated pulses from whitelisted authors: {sorted(author_whitelist)} found in OTXv2Cached's local cache after update."
                    )

            return all_fetched_pulses

        except requests.exceptions.Timeout as e:
            logger.error(
                f"OTX API update timed out after 30 seconds: {e}. Check network connectivity or OTX API status.",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"OTX API timeout: {e}") from e
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to OTX API during update: {e}. Check network connectivity and OTX API endpoint.",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"OTX API connection error: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.error(
                f"An HTTP error occurred during OTXv2Cached update: {e}", exc_info=True
            )
            raise OTXAPIUnavailable(f"OTX API HTTP error: {e}") from e
        except OTXv2RetryError as e:
            logger.error(
                f"OTX SDK exhausted retries (likely upstream 5xx): {e}", exc_info=True
            )
            raise OTXAPIUnavailable(f"OTX API exhausted retries: {e}") from e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during OTXv2Cached update: {e}",
                exc_info=True,
            )
            raise OTXAPIUnavailable(f"Unexpected OTX error: {e}") from e

    def get_pulses_by_multiple_authors(
        self, author_names: list[str], max_pulses: int | None = None
    ) -> list[dict]:
        """
        Retrieves pulses from multiple specific authors.

        Args:
            author_names: List of author names to filter by (e.g., ["AlienVault", "ThreatCrowd"])
            max_pulses: Maximum number of pulses to retrieve per author (None for unlimited)

        Returns:
            List of pulse dictionaries from the specified authors
        """
        all_pulses = []

        for author_name in author_names:
            logger.info(f"Fetching pulses from author: {author_name}")
            author_pulses = self.get_all_subscribed_pulses(
                author_name=author_name, max_pulses=max_pulses
            )
            all_pulses.extend(author_pulses)
            logger.info(f"Retrieved {len(author_pulses)} pulses from {author_name}")

        logger.info(f"Total pulses retrieved from all authors: {len(all_pulses)}")
        return all_pulses

    def get_alienvault_pulses_only(self, max_pulses: int | None = None) -> list[dict]:
        """
        Convenience method to get only AlienVault pulses.

        Args:
            max_pulses: Maximum number of pulses to retrieve (None for unlimited)

        Returns:
            List of AlienVault pulse dictionaries
        """
        return self.get_all_subscribed_pulses(
            author_name="AlienVault", max_pulses=max_pulses
        )

    def get_author_statistics(self, max_pulses: int | None = None) -> dict[str, int]:
        """
        Get statistics about pulse authors in your OTX data.

        Args:
            max_pulses: Maximum number of pulses to analyze (None for all)

        Returns:
            Dictionary with author names as keys and pulse counts as values
        """
        logger.info("Analyzing author statistics in OTX data...")

        try:
            self.otx.update()
            logger.info("OTXv2Cached update completed for author analysis.")

            author_stats = {}
            processed_count = 0

            # Get all pulses without author filtering to analyze
            for pulse in self.otx.getall(iter=True, limit=max_pulses):
                if max_pulses is not None and processed_count >= max_pulses:
                    break

                author = pulse.get("author_name", "Unknown")
                author_stats[author] = author_stats.get(author, 0) + 1
                processed_count += 1

                if processed_count % 500 == 0:
                    logger.info(
                        f"Analyzed {processed_count} pulses for author statistics..."
                    )

            # Sort by pulse count (descending)
            sorted_stats = dict(
                sorted(author_stats.items(), key=lambda x: x[1], reverse=True)
            )

            logger.info(f"Author statistics (top 10):")
            for i, (author, count) in enumerate(list(sorted_stats.items())[:10]):
                logger.info(f"  {i + 1}. {author}: {count} pulses")

            return sorted_stats

        except Exception as e:
            logger.error(f"Error analyzing author statistics: {e}", exc_info=True)
            return {}

    def _patch_session_for_real_timeout(self) -> None:
        """
        Inject `timeout=(connect, read)` into every OTX HTTPS call.

        The OTXv2 SDK constructs a requests.Session lazily on first call
        to self.session() — but `self.session` here refers to the
        UNDERLYING OTXv2 instance, not this OTXClient wrapper. We
        therefore reach through self.otx.session.

        Inside OTXv2.get/post/patch/delete it calls
        self.session().get(url, headers=..., proxies=..., verify=..., cert=...)
        with no `timeout=` kwarg. requests' default behaviour is to wait
        forever on a hung TCP connection.

        We wrap the actual Session.get / Session.post methods so every
        outbound request gets our connect+read timeout tuple as a kwarg.
        urllib3 then raises requests.exceptions.Timeout after the read
        timeout elapses, which the SDK's caller (our get_pulse_indicators)
        is already prepared to handle.

        Safe to call multiple times — patches are idempotent (we attach
        the wrapper only once per session instance).
        """
        try:
            connect = getattr(self, "_otx_connect_timeout", 10.0)
            read = getattr(self, "_otx_read_timeout", 60.0)
            timeout_tuple = (float(connect), float(read))

            # Reach the actual SDK session — self.otx is the
            # FilteredOTXv2Cached (subclass of OTXv2 which is
            # subclass of OTXv2Cached). self.otx.session() is the
            # lazy-init method that returns the requests.Session.
            otx_sdk = getattr(self, "otx", None)
            if otx_sdk is None:
                raise AttributeError("self.otx is not set — OTXClient not initialized?")

            sess = otx_sdk.session()  # <-- force lazy session creation
            if getattr(sess, "_misp_taxii_timeout_patched", False):
                logger.info(
                    "OTX requests.Session already patched for timeouts; skipping."
                )
                return  # already patched — idempotent

            original_session_get = sess.get
            original_session_post = sess.post

            def _with_timeout(caller):
                """Wrap a requests.Session method to inject `timeout=`
                if the caller didn't supply one."""

                def wrapped(url, *args, **kwargs):
                    if "timeout" not in kwargs:
                        kwargs["timeout"] = timeout_tuple
                    return caller(url, *args, **kwargs)

                wrapped.__wrapped__ = caller
                return wrapped

            sess.get = _with_timeout(original_session_get)
            sess.post = _with_timeout(original_session_post)
            sess._misp_taxii_timeout_patched = True
            logger.info(
                f"Patched OTX requests.Session with hard timeout "
                f"(connect={connect}s, read={read}s)."
            )
        except Exception as e:
            logger.warning(
                f"Could not patch OTX session for real timeout "
                f"(SDK may behave as before): {e}",
                exc_info=True,
            )

    def _install_request_throttle(self, max_list_pages: int = 0) -> None:
        """
        Wrap the OTX SDK's `get()` and `post()` methods with our throttle.

        Why `get`/`post` and not `walkapi_iter`? Because `walkapi_iter` is
        itself a generator that internally calls `self.get(url)` once per
        page — it has signature `(url, max_page=None, max_items=None,
        method='GET', body=None)` with no `params`/`headers` kwargs.
        Wrapping `get()`/`post()` is the cleanest single point of
        interception: every internal SDK caller (walkapi_iter,
        get_pulse_details, getall, etc.) funnels through them.

        The wrapper:
          1. Acquires the module-level OTX request semaphore (caps
             concurrent outbound calls to OTX_MAX_CONCURRENT_REQUESTS).
          2. Calls the original method.
          3. Optionally enforces a max_list_pages cap by short-circuiting
             the second-and-later pages of an OTX list endpoint (heuristic
             based on URL containing '/pulses/subscribed' or '/events').
          4. Sleeps OTX_LIST_PAGE_DELAY_SECONDS between list pages.

        `OTX_REQUEST_DELAY_SECONDS` is applied inside `get_pulse_indicators`
        (and any other method that wants to throttle a single request) via
        the `_sleep_after_request()` helper.

        `_patch_session_for_real_timeout` is called after this method to
        inject `timeout=` directly into the underlying requests.Session
        instance. Without that, the SDK's get() calls bypass any per-call
        timeout and can hang indefinitely on a slow OTX endpoint.
        """
        sem = _get_otx_request_semaphore()

        # Keep a counter per SDK instance so max_list_pages is enforced
        # across multiple paginated walks within the same run.
        if not hasattr(self.otx, "_throttled_list_page_count"):
            self.otx._throttled_list_page_count = 0
        if not hasattr(self.otx, "_throttled_get_count"):
            self.otx._throttled_get_count = 0

        original_get = self.otx.get
        original_post = self.otx.post

        def _is_list_endpoint(url: str) -> bool:
            """Heuristic: is this URL a paginated LIST endpoint?"""
            if not isinstance(url, str):
                return False
            return ("/pulses/subscribed" in url) or ("/api/v1/events" in url)

        def throttled_get(url, *args, **kwargs):
            with sem:
                if _is_list_endpoint(url):
                    self.otx._throttled_list_page_count += 1
                    if (
                        max_list_pages > 0
                        and self.otx._throttled_list_page_count > max_list_pages
                    ):
                        logger.info(
                            f"OTX_MAX_LIST_PAGES={max_list_pages} reached; "
                            f"refusing further list-page fetch: {url}"
                        )
                        # Return an empty page with no `next` so callers
                        # stop paginating cleanly.
                        return {"results": [], "next": None}
                self.otx._throttled_get_count += 1
                result = original_get(url, *args, **kwargs)
            # Sleep AFTER releasing the semaphore so other workers can
            # still make progress during the throttle window.
            if _is_list_endpoint(url):
                _sleep_between_list_pages()
            return result

        def throttled_post(url, *args, **kwargs):
            with sem:
                result = original_post(url, *args, **kwargs)
            _sleep_after_request()
            return result

        try:
            # Bind onto the SDK instance using normal attribute assignment.
            # `OTXv2.get` is defined on the class; assigning on the
            # instance creates an instance attribute that shadows it.
            self.otx.get = throttled_get
            self.otx.post = throttled_post
            logger.info(
                "Installed throttled get()/post() on OTX client "
                f"(max_list_pages={max_list_pages})."
            )
        except Exception as e:
            logger.error(
                f"Failed to install OTX request throttle: {e}",
                exc_info=True,
            )

        # ------------------------------------------------------------------
        # REAL TIMEOUT INJECTION
        # ------------------------------------------------------------------
        # The SDK calls self.session().get(url, headers=..., proxies=...,
        # verify=..., cert=...) inside OTXv2.get() — WITHOUT a `timeout=`
        # kwarg. That means even if `OTX_READ_TIMEOUT=60` is set, urllib3
        # can still hang forever on a half-open connection to OTX.
        #
        # We patch the underlying requests.Session.get/post to inject our
        # own `timeout=(connect, read)` tuple. This is the *only* place a
        # hard timeout is enforced — nothing in the SDK or our throttle
        # wrapper above stops a true TCP hang.
        self._patch_session_for_real_timeout()

    def get_pulse_indicators(self, pulse_id: str) -> list[dict]:
        """
        Retrieves all indicators for a specific OTX pulse.
        This uses OTXv2Cached's get_pulse_details, which likely hits API for non-cached details.

        The outbound HTTPS call is gated by a module-level semaphore
        (OTX_MAX_CONCURRENT_REQUESTS) and followed by a small sleep
        (OTX_REQUEST_DELAY_SECONDS) to keep CPU/RAM usage bounded on
        small VMs and avoid OTX rate-limits.
        """
        logger.info(f"Fetching indicators for pulse ID: {pulse_id}...")
        sem = _get_otx_request_semaphore()
        try:
            with sem:
                pulse_details = self.otx.get_pulse_details(pulse_id)
        except requests.exceptions.Timeout as e:
            logger.error(
                f"OTX API request for pulse {pulse_id} details timed out after 30 seconds: {e}.",
                exc_info=True,
            )
            _sleep_after_request()
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to OTX API for pulse {pulse_id} details: {e}.",
                exc_info=True,
            )
            _sleep_after_request()
            return []
        except requests.exceptions.RequestException as e:
            logger.error(
                f"An HTTP error occurred retrieving pulse {pulse_id} details: {e}",
                exc_info=True,
            )
            _sleep_after_request()
            return []
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while retrieving indicators for pulse {pulse_id}: {e}",
                exc_info=True,
            )
            _sleep_after_request()
            return []

        _sleep_after_request()

        if pulse_details and "indicators" in pulse_details:
            logger.info(
                f"Retrieved {len(pulse_details['indicators'])} indicators for pulse ID: {pulse_id}."
            )
            return pulse_details["indicators"]
        logger.warning(
            f"No indicators found or pulse details incomplete for ID: {pulse_id}."
        )
        return []

    def _cache_pulse(self, pulse: dict):
        """Store a pulse in Redis cache"""
        if not self.redis_client or not pulse or "id" not in pulse:
            return False

        try:
            pulse_id = pulse["id"]
            key = f"{PULSE_CACHE_KEY_PREFIX}{pulse_id}"

            self.redis_client.set(key, json.dumps(pulse), ex=PULSE_EXPIRY)
            self.redis_client.sadd(PULSE_LIST_KEY, pulse_id)

            logger.debug(f"Cached pulse {pulse_id} in Redis")
            return True
        except Exception as e:
            logger.error(f"Error caching pulse in Redis: {e}", exc_info=True)
            return False

    def _get_cached_pulse(self, pulse_id: str) -> dict | None:
        """Retrieve a pulse from Redis cache"""
        if not self.redis_client:
            return None

        try:
            key = f"{PULSE_CACHE_KEY_PREFIX}{pulse_id}"
            cached_data = self.redis_client.get(key)

            if cached_data:
                self.redis_client.expire(key, PULSE_EXPIRY)
                return json.loads(cached_data)
            return None
        except Exception as e:
            logger.error(
                f"Error retrieving cached pulse from Redis: {e}", exc_info=True
            )
            return None

    def _get_cached_pulses(self) -> list[dict]:
        """Get all cached pulses from Redis"""
        if not self.redis_client:
            return []

        try:
            pulse_ids = self.redis_client.smembers(PULSE_LIST_KEY)
            if not pulse_ids:
                return []

            pulses = []
            for pulse_id in pulse_ids:
                pulse = self._get_cached_pulse(pulse_id)
                if pulse:
                    pulses.append(pulse)

            logger.info(f"Retrieved {len(pulses)} pulses from Redis cache")
            return pulses
        except Exception as e:
            logger.error(
                f"Error retrieving cached pulses from Redis: {e}", exc_info=True
            )
            return []

    def cache_stix_uuid(self, stix_id: str, pulse_id: str) -> bool:
        """
        Store STIX UUID mapping to pulse ID to avoid duplicate database queries
        Optimized to only store UUIDs without full objects
        """
        if not self.redis_client:
            return False

        try:
            key = f"{STIX_UUID_CACHE_PREFIX}{stix_id}"
            self.redis_client.set(key, pulse_id, ex=PULSE_EXPIRY)
            self.redis_client.sadd(STIX_UUID_SET_KEY, stix_id)

            pulse_map_key = f"{PULSE_STIX_MAP_PREFIX}{pulse_id}"
            self.redis_client.sadd(pulse_map_key, stix_id)
            self.redis_client.expire(pulse_map_key, PULSE_EXPIRY)

            logger.debug(f"Cached STIX UUID {stix_id} for pulse {pulse_id}")
            return True
        except Exception as e:
            logger.error(f"Error caching STIX UUID in Redis: {e}", exc_info=True)
            return False

    def get_cached_stix_uuid(self, stix_id: str) -> str | None:
        """
        Check if a STIX UUID is already in the cache
        Returns the associated pulse ID if found
        """
        if not self.redis_client:
            return None

        try:
            key = f"{STIX_UUID_CACHE_PREFIX}{stix_id}"
            return self.redis_client.get(key)
        except Exception as e:
            logger.error(
                f"Error retrieving cached STIX UUID from Redis: {e}", exc_info=True
            )
            return None

    def is_stix_uuid_cached(self, stix_id: str) -> bool:
        """
        Efficiently check if a STIX UUID exists in the cache
        """
        if not self.redis_client:
            return False

        try:
            return self.redis_client.sismember(STIX_UUID_SET_KEY, stix_id)
        except Exception as e:
            logger.error(f"Error checking STIX UUID in Redis set: {e}", exc_info=True)
            return False

    def get_pulse_stix_uuids(self, pulse_id: str) -> set:
        """
        Get all STIX UUIDs associated with a particular pulse
        Used to identify which objects need updating when a pulse changes
        """
        if not self.redis_client:
            return set()

        try:
            pulse_map_key = f"{PULSE_STIX_MAP_PREFIX}{pulse_id}"
            uuids = self.redis_client.smembers(pulse_map_key)
            return set(uuids) if uuids else set()
        except Exception as e:
            logger.error(
                f"Error retrieving pulse STIX UUIDs from Redis: {e}", exc_info=True
            )
            return set()

    def check_pulse_changed(self, pulse_id: str, new_indicators: list) -> bool:
        """
        Check if a pulse has changed indicators compared to what's cached
        Returns True if the pulse should be updated in TAXII
        """
        try:
            cached_pulse = self._get_cached_pulse(pulse_id)
            if not cached_pulse or "indicators" not in cached_pulse:
                logger.info(
                    f"Pulse {pulse_id} not in cache or missing indicators - treating as new/changed"
                )
                return True

            if len(cached_pulse["indicators"]) != len(new_indicators):
                logger.info(
                    f"Pulse {pulse_id} indicator count changed from {len(cached_pulse['indicators'])} to {len(new_indicators)}"
                )
                return True

            cached_ids = {
                ind.get("id") for ind in cached_pulse["indicators"] if "id" in ind
            }
            new_ids = {ind.get("id") for ind in new_indicators if "id" in ind}

            if cached_ids != new_ids:
                logger.info(f"Pulse {pulse_id} indicator content has changed")
                return True

            logger.info(f"Pulse {pulse_id} has not changed since last processing")
            return False
        except Exception as e:
            logger.error(f"Error checking if pulse has changed: {e}", exc_info=True)
            return True
