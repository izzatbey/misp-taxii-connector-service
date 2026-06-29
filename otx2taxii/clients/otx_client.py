import logging
from OTXv2 import OTXv2, OTXv2Cached
from OTXv2 import RetryError as OTXv2RetryError
import os
from dotenv import load_dotenv
import datetime
import pytz
import requests
import json

from clients.redis_utility import RedisClient

logger = logging.getLogger(__name__)


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
        self.otx = OTXv2Cached(api_key, cache_dir=otx_cache_dir)
        logger.info(
            f"OTX Client initialized with OTXv2Cached. Cache directory: {otx_cache_dir}"
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

    def get_pulse_indicators(self, pulse_id: str) -> list[dict]:
        """
        Retrieves all indicators for a specific OTX pulse.
        This uses OTXv2Cached's get_pulse_details, which likely hits API for non-cached details.
        """
        logger.info(f"Fetching indicators for pulse ID: {pulse_id}...")
        try:
            pulse_details = self.otx.get_pulse_details(pulse_id)
            if pulse_details and "indicators" in pulse_details:
                logger.info(
                    f"Retrieved {len(pulse_details['indicators'])} indicators for pulse ID: {pulse_id}."
                )
                return pulse_details["indicators"]
            else:
                logger.warning(
                    f"No indicators found or pulse details incomplete for ID: {pulse_id}."
                )
                return []
        except requests.exceptions.Timeout as e:
            logger.error(
                f"OTX API request for pulse {pulse_id} details timed out after 30 seconds: {e}.",
                exc_info=True,
            )
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to OTX API for pulse {pulse_id} details: {e}.",
                exc_info=True,
            )
            return []
        except requests.exceptions.RequestException as e:
            logger.error(
                f"An HTTP error occurred retrieving pulse {pulse_id} details: {e}",
                exc_info=True,
            )
            return []
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while retrieving indicators for pulse {pulse_id}: {e}",
                exc_info=True,
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
