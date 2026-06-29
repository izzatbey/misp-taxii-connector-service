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
        self.MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))

        # OTX Author Filter Configuration
        # If set to a specific author name, only pulses from that author will be processed
        # If set to None/empty, pulses from ALL subscribed authors will be processed
        self.OTX_AUTHOR_FILTER = os.getenv("OTX_AUTHOR_FILTER", None)
        if self.OTX_AUTHOR_FILTER and self.OTX_AUTHOR_FILTER.lower() in (
            "none",
            "null",
            "",
            "all",
        ):
            self.OTX_AUTHOR_FILTER = None

        logger.info(
            f"Config loaded: TAXII_TEST_OBJECT_LIMIT={self.TAXII_TEST_OBJECT_LIMIT}, OTX_TEST_PULSE_LIMIT={self.OTX_TEST_PULSE_LIMIT}"
        )
        logger.info(
            f"OTX Author Filter: {'ALL subscribed authors' if self.OTX_AUTHOR_FILTER is None else self.OTX_AUTHOR_FILTER}"
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
