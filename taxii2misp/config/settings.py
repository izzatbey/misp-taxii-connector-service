import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration class to manage settings for the TAXII to MISP connector.
    Loads environment variables and provides access to configuration values.
    """

    def __init__(self) -> None:
        load_dotenv()

        # TAXII Server Configuration
        self.DISCOVERY_URL: str = self._get_env_variable("DISCOVERY_URL", "TAXII_URL")
        self.USERNAME: str = self._get_env_variable("USERNAME")
        self.PASSWORD: str = self._get_env_variable("PASSWORD")
        self.VERIFY_SSL: bool = self._parse_boolean(os.getenv("VERIFY_SSL", "True"))

        # MISP Server Configuration
        self.MISP_URL: str = self._get_env_variable("MISP_URL")
        self.MISP_API_KEY: str = self._get_env_variable("MISP_API_KEY")
        self.MISP_DISTRIBUTION_LEVEL: int = int(
            os.getenv("MISP_DISTRIBUTION_LEVEL", "0")
        )
        self.MISP_THREAT_LEVEL: int = int(os.getenv("MISP_THREAT_LEVEL", "2"))
        self.MISP_ANALYSIS_LEVEL: int = int(os.getenv("MISP_ANALYSIS_LEVEL", "2"))
        self.MISP_ALERT_ON_PUBLISH: bool = self._parse_boolean(
            os.getenv("MISP_ALERT_ON_PUBLISH", "false")
        )
        self.MISP_REQUEST_TIMEOUT: int = int(
            os.getenv("MISP_REQUEST_TIMEOUT", "120")
        )  # 120 seconds default

        # Processing Settings
        self.MAX_EVENTS_PER_RUN = None  # Set to None for unlimited processing
        self.SCHEDULER_INTERVAL_SECONDS: int = (
            self._get_optional_int_env("SCHEDULER_INTERVAL_SECONDS") or 3600
        )
        self.TEMP_DIR: str = os.getenv("TEMP_DIR", "./temp")
        self.UPDATE_EXISTING_EVENTS: bool = self._parse_boolean(
            os.getenv("UPDATE_EXISTING_EVENTS", "false")
        )
        self.COMPREHENSIVE_PROCESSING: bool = self._parse_boolean(
            os.getenv("COMPREHENSIVE_PROCESSING", "false")
        )

        # Advanced TAXII Settings
        self.TAXII_REQUEST_TIMEOUT: int = int(os.getenv("TAXII_REQUEST_TIMEOUT", "300"))
        self.TAXII_MAX_OBJECTS_PER_REQUEST: int = int(
            os.getenv("TAXII_MAX_OBJECTS_PER_REQUEST", "1000")
        )
        self.TAXII_GROUPING_SEARCH_PAGES: int = int(
            os.getenv("TAXII_GROUPING_SEARCH_PAGES", "50")
        )
        self.TAXII_CHUNK_SIZE: int = int(os.getenv("TAXII_CHUNK_SIZE", "1000"))

        # Resource Management Settings
        self.TAXII_BATCH_SIZE: int = int(
            os.getenv("TAXII_BATCH_SIZE", "10000")
        )  # Objects per batch
        self.TAXII_REST_SECONDS: int = int(
            os.getenv("TAXII_REST_SECONDS", "5")
        )  # Rest between batches

        # TAXII pagination page size. Keep this small (≤2000) for large
        # collections — the TAXII server / upstream proxy can close the
        # HTTP connection mid-walk when a single response carries too many
        # objects or when the response takes too long to render.
        self.TAXII_PAGE_SIZE: int = max(
            100, min(2000, int(os.getenv("TAXII_PAGE_SIZE", "2000")))
        )

        # Retry policy for transient TAXII connection failures (e.g.
        # RemoteDisconnected mid-walk). Increase on flaky networks; tune
        # to be small enough that the scheduler cycle still completes in
        # time, but enough to ride out 1-2 transient drops.
        self.TAXII_PAGE_RETRIES: int = max(
            0, min(10, int(os.getenv("TAXII_PAGE_RETRIES", "3")))
        )
        self.TAXII_PAGE_RETRY_BACKOFF_SECONDS: float = max(
            0.5, float(os.getenv("TAXII_PAGE_RETRY_BACKOFF_SECONDS", "2.0"))
        )

        # Redis Configuration for Event Tracking
        self.REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
        self.REDIS_DB: int = int(
            os.getenv("REDIS_DB", "1")
        )  # Use DB 1 for taxii2misp to avoid conflicts
        self.REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", None)
        self.REDIS_ENABLE_TRACKING: bool = self._parse_boolean(
            os.getenv("REDIS_ENABLE_TRACKING", "true")
        )
        self.REDIS_TTL_SECONDS: int = int(
            os.getenv("REDIS_TTL_SECONDS", "86400")
        )  # 24 hours default

        # Ensure temp directory exists
        os.makedirs(self.TEMP_DIR, exist_ok=True)

    def _get_env_variable(self, key: str, fallback_key: str = None) -> str:
        """
        Helper method to retrieve environment variables with a fallback key.
        Raises an error if the variable is not set in either key.
        """
        value = os.getenv(key)
        if value is None and fallback_key:
            value = os.getenv(fallback_key)

        if value is None:
            raise ValueError(f"Environment variable '{key}' is not set.")
        return value

    def _get_optional_int_env(self, key: str) -> int | None:
        """
        Helper method to retrieve optional integer environment variables.
        Returns None if the variable is explicitly set to 'None' or not set.
        """
        value = os.getenv(key)
        if value:
            # Handle explicit "None" string value
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
        """
        Helper method to parse boolean values from environment variables.
        """
        if isinstance(value, bool):
            return value
        return str(value).lower() in ("true", "1", "t", "y", "yes")
