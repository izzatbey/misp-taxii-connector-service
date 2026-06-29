import redis
import logging

logger = logging.getLogger(__name__)

class RedisClient:
    """
    A universal client for connecting to Redis.
    Provides a centralized way to get a Redis connection.
    """
    _instance = None # Singleton instance
    _redis_connection = None

    def __new__(cls, *args, **kwargs):
        """Ensures only one instance of RedisClient is created (Singleton pattern)."""
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, db: int, password: str | None = None):
        """
        Initializes the Redis client connection.
        This constructor will only run once due to the __new__ method.
        """
        if not hasattr(self, '_initialized'): # Prevent re-initialization on subsequent __init__ calls
            self.host = host
            self.port = port
            self.db = db
            self.password = password
            self._initialized = True
            logger.info("RedisClient: Initializing connection parameters (host=%s, port=%d, db=%d)", host, port, db)
            # Establish connection immediately or on first 'get_client' call
            self._connect()

    def _connect(self):
        """Establishes the actual Redis connection."""
        try:
            self._redis_connection = redis.StrictRedis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True # Important for getting strings back from Redis
            )
            self._redis_connection.ping()
            logger.info("RedisClient: Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            self._redis_connection = None
            logger.critical(f"RedisClient: Failed to connect to Redis: {e}. All Redis operations will fail.", exc_info=True)
        except Exception as e:
            self._redis_connection = None
            logger.critical(f"RedisClient: An unexpected error occurred during Redis connection: {e}", exc_info=True)

    def get_client(self):
        """
        Returns the connected Redis client instance.
        If connection failed, returns None.
        """
        if self._redis_connection is None:
            # Attempt to reconnect if previously failed, or if it was never connected
            logger.warning("RedisClient: Attempting to reconnect to Redis.")
            self._connect() # Try connecting again
        return self._redis_connection