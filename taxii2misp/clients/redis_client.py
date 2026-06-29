#!/usr/bin/env python3
"""
Redis Client for MISP Event Tracking

This module provides Redis-based tracking for MISP events to prevent duplicate processing
of STIX grouping objects that have already been converted and pushed to MISP.
"""

import logging
import redis
import json
import hashlib
from typing import Set, Dict, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Redis key patterns for MISP event tracking
PROCESSED_GROUPINGS_SET = "taxii2misp:processed_groupings"
MISP_EVENT_MAPPING_PREFIX = "taxii2misp:event_mapping:"
GROUPING_HASH_PREFIX = "taxii2misp:grouping_hash:"
PROCESSING_STATS_KEY = "taxii2misp:stats"
LAST_SYNC_TIMESTAMP_KEY = "taxii2misp:last_sync"
# New keys for event duplicate prevention
MISP_EVENT_INFO_SET = "taxii2misp:event_info_set"
MISP_EVENT_UUID_SET = "taxii2misp:event_uuid_set"
EVENT_NAME_TO_UUID_PREFIX = "taxii2misp:name_to_uuid:"
EVENT_UUID_TO_INFO_PREFIX = "taxii2misp:uuid_to_info:"

class MISPEventRedisClient:
    """
    Redis client for tracking processed MISP events and preventing duplicate processing.
    Uses a singleton pattern to ensure only one Redis connection per application.
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """Ensures only one instance of MISPEventRedisClient is created (Singleton pattern)."""
        if cls._instance is None:
            cls._instance = super(MISPEventRedisClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, host: str, port: int, db: int, password: Optional[str] = None, ttl_seconds: int = 86400):
        """
        Initializes the Redis client connection for MISP event tracking.
        This constructor will only run once due to the __new__ method.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            db: Redis database number 
            password: Redis password (optional)
            ttl_seconds: Time-to-live for cached entries in seconds
        """
        if not hasattr(self, '_initialized'):
            self.host = host
            self.port = port
            self.db = db
            self.password = password
            self.ttl_seconds = ttl_seconds
            self._initialized = True
            self._redis_client = None
            
            logger.info("MISPEventRedisClient: Initializing connection parameters (host=%s, port=%d, db=%d)", 
                       host, port, db)
            self._connect()

    def _connect(self):
        """Establish connection to Redis server."""
        try:
            self._redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self._redis_client.ping()
            logger.info("MISPEventRedisClient: Successfully connected to Redis")
            
        except Exception as e:
            logger.error(f"MISPEventRedisClient: Failed to connect to Redis: {e}")
            self._redis_client = None

    def is_connected(self) -> bool:
        """Check if Redis client is connected and available."""
        if not self._redis_client:
            return False
        try:
            self._redis_client.ping()
            return True
        except Exception:
            return False

    def _generate_grouping_hash(self, grouping_obj: Dict) -> str:
        """
        Generate a stable hash for a STIX grouping object to detect changes.
        
        Args:
            grouping_obj: STIX grouping object as dictionary
            
        Returns:
            MD5 hash string representing the grouping content
        """
        try:
            # Extract key fields that matter for processing
            key_data = {
                'id': grouping_obj.get('id', ''),
                'object_refs': sorted(grouping_obj.get('object_refs', [])),
                'modified': self._serialize_datetime(grouping_obj.get('modified', '')),
                'name': grouping_obj.get('name', ''),
                'context': grouping_obj.get('context', '')
            }
            
            # Create deterministic JSON string
            json_str = self._safe_json_dumps(key_data, sort_keys=True)
            
            # Generate MD5 hash
            return hashlib.md5(json_str.encode('utf-8')).hexdigest()
            
        except Exception as e:
            logger.error(f"Error generating grouping hash: {e}")
            # Fallback to ID-based hash
            return hashlib.md5(str(grouping_obj.get('id', '')).encode('utf-8')).hexdigest()

    def _serialize_datetime(self, datetime_obj) -> str:
        """
        Serialize datetime objects (including STIXdatetime) to string.
        
        Args:
            datetime_obj: DateTime object that might be STIXdatetime or regular datetime
            
        Returns:
            String representation of the datetime
        """
        try:
            if datetime_obj is None:
                return ''
            
            # Handle string values (already serialized)
            if isinstance(datetime_obj, str):
                return datetime_obj
            
            # Handle STIXdatetime and regular datetime objects
            if hasattr(datetime_obj, 'isoformat'):
                return datetime_obj.isoformat()
            
            # Handle STIX datetime that might have different attributes
            if hasattr(datetime_obj, '__str__'):
                return str(datetime_obj)
            
            # Fallback
            return str(datetime_obj)
            
        except Exception as e:
            logger.warning(f"Error serializing datetime {datetime_obj}: {e}")
            return str(datetime_obj) if datetime_obj is not None else ''

    def _safe_json_dumps(self, obj, **kwargs) -> str:
        """
        Safely serialize objects to JSON, handling STIX datetime and other non-serializable objects.
        
        Args:
            obj: Object to serialize
            **kwargs: Additional arguments for json.dumps
            
        Returns:
            JSON string representation
        """
        def stix_serializer(obj):
            """Custom serializer for STIX objects."""
            # Handle datetime objects (including STIXdatetime)
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            
            # Handle STIX objects that have a serialize method
            if hasattr(obj, 'serialize'):
                return obj.serialize()
            
            # Handle STIX objects that have _inner attribute
            if hasattr(obj, '_inner'):
                return obj._inner
            
            # Handle objects that can be converted to dict
            if hasattr(obj, '__dict__'):
                return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
            
            # Fallback to string representation
            return str(obj)
        
        try:
            return json.dumps(obj, default=stix_serializer, **kwargs)
        except Exception as e:
            logger.error(f"Error serializing object to JSON: {e}")
            # Final fallback - convert to string
            return json.dumps(str(obj), **kwargs)

    def is_grouping_processed(self, grouping_id: str) -> bool:
        """
        Check if a STIX grouping has already been processed and pushed to MISP.
        
        Args:
            grouping_id: STIX grouping ID (e.g., "grouping--uuid")
            
        Returns:
            True if already processed, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, cannot check processed status")
            return False
            
        try:
            return self._redis_client.sismember(PROCESSED_GROUPINGS_SET, grouping_id)
            
        except Exception as e:
            logger.error(f"Error checking if grouping is processed: {e}")
            return False

    def mark_grouping_as_processed(self, grouping_id: str, grouping_obj: Dict, 
                                  misp_event_id: Optional[str] = None, 
                                  misp_event_uuid: Optional[str] = None) -> bool:
        """
        Mark a STIX grouping as processed and store associated MISP event information.
        
        Args:
            grouping_id: STIX grouping ID
            grouping_obj: Full STIX grouping object 
            misp_event_id: MISP event ID (if created)
            misp_event_uuid: MISP event UUID (if created)
            
        Returns:
            True if successfully stored, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, cannot mark as processed")
            return False
            
        try:
            # Generate content hash for change detection
            content_hash = self._generate_grouping_hash(grouping_obj)
            
            # Create mapping record
            mapping_data = {
                'grouping_id': grouping_id,
                'processed_at': datetime.utcnow().isoformat(),
                'content_hash': content_hash,
                'misp_event_id': misp_event_id,
                'misp_event_uuid': misp_event_uuid,
                'object_refs_count': len(grouping_obj.get('object_refs', [])),
                'grouping_name': grouping_obj.get('name', ''),
                'grouping_context': grouping_obj.get('context', '')
            }
            
            # Use pipeline for atomic operations
            pipe = self._redis_client.pipeline()
            
            # Add to processed set
            pipe.sadd(PROCESSED_GROUPINGS_SET, grouping_id)
            
            # Store detailed mapping
            mapping_key = f"{MISP_EVENT_MAPPING_PREFIX}{grouping_id}"
            pipe.set(mapping_key, self._safe_json_dumps(mapping_data), ex=self.ttl_seconds)
            
            # Store content hash for change detection
            hash_key = f"{GROUPING_HASH_PREFIX}{grouping_id}"
            pipe.set(hash_key, content_hash, ex=self.ttl_seconds)
            
            # Execute pipeline
            pipe.execute()
            
            logger.info(f"Marked grouping as processed: {grouping_id} -> MISP Event {misp_event_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error marking grouping as processed: {e}")
            return False

    def get_processed_groupings(self) -> Set[str]:
        """
        Get all processed grouping IDs.
        
        Returns:
            Set of processed grouping IDs
        """
        if not self.is_connected():
            return set()
            
        try:
            return self._redis_client.smembers(PROCESSED_GROUPINGS_SET)
            
        except Exception as e:
            logger.error(f"Error getting processed groupings: {e}")
            return set()

    def get_misp_event_info(self, grouping_id: str) -> Optional[Dict]:
        """
        Get MISP event information for a processed grouping.
        
        Args:
            grouping_id: STIX grouping ID
            
        Returns:
            Dictionary with MISP event info or None if not found
        """
        if not self.is_connected():
            return None
            
        try:
            mapping_key = f"{MISP_EVENT_MAPPING_PREFIX}{grouping_id}"
            data = self._redis_client.get(mapping_key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting MISP event info: {e}")
            return None

    def has_grouping_changed(self, grouping_id: str, grouping_obj: Dict) -> bool:
        """
        Check if a grouping has changed since last processing.
        
        Args:
            grouping_id: STIX grouping ID
            grouping_obj: Current STIX grouping object
            
        Returns:
            True if changed or no previous hash found, False if unchanged
        """
        if not self.is_connected():
            return True  # Assume changed if can't check
            
        try:
            current_hash = self._generate_grouping_hash(grouping_obj)
            hash_key = f"{GROUPING_HASH_PREFIX}{grouping_id}"
            stored_hash = self._redis_client.get(hash_key)
            
            if not stored_hash:
                return True  # No previous hash, consider as changed
                
            return current_hash != stored_hash
            
        except Exception as e:
            logger.error(f"Error checking if grouping changed: {e}")
            return True  # Assume changed on error

    def filter_unprocessed_groupings(self, grouping_ids: List[str]) -> List[str]:
        """
        Filter out groupings that have already been processed.
        
        Args:
            grouping_ids: List of STIX grouping IDs
            
        Returns:
            List of unprocessed grouping IDs
        """
        if not self.is_connected():
            logger.warning("Redis not connected, returning all groupings as unprocessed")
            return grouping_ids
            
        try:
            processed_set = self.get_processed_groupings()
            unprocessed = [gid for gid in grouping_ids if gid not in processed_set]
            
            logger.info(f"Filtered groupings: {len(grouping_ids)} total, "
                       f"{len(processed_set)} already processed, "
                       f"{len(unprocessed)} remaining to process")
            
            return unprocessed
            
        except Exception as e:
            logger.error(f"Error filtering unprocessed groupings: {e}")
            return grouping_ids

    def update_processing_stats(self, processed_count: int, total_count: int):
        """
        Update processing statistics in Redis.
        
        Args:
            processed_count: Number of events processed in this run
            total_count: Total number of events available
        """
        if not self.is_connected():
            return
            
        try:
            stats = {
                'last_run_at': datetime.utcnow().isoformat(),
                'events_processed_this_run': processed_count,
                'total_events_available': total_count,
                'total_processed_groupings': len(self.get_processed_groupings())
            }
            
            self._redis_client.set(PROCESSING_STATS_KEY, json.dumps(stats), ex=self.ttl_seconds)
            
        except Exception as e:
            logger.error(f"Error updating processing stats: {e}")

    def get_processing_stats(self) -> Optional[Dict]:
        """
        Get current processing statistics.
        
        Returns:
            Dictionary with processing stats or None if not available
        """
        if not self.is_connected():
            return None
            
        try:
            data = self._redis_client.get(PROCESSING_STATS_KEY)
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return None

    def clear_processed_groupings(self) -> bool:
        """
        Clear all processed grouping tracking data (use with caution).
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            return False
            
        try:
            # Get all processed groupings first
            processed_groupings = self.get_processed_groupings()
            
            # Use pipeline for atomic operations
            pipe = self._redis_client.pipeline()
            
            # Clear the main set
            pipe.delete(PROCESSED_GROUPINGS_SET)
            
            # Clear all mapping and hash keys
            for grouping_id in processed_groupings:
                mapping_key = f"{MISP_EVENT_MAPPING_PREFIX}{grouping_id}"
                hash_key = f"{GROUPING_HASH_PREFIX}{grouping_id}"
                pipe.delete(mapping_key)
                pipe.delete(hash_key)
            
            # Clear stats
            pipe.delete(PROCESSING_STATS_KEY)
            pipe.delete(LAST_SYNC_TIMESTAMP_KEY)
            
            # Execute pipeline
            pipe.execute()
            
            logger.info(f"Cleared {len(processed_groupings)} processed grouping records")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing processed groupings: {e}")
            return False

    def get_connection_info(self) -> Dict:
        """
        Get Redis connection information and status.
        
        Returns:
            Dictionary with connection details
        """
        return {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'connected': self.is_connected(),
            'ttl_seconds': self.ttl_seconds
        }

    def is_event_duplicate(self, event_info: str, event_uuid: Optional[str] = None) -> bool:
        """
        Check if an event with the same info or UUID already exists in Redis cache.
        
        Args:
            event_info: MISP event info/name to check
            event_uuid: MISP event UUID to check (optional)
            
        Returns:
            True if duplicate found, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, cannot check for duplicates")
            return False
            
        try:
            # Normalize event info for comparison
            normalized_info = event_info.strip().lower()
            
            # Check if event info already exists
            if self._redis_client.sismember(MISP_EVENT_INFO_SET, normalized_info):
                logger.info(f"Found duplicate event in Redis cache by info: '{event_info}'")
                return True
            
            # Check UUID if provided
            if event_uuid and self._redis_client.sismember(MISP_EVENT_UUID_SET, event_uuid):
                logger.info(f"Found duplicate event in Redis cache by UUID: {event_uuid}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for event duplicates: {e}")
            return False

    def cache_misp_event(self, event_info: str, event_uuid: str, misp_event_id: Optional[str] = None) -> bool:
        """
        Cache MISP event information to prevent future duplicates.
        
        Args:
            event_info: MISP event info/name
            event_uuid: MISP event UUID  
            misp_event_id: MISP event ID (optional)
            
        Returns:
            True if successfully cached, False otherwise
        """
        if not self.is_connected():
            logger.warning("Redis not connected, cannot cache event")
            return False
            
        try:
            # Normalize event info
            normalized_info = event_info.strip().lower()
            
            # Use pipeline for atomic operations
            pipe = self._redis_client.pipeline()
            
            # Add to event info and UUID sets
            pipe.sadd(MISP_EVENT_INFO_SET, normalized_info)
            pipe.sadd(MISP_EVENT_UUID_SET, event_uuid)
            
            # Store bidirectional mappings
            name_to_uuid_key = f"{EVENT_NAME_TO_UUID_PREFIX}{normalized_info}"
            uuid_to_info_key = f"{EVENT_UUID_TO_INFO_PREFIX}{event_uuid}"
            
            event_data = {
                'event_info': event_info,
                'event_uuid': event_uuid,
                'misp_event_id': misp_event_id,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            pipe.set(name_to_uuid_key, json.dumps(event_data), ex=self.ttl_seconds)
            pipe.set(uuid_to_info_key, json.dumps(event_data), ex=self.ttl_seconds)
            
            # Execute pipeline
            pipe.execute()
            
            logger.info(f"Cached MISP event to prevent duplicates: '{event_info}' (UUID: {event_uuid})")
            return True
            
        except Exception as e:
            logger.error(f"Error caching MISP event: {e}")
            return False

    def get_cached_event_by_info(self, event_info: str) -> Optional[Dict]:
        """
        Get cached event data by event info.
        
        Args:
            event_info: Event info to search for
            
        Returns:
            Cached event data dictionary or None if not found
        """
        if not self.is_connected():
            return None
            
        try:
            normalized_info = event_info.strip().lower()
            name_to_uuid_key = f"{EVENT_NAME_TO_UUID_PREFIX}{normalized_info}"
            
            data = self._redis_client.get(name_to_uuid_key)
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting cached event by info: {e}")
            return None

    def get_cached_event_by_uuid(self, event_uuid: str) -> Optional[Dict]:
        """
        Get cached event data by UUID.
        
        Args:
            event_uuid: Event UUID to search for
            
        Returns:
            Cached event data dictionary or None if not found
        """
        if not self.is_connected():
            return None
            
        try:
            uuid_to_info_key = f"{EVENT_UUID_TO_INFO_PREFIX}{event_uuid}"
            
            data = self._redis_client.get(uuid_to_info_key)
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting cached event by UUID: {e}")
            return None

    def clear_event_cache(self) -> bool:
        """
        Clear all cached event data (use with caution).
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            return False
            
        try:
            # Get all cached event info and UUIDs
            cached_info_set = self._redis_client.smembers(MISP_EVENT_INFO_SET)
            cached_uuid_set = self._redis_client.smembers(MISP_EVENT_UUID_SET)
            
            # Use pipeline for atomic operations
            pipe = self._redis_client.pipeline()
            
            # Clear main sets
            pipe.delete(MISP_EVENT_INFO_SET)
            pipe.delete(MISP_EVENT_UUID_SET)
            
            # Clear all mapping keys
            for info in cached_info_set:
                name_to_uuid_key = f"{EVENT_NAME_TO_UUID_PREFIX}{info}"
                pipe.delete(name_to_uuid_key)
            
            for uuid in cached_uuid_set:
                uuid_to_info_key = f"{EVENT_UUID_TO_INFO_PREFIX}{uuid}"
                pipe.delete(uuid_to_info_key)
            
            # Execute pipeline
            pipe.execute()
            
            logger.info(f"Cleared {len(cached_info_set)} cached event info records and {len(cached_uuid_set)} UUID records")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing event cache: {e}")
            return False

    def get_cached_data(self, key: str) -> Optional[Dict]:
        """
        Get cached data from Redis.
        
        Args:
            key: Cache key
            
        Returns:
            Cached data as dictionary or None if not found
        """
        try:
            data = self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error getting cached data for key {key}: {e}")
            return None

    def set_cached_data(self, key: str, data: Dict, ttl_seconds: int = None) -> bool:
        """
        Set cached data in Redis.
        
        Args:
            key: Cache key
            data: Data to cache
            ttl_seconds: Time to live in seconds (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            ttl = ttl_seconds or self.ttl_seconds
            result = self._redis_client.setex(key, ttl, json.dumps(data, default=str))
            return result
        except Exception as e:
            logger.error(f"Error setting cached data for key {key}: {e}")
            return False
