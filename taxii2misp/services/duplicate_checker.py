#!/usr/bin/env python3
"""
Duplicate Event Checker Service

This service provides comprehensive duplicate detection for MISP events by checking:
1. Redis cache for previously processed events
2. MISP database for existing events
3. STIX grouping hashes to detect content changes

This prevents duplicate events from being created when the same STIX data is processed multiple times.
"""

import logging
import hashlib
from typing import Optional, Dict, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class DuplicateChecker:
    """
    Service for checking and preventing duplicate MISP events.
    Combines Redis caching and MISP database searches for comprehensive duplicate detection.
    """
    
    def __init__(self, redis_client, misp_client):
        """
        Initialize the duplicate checker.
        
        Args:
            redis_client: MISPEventRedisClient instance
            misp_client: MISPClient instance
        """
        self.redis_client = redis_client
        self.misp_client = misp_client
        
    def check_event_exists(self, event_info: str, event_uuid: Optional[str] = None, 
                          grouping_id: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Comprehensive check to see if an event already exists.
        
        Args:
            event_info: MISP event info/name to check
            event_uuid: MISP event UUID to check (optional)
            grouping_id: STIX grouping ID (optional)
            
        Returns:
            Tuple of (exists: bool, existing_event_data: Dict or None)
        """
        try:
            logger.debug(f"Checking for duplicate event: '{event_info}' (UUID: {event_uuid})")
            
            # Step 1: Check Redis cache first (fastest)
            cache_result = self._check_redis_cache(event_info, event_uuid)
            if cache_result[0]:
                logger.info(f"Found duplicate in Redis cache: '{event_info}'")
                return cache_result
            
            # Step 2: Check if grouping was already processed
            if grouping_id and self.redis_client.is_grouping_processed(grouping_id):
                grouping_info = self.redis_client.get_misp_event_info(grouping_id)
                if grouping_info:
                    logger.info(f"Grouping already processed: {grouping_id} -> Event {grouping_info.get('misp_event_id')}")
                    return True, grouping_info
            
            # Step 3: Search MISP database for existing events
            misp_result = self._check_misp_database(event_info, event_uuid)
            if misp_result[0]:
                logger.info(f"Found duplicate in MISP database: '{event_info}'")
                # Cache the result for future checks
                if misp_result[1]:
                    self._cache_existing_event(misp_result[1])
                return misp_result
            
            logger.debug(f"No duplicate found for event: '{event_info}'")
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking for duplicate event: {e}")
            # On error, assume no duplicate to avoid blocking legitimate events
            return False, None
    
    def _check_redis_cache(self, event_info: str, event_uuid: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check Redis cache for duplicate events.
        
        Args:
            event_info: Event info to check
            event_uuid: Event UUID to check (optional)
            
        Returns:
            Tuple of (found: bool, event_data: Dict or None)
        """
        try:
            # Check by event info
            if self.redis_client.is_event_duplicate(event_info, event_uuid):
                cached_data = self.redis_client.get_cached_event_by_info(event_info)
                if not cached_data and event_uuid:
                    cached_data = self.redis_client.get_cached_event_by_uuid(event_uuid)
                
                return True, cached_data
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking Redis cache: {e}")
            return False, None
    
    def _check_misp_database(self, event_info: str, event_uuid: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check MISP database for existing events.
        
        Args:
            event_info: Event info to check
            event_uuid: Event UUID to check (optional)
            
        Returns:
            Tuple of (found: bool, event_data: Dict or None)
        """
        try:
            # Check by event info first
            existing_event = self.misp_client.check_event_exists_by_info(event_info)
            if existing_event:
                return True, existing_event
            
            # Check by UUID if provided
            if event_uuid:
                existing_event = self.misp_client.search_events_by_uuid(event_uuid)
                if existing_event:
                    return True, existing_event
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking MISP database: {e}")
            return False, None
    
    def _cache_existing_event(self, event_data: Dict):
        """
        Cache an existing event found in MISP to speed up future checks.
        
        Args:
            event_data: Event data from MISP
        """
        try:
            event_info = event_data.get('info', '')
            event_uuid = event_data.get('uuid', '')
            event_id = event_data.get('id', '')
            
            if event_info and event_uuid:
                self.redis_client.cache_misp_event(event_info, event_uuid, event_id)
                
        except Exception as e:
            logger.error(f"Error caching existing event: {e}")
    
    def mark_event_created(self, event_info: str, event_uuid: str, misp_event_id: str, 
                          grouping_id: Optional[str] = None, grouping_obj: Optional[Dict] = None) -> bool:
        """
        Mark an event as created to prevent future duplicates.
        
        Args:
            event_info: MISP event info/name
            event_uuid: MISP event UUID
            misp_event_id: MISP event ID
            grouping_id: STIX grouping ID (optional)
            grouping_obj: STIX grouping object (optional)
            
        Returns:
            True if successfully marked, False otherwise
        """
        try:
            success = True
            
            # Cache the event in Redis
            if not self.redis_client.cache_misp_event(event_info, event_uuid, misp_event_id):
                logger.warning("Failed to cache MISP event in Redis")
                success = False
            
            # Mark grouping as processed if provided
            if grouping_id and grouping_obj:
                if not self.redis_client.mark_grouping_as_processed(
                    grouping_id, grouping_obj, misp_event_id, event_uuid
                ):
                    logger.warning("Failed to mark grouping as processed")
                    success = False
            
            logger.info(f"Marked event as created: '{event_info}' (ID: {misp_event_id}, UUID: {event_uuid})")
            return success
            
        except Exception as e:
            logger.error(f"Error marking event as created: {e}")
            return False
    
    def generate_event_uuid(self, grouping_obj: Dict) -> str:
        """
        Generate a deterministic UUID for an event based on STIX grouping content.
        This ensures consistent UUIDs for the same content.
        
        Args:
            grouping_obj: STIX grouping object
            
        Returns:
            Generated UUID string
        """
        try:
            # Create deterministic content for UUID generation
            content_data = {
                'grouping_id': grouping_obj.get('id', ''),
                'name': grouping_obj.get('name', ''),
                'context': grouping_obj.get('context', ''),
                'object_refs_count': len(grouping_obj.get('object_refs', [])),
                'created': grouping_obj.get('created', ''),
            }
            
            # Create stable hash
            content_str = str(sorted(content_data.items()))
            content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
            
            # Generate UUID5 from namespace and hash
            import uuid
            namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # DNS namespace
            event_uuid = str(uuid.uuid5(namespace, content_hash))
            
            logger.debug(f"Generated deterministic UUID for grouping {grouping_obj.get('id')}: {event_uuid}")
            return event_uuid
            
        except Exception as e:
            logger.error(f"Error generating event UUID: {e}")
            # Fallback to random UUID
            import uuid
            return str(uuid.uuid4())
    
    def get_duplicate_statistics(self) -> Dict:
        """
        Get statistics about duplicate detection and caching.
        
        Returns:
            Dictionary with duplicate detection statistics
        """
        try:
            stats = {
                'redis_connected': self.redis_client.is_connected(),
                'cached_events_count': 0,
                'processed_groupings_count': 0,
                'last_check_time': datetime.utcnow().isoformat()
            }
            
            if self.redis_client.is_connected():
                # Count cached events
                cached_info = self.redis_client._redis_client.scard('taxii2misp:event_info_set')
                cached_uuids = self.redis_client._redis_client.scard('taxii2misp:event_uuid_set')
                processed_groupings = self.redis_client._redis_client.scard('taxii2misp:processed_groupings')
                
                stats.update({
                    'cached_events_by_info': cached_info,
                    'cached_events_by_uuid': cached_uuids,
                    'processed_groupings_count': processed_groupings
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting duplicate statistics: {e}")
            return {'error': str(e)}
