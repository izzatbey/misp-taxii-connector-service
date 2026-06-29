#!/usr/bin/env python3
"""
Enhanced duplicate checker service that improves event name matching and provides
better duplicate detection capabilities.
"""

import os
import sys
import json
import hashlib
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

# Add project root to Python path
sys.path.insert(0, '/home/admin/misp-taxii-connector/taxii2misp')

from clients.redis_client import MISPEventRedisClient
from clients.misp_client import MISPClient

logger = logging.getLogger(__name__)

class EnhancedDuplicateChecker:
    """
    Enhanced duplicate checker with improved event name matching and comprehensive
    duplicate detection capabilities.
    """
    
    def __init__(self, redis_client: MISPEventRedisClient, misp_client: MISPClient):
        """
        Initialize the enhanced duplicate checker.
        
        Args:
            redis_client: Redis client for caching
            misp_client: MISP client for database queries
        """
        self.redis_client = redis_client
        self.misp_client = misp_client
        
        # Enhanced normalization patterns
        self.normalization_patterns = [
            (r'-', ' '),  # Replace hyphens with spaces
            (r'\s+', ' '),  # Multiple spaces to single space
            (r'[^\w\s]', ''),  # Remove special characters except spaces
            (r'^\s+|\s+$', ''),  # Trim leading/trailing spaces
        ]
    
    def normalize_event_name(self, event_name: str) -> str:
        """
        Enhanced normalization of event names for better matching.
        
        Args:
            event_name: Original event name
            
        Returns:
            Normalized event name
        """
        if not event_name:
            return ""
        
        # Start with lowercase
        normalized = event_name.lower()
        
        # Apply normalization patterns
        for pattern, replacement in self.normalization_patterns:
            normalized = re.sub(pattern, replacement, normalized)
        
        return normalized.strip()
    
    def generate_event_fingerprint(self, event_info: str, additional_data: Optional[Dict] = None) -> str:
        """
        Generate a unique fingerprint for an event based on its content.
        
        Args:
            event_info: Event info/name
            additional_data: Additional data to include in fingerprint
            
        Returns:
            Event fingerprint hash
        """
        # Start with normalized event name
        fingerprint_data = {
            'normalized_info': self.normalize_event_name(event_info),
            'original_info': event_info.strip()
        }
        
        # Add additional data if provided
        if additional_data:
            fingerprint_data.update(additional_data)
        
        # Create deterministic hash
        fingerprint_str = json.dumps(fingerprint_data, sort_keys=True)
        return hashlib.sha256(fingerprint_str.encode('utf-8')).hexdigest()[:16]
    
    def simple_misp_duplicate_check(self, event_info: str) -> Tuple[bool, Optional[Dict]]:
        """
        Simple but effective MISP duplicate check using direct database query.
        This is used as a primary check before creating events.
        
        Args:
            event_info: Event info string to check
            
        Returns:
            Tuple of (is_duplicate: bool, event_data: Dict or None)
        """
        try:
            logger.info(f"🔍 SIMPLE DUPLICATE CHECK: Searching MISP for '{event_info}'")
            
            # Clean and normalize the input
            clean_info = event_info.strip()
            
            # Use PyMISP search_index to find events by info
            search_results = self.misp_client.misp.search_index(
                eventinfo=clean_info,
                limit=20,
                pythonify=True
            )
            
            if not search_results:
                logger.info(f"✅ No duplicates found for '{event_info}'")
                return False, None
            
            logger.info(f"Found {len(search_results)} potential matches, checking for exact matches...")
            
            # Check each result for exact match
            for result in search_results:
                try:
                    # Handle different response formats
                    if isinstance(result, dict):
                        # Could be direct event dict or wrapped in 'Event' key
                        event_data = result.get('Event', result)
                    else:
                        # Try to get attributes if it's an object
                        if hasattr(result, 'info'):
                            event_data = {
                                'id': getattr(result, 'id', None),
                                'uuid': getattr(result, 'uuid', None),
                                'info': getattr(result, 'info', ''),
                            }
                        else:
                            continue
                    
                    if not isinstance(event_data, dict):
                        continue
                    
                    existing_info = event_data.get('info', '').strip()
                    
                    # Exact case-insensitive match
                    if existing_info.lower() == clean_info.lower():
                        logger.info(f"🚫 DUPLICATE FOUND: '{existing_info}' matches '{clean_info}'")
                        return True, {
                            'id': event_data.get('id'),
                            'uuid': event_data.get('uuid'),
                            'info': existing_info,
                            'source': 'misp_direct_check'
                        }
                
                except Exception as e:
                    logger.warning(f"Error processing search result: {e}")
                    continue
            
            logger.info(f"✅ No exact matches found for '{event_info}'")
            return False, None
            
        except Exception as e:
            logger.error(f"Error in simple MISP duplicate check: {e}")
            # Return False on error to avoid blocking legitimate events
            return False, None

    def check_event_exists(self, event_info: str, event_uuid: Optional[str] = None, 
                          grouping_id: Optional[str] = None, 
                          additional_checks: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Enhanced check for event existence with multiple detection methods.
        
        Args:
            event_info: Event info to check
            event_uuid: Event UUID to check (optional)
            grouping_id: STIX grouping ID (optional)
            additional_checks: Additional data for fingerprinting
            
        Returns:
            Tuple of (found: bool, event_data: Dict or None)
        """
        try:
            # Method 1: Check Redis cache (existing method)
            cache_found, cache_data = self._check_redis_cache(event_info, event_uuid)
            if cache_found:
                logger.info(f"Duplicate found in Redis cache: {event_info}")
                return True, cache_data
            
            # Method 2: Enhanced Redis name checking
            enhanced_found, enhanced_data = self._check_enhanced_redis_names(event_info)
            if enhanced_found:
                logger.info(f"Duplicate found via enhanced Redis checking: {event_info}")
                return True, enhanced_data
            
            # Method 3: Check MISP database (existing method)
            misp_found, misp_data = self._check_misp_database(event_info, event_uuid)
            if misp_found:
                logger.info(f"Duplicate found in MISP database: {event_info}")
                # Cache the found event for future checks
                if misp_data and 'uuid' in misp_data:
                    self.redis_client.cache_misp_event(
                        event_info, misp_data['uuid'], misp_data.get('id')
                    )
                return True, misp_data
            
            # Method 4: Fingerprint-based checking
            fingerprint_found, fingerprint_data = self._check_event_fingerprint(
                event_info, additional_checks
            )
            if fingerprint_found:
                logger.info(f"Duplicate found via fingerprint matching: {event_info}")
                return True, fingerprint_data
            
            # Method 5: Grouping-based checking
            if grouping_id:
                grouping_found, grouping_data = self._check_grouping_processed(grouping_id)
                if grouping_found:
                    logger.info(f"Duplicate found via grouping check: {grouping_id}")
                    return True, grouping_data
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking for event duplicates: {e}")
            # On error, assume no duplicate to avoid blocking legitimate events
            return False, None
    
    def _check_redis_cache(self, event_info: str, event_uuid: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check Redis cache for duplicate events (existing method).
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
    
    def _check_enhanced_redis_names(self, event_info: str) -> Tuple[bool, Optional[Dict]]:
        """
        Enhanced Redis checking with better name normalization.
        """
        try:
            # Get normalized version
            normalized_info = self.normalize_event_name(event_info)
            
            # Check all cached event names for similar matches
            info_set_members = self.redis_client._redis_client.smembers("taxii2misp:event_info_set")
            
            for member in info_set_members:
                try:
                    cached_name = member.decode('utf-8')
                    if self.normalize_event_name(cached_name) == normalized_info:
                        # Found a match, get the cached data
                        cached_data = self.redis_client.get_cached_event_by_info(cached_name)
                        if cached_data:
                            logger.info(f"Enhanced match: '{event_info}' matches cached '{cached_name}'")
                            return True, cached_data
                except:
                    continue
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error in enhanced Redis name checking: {e}")
            return False, None
    
    def _check_misp_database(self, event_info: str, event_uuid: Optional[str] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check MISP database for existing events with comprehensive matching.
        """
        try:
            # First, do a direct exact search by event info
            logger.info(f"Checking MISP database for exact match: '{event_info}'")
            events = self.misp_client.misp.search_index(
                eventinfo=event_info.strip(),
                limit=10,  # Increased limit to catch more potential matches
                pythonify=True
            )
            
            if events:
                logger.info(f"Found {len(events)} potential matches in MISP")
                for event in events:
                    try:
                        # Handle different types of event responses
                        if isinstance(event, str):
                            continue
                        
                        if not isinstance(event, dict):
                            # Try to convert to dict if it has attributes
                            if hasattr(event, '__dict__'):
                                event = event.__dict__
                            else:
                                continue
                        
                        # Handle nested Event structure
                        event_obj = event.get('Event', event) if 'Event' in event else event
                        
                        if not isinstance(event_obj, dict):
                            continue
                        
                        event_data = {
                            'id': event_obj.get('id'),
                            'uuid': event_obj.get('uuid'),
                            'info': event_obj.get('info'),
                            'source': 'misp_database'
                        }
                        
                        # Check for exact match (case-insensitive)
                        cached_info = event_obj.get('info', '').strip()
                        input_info = event_info.strip()
                        
                        logger.info(f"  Comparing: '{input_info}' vs '{cached_info}'")
                        
                        if cached_info.lower() == input_info.lower():
                            logger.info(f"✓ EXACT MATCH found: '{cached_info}'")
                            return True, event_data
                        
                        # Check for normalized match
                        if (self.normalize_event_name(cached_info) == 
                            self.normalize_event_name(input_info)):
                            logger.info(f"✓ NORMALIZED MATCH found: '{cached_info}' matches '{input_info}'")
                            return True, event_data
                            
                    except Exception as e:
                        logger.warning(f"Error processing event result: {e}")
                        continue
            
            # Second, try a broader search with normalized terms
            normalized_info = self.normalize_event_name(event_info)
            if normalized_info and normalized_info != event_info.strip():
                logger.info(f"Trying broader search with normalized info: '{normalized_info}'")
                normalized_events = self.misp_client.misp.search_index(
                    eventinfo=normalized_info,
                    limit=10,
                    pythonify=True
                )
                
                if normalized_events:
                    for event in normalized_events:
                        try:
                            if not isinstance(event, dict):
                                if hasattr(event, '__dict__'):
                                    event = event.__dict__
                                else:
                                    continue
                            
                            event_obj = event.get('Event', event) if 'Event' in event else event
                            if not isinstance(event_obj, dict):
                                continue
                                
                            cached_info = event_obj.get('info', '').strip()
                            if (self.normalize_event_name(cached_info) == 
                                self.normalize_event_name(event_info)):
                                
                                event_data = {
                                    'id': event_obj.get('id'),
                                    'uuid': event_obj.get('uuid'),
                                    'info': cached_info,
                                    'source': 'misp_database_normalized'
                                }
                                logger.info(f"✓ NORMALIZED BROADER MATCH: '{cached_info}' matches '{event_info}'")
                                return True, event_data
                                
                        except Exception as e:
                            logger.warning(f"Error processing normalized event result: {e}")
                            continue
            
            # Search by UUID if provided
            if event_uuid:
                try:
                    event = self.misp_client.misp.get_event(event_uuid)
                    if event and isinstance(event, dict) and 'Event' in event:
                        event_data = {
                            'id': event['Event'].get('id'),
                            'uuid': event['Event'].get('uuid'),
                            'info': event['Event'].get('info'),
                            'source': 'misp_database'
                        }
                        return True, event_data
                except:
                    pass
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking MISP database: {e}")
            return False, None
    
    def _check_event_fingerprint(self, event_info: str, additional_data: Optional[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Check for duplicates using event fingerprinting.
        """
        try:
            # Generate fingerprint for current event
            current_fingerprint = self.generate_event_fingerprint(event_info, additional_data)
            
            # Check if this fingerprint exists in Redis
            fingerprint_key = f"taxii2misp:event_fingerprint:{current_fingerprint}"
            cached_data = self.redis_client._redis_client.get(fingerprint_key)
            
            if cached_data:
                try:
                    data = json.loads(cached_data)
                    logger.info(f"Fingerprint match found for event: {event_info}")
                    return True, data
                except:
                    pass
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking event fingerprint: {e}")
            return False, None
    
    def _check_grouping_processed(self, grouping_id: str) -> Tuple[bool, Optional[Dict]]:
        """
        Check if a STIX grouping has already been processed.
        """
        try:
            is_processed = self.redis_client.is_grouping_processed(grouping_id)
            if is_processed:
                # Return basic info about the processed grouping
                grouping_data = {
                    'grouping_id': grouping_id,
                    'source': 'redis_grouping_cache',
                    'processed': True
                }
                return True, grouping_data
            return False, None
        except Exception as e:
            logger.error(f"Error checking grouping processed: {e}")
            return False, None
    
    def mark_event_created(self, event_info: str, event_uuid: str, misp_event_id: str,
                          grouping_id: Optional[str] = None, grouping_obj: Optional[Dict] = None,
                          additional_data: Optional[Dict] = None) -> bool:
        """
        Enhanced marking of event creation with fingerprinting.
        """
        try:
            success = True
            
            # Standard caching (existing method)
            if not self.redis_client.cache_misp_event(event_info, event_uuid, misp_event_id):
                logger.warning("Failed to cache MISP event in Redis")
                success = False
            
            # Cache event fingerprint
            try:
                fingerprint = self.generate_event_fingerprint(event_info, additional_data)
                fingerprint_key = f"taxii2misp:event_fingerprint:{fingerprint}"
                fingerprint_data = {
                    'event_info': event_info,
                    'event_uuid': event_uuid,
                    'misp_event_id': misp_event_id,
                    'fingerprint': fingerprint,
                    'created_at': datetime.utcnow().isoformat()
                }
                
                # Store fingerprint with TTL
                self.redis_client._redis_client.setex(
                    fingerprint_key, 
                    self.redis_client.ttl_seconds,
                    json.dumps(fingerprint_data)
                )
                
            except Exception as e:
                logger.warning(f"Failed to cache event fingerprint: {e}")
                success = False
            
            # Mark grouping as processed if provided
            if grouping_id and grouping_obj:
                if not self.redis_client.mark_grouping_as_processed(
                    grouping_id, grouping_obj, misp_event_id, event_uuid
                ):
                    logger.warning("Failed to mark grouping as processed")
                    success = False
            
            return success
            
        except Exception as e:
            logger.error(f"Error marking event as created: {e}")
            return False
    
    def populate_cache_from_misp(self, days_back: int = 7, max_events: int = 1000) -> Dict[str, int]:
        """
        Populate Redis cache with recent MISP events to prevent duplicates.
        
        Args:
            days_back: Number of days to go back
            max_events: Maximum number of events to cache
            
        Returns:
            Statistics about the population process
        """
        stats = {
            'events_processed': 0,
            'events_cached': 0,
            'errors': 0
        }
        
        try:
            # Calculate date range
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"Populating cache with MISP events from {start_date} to {end_date}")
            
            # Search for recent events
            events = self.misp_client.misp.search_index(
                date_from=start_date.strftime('%Y-%m-%d'),
                date_to=end_date.strftime('%Y-%m-%d'),
                limit=max_events
            )
            
            if not events:
                logger.warning("No events found in MISP for the specified date range")
                return stats
            
            logger.info(f"Found {len(events)} events to process")
            
            for event in events:
                try:
                    stats['events_processed'] += 1
                    
                    # Handle different types of event responses
                    if isinstance(event, str):
                        stats['errors'] += 1
                        continue
                    
                    if not isinstance(event, dict):
                        # Try to convert to dict if it has attributes
                        if hasattr(event, '__dict__'):
                            event = event.__dict__
                        else:
                            stats['errors'] += 1
                            continue
                    
                    # Handle nested Event structure
                    event_obj = event.get('Event', event) if 'Event' in event else event
                    
                    if not isinstance(event_obj, dict):
                        stats['errors'] += 1
                        continue
                    
                    event_info = event_obj.get('info', '')
                    event_uuid = event_obj.get('uuid', '')
                    event_id = event_obj.get('id', '')
                    
                    if event_info and event_uuid:
                        # Cache the event
                        if self.redis_client.cache_misp_event(event_info, event_uuid, str(event_id)):
                            stats['events_cached'] += 1
                            
                            # Also cache fingerprint
                            try:
                                fingerprint = self.generate_event_fingerprint(event_info)
                                fingerprint_key = f"taxii2misp:event_fingerprint:{fingerprint}"
                                fingerprint_data = {
                                    'event_info': event_info,
                                    'event_uuid': event_uuid,
                                    'misp_event_id': str(event_id),
                                    'fingerprint': fingerprint,
                                    'cached_from_misp': True,
                                    'created_at': datetime.utcnow().isoformat()
                                }
                                
                                self.redis_client._redis_client.setex(
                                    fingerprint_key,
                                    self.redis_client.ttl_seconds,
                                    json.dumps(fingerprint_data)
                                )
                                
                            except Exception as e:
                                logger.warning(f"Failed to cache fingerprint for event {event_id}: {e}")
                        
                        if stats['events_processed'] % 100 == 0:
                            logger.info(f"Processed {stats['events_processed']} events...")
                    
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"Error processing event {event}: {e}")
            
            logger.info(f"Cache population complete: {stats}")
            
        except Exception as e:
            logger.error(f"Error populating cache from MISP: {e}")
            stats['errors'] += 1
        
        return stats
    
    def get_duplicate_analysis(self) -> Dict[str, Any]:
        """
        Analyze current duplicate detection state and provide recommendations.
        """
        analysis = {
            'redis_connected': False,
            'cache_stats': {},
            'sample_events': [],
            'recommendations': []
        }
        
        try:
            # Check Redis connection
            analysis['redis_connected'] = self.redis_client.is_connected()
            
            if analysis['redis_connected']:
                # Get cache statistics
                try:
                    stats = self.redis_client.get_processing_stats()
                    analysis['cache_stats'] = stats or {}
                    
                    # Get sample cached events
                    info_set_members = self.redis_client._redis_client.smembers("taxii2misp:event_info_set")
                    analysis['sample_events'] = [
                        member.decode('utf-8') for member in list(info_set_members)[:10]
                    ]
                    
                    # Generate recommendations
                    if len(info_set_members) == 0:
                        analysis['recommendations'].append(
                            "Redis cache is empty. Run cache population from MISP."
                        )
                    elif len(info_set_members) < 100:
                        analysis['recommendations'].append(
                            "Redis cache has few entries. Consider running cache population."
                        )
                    else:
                        analysis['recommendations'].append(
                            "Redis cache looks healthy with sufficient entries."
                        )
                
                except Exception as e:
                    analysis['cache_stats'] = {'error': str(e)}
                    analysis['recommendations'].append(f"Error accessing cache: {e}")
            else:
                analysis['recommendations'].append("Redis is not connected. Check Redis configuration.")
        
        except Exception as e:
            analysis['recommendations'].append(f"Error analyzing duplicate detection: {e}")
        
        return analysis
