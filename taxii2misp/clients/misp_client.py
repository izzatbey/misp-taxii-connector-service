import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json
import requests

from pymisp import PyMISP, MISPEvent, MISPAttribute
from urllib3.exceptions import InsecureRequestWarning
import warnings

warnings.simplefilter('ignore', InsecureRequestWarning)

logger = logging.getLogger(__name__)

class MISPClient:
    def __init__(self, misp_url, misp_api_key, verify_ssl=True, distribution=None, threat_level=None, analysis=None, request_timeout=120):
        self.misp_url = misp_url
        self.misp_api_key = misp_api_key
        self.verify_ssl = verify_ssl
        self.distribution = distribution or 0
        self.threat_level = threat_level or 2
        self.analysis = analysis or 2
        self.request_timeout = request_timeout
        self.misp = None
        # Initialize MISP connection
        self._initialize_client()
    
    @classmethod
    def create_simple_client(cls, misp_url, misp_api_key, verify_ssl=True, distribution=None, threat_level=None, analysis=None, request_timeout=120):
        """
        Alternative constructor that creates a MISP client with minimal validation.
        Use this if the normal constructor fails due to version checking issues.
        """
        # Create instance without calling normal init
        instance = cls.__new__(cls)
        
        # Set basic properties
        instance.misp_url = misp_url
        instance.misp_api_key = misp_api_key
        instance.verify_ssl = verify_ssl
        instance.distribution = distribution or 0
        instance.threat_level = threat_level or 2
        instance.analysis = analysis or 2
        instance.request_timeout = request_timeout
        
        try:
            # Create PyMISP bypassing version checks
            logger.info("Creating simple MISP client bypassing version compatibility checks...")
            
            # Import PyMISP components directly
            import requests
            from pymisp.api import PyMISP
            
            # Create PyMISP object but override the problematic initialization
            instance.misp = object.__new__(PyMISP)
            
            # Set essential PyMISP attributes manually
            instance.misp.root_url = misp_url.rstrip('/')
            instance.misp.key = misp_api_key
            instance.misp.ssl = verify_ssl
            instance.misp.proxies = None
            instance.misp.cert = None
            instance.misp.auth = None
            # Set timeout
            instance.misp.timeout = instance.request_timeout
            instance.misp.global_pythonify = False
            
            # Create session with proper headers
            instance.misp._PyMISP__session = requests.Session()
            instance.misp._PyMISP__session.verify = verify_ssl
            instance.misp._PyMISP__session.headers.update({
                'Authorization': misp_api_key,
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'User-Agent': f'PyMISP 2.5.12 - Python 3.11 - Custom Client'
            })
            
            # Set up basic PyMISP internals
            instance.misp.categories = []
            instance.misp.types = []
            instance.misp.category_type_mapping = {}
            instance.misp.sane_default = {}
            instance.misp.describe_types = {}
            instance.misp._current_user = None
            instance.misp._current_role = None
            instance.misp._misp_version = (2, 5, 8)  # Set known version
            
            # Mark as fallback client
            instance.misp._is_fallback_client = True
            
            # Add required PyMISP methods manually
            def _prepare_request(method, url, data=None, **kwargs):
                """Custom request preparation for fallback client."""
                if not url.startswith('http'):
                    url = f"{instance.misp.root_url}/{url.lstrip('/')}"
                
                if data is not None:
                    kwargs['json'] = data
                
                return instance.misp._PyMISP__session.request(method, url, timeout=instance.misp.timeout, **kwargs)
            
            def _check_json_response(response):
                """Custom response checker for fallback client."""
                try:
                    if response.status_code in [200, 201]:  # Include 201 for successful creation
                        return response.json()
                    else:
                        return {'errors': [f'HTTP {response.status_code}: {response.text[:200]}']}
                except Exception as e:
                    return {'errors': [f'Response parsing error: {e}']}
            
            instance.misp._prepare_request = _prepare_request
            instance.misp._check_json_response = _check_json_response
            
            logger.info("Simple MISP client created successfully (bypassing version checks)")
            return instance
            
        except Exception as e:
            logger.error(f"Simple MISP client creation failed: {e}")
            raise

    def _initialize_client(self):
        """Initialize the PyMISP client following the api.py pattern."""
        try:
            logger.info(f"Initializing MISP client for: {self.misp_url}")
            
            # Primary method: Use standard PyMISP initialization
            # This follows the pattern from api.py __init__ method
            self.misp = PyMISP(
                url=self.misp_url,
                key=self.misp_api_key,
                ssl=self.verify_ssl,
                debug=False,
                timeout=self.request_timeout
            )
            
            # Test connectivity by getting version info (like api.py does)
            try:
                version_info = self.misp.misp_instance_version
                if 'version' in version_info:
                    logger.info(f"Connected to MISP (version {version_info['version']}) at {self.misp_url}")
                else:
                    logger.info(f"Connected to MISP (version unknown) at {self.misp_url}")
            except Exception as version_error:
                logger.warning(f"Version check failed but connection may still work: {version_error}")
            
            return

        except Exception as e:
            logger.warning(f"Standard PyMISP initialization failed: {e}")
            
            # Fallback: Try with minimal error handling
            try:
                logger.info("Attempting fallback initialization...")
                
                # Create PyMISP with basic parameters only
                self.misp = PyMISP(
                    url=self.misp_url,
                    key=self.misp_api_key,
                    ssl=self.verify_ssl,
                    timeout=self.request_timeout
                )
                
                # Mark as fallback client for different handling
                self.misp._is_fallback_client = True
                logger.warning("Fallback MISP client initialized - some features may be limited")
                
            except Exception as fallback_error:
                logger.error(f"All MISP client initialization methods failed: {fallback_error}")
                raise RuntimeError(f"Could not initialize MISP client: {fallback_error}")

    def add_event(self, event):
        """Add a MISPEvent to MISP following PyMISP api.py pattern."""
        try:
            if not isinstance(event, MISPEvent):
                logger.error("Event must be a MISPEvent instance")
                return None
            
            logger.info(f"Adding event to MISP: {event.info}")
            
            # Handle fallback client with custom implementation
            if hasattr(self.misp, '_is_fallback_client') and self.misp._is_fallback_client:
                return self._add_event_fallback(event)
            
            # Use PyMISP add_event method as shown in api.py
            # def add_event(self, event: MISPEvent, pythonify: bool = False, metadata: bool = False)
            response = self.misp.add_event(event, pythonify=False)
            
            # Check for errors following PyMISP pattern
            if isinstance(response, dict) and 'errors' in response:
                logger.error(f"MISP returned errors: {response['errors']}")
                return None
            
            return response
            
        except Exception as e:
            logger.error(f"Error adding event to MISP: {e}")
            return None

    def _add_event_fallback(self, event):
        """Add event using custom method for fallback clients."""
        try:
            # Convert MISPEvent to JSON properly
            # Use PyMISP's built-in JSON serialization
            if hasattr(event, 'to_json'):
                # Use the built-in JSON serialization method - this works correctly
                event_json_str = event.to_json()
                event_data = json.loads(event_json_str)
                logger.debug(f"Using to_json() for event serialization")
            elif hasattr(event, 'to_dict'):
                # Fallback: convert to_dict and manually serialize MISPAttribute objects
                event_dict = event.to_dict()
                event_data = self._serialize_misp_objects(event_dict)
                logger.debug(f"Using to_dict() with custom serialization for event")
            else:
                # Last resort: try to convert to dict manually
                logger.debug(f"Using manual conversion for event")
                event_data = {
                    'info': getattr(event, 'info', 'Unknown Event'),
                    'distribution': getattr(event, 'distribution', self.distribution),
                    'threat_level_id': getattr(event, 'threat_level_id', self.threat_level),
                    'analysis': getattr(event, 'analysis', self.analysis),
                    'Attribute': []
                }
                
                # Add attributes if they exist
                if hasattr(event, 'attributes') and event.attributes:
                    for attr in event.attributes:
                        if hasattr(attr, 'to_dict'):
                            event_data['Attribute'].append(attr.to_dict())
                        else:
                            # Manual attribute conversion
                            attr_dict = {
                                'type': getattr(attr, 'type', 'text'),
                                'value': getattr(attr, 'value', ''),
                                'category': getattr(attr, 'category', 'Other'),
                                'distribution': getattr(attr, 'distribution', self.distribution)
                            }
                            event_data['Attribute'].append(attr_dict)
            
            # Make request using custom method
            response = self.misp._prepare_request('POST', 'events/add', data=event_data)
            response_data = self.misp._check_json_response(response)
            
            if isinstance(response_data, dict) and 'errors' not in response_data:
                logger.info(f"Event added successfully via fallback method")
                return response_data
            else:
                logger.error(f"Failed to add event via fallback method: {response_data}")
                return None
                
        except Exception as e:
            logger.error(f"Error in fallback add_event: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _serialize_misp_objects(self, obj):
        """Recursively serialize MISP objects to JSON-compatible format."""
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif isinstance(obj, dict):
            return {k: self._serialize_misp_objects(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_misp_objects(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            # Handle objects with attributes
            return {k: self._serialize_misp_objects(v) for k, v in obj.__dict__.items() 
                   if not k.startswith('_')}
        else:
            return obj

    def add_event_with_deduplication(self, event, similarity_threshold=0.7):
        """
        Add event with comprehensive duplicate detection and optional updating.
        
        Args:
            event: MISPEvent to add
            similarity_threshold: Threshold for attribute similarity (0.0-1.0)
        
        Returns:
            MISP response or None if failed
        """
        try:
            if not isinstance(event, MISPEvent):
                logger.error("Event must be a MISPEvent instance")
                return None
            
            event_info = getattr(event, 'info', 'Unknown Event')
            logger.info(f"Adding event with deduplication check: {event_info}")
            
            # Check for duplicates
            is_duplicate, existing_event = self._is_duplicate_event(event, similarity_threshold)
            
            if is_duplicate:
                existing_id = existing_event.get('Event', {}).get('id', 'Unknown') if existing_event else 'Unknown'
                logger.warning(f"🔄 DUPLICATE DETECTED: Event '{event_info}' is similar to existing event ID {existing_id} - SKIPPING")
                
                # Return existing event to indicate successful processing but no new creation
                return existing_event
            
            # Not a duplicate, add new event
            logger.info(f"✅ No duplicate found for '{event_info}' - proceeding with creation")
            response = self.add_event(event)
            
            if response and 'Event' in response:
                new_event_id = response['Event']['id']
                logger.info(f"🎉 NEW EVENT CREATED: '{event_info}' with ID {new_event_id}")
            else:
                logger.error(f"❌ Failed to create event: '{event_info}'")
            
            return response
            
        except Exception as e:
            logger.error(f"Error in add_event_with_deduplication: {e}")
            return None

    def _is_duplicate_event(self, new_event, similarity_threshold=0.7):
        """
        Check if an event is a duplicate using multiple criteria.
        
        Returns:
            Tuple of (is_duplicate: bool, existing_event: dict or None)
        """
        try:
            # For fallback clients, use a simplified but effective deduplication approach
            if hasattr(self.misp, '_is_fallback_client') and self.misp._is_fallback_client:
                logger.debug("Fallback client detected - using simplified deduplication check")
                return self._is_duplicate_event_fallback(new_event, similarity_threshold)
            
            # Check by UUID first (most reliable) - following PyMISP get_event pattern
            if hasattr(new_event, 'uuid') and new_event.uuid:
                try:
                    existing = self.misp.get_event(new_event.uuid, pythonify=False)
                    if existing and 'Event' in existing and 'errors' not in existing:
                        logger.debug(f"Found exact UUID match: {new_event.uuid}")
                        return True, existing
                except Exception as uuid_error:
                    logger.debug(f"UUID search failed: {uuid_error}")
            
            # Search by title/info (exact match) - following PyMISP search pattern
            if hasattr(new_event, 'info') and new_event.info:
                try:
                    search_results = self.misp.search(eventinfo=new_event.info, pythonify=False)
                    if search_results and isinstance(search_results, list) and len(search_results) > 0:
                        for result in search_results:
                            if isinstance(result, dict) and 'Event' in result:
                                existing_event = result
                                logger.debug(f"Found title match: {new_event.info}")
                                
                                # Additional check: compare attributes for similarity
                                if self._has_similar_attributes(new_event, existing_event, similarity_threshold):
                                    return True, existing_event
                except Exception as search_error:
                    logger.debug(f"Title search failed: {search_error}")
            
            # Search for recent events with similar content
            try:
                recent_events = self._get_recent_events(days=7)
                for existing_event in recent_events:
                    if self._has_similar_attributes(new_event, existing_event, similarity_threshold):
                        logger.debug(f"Found similar recent event: {existing_event.get('Event', {}).get('id', 'Unknown')}")
                        return True, existing_event
            except Exception as recent_error:
                logger.debug(f"Recent events search failed: {recent_error}")
            
            return False, None
            
        except Exception as e:
            logger.error(f"Error checking for duplicate events: {e}")
            return False, None

    def _has_similar_attributes(self, new_event, existing_event_response, threshold=0.7):
        """
        Compare attributes between new and existing events for similarity.
        
        Args:
            new_event: MISPEvent object
            existing_event_response: MISP API response dict
            threshold: Similarity threshold (0.0-1.0)
        
        Returns:
            bool: True if events are similar enough to be considered duplicates
        """
        try:
            # Extract existing event data
            if 'Event' not in existing_event_response:
                return False
            
            existing_event_data = existing_event_response['Event']
            existing_attributes = existing_event_data.get('Attribute', [])
            
            # Get new event attributes
            new_attributes = []
            if hasattr(new_event, 'attributes'):
                for attr in new_event.attributes:
                    if hasattr(attr, 'value'):
                        new_attributes.append(attr.value)
            
            if not new_attributes or not existing_attributes:
                return False
            
            # Convert existing attributes to values
            existing_values = []
            for attr in existing_attributes:
                if isinstance(attr, dict) and 'value' in attr:
                    existing_values.append(attr['value'])
                elif hasattr(attr, 'value'):
                    existing_values.append(attr.value)
            
            # Calculate similarity
            matching_attributes = 0
            total_new_attributes = len(new_attributes)
            
            for new_attr in new_attributes:
                # Create hash for comparison
                new_attr_hash = hashlib.md5(str(new_attr).lower().strip().encode()).hexdigest()
                
                for existing_attr in existing_values:
                    existing_attr_hash = hashlib.md5(str(existing_attr).lower().strip().encode()).hexdigest()
                    
                    if new_attr_hash == existing_attr_hash:
                        matching_attributes += 1
                        break
            
            similarity_ratio = matching_attributes / total_new_attributes if total_new_attributes > 0 else 0
            
            logger.debug(f"Attribute similarity: {similarity_ratio:.2f} ({matching_attributes}/{total_new_attributes})")
            
            return similarity_ratio >= threshold
            
        except Exception as e:
            logger.error(f"Error comparing event attributes: {e}")
            return False

    def _get_recent_events(self, days=7):
        """Get events from the last N days for duplicate checking."""
        try:
            date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            search_results = self.misp.search(
                date_from=date_from,
                limit=1000,  # Increased limit for comprehensive deduplication check
                pythonify=False
            )
            
            if search_results and isinstance(search_results, list):
                return search_results
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting recent events: {e}")
            return []

    def _is_duplicate_event_fallback(self, new_event, similarity_threshold=0.7):
        """
        Simplified deduplication check for fallback clients.
        Uses custom API calls to check for duplicates.
        
        Returns:
            Tuple of (is_duplicate: bool, existing_event: dict or None)
        """
        try:
            logger.info(f"Checking for duplicates of event: {getattr(new_event, 'info', 'Unknown')}")
            
            # Strategy 1: Get recent events and search for title matches
            if hasattr(new_event, 'info') and new_event.info:
                try:
                    # Use events/index endpoint to get recent events
                    search_url = 'events/index'
                    
                    response = self.misp._prepare_request('GET', search_url)
                    response_data = self.misp._check_json_response(response)
                    
                    if isinstance(response_data, list):
                        # Response is a list of events
                        events_list = response_data
                    elif isinstance(response_data, dict) and 'response' in response_data:
                        # Response has nested structure
                        events_list = response_data['response']
                    else:
                        logger.debug(f"Unexpected response format from events/index: {type(response_data)}")
                        events_list = []
                    
                    logger.debug(f"Found {len(events_list)} events to check for duplicates")
                    
                    new_info = new_event.info.strip().lower()
                    
                    for event_item in events_list[:50]:  # Limit to first 50 events for performance
                        try:
                            # Handle different response formats
                            if isinstance(event_item, dict):
                                if 'Event' in event_item:
                                    existing_event_data = event_item['Event']
                                else:
                                    existing_event_data = event_item
                                
                                existing_info = existing_event_data.get('info', '').strip().lower()
                                
                                # Check for title match
                                if existing_info == new_info:
                                    logger.info(f"Found exact title match for '{new_event.info}'")
                                    
                                    # Create response format
                                    existing_event = {'Event': existing_event_data}
                                    
                                    # Check attribute similarity
                                    if self._has_similar_attributes_fallback(new_event, existing_event_data, similarity_threshold):
                                        logger.info(f"Event is duplicate of existing event ID: {existing_event_data.get('id', 'Unknown')}")
                                        return True, existing_event
                                    
                        except Exception as item_error:
                            logger.debug(f"Error processing event item: {item_error}")
                            continue
                                
                except Exception as search_error:
                    logger.debug(f"Fallback events search failed: {search_error}")
            
            logger.debug("No duplicate events found")
            return False, None
            
        except Exception as e:
            logger.error(f"Error in fallback duplicate check: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False, None

    def _has_similar_attributes_fallback(self, new_event, existing_event_data, threshold=0.7):
        """
        Compare attributes for fallback clients using simplified approach.
        """
        try:
            # Get new event attributes
            new_attributes = []
            if hasattr(new_event, 'attributes'):
                for attr in new_event.attributes:
                    if hasattr(attr, 'value'):
                        new_attributes.append(str(attr.value).lower().strip())
            
            if not new_attributes:
                return False
            
            # Get existing event attributes by fetching event details
            try:
                event_id = existing_event_data.get('id')
                if not event_id:
                    return False
                
                # Fetch event details to get attributes
                event_url = f'events/view/{event_id}'
                response = self.misp._prepare_request('GET', event_url)
                response_data = self.misp._check_json_response(response)
                
                existing_attributes = []
                if isinstance(response_data, dict) and 'Event' in response_data:
                    event_data = response_data['Event']
                    attributes_list = event_data.get('Attribute', [])
                    
                    for attr in attributes_list:
                        if isinstance(attr, dict) and 'value' in attr:
                            existing_attributes.append(str(attr['value']).lower().strip())
                
                if not existing_attributes:
                    return False
                
                # Calculate similarity
                matching_attributes = 0
                for new_attr in new_attributes:
                    if new_attr in existing_attributes:
                        matching_attributes += 1
                
                similarity_ratio = matching_attributes / len(new_attributes) if new_attributes else 0
                
                logger.debug(f"Attribute similarity: {similarity_ratio:.2f} ({matching_attributes}/{len(new_attributes)})")
                
                return similarity_ratio >= threshold
                
            except Exception as attr_error:
                logger.debug(f"Error comparing attributes: {attr_error}")
                return False
            
        except Exception as e:
            logger.error(f"Error in fallback attribute comparison: {e}")
            return False

    def publish_event(self, event_id, alert=False):
        """Publish an event in MISP following PyMISP api.py pattern."""
        try:
            logger.info(f"Publishing MISP event {event_id} (alert={alert})")
            
            # Handle fallback client with custom implementation
            if hasattr(self.misp, '_is_fallback_client') and self.misp._is_fallback_client:
                return self._publish_event_fallback(event_id, alert)
            
            # Use the PyMISP publish method directly as shown in api.py
            # def publish(self, event: MISPEvent | int | str | UUID, alert: bool = False)
            response = self.misp.publish(event_id, alert=alert)
            
            # Check for errors in response following PyMISP pattern
            if isinstance(response, dict) and 'errors' in response:
                logger.error(f"Error publishing event: {response['errors']}")
                return False
            
            logger.info(f"Event {event_id} published successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing event {event_id}: {e}")
            return False

    def _publish_event_fallback(self, event_id, alert=False):
        """Publish event using custom method for fallback clients."""
        try:
            # Choose URL based on alert flag
            url = f'events/alert/{event_id}' if alert else f'events/publish/{event_id}'
            
            # Make request using custom method
            response = self.misp._prepare_request('POST', url, data={})
            response_data = self.misp._check_json_response(response)
            
            if isinstance(response_data, dict) and 'errors' not in response_data:
                logger.info(f"Event {event_id} published successfully via fallback method")
                return True
            else:
                logger.error(f"Failed to publish event {event_id} via fallback method: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"Error in fallback publish_event: {e}")
            return False

    def get_event(self, event_id):
        """Retrieve an event from MISP."""
        try:
            response = self.misp.get_event(event_id, pythonify=False)
            
            if 'errors' in response:
                logger.error(f"Error retrieving event {event_id}: {response['errors']}")
                return None
            
            return response
            
        except Exception as e:
            logger.error(f"Error retrieving event {event_id}: {e}")
            return None

    def search_events(self, **kwargs):
        """Search for events in MISP."""
        try:
            response = self.misp.search(**kwargs)
            
            if isinstance(response, dict) and 'errors' in response:
                logger.error(f"Error searching events: {response['errors']}")
                return []
            
            return response if response else []
            
        except Exception as e:
            logger.error(f"Error searching events: {e}")
            return []

    def update_event(self, event_id, event_data):
        """Update an existing event in MISP."""
        try:
            logger.info(f"Updating MISP event {event_id}")
            response = self.misp.update_event(event_data, event_id)
            
            if 'errors' in response:
                logger.error(f"Error updating event: {response['errors']}")
                return None
            
            return response
            
        except Exception as e:
            logger.error(f"Error updating event {event_id}: {e}")
            return None

    def delete_event(self, event_id):
        """Delete an event from MISP."""
        try:
            logger.info(f"Deleting MISP event {event_id}")
            response = self.misp.delete_event(event_id)
            
            if 'errors' in response:
                logger.error(f"Error deleting event: {response['errors']}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting event {event_id}: {e}")
            return False

    def test_connection(self):
        """Test the MISP connection following PyMISP api.py patterns."""
        try:
            if not self.misp:
                return False, "MISP client not initialized"
            
            # Test using PyMISP's instance version property like in api.py
            try:
                version_info = self.misp.misp_instance_version
                if isinstance(version_info, dict) and 'version' in version_info:
                    return True, {
                        'version': version_info['version'],
                        'url': self.misp_url,
                        'status': 'Connected'
                    }
                else:
                    return True, {
                        'version': 'Unknown',
                        'url': self.misp_url,
                        'status': 'Connected (limited info)'
                    }
            except Exception as version_error:
                # Fallback: try a simple get request (events list)
                try:
                    events = self.misp.events(pythonify=False)
                    if isinstance(events, (list, dict)):
                        return True, {
                            'version': 'Unknown',
                            'url': self.misp_url,
                            'status': 'Connected (fallback test)'
                        }
                    else:
                        return False, f"Unexpected response from events API: {type(events)}"
                except Exception as fallback_error:
                    return False, f"Connection test failed: {str(fallback_error)}"
            
        except Exception as e:
            logger.error(f"MISP connection test failed: {e}")
            return False, str(e)
    
    def search_events_by_info(self, event_info: str, limit: int = 10) -> List[Dict]:
        """
        Search for existing events in MISP by event info/name.
        
        Args:
            event_info: Event info/name to search for
            limit: Maximum number of results to return
            
        Returns:
            List of matching event dictionaries
        """
        try:
            logger.debug(f"Searching MISP for events with info: '{event_info}'")
            
            # Handle fallback client
            if hasattr(self.misp, '_is_fallback_client') and self.misp._is_fallback_client:
                return self._search_events_fallback(event_info, limit)
            
            # Use PyMISP search with event info filter
            response = self.misp.search(
                controller='events',
                eventinfo=event_info,
                limit=limit,
                pythonify=False
            )
            
            if isinstance(response, dict) and 'errors' in response:
                logger.warning(f"Error searching events by info: {response['errors']}")
                return []
            
            # Handle different response formats
            if isinstance(response, list):
                return response
            elif isinstance(response, dict) and 'Event' in response:
                return [response]
            elif isinstance(response, dict) and 'response' in response:
                events = response['response']
                if isinstance(events, list):
                    return events
                elif isinstance(events, dict) and 'Event' in events:
                    return [events]
            
            return []
            
        except Exception as e:
            logger.error(f"Error searching events by info '{event_info}': {e}")
            return []
    
    def _search_events_fallback(self, event_info: str, limit: int = 10) -> List[Dict]:
        """Fallback search method for simple clients."""
        try:
            # Use direct API call for fallback clients
            url = f"{self.misp_url.rstrip('/')}/events/index"
            
            params = {
                'searcheventinfo': event_info,
                'limit': limit
            }
            
            response = self.misp._prepare_request('GET', url, params=params)
            result = self.misp._check_json_response(response)
            
            if isinstance(result, dict) and 'errors' not in result:
                # Try to extract events from response
                if 'Event' in result:
                    return [result]
                elif 'response' in result:
                    return result['response'] if isinstance(result['response'], list) else []
            
            return []
            
        except Exception as e:
            logger.error(f"Fallback search failed: {e}")
            return []
    
    def check_event_exists_by_info(self, event_info: str) -> Optional[Dict]:
        """
        Check if an event with the given info already exists in MISP.
        
        Args:
            event_info: Event info/name to check
            
        Returns:
            Event dictionary if found, None otherwise
        """
        try:
            events = self.search_events_by_info(event_info, limit=1)
            
            for event_data in events:
                # Handle different response structures
                event = event_data.get('Event', event_data) if isinstance(event_data, dict) else event_data
                
                if isinstance(event, dict):
                    existing_info = event.get('info', '')
                    if existing_info.strip().lower() == event_info.strip().lower():
                        logger.info(f"Found existing event with matching info: '{event_info}' (ID: {event.get('id', 'unknown')})")
                        return event
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking if event exists: {e}")
            return None
    
    def search_events_by_uuid(self, event_uuid: str) -> Optional[Dict]:
        """
        Search for an event by UUID.
        
        Args:
            event_uuid: Event UUID to search for
            
        Returns:
            Event dictionary if found, None otherwise
        """
        try:
            logger.debug(f"Searching MISP for event with UUID: {event_uuid}")
            
            # Handle fallback client
            if hasattr(self.misp, '_is_fallback_client') and self.misp._is_fallback_client:
                return self._search_event_by_uuid_fallback(event_uuid)
            
            response = self.misp.search(
                controller='events',
                uuid=event_uuid,
                pythonify=False
            )
            
            if isinstance(response, dict) and 'errors' in response:
                logger.warning(f"Error searching event by UUID: {response['errors']}")
                return None
            
            # Handle response
            if isinstance(response, list) and len(response) > 0:
                event_data = response[0]
                return event_data.get('Event', event_data) if isinstance(event_data, dict) else event_data
            elif isinstance(response, dict) and 'Event' in response:
                return response['Event']
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching event by UUID '{event_uuid}': {e}")
            return None
    
    def _search_event_by_uuid_fallback(self, event_uuid: str) -> Optional[Dict]:
        """Fallback UUID search for simple clients."""
        try:
            url = f"{self.misp_url.rstrip('/')}/events/view/{event_uuid}"
            response = self.misp._prepare_request('GET', url)
            result = self.misp._check_json_response(response)
            
            if isinstance(result, dict) and 'Event' in result and 'errors' not in result:
                return result['Event']
            
            return None
            
        except Exception as e:
            logger.debug(f"Fallback UUID search failed (event may not exist): {e}")
            return None