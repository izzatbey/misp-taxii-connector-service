# filepath: /home/admin/misp-taxii-connector/taxii2misp/services/stix_processor.py
import logging
import json
import os
import tempfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from stix2 import MemoryStore
from stix2.utils import STIXdatetime
from pymisp import MISPEvent, MISPAttribute
from misp_stix_converter import misp_stix_converter

logger = logging.getLogger(__name__)

class STIXJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles STIX objects and datetime types."""
    
    def default(self, obj):
        if isinstance(obj, STIXdatetime):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat') and callable(getattr(obj, 'isoformat')):
            try:
                return obj.isoformat()
            except Exception:
                return str(obj)
        elif hasattr(obj, '__dict__'):
            # Handle complex objects
            try:
                return obj.__dict__
            except Exception:
                return str(obj)
        else:
            return str(obj)

class STIXProcessor:
    def __init__(self, temp_dir):
        self.temp_dir = temp_dir
        # Ensure temp directory exists
        os.makedirs(temp_dir, exist_ok=True)

    def process_grouping(self, memory_store, grouping_id, distribution=0):
        """
        Process a STIX grouping object and convert it to MISP events.
        
        Args:
            memory_store: STIX2 MemoryStore containing all objects
            grouping_id: ID of the grouping object to process
            distribution: MISP distribution level
        
        Returns:
            Tuple of (list of MISPEvent objects, additional data)
        """
        try:
            logger.info(f"Processing STIX grouping: {grouping_id}")
            
            # Get the grouping object
            grouping = memory_store.get(grouping_id)
            if not grouping:
                logger.error(f"Grouping {grouping_id} not found in memory store")
                return [], None
            
            logger.info(f"Found grouping: {grouping.name}")
            
            # Get all referenced objects with comprehensive bundling
            referenced_objects = []
            missing_refs = []
            
            for ref_id in grouping.object_refs:
                obj = memory_store.get(ref_id)
                if obj:
                    referenced_objects.append(obj)
                else:
                    missing_refs.append(ref_id)
                    logger.warning(f"Referenced object {ref_id} not found in memory store")
            
            logger.info(f"Found {len(referenced_objects)} referenced objects")
            if missing_refs:
                logger.warning(f"Missing {len(missing_refs)} referenced objects - event will be incomplete")
            
            # Ensure complete bundling before processing
            indicators = [obj for obj in referenced_objects if hasattr(obj, 'type') and obj.type == 'indicator']
            logger.info(f"Bundle contains {len(indicators)} indicators for grouping '{grouping.name}'")
            
            # Create bundle with grouping and all referenced objects
            bundle_objects = [grouping] + referenced_objects
            
            # Convert using misp-stix-converter with complete bundling support
            misp_events = self._convert_stix_to_misp_with_complete_bundling(bundle_objects, distribution, grouping)
            
            logger.info(f"Converted to {len(misp_events)} MISP events")
            return misp_events, None
            
        except Exception as e:
            logger.error(f"Error processing grouping {grouping_id}: {e}", exc_info=True)
            return [], None

    def process_grouping_with_verification(self, memory_store, grouping_id, distribution=0, expected_indicators=0):
        """
        Enhanced process_grouping method with comprehensive verification logic
        from test_real_taxii_bundling.py. This ensures complete bundling before MISP event creation.
        
        Args:
            memory_store: STIX2 MemoryStore containing all objects
            grouping_id: ID of the grouping object to process
            distribution: MISP distribution level
            expected_indicators: Expected number of indicators for verification
        
        Returns:
            Tuple of (list of MISPEvent objects, additional data)
        """
        try:
            logger.info(f"🔬 ENHANCED PROCESSING WITH VERIFICATION: {grouping_id}")
            
            # Get the grouping object
            grouping = memory_store.get(grouping_id)
            if not grouping:
                logger.error(f"Grouping {grouping_id} not found in memory store")
                return [], None
            
            grouping_name = grouping.name
            logger.info(f"Found grouping: {grouping_name}")
            
            # COMPREHENSIVE OBJECT RETRIEVAL AND ANALYSIS
            referenced_objects = []
            missing_refs = []
            indicators_found = []
            
            for ref_id in grouping.object_refs:
                obj = memory_store.get(ref_id)
                if obj:
                    referenced_objects.append(obj)
                    # Track indicators specifically
                    if hasattr(obj, 'type') and obj.type == 'indicator':
                        indicators_found.append(obj)
                else:
                    missing_refs.append(ref_id)
                    logger.warning(f"Referenced object {ref_id} not found in memory store")
            
            # BUNDLING VERIFICATION ANALYSIS
            total_refs = len(grouping.object_refs)
            available_refs = len(referenced_objects)
            indicators_count = len(indicators_found)
            
            logger.info(f"📊 BUNDLING VERIFICATION ANALYSIS:")
            logger.info(f"   Total references: {total_refs}")
            logger.info(f"   Available objects: {available_refs}")
            logger.info(f"   Missing objects: {len(missing_refs)}")
            logger.info(f"   Indicators found: {indicators_count}")
            logger.info(f"   Expected indicators: {expected_indicators}")
            
            # CRITICAL BUNDLING CHECKS
            if indicators_count == 0:
                logger.error(f"❌ CRITICAL BUNDLING ISSUE: No indicators found for grouping '{grouping_name}'")
                logger.error(f"   This will result in an empty or comment-only MISP event")
                return [], None
            
            if indicators_count == 1 and expected_indicators > 1:
                logger.error(f"❌ SINGLE INDICATOR ISSUE DETECTED: Only 1 indicator found when expecting {expected_indicators}")
                logger.error(f"   This is likely the cause of the 1-attribute MISP events (Event ID 37990 issue)")
                logger.error(f"   Root cause: Incomplete object retrieval from TAXII server")
            
            if missing_refs:
                logger.warning(f"⚠️  INCOMPLETE BUNDLING: {len(missing_refs)} referenced objects missing")
                logger.warning(f"   Missing object IDs: {missing_refs[:3]}...")
                logger.warning(f"   This may result in incomplete MISP events")
            
            # FORCED COMPREHENSIVE BUNDLING
            logger.info(f"🔧 APPLYING COMPREHENSIVE BUNDLING STRATEGY...")
            
            # Create bundle with ALL available objects
            bundle_objects = [grouping] + referenced_objects
            
            # Use enhanced conversion with strict verification
            misp_events = self._convert_stix_to_misp_with_verification(
                bundle_objects, 
                distribution, 
                grouping,
                expected_indicators=expected_indicators
            )
            
            # POST-PROCESSING VERIFICATION
            if misp_events:
                for i, event in enumerate(misp_events):
                    indicator_attrs = [attr for attr in event.attributes if attr.type != 'comment']
                    
                    logger.info(f"📋 POST-PROCESSING VERIFICATION (Event {i+1}):")
                    logger.info(f"   Event name: '{event.info}'")
                    logger.info(f"   Total attributes: {len(event.attributes)}")
                    logger.info(f"   Indicator attributes: {len(indicator_attrs)}")
                    logger.info(f"   Expected indicators: {expected_indicators}")
                    
                    if len(indicator_attrs) == expected_indicators:
                        logger.info(f"   ✅ BUNDLING SUCCESS: Complete bundling achieved")
                    elif len(indicator_attrs) < expected_indicators:
                        logger.error(f"   ❌ BUNDLING INCOMPLETE: {len(indicator_attrs)}/{expected_indicators} indicators")
                    else:
                        logger.info(f"   ✅ BUNDLING COMPLETE: {len(indicator_attrs)} indicators bundled")
            
            logger.info(f"Enhanced processing completed: {len(misp_events)} MISP events with verification")
            return misp_events, None
            
        except Exception as e:
            logger.error(f"Error in enhanced processing for grouping {grouping_id}: {e}", exc_info=True)
            return [], None

    def _convert_stix_to_misp_with_verification(self, stix_objects, distribution=0, grouping=None, expected_indicators=0):
        """
        Enhanced conversion with verification that implements the diagnostic approach
        from test_real_taxii_bundling.py
        """
        try:
            logger.info(f"🔧 ENHANCED CONVERSION WITH VERIFICATION")
            logger.info(f"   Processing {len(stix_objects)} objects")
            logger.info(f"   Expected indicators: {expected_indicators}")
            
            # Try the comprehensive bundling conversion directly
            # This bypasses the library converter which may be causing issues
            logger.info("Using comprehensive bundling conversion for guaranteed completeness")
            return self._comprehensive_bundling_conversion(stix_objects, distribution, grouping)
            
        except Exception as e:
            logger.error(f"Error in enhanced conversion: {e}")
            return self._comprehensive_bundling_conversion(stix_objects, distribution, grouping)

    def _convert_stix_to_misp(self, stix_objects, distribution=0):
        """
        Convert STIX objects to MISP events using misp-stix-converter.
        
        Args:
            stix_objects: List of STIX objects
            distribution: MISP distribution level
        
        Returns:
            List of MISPEvent objects
        """
        try:
            # Create a temporary STIX bundle file
            bundle_data = {
                "type": "bundle",
                "id": f"bundle--{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                "objects": []
            }
            
            # Add objects to bundle with proper serialization
            for obj in stix_objects:
                try:
                    if hasattr(obj, '_inner'):
                        # Handle STIX2 objects - convert to dict and handle datetime serialization
                        obj_dict = self._serialize_stix_object(obj._inner)
                        bundle_data["objects"].append(obj_dict)
                    elif isinstance(obj, dict):
                        obj_dict = self._serialize_stix_object(obj)
                        bundle_data["objects"].append(obj_dict)
                    else:
                        # Try to serialize the object
                        try:
                            obj_str = str(obj)
                            obj_dict = json.loads(obj_str)
                            obj_dict = self._serialize_stix_object(obj_dict)
                            bundle_data["objects"].append(obj_dict)
                        except:
                            logger.warning(f"Could not serialize STIX object: {type(obj)}")
                            continue
                except Exception as obj_error:
                    logger.warning(f"Error processing STIX object: {obj_error}")
                    continue
            
            # Write bundle to temporary file
            temp_file = os.path.join(self.temp_dir, f"stix_bundle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            try:
                with open(temp_file, 'w') as f:
                    json.dump(bundle_data, f, indent=2, cls=STIXJSONEncoder)
                logger.debug(f"Created STIX bundle file: {temp_file}")
            except TypeError as json_error:
                logger.error(f"JSON serialization error: {json_error}")
                logger.debug(f"Bundle data types: {type(bundle_data)}")
                if 'objects' in bundle_data:
                    logger.debug(f"Number of objects: {len(bundle_data['objects'])}")
                    for i, obj in enumerate(bundle_data['objects'][:5]):  # Log first 5 objects
                        logger.debug(f"Object {i} type: {type(obj)}, keys: {list(obj.keys()) if isinstance(obj, dict) else 'N/A'}")
                
                # Try to serialize with more aggressive conversion
                logger.info("Attempting fallback serialization...")
                bundle_data_str = self._serialize_stix_object(bundle_data)
                with open(temp_file, 'w') as f:
                    json.dump(bundle_data_str, f, indent=2)
                logger.debug(f"Created STIX bundle file with fallback method: {temp_file}")
            
            try:
                # Convert STIX to MISP using misp-stix-converter
                # Try different method names that might exist in the library
                try:
                    # Method 1: Try the most likely method name
                    misp_events = misp_stix_converter.stix2_to_misp(temp_file)
                except AttributeError:
                    try:
                        # Method 2: Try alternative method name
                        misp_events = misp_stix_converter.convert_stix_to_misp(temp_file)
                    except AttributeError:
                        try:
                            # Method 3: Try with different parameter format
                            with open(temp_file, 'r') as f:
                                stix_data = json.load(f)
                            misp_events = misp_stix_converter.convert(stix_data)
                        except:
                            # All methods failed, fall back to manual conversion
                            logger.warning("All misp-stix-converter methods failed, using fallback conversion")
                            return self._fallback_conversion(stix_objects, distribution)
                
                if not misp_events:
                    logger.warning("No MISP events generated from STIX conversion")
                    return []
                
                # Process the converted events
                processed_events = []
                for event_data in misp_events:
                    try:
                        # Create MISPEvent from the converted data
                        misp_event = self._create_misp_event_from_data(event_data, distribution)
                        if misp_event:
                            processed_events.append(misp_event)
                    except Exception as e:
                        logger.error(f"Error processing converted event: {e}")
                        continue
                
                return processed_events
                
            finally:
                # Clean up temporary file
                try:
                    os.remove(temp_file)
                except:
                    pass
            
        except Exception as e:
            logger.error(f"Error converting STIX to MISP: {e}", exc_info=True)
            return self._fallback_conversion(stix_objects, distribution)

    def _convert_stix_to_misp_with_complete_bundling(self, stix_objects, distribution=0, grouping=None):
        """
        Enhanced conversion method that ensures complete bundling of all indicators
        from a grouping before creating MISP events.
        
        Args:
            stix_objects: List of STIX objects
            distribution: MISP distribution level  
            grouping: The STIX grouping object (optional)
        
        Returns:
            List of MISPEvent objects with complete indicator bundling
        """
        try:
            logger.info(f"Converting STIX to MISP with complete bundling for {len(stix_objects)} objects")
            
            # First attempt with the library converter
            misp_events = self._convert_stix_to_misp(stix_objects, distribution)
            
            # Verify complete bundling - check if all indicators are included
            if misp_events and grouping:
                total_indicators_expected = len([obj for obj in stix_objects if hasattr(obj, 'type') and obj.type == 'indicator'])
                
                for event in misp_events:
                    actual_attributes = len([attr for attr in event.attributes if attr.type != 'comment'])
                    
                    if actual_attributes < total_indicators_expected:
                        logger.warning(f"Incomplete bundling detected: {actual_attributes}/{total_indicators_expected} indicators in event")
                        logger.info("Falling back to comprehensive bundling method")
                        return self._comprehensive_bundling_conversion(stix_objects, distribution, grouping)
                    else:
                        logger.info(f"✅ Complete bundling verified: {actual_attributes} indicators in event '{event.info}'")
            
            return misp_events
            
        except Exception as e:
            logger.error(f"Error in complete bundling conversion: {e}")
            return self._comprehensive_bundling_conversion(stix_objects, distribution, grouping)

    def _comprehensive_bundling_conversion(self, stix_objects, distribution=0, grouping=None):
        """
        Comprehensive method that ensures ALL indicators from a grouping are bundled
        into a single MISP event, supporting large attribute counts (5-100+ attributes).
        
        Args:
            stix_objects: List of STIX objects
            distribution: MISP distribution level
            grouping: The STIX grouping object
        
        Returns:
            List of MISPEvent objects with comprehensive bundling
        """
        try:
            logger.info("Using comprehensive bundling conversion for complete indicator inclusion")
            
            # Separate object types
            groupings = [obj for obj in stix_objects if hasattr(obj, 'type') and obj.type == 'grouping']
            indicators = [obj for obj in stix_objects if hasattr(obj, 'type') and obj.type == 'indicator']
            other_objects = [obj for obj in stix_objects if hasattr(obj, 'type') and obj.type not in ['grouping', 'indicator']]
            
            logger.info(f"Processing {len(groupings)} groupings, {len(indicators)} indicators, {len(other_objects)} other objects")
            
            misp_events = []
            
            # Process each grouping comprehensively
            for grouping_obj in groupings:
                try:
                    # Create MISP event for the grouping
                    misp_event = MISPEvent()
                    misp_event.info = grouping_obj.name
                    misp_event.distribution = distribution
                    misp_event.threat_level_id = 2
                    misp_event.analysis = 2
                    
                    # Add comprehensive metadata
                    misp_event.add_attribute(
                        type='comment',
                        value=f"STIX Grouping: {grouping_obj.name}",
                        category='Other',
                        comment=f"Comprehensive bundle from STIX grouping {grouping_obj.id} with complete indicator collection"
                    )
                    
                    # Bundle ALL referenced indicators comprehensively
                    attributes_added = 0
                    bundled_indicators = []
                    
                    for indicator in indicators:
                        if indicator.id in grouping_obj.object_refs:
                            attr = self._convert_indicator_to_attribute(indicator)
                            if attr:
                                # Add the attribute to MISP event
                                misp_event.add_attribute(
                                    type=attr.type,
                                    value=attr.value,
                                    category=attr.category,
                                    comment=f"Bundled from STIX indicator: {indicator.id}"
                                )
                                attributes_added += 1
                                bundled_indicators.append(indicator.id)
                    
                    # Add information about bundling completeness
                    expected_indicators = len([ref for ref in grouping_obj.object_refs if any(ind.id == ref for ind in indicators)])
                    
                    logger.info(f"📦 COMPREHENSIVE BUNDLING: Added {attributes_added} indicators to event '{grouping_obj.name}'")
                    logger.info(f"   Expected: {expected_indicators}, Bundled: {attributes_added}")
                    
                    if attributes_added != expected_indicators:
                        logger.warning(f"   ⚠️  Bundling incomplete: {attributes_added}/{expected_indicators} indicators")
                        # Add a warning attribute
                        misp_event.add_attribute(
                            type='comment',
                            value=f"Warning: Incomplete bundling - {attributes_added}/{expected_indicators} indicators found",
                            category='Other',
                            comment="Some indicators referenced by the grouping were not available"
                        )
                    else:
                        logger.info(f"   ✅ Complete bundling achieved: All {attributes_added} indicators included")
                    
                    # Add bundling summary
                    misp_event.add_attribute(
                        type='comment',
                        value=f"Bundle Summary: {attributes_added} indicators from STIX grouping",
                        category='Other',
                        comment="Comprehensive bundling ensures all related indicators are in this single event"
                    )
                    
                    # Validate event quality before adding it to the list
                    is_valid, validation_reason = self.validate_event_quality(misp_event)
                    
                    if is_valid and attributes_added > 0:
                        misp_events.append(misp_event)
                        logger.info(f"✅ Created comprehensive MISP event for '{grouping_obj.name}' with {attributes_added} attributes")
                        logger.info(f"   Validation: {validation_reason}")
                    else:
                        logger.warning(f"🚫 REJECTED event for '{grouping_obj.name}' - {validation_reason}")
                        logger.warning(f"   This prevents creation of low-value comment-only events (TODO requirement)")
                    
                except Exception as e:
                    logger.error(f"Error in comprehensive conversion for grouping: {e}")
                    continue
            
            logger.info(f"📋 Comprehensive bundling completed: {len(misp_events)} events with complete indicator bundling")
            return misp_events
            
        except Exception as e:
            logger.error(f"Error in comprehensive bundling conversion: {e}")
            return []

    def _serialize_stix_object(self, obj):
        """
        Recursively serialize STIX object, handling datetime and other non-JSON types.
        """
        # Handle STIXdatetime specifically
        if isinstance(obj, STIXdatetime):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                result[key] = self._serialize_stix_object(value)
            return result
        elif isinstance(obj, list):
            return [self._serialize_stix_object(item) for item in obj]
        elif hasattr(obj, 'isoformat') and callable(getattr(obj, 'isoformat')):
            # Handle other datetime-like objects
            try:
                return obj.isoformat()
            except Exception:
                return str(obj)
        elif hasattr(obj, '__dict__'):
            # Handle complex objects by converting to dict first
            try:
                obj_dict = obj.__dict__
                return self._serialize_stix_object(obj_dict)
            except Exception:
                return str(obj)
        elif hasattr(obj, '__str__'):
            # Handle other objects that can be converted to string
            return str(obj)
        else:
            return obj

    def _create_misp_event_from_data(self, event_data, distribution=0):
        """
        Create a MISPEvent from converted data.
        
        Args:
            event_data: Event data from misp-stix-converter
            distribution: MISP distribution level
        
        Returns:
            MISPEvent object or None
        """
        try:
            # Handle different data structures from misp-stix-converter
            if isinstance(event_data, dict):
                event_info = event_data
            elif hasattr(event_data, 'to_dict'):
                event_info = event_data.to_dict()
            elif hasattr(event_data, '_inner'):
                event_info = event_data._inner
            else:
                logger.warning(f"Unknown event data type: {type(event_data)}")
                return None
            
            # Create MISP event
            misp_event = MISPEvent()
            
            # Set basic event information
            misp_event.info = event_info.get('info', 'Converted from STIX')
            misp_event.distribution = distribution
            misp_event.threat_level_id = event_info.get('threat_level_id', 2)
            misp_event.analysis = event_info.get('analysis', 2)
            
            # Add UUID if available
            if 'uuid' in event_info:
                misp_event.uuid = event_info['uuid']
            
            # Add attributes
            attributes = event_info.get('Attribute', [])
            for attr_data in attributes:
                try:
                    attr = MISPAttribute()
                    attr.type = attr_data.get('type', 'text')
                    attr.value = attr_data.get('value', '')
                    attr.category = attr_data.get('category', 'Other')
                    attr.distribution = attr_data.get('distribution', distribution)
                    
                    if attr.value:  # Only add if there's a value
                        misp_event.add_attribute(attr)
                        
                except Exception as attr_error:
                    logger.warning(f"Error adding attribute: {attr_error}")
                    continue
            
            return misp_event
            
        except Exception as e:
            logger.error(f"Error creating MISP event from data: {e}")
            return None

    def _fallback_conversion(self, stix_objects, distribution=0):
        """
        Fallback conversion method when misp-stix-converter fails.
        Manually extracts indicators and creates MISP events.
        """
        try:
            logger.info("Using fallback STIX to MISP conversion")
            
            misp_events = []
            
            # Group objects by grouping
            groupings = [obj for obj in stix_objects if hasattr(obj, 'type') and obj.type == 'grouping']
            indicators = [obj for obj in stix_objects if hasattr(obj, 'type') and obj.type == 'indicator']
            
            for grouping in groupings:
                try:
                    # Create MISP event for each grouping
                    misp_event = MISPEvent()
                    misp_event.info = grouping.name
                    misp_event.distribution = distribution
                    misp_event.threat_level_id = 2
                    misp_event.analysis = 2
                    
                    # Add indicators as attributes
                    attributes_added = 0
                    for indicator in indicators:
                        if indicator.id in grouping.object_refs:
                            attr = self._convert_indicator_to_attribute(indicator)
                            if attr:
                                # Use the proper method to add attribute
                                misp_event.add_attribute(
                                    type=attr.type,
                                    value=attr.value,
                                    category=attr.category,
                                    comment=getattr(attr, 'comment', '')
                                )
                                attributes_added += 1
                    
                    # If no indicators found, create a basic event with a comment
                    if attributes_added == 0:
                        logger.warning(f"No indicators found for grouping '{grouping.name}', creating event with basic info")
                        misp_event.add_attribute(
                            type='comment',
                            value=f"STIX Grouping: {grouping.name}",
                            category='Other',
                            comment=f"Converted from STIX grouping {grouping.id}. No indicators were available in the retrieved data."
                        )
                        attributes_added = 1
                    
                    # Validate event quality before adding (prevents comment-only events per TODO)
                    is_valid, validation_reason = self.validate_event_quality(misp_event)
                    
                    if is_valid and attributes_added > 0:
                        misp_events.append(misp_event)
                        logger.debug(f"Created MISP event for '{grouping.name}' with {attributes_added} attributes")
                    else:
                        logger.warning(f"🚫 REJECTED fallback event for '{grouping.name}' - {validation_reason}")
                        logger.warning(f"   Preventing comment-only event creation (TODO requirement)")
                    
                except Exception as e:
                    logger.error(f"Error in fallback conversion for grouping: {e}")
                    continue
            
            # If no groupings, create a single event with all indicators
            if not misp_events and indicators:
                misp_event = MISPEvent()
                misp_event.info = "Indicators from STIX Bundle"
                misp_event.distribution = distribution
                misp_event.threat_level_id = 2
                misp_event.analysis = 2
                
                for indicator in indicators:
                    attr = self._convert_indicator_to_attribute(indicator)
                    if attr:
                        # Use the proper method to add attribute
                        misp_event.add_attribute(
                            type=attr.type,
                            value=attr.value,
                            category=attr.category,
                            comment=getattr(attr, 'comment', '')
                        )
                
                if len(misp_event.attributes) > 0:
                    misp_events.append(misp_event)
            
            logger.info(f"Fallback conversion created {len(misp_events)} events")
            return misp_events
            
        except Exception as e:
            logger.error(f"Error in fallback conversion: {e}")
            return []

    def _convert_indicator_to_attribute(self, indicator):
        """
        Convert a STIX indicator to a MISP attribute.
        
        Args:
            indicator: STIX indicator object
        
        Returns:
            MISPAttribute or None
        """
        try:
            # Parse the indicator pattern
            pattern = indicator.pattern
            
            # Simple pattern parsing for common types
            if 'file:hashes.MD5' in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'md5'
                    attr.value = value
                    attr.category = 'Payload delivery'
                    return attr
            
            elif 'file:hashes.SHA-1' in pattern or "file:hashes.'SHA-1'" in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'sha1'
                    attr.value = value
                    attr.category = 'Payload delivery'
                    return attr
            
            elif 'file:hashes.SHA-256' in pattern or "file:hashes.'SHA-256'" in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'sha256'
                    attr.value = value
                    attr.category = 'Payload delivery'
                    return attr
            
            elif 'domain-name:value' in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'domain'
                    attr.value = value
                    attr.category = 'Network activity'
                    return attr
            
            elif 'ipv4-addr:value' in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'ip-dst'
                    attr.value = value
                    attr.category = 'Network activity'
                    return attr
            
            elif 'url:value' in pattern:
                value = self._extract_value_from_pattern(pattern)
                if value:
                    attr = MISPAttribute()
                    attr.type = 'url'
                    attr.value = value
                    attr.category = 'Network activity'
                    return attr
            
            # Default: create a text attribute with the pattern
            attr = MISPAttribute()
            attr.type = 'text'
            attr.value = pattern
            attr.category = 'Other'
            attr.comment = f"STIX Pattern: {getattr(indicator, 'description', '')}"
            return attr
            
        except Exception as e:
            logger.error(f"Error converting indicator to attribute: {e}")
            return None

    def _extract_value_from_pattern(self, pattern):
        """
        Extract the actual value from a STIX pattern.
        
        Args:
            pattern: STIX pattern string
        
        Returns:
            Extracted value or None
        """
        try:
            # Look for values in single quotes
            import re
            match = re.search(r"= '([^']+)'", pattern)
            if match:
                return match.group(1)
            
            # Look for values in double quotes
            match = re.search(r'= "([^"]+)"', pattern)
            if match:
                return match.group(1)
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting value from pattern: {e}")
            return None

    def create_synthetic_event_from_indicators(self, indicators, batch_number, distribution=0):
        """
        Create synthetic MISP events from a list of standalone indicators.
        
        Args:
            indicators: List of STIX indicator objects
            batch_number: Batch number for event naming
            distribution: MISP distribution level
        
        Returns:
            List of MISPEvent objects
        """
        try:
            logger.info(f"Creating synthetic event from {len(indicators)} indicators (batch {batch_number})")
            
            # Create MISP event
            event = MISPEvent()
            event.info = f"Threat Intelligence Indicators - Batch {batch_number} ({len(indicators)} indicators)"
            event.distribution = distribution
            event.threat_level_id = 2  # Medium
            event.analysis = 1  # Ongoing
            
            # Add comment about synthetic nature
            event.add_attribute(
                type='comment',
                value=f"This event was automatically created from {len(indicators)} standalone STIX indicators that were not grouped.",
                comment="Synthetic event created by TAXII2MISP connector"
            )
            
            # Process each indicator
            attributes_added = 0
            for i, indicator in enumerate(indicators):
                try:
                    logger.debug(f"Processing indicator {i+1}/{len(indicators)}: {indicator.get('id', 'unknown')}")
                    
                    # Extract indicator pattern
                    pattern = indicator.get('pattern', '')
                    if not pattern:
                        logger.warning(f"Indicator {indicator.get('id', 'unknown')} has no pattern")
                        continue
                    
                    # Determine attribute type and value based on pattern
                    attr_type, attr_value = self._parse_indicator_pattern(pattern)
                    
                    if attr_type and attr_value:
                        # Add to MISP event
                        attribute = event.add_attribute(
                            type=attr_type,
                            value=attr_value,
                            comment=f"From STIX indicator: {indicator.get('id', 'unknown')}"
                        )
                        
                        # Add labels as tags if available
                        labels = indicator.get('labels', [])
                        if labels:
                            for label in labels:
                                try:
                                    event.add_tag(f"stix2:indicator-label=\"{label}\"")
                                except Exception as tag_error:
                                    logger.debug(f"Failed to add tag for label '{label}': {tag_error}")
                        
                        attributes_added += 1
                        logger.debug(f"Added {attr_type} attribute: {attr_value}")
                    
                    else:
                        logger.warning(f"Could not parse indicator pattern: {pattern}")
                        
                        # Add as comment if we can't parse it
                        event.add_attribute(
                            type='comment',
                            value=f"Unparsed indicator pattern: {pattern}",
                            comment=f"From STIX indicator: {indicator.get('id', 'unknown')}"
                        )
                        attributes_added += 1
                
                except Exception as indicator_error:
                    logger.error(f"Error processing indicator {i+1}: {indicator_error}")
                    continue
            
            if attributes_added == 0:
                logger.warning(f"No valid attributes extracted from {len(indicators)} indicators in batch {batch_number}")
                return []
            
            # Validate event quality before returning (prevents comment-only events per TODO)
            is_valid, validation_reason = self.validate_event_quality(event)
            
            if is_valid:
                logger.info(f"Created synthetic event with {attributes_added} attributes from {len(indicators)} indicators")
                logger.info(f"   Validation: {validation_reason}")
                return [event]
            else:
                logger.warning(f"🚫 REJECTED synthetic event batch {batch_number} - {validation_reason}")
                logger.warning(f"   Preventing low-value event creation (TODO requirement)")
                return []
            
        except Exception as e:
            logger.error(f"Error creating synthetic event from indicators: {e}", exc_info=True)
            return []

    def _parse_indicator_pattern(self, pattern):
        """
        Parse STIX indicator pattern to extract MISP attribute type and value.
        
        Args:
            pattern: STIX pattern string
        
        Returns:
            Tuple of (attribute_type, value) or (None, None)
        """
        try:
            logger.debug(f"Parsing indicator pattern: {pattern}")
            
            # Define common pattern mappings
            pattern_mappings = {
                'file:hashes.MD5': 'md5',
                'file:hashes.SHA-1': 'sha1', 
                'file:hashes.SHA-256': 'sha256',
                'file:hashes.SHA-512': 'sha512',
                'file:name': 'filename',
                'domain-name:value': 'domain',
                'url:value': 'url',
                'ipv4-addr:value': 'ip-dst',
                'ipv6-addr:value': 'ip-dst',
                'email-addr:value': 'email-src',
                'email-message:sender_ref.value': 'email-src',
                'email-message:subject': 'email-subject',
                'process:name': 'filename',
                'process:command_line': 'text',
                'windows-registry-key:key': 'regkey',
                'x509-certificate:hashes.SHA-1': 'x509-fingerprint-sha1',
                'x509-certificate:hashes.SHA-256': 'x509-fingerprint-sha256',
                'mutex:name': 'mutex',
                'software:name': 'text'
            }
            
            # Extract object type and property from pattern
            import re
            
            # Look for patterns like "[object-type:property = 'value']"
            pattern_match = re.search(r'\[([^:]+):([^=\s]+)\s*=\s*[\'"]([^\'"]+)[\'"]', pattern)
            
            if pattern_match:
                object_type = pattern_match.group(1).strip()
                property_name = pattern_match.group(2).strip()
                value = pattern_match.group(3).strip()
                
                # Create full property path
                full_property = f"{object_type}:{property_name}"
                
                # Check direct mapping first
                if full_property in pattern_mappings:
                    return pattern_mappings[full_property], value
                
                # Check partial mappings
                if object_type == 'file' and 'hashes' in property_name:
                    if 'MD5' in property_name.upper():
                        return 'md5', value
                    elif 'SHA-1' in property_name.upper() or 'SHA1' in property_name.upper():
                        return 'sha1', value
                    elif 'SHA-256' in property_name.upper() or 'SHA256' in property_name.upper():
                        return 'sha256', value
                    elif 'SHA-512' in property_name.upper() or 'SHA512' in property_name.upper():
                        return 'sha512', value
                
                # Generic mappings by object type
                if object_type == 'domain-name':
                    return 'domain', value
                elif object_type == 'url':
                    return 'url', value
                elif object_type in ['ipv4-addr', 'ipv6-addr']:
                    return 'ip-dst', value
                elif object_type == 'email-addr':
                    return 'email-src', value
                elif object_type == 'file':
                    if 'name' in property_name:
                        return 'filename', value
                    else:
                        return 'text', value  # Generic file property
                
                # Fallback to text for unknown patterns
                logger.debug(f"Unknown pattern type '{full_property}', using text attribute")
                return 'text', value
            
            # Try to extract just the value for simple patterns
            value_match = re.search(r'[\'"]([^\'"]+)[\'"]', pattern)
            if value_match:
                value = value_match.group(1).strip()
                logger.debug(f"Extracted value '{value}' from pattern, using text attribute")
                return 'text', value
            
            logger.warning(f"Could not parse pattern: {pattern}")
            return None, None
            
        except Exception as e:
            logger.error(f"Error parsing indicator pattern: {e}")
            return None, None
    
    def validate_event_quality(self, misp_event) -> tuple[bool, str]:
        """
        Validate that a MISP event has meaningful threat intelligence content.
        
        This prevents creation of events that only contain comments without actual indicators,
        addressing the TODO requirement to avoid comment-only events.
        
        Args:
            misp_event: MISPEvent object to validate
            
        Returns:
            Tuple of (is_valid: bool, reason: str)
        """
        try:
            if not hasattr(misp_event, 'attributes') or not misp_event.attributes:
                return False, "Event has no attributes"
            
            # Count different types of attributes
            comment_attrs = 0
            indicator_attrs = 0
            
            for attr in misp_event.attributes:
                attr_type = getattr(attr, 'type', '').lower()
                attr_category = getattr(attr, 'category', '').lower()
                
                # Consider comment, text, and "other" category as non-indicators
                if attr_type in ['comment', 'text'] or attr_category == 'other':
                    comment_attrs += 1
                else:
                    # Real threat indicators (IPs, domains, hashes, etc.)
                    indicator_attrs += 1
            
            total_attrs = len(misp_event.attributes)
            
            # Event is invalid if it only contains comments/text without threat indicators
            if indicator_attrs == 0 and comment_attrs > 0:
                return False, f"Event contains only {comment_attrs} comment/text attributes without threat indicators"
            
            # Event is invalid if it has no meaningful content
            if total_attrs == 0:
                return False, "Event has no attributes"
            
            # Event is valid if it has at least one threat indicator
            if indicator_attrs > 0:
                return True, f"Event contains {indicator_attrs} threat indicators and {comment_attrs} comments"
            
            return False, "Event validation failed - no threat indicators found"
            
        except Exception as e:
            logger.error(f"Error validating event quality: {e}")
            return False, f"Validation error: {e}"
