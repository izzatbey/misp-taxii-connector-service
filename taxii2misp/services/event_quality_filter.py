"""
Event Quality Filter Service

This service implements TODO 2 requirements by filtering out low-quality events
before they are published to MISP. It prevents comment-only events and DGA
pattern events from being created in the first place.
"""

import logging
import re
from typing import List, Dict, Tuple, Optional
from pymisp import MISPEvent

logger = logging.getLogger(__name__)

class EventQualityFilter:
    """
    Service to filter out low-quality events before MISP publication.
    
    Implements TODO 2 requirements:
    - Skip events with names matching DGA patterns
    - Skip events that only contain comment-type attributes
    - Integrate directly into taxii2misp workflow
    """
    
    def __init__(self):
        """Initialize the quality filter with patterns from TODO."""
        
        # DGA patterns from TODO examples
        self.skip_patterns = [
            r"Active\s+\w+\s+DGA\(s\)\s+for\s+\d{8}",  # Main DGA pattern
            r"Active\s+\w+\s+DGA\s+for\s+\d{8}",       # Alternative DGA pattern
            r"STIX\s+Grouping:\s+Newly\s+registered\s+domain\s+names.*Covid-19.*\d{8}",  # STIX grouping pattern
        ]
        
        # Additional patterns that indicate low-value events
        self.additional_skip_patterns = [
            r"STIX\s+Grouping:.*DGA.*\d{8}",           # General STIX DGA groupings
            r"Active\s+\w+\s+domains?\s+for\s+\d{8}",  # Domain generation patterns
        ]
        
        self.all_patterns = self.skip_patterns + self.additional_skip_patterns
        
        logger.info(f"🛡️  EventQualityFilter initialized with {len(self.all_patterns)} skip patterns")
    
    def should_skip_event(self, event: MISPEvent) -> Tuple[bool, str]:
        """
        Determine if an event should be skipped based on TODO 2 criteria.
        
        Args:
            event: MISPEvent object to evaluate
            
        Returns:
            Tuple of (should_skip: bool, reason: str)
        """
        try:
            event_info = getattr(event, 'info', 'Unknown Event')
            
            # Check 1: Pattern matching from TODO
            pattern_match = self._check_skip_patterns(event_info)
            if pattern_match:
                return True, f"Event name matches skip pattern: {pattern_match}"
            
            # Check 2: Comment-only content
            is_comment_only, comment_reason = self._is_comment_only_event(event)
            if is_comment_only:
                return True, f"Comment-only event: {comment_reason}"
            
            # Check 3: Empty or low-value event
            if not hasattr(event, 'attributes') or not event.attributes:
                return True, "Event has no attributes"
            
            # Event passes all quality checks
            return False, "Event passed quality checks"
            
        except Exception as e:
            logger.error(f"❌ Error evaluating event quality: {e}")
            # Default to not skipping on errors to avoid blocking valid events
            return False, f"Quality check failed but allowing event: {e}"
    
    def _check_skip_patterns(self, event_info: str) -> Optional[str]:
        """
        Check if event info matches any skip patterns from TODO.
        
        Args:
            event_info: Event info string to check
            
        Returns:
            Matched pattern string or None
        """
        try:
            for pattern in self.all_patterns:
                if re.search(pattern, event_info, re.IGNORECASE):
                    logger.debug(f"Event info '{event_info}' matches pattern: {pattern}")
                    return pattern
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking skip patterns: {e}")
            return None
    
    def _is_comment_only_event(self, event: MISPEvent) -> Tuple[bool, str]:
        """
        Check if event contains only comment-type attributes.
        
        Args:
            event: MISPEvent to check
            
        Returns:
            Tuple of (is_comment_only: bool, reason: str)
        """
        try:
            if not hasattr(event, 'attributes') or not event.attributes:
                return True, "No attributes"
            
            # Count different attribute types
            comment_count = 0
            indicator_count = 0
            
            for attr in event.attributes:
                attr_type = getattr(attr, 'type', '').lower()
                attr_category = getattr(attr, 'category', '').lower()
                
                # Comment-type attributes (matching TODO description)
                if attr_type in ['comment', 'text'] or attr_category == 'other':
                    comment_count += 1
                else:
                    # Real threat indicators
                    indicator_count += 1
            
            total_attrs = len(event.attributes)
            
            # Event is comment-only if it has no threat indicators
            is_comment_only = (indicator_count == 0 and comment_count > 0)
            
            if is_comment_only:
                return True, f"{comment_count} comment attributes, 0 threat indicators"
            else:
                return False, f"{indicator_count} threat indicators, {comment_count} comments"
                
        except Exception as e:
            logger.error(f"Error checking comment-only status: {e}")
            return False, f"Check failed: {e}"
    
    def filter_events(self, events: List[MISPEvent]) -> Tuple[List[MISPEvent], List[Dict]]:
        """
        Filter a list of events, removing low-quality ones.
        
        Args:
            events: List of MISPEvent objects to filter
            
        Returns:
            Tuple of (filtered_events: List[MISPEvent], skipped_events: List[Dict])
        """
        try:
            filtered_events = []
            skipped_events = []
            
            for i, event in enumerate(events):
                event_info = getattr(event, 'info', f'Unknown Event {i+1}')
                
                should_skip, reason = self.should_skip_event(event)
                
                if should_skip:
                    skipped_info = {
                        'event_info': event_info,
                        'reason': reason,
                        'attributes_count': len(getattr(event, 'attributes', [])),
                        'index': i
                    }
                    skipped_events.append(skipped_info)
                    logger.info(f"🚫 SKIPPING event '{event_info}' - {reason}")
                else:
                    filtered_events.append(event)
                    logger.debug(f"✅ KEEPING event '{event_info}' - {reason}")
            
            logger.info(f"📊 Event filtering results: {len(filtered_events)} kept, {len(skipped_events)} skipped")
            
            return filtered_events, skipped_events
            
        except Exception as e:
            logger.error(f"❌ Error filtering events: {e}")
            # Return original events on error to avoid blocking processing
            return events, []
    
    def log_filter_summary(self, skipped_events: List[Dict]) -> None:
        """
        Log a summary of filtered events.
        
        Args:
            skipped_events: List of skipped event information
        """
        if not skipped_events:
            logger.info("✅ No events were filtered - all events passed quality checks")
            return
        
        logger.info(f"🛡️  EVENT QUALITY FILTER SUMMARY")
        logger.info(f"   Total events skipped: {len(skipped_events)}")
        
        # Group by reason
        reason_counts = {}
        for skipped in skipped_events:
            reason = skipped['reason']
            if 'pattern' in reason.lower():
                category = "DGA/Pattern Match"
            elif 'comment-only' in reason.lower():
                category = "Comment-Only Content"
            elif 'no attributes' in reason.lower():
                category = "Empty Event"
            else:
                category = "Other"
            
            reason_counts[category] = reason_counts.get(category, 0) + 1
        
        for category, count in reason_counts.items():
            logger.info(f"   {category}: {count} events")
        
        # Log first few examples
        logger.info(f"   Examples of skipped events:")
        for i, skipped in enumerate(skipped_events[:3]):
            logger.info(f"     {i+1}. '{skipped['event_info']}' - {skipped['reason']}")
        
        if len(skipped_events) > 3:
            logger.info(f"     ... and {len(skipped_events) - 3} more")
    
    def get_pattern_list(self) -> List[str]:
        """
        Get the list of skip patterns for monitoring/debugging.
        
        Returns:
            List of regex patterns used for filtering
        """
        return self.all_patterns.copy()


# Utility function for easy integration
def create_quality_filter() -> EventQualityFilter:
    """
    Factory function to create a quality filter instance.
    
    Returns:
        Configured EventQualityFilter instance
    """
    return EventQualityFilter()
