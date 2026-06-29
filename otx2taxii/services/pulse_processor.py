import logging
import uuid
import re
import html
from datetime import datetime, timezone
from urllib.parse import unquote

from stix2 import Indicator, TLP_WHITE, TLP_GREEN, TLP_AMBER, TLP_RED, Identity, Grouping, Bundle, parse

logger = logging.getLogger(__name__)

class PulseProcessor:
    """
    Processes OTX pulse data and transforms it into STIX 2.1 Bundles.
    Handles STIX object creation and de-duplication checks.
    """
    def __init__(self, custom_stix_namespace: uuid.UUID):
        self.custom_stix_namespace = custom_stix_namespace
        # Define OTX Identity once as it's static
        # We generate a consistent ID for the OTX Identity using the custom namespace
        otx_identity_name = "AlienVault OTX"
        otx_identity_class = "organization"
        otx_identity_hash_data = f"{otx_identity_name}-{otx_identity_class}"
        otx_identity_id = uuid.uuid5(self.custom_stix_namespace, otx_identity_hash_data)
        self.otx_identity = Identity(
            id=f"identity--{str(otx_identity_id)}",
            name=otx_identity_name,
            identity_class=otx_identity_class,
            type="identity",
            # Set created/modified to a fixed or current timestamp, or derive if available in OTX global data
            created=datetime(2018, 4, 26, 23, 55, 4, 672000, tzinfo=timezone.utc), # Using a consistent historical date from your example
            modified=datetime(2018, 4, 26, 23, 55, 4, 672000, tzinfo=timezone.utc),
        )
        logger.info(f"PulseProcessor initialized with OTX Identity: {self.otx_identity.id}")

    def _map_tlp_to_stix(self, tlp_string: str):
        """Maps an OTX TLP string to a STIX TLP marking definition reference."""
        tlp_string = tlp_string.lower()
        if tlp_string == 'green':
            return TLP_GREEN
        elif tlp_string == 'amber':
            return TLP_AMBER
        elif tlp_string == 'red':
            return TLP_RED
        # Default to TLP_WHITE if not recognized or explicitly set to white
        return TLP_WHITE

    def _convert_timestamp(self, timestamp_str: str) -> datetime:
        """Converts an OTX timestamp string to a timezone-aware datetime object."""
        try:
            # Handle 'Z' suffix for UTC and ensure timezone awareness
            dt_obj = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return dt_obj
        except ValueError:
            logger.warning(f"Invalid datetime format: {timestamp_str}. Using current UTC time.")
            return datetime.now(timezone.utc)

    def _sanitize_indicator_value(self, value: str, indicator_type: str) -> str:
        """
        Sanitize and clean indicator values to ensure they're valid for STIX patterns.
        
        Args:
            value: The raw indicator value
            indicator_type: The type of indicator (URL, domain, etc.)
            
        Returns:
            Cleaned and sanitized value
        """
        if not value:
            return value
            
        # Start with the original value
        cleaned_value = value
        
        # Decode literal unicode escapes (\\u0026 -> &, \\u003e -> >)
        # This handles cases where unicode escapes are stored as literal strings
        if '\\u' in cleaned_value:
            # Replace common literal unicode escapes
            unicode_replacements = {
                '\\u0026': '&',  # &
                '\\u003e': '>',  # >
                '\\u003c': '<',  # <
                '\\u0022': '"',  # "
                '\\u0027': "'",  # '
                '\\u002f': '/',  # /
                '\\u003a': ':',  # :
                '\\u003b': ';',  # ;
                '\\u003d': '=',  # =
                '\\u003f': '?',  # ?
                '\\u0040': '@',  # @
            }
            
            for literal, replacement in unicode_replacements.items():
                cleaned_value = cleaned_value.replace(literal, replacement)
        
        # Decode HTML entities (&amp; -> &, &lt; -> <, etc.)
        cleaned_value = html.unescape(cleaned_value)
        
        # Decode URL encoding (%20 -> space, etc.)
        cleaned_value = unquote(cleaned_value)
        
        # For URLs, handle special cases
        if indicator_type in ["URL", "URI"]:
            # Remove HTML tags and content that might be appended
            # Pattern to match "><span or similar HTML fragments at the end
            cleaned_value = re.sub(r'["\']?\s*>\s*<[^>]*.*$', '', cleaned_value)
            
            # Remove trailing quotes that might break patterns
            cleaned_value = cleaned_value.strip('"\'')
            
            # Remove any remaining HTML tags
            cleaned_value = re.sub(r'<[^>]*>', '', cleaned_value)
            
        # Escape single quotes in the value for STIX pattern safety
        # In STIX patterns, single quotes need to be escaped as \'
        cleaned_value = cleaned_value.replace("'", "\\'")
        
        return cleaned_value.strip()

    def _escape_stix_pattern_value(self, value: str) -> str:
        """
        Properly escape a value for use in STIX patterns.
        
        Args:
            value: The value to escape
            
        Returns:
            Properly escaped value for STIX patterns
        """
        if not value:
            return value
            
        # Escape backslashes first
        escaped = value.replace('\\', '\\\\')
        
        # Escape single quotes
        escaped = escaped.replace("'", "\\'")
        
        return escaped

    def process_pulse_data(
        self, 
        pulse_detail: dict, 
        pulse_indicators: list[dict], 
        existing_stix_ids: set[str]
    ) -> Bundle | None:
        """
        Transforms an OTX pulse and its indicators into a STIX Bundle.
        Performs de-duplication checks based on existing_stix_ids.

        Args:
            pulse_detail (dict): The main dictionary containing pulse metadata.
            pulse_indicators (list[dict]): A list of dictionaries, each representing an indicator in the pulse.
            existing_stix_ids (set[str]): A set of STIX object IDs already present in the TAXII collection.

        Returns:
            Bundle | None: A STIX Bundle containing the transformed objects, or None if the grouping
                           for this pulse already exists in the TAXII collection.
        """
        pulse_id = pulse_detail.get('id')
        pulse_name = pulse_detail.get('name', 'Unknown Pulse')
        pulse_description = pulse_detail.get('description', 'No description.')
        pulse_author = pulse_detail.get('author_name', 'Unknown Author')
        pulse_created_str = pulse_detail.get('created', datetime.now(timezone.utc).isoformat())
        pulse_tlp_str = pulse_detail.get('TLP', 'green').lower()

        pulse_created_dt = self._convert_timestamp(pulse_created_str)
        stix_tlp_ref = self._map_tlp_to_stix(pulse_tlp_str)

        all_stix_objects = []
        processed_indicator_ids = []

        logger.info(f"\n--- Processing Pulse ID: {pulse_id} ('{pulse_name}' by '{pulse_author}') ---")

        # Add the OTX Identity to the bundle if it doesn't already exist
        # Note: Identity should ideally be a singleton and only added once across all pushes,
        # but for simplicity, we check per-bundle here and rely on TAXII server de-duplication for its ID.
        if self.otx_identity.id not in existing_stix_ids:
            all_stix_objects.append(self.otx_identity)
            logger.debug(f"Added OTX Identity with ID: {self.otx_identity.id}")
        else:
            logger.debug(f"OTX Identity with ID: {self.otx_identity.id} already exists (or was added). Skipping creation.")


        # --- Process Indicators ---
        if not pulse_indicators:
            logger.info(f"No indicators found for Pulse ID: {pulse_id}.")
            # If no indicators, we might still create a grouping, or just return None
            # For now, let's proceed to grouping even if no indicators, as identity is still present.
        else:
            logger.info(f"Found {len(pulse_indicators)} indicators for this pulse.")
            for idx, indicator_data in enumerate(pulse_indicators):
                indicator_value = indicator_data.get('indicator')
                indicator_type_otx = indicator_data.get('type')
                indicator_type_title = indicator_data.get('type_title', indicator_type_otx)
                indicator_description = indicator_data.get('description', '')
                indicator_created_otx_str = indicator_data.get('created', datetime.now(timezone.utc).isoformat())
                indicator_expiration_otx_str = indicator_data.get('expiration')

                stix_pattern = None
                stix_valid_until = None

                created_dt = self._convert_timestamp(indicator_created_otx_str)
                if indicator_expiration_otx_str:
                    stix_valid_until = self._convert_timestamp(indicator_expiration_otx_str)

                # Sanitize the indicator value before using it in patterns
                sanitized_value = self._sanitize_indicator_value(indicator_value, indicator_type_otx)
                
                # Skip if sanitization resulted in empty value
                if not sanitized_value:
                    logger.warning(f"[{idx + 1}] Indicator value became empty after sanitization. Original: '{indicator_value}'. Skipping indicator.")
                    continue
                
                # Further escape for STIX pattern safety
                escaped_value = self._escape_stix_pattern_value(sanitized_value)

                # --- OTX to STIX Pattern Mapping ---
                if indicator_type_otx == "IPv4":
                    stix_pattern = f"[ipv4-addr:value = '{escaped_value}']"
                elif indicator_type_otx == "IPv6":
                    stix_pattern = f"[ipv6-addr:value = '{escaped_value}']"
                elif indicator_type_otx == "domain":
                    stix_pattern = f"[domain-name:value = '{escaped_value}']"
                elif indicator_type_otx == "hostname": # Often treated as domain-name
                    stix_pattern = f"[domain-name:value = '{escaped_value}']"
                elif indicator_type_otx == "URL" or indicator_type_otx == "URI":
                    stix_pattern = f"[url:value = '{escaped_value}']"
                elif indicator_type_otx == "FileHash-MD5":
                    stix_pattern = f"[file:hashes.MD5 = '{escaped_value}']"
                elif indicator_type_otx == "FileHash-SHA1":
                    stix_pattern = f"[file:hashes.'SHA-1' = '{escaped_value}']"
                elif indicator_type_otx == "FileHash-SHA256":
                    stix_pattern = f"[file:hashes.'SHA-256' = '{escaped_value}']"
                elif indicator_type_otx == "FileHash-PEHASH": # Not standard STIX, but common extension
                    stix_pattern = f"[file:hashes.'PEHASH' = '{escaped_value}']"
                elif indicator_type_otx == "FileHash-IMPHASH": # Not standard STIX, but common extension
                    stix_pattern = f"[file:hashes.'IMPHASH' = '{escaped_value}']"
                elif indicator_type_otx == "email":
                    stix_pattern = f"[email-addr:value = '{escaped_value}']"
                elif indicator_type_otx == "CIDR":
                    if '.' in escaped_value:
                        stix_pattern = f"[ipv4-addr:value = '{escaped_value}']"
                    elif ':' in escaped_value:
                        stix_pattern = f"[ipv6-addr:value = '{escaped_value}']"
                    else:
                        logger.warning(f"[{idx + 1}] Unknown CIDR Format: {escaped_value}. Skipping indicator.")
                        continue
                elif indicator_type_otx == "FilePath":
                    stix_pattern = f"[file:path = '{escaped_value}']"
                elif indicator_type_otx == "Mutex":
                    stix_pattern = f"[windows-mutex:name = '{escaped_value}']"
                elif indicator_type_otx == "CVE":
                    stix_pattern = f"[vulnerability:cve_id = '{escaped_value}']"
                elif indicator_type_otx == 'malware-sample': # OTX often provides MD5 for malware samples
                    logger.warning(f"[{idx + 1}] Converting OTX type 'malware-sample' to FileHash-MD5 indicator.")
                    stix_pattern = f"[file:hashes.'MD5' = '{escaped_value}']"
                    indicator_type_title = "Malware Sample (MD5)"
                elif indicator_type_otx == 'YARA':
                    logger.info(f"[{idx + 1}] Found YARA Rules. Creating indicator for YARA rule name.")
                    # A YARA rule is typically a file, so we can map to a file:name pattern
                    stix_pattern = f"[file:name = '{escaped_value}.yar']"
                    indicator_description = f"YARA Rules: {sanitized_value}\nContent:\n{indicator_data.get('content', 'No Content Available')}\n\n{indicator_description}"
                else:
                    logger.warning(f"[{idx + 1}] Unhandled OTX indicator type: '{indicator_type_otx}' for value '{sanitized_value}'. Skipping indicator.")
                    continue

                if stix_pattern:
                    try:
                        stix_desc = f"{indicator_type_title} '{pulse_name}': {indicator_description.strip()}"
                        if not indicator_description.strip():
                            stix_desc = f"{indicator_type_title} '{pulse_name}'."
                        
                        # Generate a consistent STIX ID for the indicator using the namespace
                        indicator_hash_data = f"{pulse_id}-{indicator_type_otx}-{stix_pattern}"
                        stix_indicator_id_det = uuid.uuid5(self.custom_stix_namespace, indicator_hash_data)
                        proposed_indicator_id = f"indicator--{str(stix_indicator_id_det)}"

                        if proposed_indicator_id not in existing_stix_ids:
                            try:
                                stix_indicator = Indicator(
                                    type="indicator",
                                    id=proposed_indicator_id,
                                    created_by_ref=self.otx_identity.id,
                                    pattern_type="stix",
                                    pattern=stix_pattern,
                                    created=created_dt, 
                                    modified=created_dt, 
                                    valid_from=created_dt,
                                    valid_until=stix_valid_until,
                                    description=stix_desc,
                                    object_marking_refs=[stix_tlp_ref],
                                    pattern_version="2.1" # Explicitly set pattern version for STIX 2.1
                                )
                                all_stix_objects.append(stix_indicator)
                                processed_indicator_ids.append(stix_indicator.id)
                                logger.debug(f"Converted and added STIX Indicator: {stix_indicator.id}")
                            except Exception as stix_error:
                                logger.error(f"Failed to create STIX indicator for '{sanitized_value}' (type: {indicator_type_otx})")
                                logger.error(f"STIX pattern: {stix_pattern}")
                                logger.error(f"Original value: '{indicator_value}'")
                                logger.error(f"Sanitized value: '{sanitized_value}'")
                                logger.error(f"Escaped value: '{escaped_value}'")
                                logger.error(f"STIX creation error: {stix_error}")
                                continue
                        else:
                            logger.info(f"Indicator '{sanitized_value}' (ID: {proposed_indicator_id}) already exists in TAXII. Skipping.")
                            # Even if the indicator exists, we still want to link it to the grouping.
                            # So, add its ID to processed_indicator_ids if it's already in existing_stix_ids
                            processed_indicator_ids.append(proposed_indicator_id)

                    except Exception as e:
                        logger.error(f"Error processing indicator '{sanitized_value}' (original: '{indicator_value}') from pulse '{pulse_name}': {e}")
                        logger.error(f"Indicator type: {indicator_type_otx}, Pattern: {stix_pattern}")
                        if "STIX pattern" in str(e) or "pattern" in str(e).lower():
                            logger.error("This appears to be a STIX pattern validation error. The indicator value may contain invalid characters.")
                        continue
                
        # --- Create Grouping Object ---
        # Generate a consistent STIX ID for the grouping using the namespace
        # We include the sorted list of object refs in the hash to make it unique per set of indicators
        all_refs_for_grouping_hash = [str(self.otx_identity.id),] + sorted(processed_indicator_ids)
        grouping_hash_data = f"otx_pulse-{pulse_id}-{pulse_name}-{pulse_description}-{'|'.join(all_refs_for_grouping_hash)}"
        stix_grouping_id_det = uuid.uuid5(self.custom_stix_namespace, grouping_hash_data)
        proposed_grouping_id = f"grouping--{str(stix_grouping_id_det)}"
        
        if proposed_grouping_id in existing_stix_ids:
            logger.info(f"Grouping for Pulse '{pulse_name}' (ID: {proposed_grouping_id}) already exists in TAXII Server. Skipping this pulse.")
            return None # Indicate that no new bundle needs to be pushed for this pulse
        
        # If we reach here, the grouping does not exist, so create it and its associated objects
        stix_grouping = Grouping(
            type="grouping",
            spec_version="2.1",
            id=proposed_grouping_id,
            created_by_ref=self.otx_identity.id,
            created=pulse_created_dt,
            modified=pulse_created_dt,
            name=f"{pulse_name}",
            context=["threat-report", "otx-pulse"], # Add 'otx-pulse' context for clarity
            object_refs=[self.otx_identity.id] + processed_indicator_ids,
        )
        all_stix_objects.append(stix_grouping)
        logger.info(f"Created Grouping SDO: {stix_grouping.id} for Pulse '{pulse_name}'")
        
        if not all_stix_objects:
            logger.warning("No STIX Objects were generated for this pulse (after de-duplication checks). Skipping bundle creation.")
            return None

        # --- Create STIX Bundle ---
        bundle = Bundle(
            type="bundle",
            id=f"bundle--{str(uuid.uuid4())}", # Bundles usually get a new UUID each time
            objects=all_stix_objects,
        )
        logger.info(f"Successfully created STIX Bundle with {len(all_stix_objects)} objects for Pulse '{pulse_name}'.")
        return bundle