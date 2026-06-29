#!/usr/bin/env python3
"""
Enhanced Redis Caching Example for OTX to TAXII

This script demonstrates the improved Redis caching functionality
for storing and checking STIX object IDs efficiently.
"""

import os
import sys
import json
from dotenv import load_dotenv

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Config
from clients.taxii_client import TAXIIClient

def demonstrate_redis_caching():
    """Demonstrate the enhanced Redis caching functionality"""
    
    print("=== Enhanced Redis Caching Demo ===\n")
    
    # Load configuration
    load_dotenv()
    config = Config()
    
    # Initialize TAXII client
    taxii_client = TAXIIClient(
        taxii_url=config.TAXII_URL,
        username=config.USERNAME,
        password=config.PASSWORD,
        verify_ssl=config.VERIFY_SSL,
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_db=config.REDIS_DB,
        redis_password=config.REDIS_PASSWORD,
        cache_ttl_seconds=config.CACHE_TTL_SECONDS
    )
    
    # Connect to TAXII server
    try:
        taxii_client.connect_to_server()
        collection = taxii_client.get_default_collection()
        print(f"Connected to TAXII collection: {collection.title}\n")
    except Exception as e:
        print(f"Failed to connect to TAXII server: {e}")
        return
    
    # 1. Check cache statistics
    print("1. Current cache statistics:")
    stats = taxii_client.get_cache_statistics()
    print(json.dumps(stats, indent=2))
    print()
    
    # 2. Get existing STIX IDs (this will populate cache if empty)
    print("2. Getting existing STIX IDs...")
    existing_ids = taxii_client.get_existing_stix_ids()
    print(f"Retrieved {len(existing_ids)} existing STIX IDs")
    print()
    
    # 3. Check cache statistics after population
    print("3. Cache statistics after population:")
    stats = taxii_client.get_cache_statistics()
    print(json.dumps(stats, indent=2))
    print()
    
    # 4. Test individual ID checking
    if existing_ids:
        sample_id = next(iter(existing_ids))
        print(f"4. Testing individual ID check for: {sample_id}")
        exists = taxii_client.is_stix_id_cached(sample_id)
        print(f"ID exists in cache: {exists}")
        print()
    
    # 5. Test batch ID checking
    print("5. Testing batch ID existence checking...")
    test_ids = set(list(existing_ids)[:5]) if len(existing_ids) >= 5 else existing_ids
    test_ids.add("test-fake-id-12345")  # Add a fake ID
    
    existing_test_ids, new_test_ids = taxii_client.check_stix_ids_existence(test_ids)
    print(f"Test IDs - Existing: {len(existing_test_ids)}, New: {len(new_test_ids)}")
    print(f"New IDs found: {new_test_ids}")
    print()
    
    # 6. Test adding new IDs to cache
    print("6. Testing adding new IDs to cache...")
    new_fake_ids = {"fake-id-1", "fake-id-2", "fake-id-3"}
    taxii_client.add_stix_ids_to_cache(new_fake_ids)
    
    # Verify they were added
    existing_fake, new_fake = taxii_client.check_stix_ids_existence(new_fake_ids)
    print(f"Fake IDs after adding - Existing: {len(existing_fake)}, New: {len(new_fake)}")
    print()
    
    # 7. Test cache refresh
    print("7. Testing cache refresh...")
    refresh_success = taxii_client.refresh_cache_from_taxii(force=True)
    print(f"Cache refresh successful: {refresh_success}")
    print()
    
    # 8. Final cache statistics
    print("8. Final cache statistics:")
    stats = taxii_client.get_cache_statistics()
    print(json.dumps(stats, indent=2))
    print()
    
    print("=== Demo Complete ===")

def demonstrate_bundle_prevalidation():
    """Demonstrate bundle pre-validation using Redis cache"""
    
    print("=== Bundle Pre-validation Demo ===\n")
    
    # Load configuration
    load_dotenv()
    config = Config()
    
    # Initialize TAXII client
    taxii_client = TAXIIClient(
        taxii_url=config.TAXII_URL,
        username=config.USERNAME,
        password=config.PASSWORD,
        verify_ssl=config.VERIFY_SSL,
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_db=config.REDIS_DB,
        redis_password=config.REDIS_PASSWORD,
        cache_ttl_seconds=config.CACHE_TTL_SECONDS
    )
    
    # Connect to TAXII server
    try:
        taxii_client.connect_to_server()
        collection = taxii_client.get_default_collection()
        print(f"Connected to TAXII collection: {collection.title}\n")
    except Exception as e:
        print(f"Failed to connect to TAXII server: {e}")
        return
    
    # Ensure cache is populated
    existing_ids = taxii_client.get_existing_stix_ids()
    print(f"Cache populated with {len(existing_ids)} existing STIX IDs\n")
    
    # Create a sample bundle with mix of existing and new objects
    sample_bundle = {
        "type": "bundle",
        "id": "bundle--sample-demo",
        "objects": [
            {
                "type": "indicator",
                "id": "indicator--existing-1",
                "created": "2023-01-01T00:00:00.000Z",
                "modified": "2023-01-01T00:00:00.000Z",
                "pattern": "[file:hashes.MD5 = 'existing1']",
                "labels": ["malicious-activity"]
            },
            {
                "type": "indicator", 
                "id": "indicator--new-1",
                "created": "2023-01-01T00:00:00.000Z",
                "modified": "2023-01-01T00:00:00.000Z",
                "pattern": "[file:hashes.MD5 = 'new1']",
                "labels": ["malicious-activity"]
            },
            {
                "type": "indicator",
                "id": "indicator--new-2", 
                "created": "2023-01-01T00:00:00.000Z",
                "modified": "2023-01-01T00:00:00.000Z",
                "pattern": "[file:hashes.MD5 = 'new2']",
                "labels": ["malicious-activity"]
            }
        ]
    }
    
    # Add one existing ID to the cache for demo
    if existing_ids:
        sample_bundle["objects"][0]["id"] = next(iter(existing_ids))
    
    print("Sample bundle created with 3 objects (1 existing, 2 new)")
    print("Original bundle object count:", len(sample_bundle["objects"]))
    print()
    
    # Pre-validate the bundle
    print("Pre-validating bundle...")
    validated_bundle = taxii_client.pre_validate_bundle_objects(sample_bundle)
    print("Validated bundle object count:", len(validated_bundle["objects"]))
    print()
    
    # Show the filtering results
    original_ids = {obj["id"] for obj in sample_bundle["objects"]}
    validated_ids = {obj["id"] for obj in validated_bundle["objects"]}
    filtered_out = original_ids - validated_ids
    
    print("Filtering results:")
    print(f"  Original IDs: {original_ids}")
    print(f"  Validated IDs: {validated_ids}")
    print(f"  Filtered out (duplicates): {filtered_out}")
    print()
    
    print("=== Pre-validation Demo Complete ===")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "prevalidation":
        demonstrate_bundle_prevalidation()
    else:
        demonstrate_redis_caching()
