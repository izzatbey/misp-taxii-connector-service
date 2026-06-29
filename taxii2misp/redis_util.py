#!/usr/bin/env python3
"""
Redis Cache Management Utility for TAXII2MISP

This utility script provides commands to manage the Redis cache for tracking processed MISP events.
"""

import sys
import os
import json
from datetime import datetime

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import Config
from clients.redis_client import MISPEventRedisClient

def show_stats(redis_client):
    """Show current processing statistics."""
    print("=== MISP Event Processing Statistics ===\n")
    
    if not redis_client.is_connected():
        print("❌ Redis is not connected.")
        return
    
    # Connection info
    conn_info = redis_client.get_connection_info()
    print(f"📡 Redis Connection: {conn_info['host']}:{conn_info['port']} (DB {conn_info['db']})")
    print(f"🔗 Connected: {'✅ Yes' if conn_info['connected'] else '❌ No'}")
    print(f"⏰ TTL: {conn_info['ttl_seconds']} seconds\n")
    
    # Processing stats
    stats = redis_client.get_processing_stats()
    if stats:
        print(f"📊 Last Run: {stats.get('last_run_at', 'Never')}")
        print(f"📈 Events Processed (Last Run): {stats.get('events_processed_this_run', 0)}")
        print(f"📋 Total Events Available: {stats.get('total_events_available', 0)}")
        print(f"✅ Total Processed Groupings: {stats.get('total_processed_groupings', 0)}")
    else:
        print("📊 No processing statistics available")
    
    # Processed groupings count
    processed_count = len(redis_client.get_processed_groupings())
    print(f"\n🎯 Currently Tracked Groupings: {processed_count}")

def list_processed(redis_client, limit=10):
    """List processed groupings with their MISP event information."""
    print(f"=== Processed Groupings (Last {limit}) ===\n")
    
    if not redis_client.is_connected():
        print("❌ Redis is not connected.")
        return
    
    processed_groupings = list(redis_client.get_processed_groupings())
    
    if not processed_groupings:
        print("📭 No processed groupings found.")
        return
    
    print(f"📝 Found {len(processed_groupings)} processed groupings\n")
    
    # Show last N groupings
    for i, grouping_id in enumerate(processed_groupings[-limit:], 1):
        event_info = redis_client.get_misp_event_info(grouping_id)
        
        print(f"{i}. Grouping: {grouping_id}")
        
        if event_info:
            print(f"   ├─ MISP Event ID: {event_info.get('misp_event_id', 'N/A')}")
            print(f"   ├─ MISP Event UUID: {event_info.get('misp_event_uuid', 'N/A')}")
            print(f"   ├─ Processed At: {event_info.get('processed_at', 'N/A')}")
            print(f"   ├─ Object Refs Count: {event_info.get('object_refs_count', 'N/A')}")
            print(f"   ├─ Grouping Name: {event_info.get('grouping_name', 'N/A')}")
            print(f"   └─ Context: {event_info.get('grouping_context', 'N/A')}")
        else:
            print("   └─ ⚠️  No detailed information available")
        
        print()

def clear_cache(redis_client, confirm=False):
    """Clear all processed grouping data."""
    print("=== Clear Redis Cache ===\n")
    
    if not redis_client.is_connected():
        print("❌ Redis is not connected.")
        return
    
    processed_count = len(redis_client.get_processed_groupings())
    
    if processed_count == 0:
        print("📭 No processed groupings to clear.")
        return
    
    print(f"⚠️  This will clear {processed_count} processed grouping records.")
    
    if not confirm:
        response = input("Are you sure you want to continue? (yes/no): ").lower()
        if response != 'yes':
            print("❌ Operation cancelled.")
            return
    
    print("\n🧹 Clearing cache...")
    success = redis_client.clear_processed_groupings()
    
    if success:
        print("✅ Cache cleared successfully.")
    else:
        print("❌ Failed to clear cache.")

def check_grouping(redis_client, grouping_id):
    """Check if a specific grouping has been processed."""
    print(f"=== Check Grouping: {grouping_id} ===\n")
    
    if not redis_client.is_connected():
        print("❌ Redis is not connected.")
        return
    
    is_processed = redis_client.is_grouping_processed(grouping_id)
    
    if is_processed:
        print("✅ Grouping has been processed.")
        
        event_info = redis_client.get_misp_event_info(grouping_id)
        if event_info:
            print("\n📋 Details:")
            print(f"   MISP Event ID: {event_info.get('misp_event_id', 'N/A')}")
            print(f"   MISP Event UUID: {event_info.get('misp_event_uuid', 'N/A')}")
            print(f"   Processed At: {event_info.get('processed_at', 'N/A')}")
            print(f"   Object Refs Count: {event_info.get('object_refs_count', 'N/A')}")
            print(f"   Grouping Name: {event_info.get('grouping_name', 'N/A')}")
            print(f"   Context: {event_info.get('grouping_context', 'N/A')}")
            print(f"   Content Hash: {event_info.get('content_hash', 'N/A')}")
    else:
        print("❌ Grouping has not been processed yet.")

def main():
    if len(sys.argv) < 2:
        print("TAXII2MISP Redis Cache Management Utility")
        print("\nUsage:")
        print("  python redis_util.py stats                    - Show processing statistics")
        print("  python redis_util.py list [limit]             - List processed groupings (default: 10)")
        print("  python redis_util.py check <grouping_id>      - Check if grouping is processed")
        print("  python redis_util.py clear [--confirm]        - Clear all processed data")
        print("\nExamples:")
        print("  python redis_util.py stats")
        print("  python redis_util.py list 20")
        print("  python redis_util.py check grouping--12345")
        print("  python redis_util.py clear --confirm")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    # Load configuration and initialize Redis client
    try:
        config = Config()
        
        if not config.REDIS_ENABLE_TRACKING:
            print("❌ Redis tracking is disabled in configuration.")
            sys.exit(1)
        
        redis_client = MISPEventRedisClient(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            ttl_seconds=config.REDIS_TTL_SECONDS
        )
        
    except Exception as e:
        print(f"❌ Failed to initialize Redis client: {e}")
        sys.exit(1)
    
    # Execute commands
    try:
        if command == "stats":
            show_stats(redis_client)
            
        elif command == "list":
            limit = 10
            if len(sys.argv) > 2:
                try:
                    limit = int(sys.argv[2])
                except ValueError:
                    print("❌ Invalid limit value. Using default (10).")
            
            list_processed(redis_client, limit)
            
        elif command == "check":
            if len(sys.argv) < 3:
                print("❌ Please provide a grouping ID to check.")
                sys.exit(1)
            
            grouping_id = sys.argv[2]
            check_grouping(redis_client, grouping_id)
            
        elif command == "clear":
            confirm = "--confirm" in sys.argv
            clear_cache(redis_client, confirm)
            
        else:
            print(f"❌ Unknown command: {command}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error executing command: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
