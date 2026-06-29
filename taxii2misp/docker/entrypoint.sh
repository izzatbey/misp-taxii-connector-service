#!/bin/sh

# Wait for the MISP service to be available
until curl -s -o /dev/null -f "$MISP_URL"; do
    echo "Waiting for MISP service to be available at $MISP_URL..."
    sleep 5
done

# Execute the main application
exec python3 /app/main.py