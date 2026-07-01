#!/bin/sh
# supervisor.sh — start ingest.py and main.py as separate processes.
#
# Architecture:
#   ingest.py  --loop    : OTX ingestion, writes STIX chunks to <outbox>/pending/
#   main.py             : Reads <outbox>/pending/, pushes to TAXII, moves to processed/
#
# Both run as long-lived background processes inside the same container.
# They share <outbox> as their contract boundary.
#
# If either dies, we wait briefly and restart it. Docker's restart policy
# covers the case where the whole supervisor dies.

set -e

OUTBOX_DIR="${STIX_OUTBOX_DIR:-/app/stix_outbox}"
mkdir -p "$OUTBOX_DIR/pending" "$OUTBOX_DIR/processed"

start_ingest() {
    echo "[supervisor] Starting ingest.py (OTX ingestion loop)..."
    python -u ingest.py --loop &
    INGEST_PID=$!
    echo "$INGEST_PID" > /tmp/.supervisor_ingest_pid
}

start_main() {
    echo "[supervisor] Starting main.py (TAXII push loop)..."
    python -u main.py &
    MAIN_PID=$!
    echo "$MAIN_PID" > /tmp/.supervisor_main_pid
}

# Forward signals to children so Docker stop works cleanly.
shutdown_children() {
    echo "[supervisor] SIGTERM received; forwarding to children..."
    if [ -f /tmp/.supervisor_ingest_pid ]; then
        kill -TERM "$(cat /tmp/.supervisor_ingest_pid)" 2>/dev/null || true
    fi
    if [ -f /tmp/.supervisor_main_pid ]; then
        kill -TERM "$(cat /tmp/.supervisor_main_pid)" 2>/dev/null || true
    fi
    rm -f /tmp/.supervisor_ingest_pid /tmp/.supervisor_main_pid
    exit 0
}
trap shutdown_children TERM INT

# Start ingest first so it has a head start writing chunks before main
# begins to drain them.
start_ingest
sleep 5
start_main

INGEST_PID=$(cat /tmp/.supervisor_ingest_pid 2>/dev/null || echo "")
MAIN_PID=$(cat /tmp/.supervisor_main_pid 2>/dev/null || echo "")
echo "[supervisor] ingest.py PID=$INGEST_PID, main.py PID=$MAIN_PID"

# Wait for the children. If either exits non-zero, we restart it so the
# surviving process doesn't end up alone forever. SIGTERM/SIGINT still
# propagate to both via the trap above.
while true; do
    # Check ingest
    if [ -n "$INGEST_PID" ] && ! kill -0 "$INGEST_PID" 2>/dev/null; then
        wait "$INGEST_PID" 2>/dev/null || true
        echo "[supervisor] ingest.py exited; restarting in 5s..."
        sleep 5
        start_ingest
        INGEST_PID=$(cat /tmp/.supervisor_ingest_pid 2>/dev/null || echo "")
    fi
    # Check main
    if [ -n "$MAIN_PID" ] && ! kill -0 "$MAIN_PID" 2>/dev/null; then
        wait "$MAIN_PID" 2>/dev/null || true
        echo "[supervisor] main.py exited; restarting in 5s..."
        sleep 5
        start_main
        MAIN_PID=$(cat /tmp/.supervisor_main_pid 2>/dev/null || echo "")
    fi
    # Sleep briefly so we don't busy-loop
    sleep 2
done