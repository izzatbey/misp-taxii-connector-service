#!/bin/sh
# supervisor.sh — start ingest.py and main.py as separate processes.
#
# Architecture:
#   ingest.py  --loop    : OTX ingestion, writes STIX chunks to /stix_outbox/pending/
#   main.py             : Reads /stix_outbox/pending/, pushes to TAXII, moves to processed/
#
# Both run as long-lived background processes inside the same container.
# They share /stix_outbox/ as their contract boundary.
#
# If either dies, we wait briefly and restart it. Docker's restart policy
# covers the case where the whole supervisor dies.

set -e

OUTBOX_DIR="${STIX_OUTBOX_DIR:-/app/stix_outbox}"
mkdir -p "$OUTBOX_DIR/pending" "$OUTBOX_DIR/processed"

echo "[supervisor] Starting ingest.py (OTX ingestion loop)..."
python -u ingest.py --loop &
INGEST_PID=$!

# Give ingest a head start so it doesn't race main on the outbox.
sleep 5

echo "[supervisor] Starting main.py (TAXII push loop)..."
python -u main.py &
MAIN_PID=$!

echo "[supervisor] ingest.py PID=$INGEST_PID, main.py PID=$MAIN_PID"

# Forward signals to children so Docker stop works cleanly.
trap "echo '[supervisor] SIGTERM received'; kill -TERM $INGEST_PID $MAIN_PID 2>/dev/null || true; exit 0" TERM INT

# Wait for either child to die. If one dies, log it; the other keeps running.
wait -n $INGEST_PID $MAIN_PID
EXITED_PID=$?
echo "[supervisor] A child process exited (pid=$EXITED_PID). Continuing with the other."

# Wait for the survivor too (or until we're signalled).
wait