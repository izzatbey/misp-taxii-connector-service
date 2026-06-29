# filepath: /home/admin/misp-taxii-connector/taxii2misp/utils/signal_handlers.py
import signal
import sys

shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print("Shutdown requested. Exiting gracefully...")

def setup_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def is_shutdown_requested():
    return shutdown_requested