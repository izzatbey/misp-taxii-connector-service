FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all necessary files
COPY main.py .
COPY ingest.py .
COPY supervisor.sh .
COPY config/ ./config/
COPY clients/ ./clients/
COPY services/ ./services/
COPY utils/ ./utils/
COPY converters/ ./converters/

# Create output + outbox directories
RUN mkdir -p output stix_outbox/pending stix_outbox/processed

# Make the supervisor executable
RUN chmod +x supervisor.sh

# Set environment variables for proper signal handling
ENV PYTHONUNBUFFERED=1

# Run the supervisor, which starts BOTH ingest.py and main.py as separate
# long-lived processes. Each handles its own signals + scheduler loop.
CMD ["./supervisor.sh"]