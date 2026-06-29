FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all necessary files
COPY main.py .
COPY config/ ./config/
COPY clients/ ./clients/
COPY services/ ./services/
COPY utils/ ./utils/
COPY converters/ ./converters/

# Create output directory
RUN mkdir -p output

# Set environment variables for proper signal handling
ENV PYTHONUNBUFFERED=1

# Run the scheduler as the main process
CMD ["python", "-u", "main.py"]