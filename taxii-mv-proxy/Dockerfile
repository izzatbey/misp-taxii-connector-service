FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc libpq-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROXY_HOST=0.0.0.0 \
    PROXY_PORT=9000

EXPOSE 9000

# 1 worker because psycopg2 is not process-safe on a single shared
# connection. For higher concurrency use multiple replicas behind an
# LB instead of --workers N.
CMD ["uvicorn", "proxy:app", \
     "--host", "0.0.0.0", \
     "--port", "9000", \
     "--log-level", "info", \
     "--proxy-headers"]
