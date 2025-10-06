# Simple Dockerfile mirroring the drain3-service approach
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install service deps
COPY services/streaming-analytics/pyproject.toml /app/
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir .

# Copy library + service code
COPY log-anomaly-analysis /app/log-anomaly-analysis
COPY services/streaming-analytics/streaming_analytics /app/streaming_analytics

# Install the library in editable mode
RUN pip install --no-cache-dir -e /app/log-anomaly-analysis

EXPOSE 8001

CMD ["uvicorn", "streaming_analytics.app:app", "--host", "0.0.0.0", "--port", "8001"]

