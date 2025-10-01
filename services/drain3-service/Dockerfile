FROM python:3.11-slim AS analytics-builder

WORKDIR /build/log-anomaly-analysis

RUN pip install --no-cache-dir build

COPY log-anomaly-analysis/pyproject.toml ./pyproject.toml
COPY log-anomaly-analysis/poetry.lock ./poetry.lock
COPY log-anomaly-analysis/README.md ./README.md
COPY log-anomaly-analysis/src ./src
RUN python -m build --wheel

FROM python:3.11-slim
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir fastapi "uvicorn[standard]" polars loguru httpx

COPY --from=analytics-builder /build/log-anomaly-analysis/dist/log_anomaly_analysis-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/log_anomaly_analysis-*.whl \
    && rm /tmp/log_anomaly_analysis-*.whl

COPY services/drain3-service/drain3_service /app/drain3_service

CMD ["uvicorn", "drain3_service.app:app", "--host", "0.0.0.0", "--port", "8000"]
