# Drain3 Service

FastAPI microservice that reuses the `log-anomaly-analysis` package to perform
per-datastream preprocessing and template mining with Drain3.  It exposes a
simple `/ingest` endpoint that accepts raw log lines and returns structured
events (template ID, parameters, metadata).

## Endpoints
- `POST /ingest` — body: `{ "message": str, "datastream": str, "timestamp": Optional[str] }`
- `GET /healthz` — basic health probe.

## Configuration
- Config files (YAML) for each datastream are expected in `/configs`.  The
  filename should match the `datastream` field (e.g. `bgl.yaml`).
- Each config reuses the standard pipeline schema but only the preprocessing
  and parsing sections are required.

## Running locally

```bash
poetry install
poetry run uvicorn drain3_service.app:app --reload --port 8000
```

In docker-compose, mount the configs directory and invoke the service via
`
http://drain3-service:8000/ingest
`.
