Streaming Analytics Service (Loki tail -> Accumulator -> PCA)

Overview
- Tails Loki via the Tail WebSocket API with a label selector (e.g. {datastream="bgl"}).
- Accumulates events into epoch-aligned tumbling windows using hashed features.
- After each window flush, builds a feature matrix row and runs incremental PCA scoring
  using the StreamingPCAScorer (SPE + J–M threshold). The model warms incrementally,
  then freezes after warmup_windows.
- Pushes per-window anomaly results back to Loki for dashboards.

Configuration (env vars)
- LOKI_BASE_URL: Base URL for Loki (default: http://loki:3100)
- LOKI_QUERY: LogQL selector to tail (e.g. {datastream="bgl"})
- LOKI_PUSH_URL: Push endpoint (default: http://loki:3100/loki/api/v1/push)
- DATASTREAM: Logical name used in pushed anomaly labels (e.g. bgl)

- ACC_WINDOW_SIZE: e.g. 10m
- ACC_ALLOWED_LATENESS: e.g. 0s
- ACC_HASH_BINS: e.g. 4096
- ACC_HASH_SIGNED: true/false

- SCORER_VARIANCE_THRESHOLD: e.g. 0.90
- SCORER_ALPHA: e.g. 0.001
- SCORER_USE_SCALING: true/false
- SCORER_WARMUP_WINDOWS: e.g. 1000
- SCORER_MAX_COMPONENTS: e.g. 512
- SCORER_MIN_RESIDUAL_EIGS: e.g. 64

API
- GET /healthz -> { "status": "ok" }
- GET /stats -> basic counters + scorer status

Notes
- The service assumes the upstream Drain3 service pushed structured JSON per log line
  to Loki with the original event-time as the Loki timestamp. The field TemplateId is
  preferred for features; falls back to EventTemplate if missing.

