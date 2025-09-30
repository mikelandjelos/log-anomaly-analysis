# Demo Streaming System

This directory contains the docker-compose scaffolding for the streaming demo
stack.  It intentionally has **no** Python package of its own; all application
code lives in sibling projects (e.g. the Drain3 FastAPI service and the
`log-anomaly-analysis` library).  The compose stack stitches them together.

## Layout

```
demo-system/
├── docker-compose.yml        # Entry point for the stack
├── configs/                  # Minimal Drain3 configs per datastream
├── logstash/                 # Pipeline definitions and configs
├── loki/                     # Loki configuration / provisioning
├── grafana/                  # Datasources + dashboards
├── replay/                   # Log replay utilities for demo datasets
└── README.md                 # This file
```

The Drain3/Preprocessing service is packaged under `services/drain3-service`
and referenced from `docker-compose.yml`.  Adjust volume mounts and environment
variables there when wiring in new datastreams.

## Quick start

1. Install dependencies in the sibling Python projects:

    ```bash
    cd log-anomaly-analysis && poetry install --with dev
    cd ../services/drain3-service && poetry install --with dev
    ```

2. Bring up the supporting services (Loki, Grafana, Logstash, Drain3):

    ```bash
    cd demo-system
    docker compose up --build
    ```

3. Replay a dataset into Logstash using the helper script:

    ```bash
    # Example: BGL dataset
    python replay/send_logs.py \
      ../log-anomaly-analysis/data/raw/labeled/BGL/BGL.log \
      bgl \
      --timestamp-regex '(?P<ts>\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+)' \
      --timestamp-format '%Y-%m-%d-%H.%M.%S.%f' \
      --speed 10

    # Example: Apache dataset
    python replay/send_logs.py \
      ../log-anomaly-analysis/data/raw/loghub_full/Apache/Apache_full.log \
      apache \
      --timestamp-regex '\[(?P<ts>[^]]+)]' \
      --timestamp-format '%a %b %d %H:%M:%S %Y' \
      --speed 10
    ```

The script streams logs over TCP as JSON; Logstash enriches and forwards them
to the Drain3 service, which extracts templates and pushes structured events
into Loki. Visit Grafana (<http://localhost:3000>, admin/admin) and open the
"Structured Logs" dashboard. Use the Datastream and Event Template filters to
zoom in on a feed, inspect log volume over time, and review the top templates
seen in the selected time window.
