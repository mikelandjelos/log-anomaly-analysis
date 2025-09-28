# Demo Streaming System

This directory contains the docker-compose scaffolding for the streaming demo
stack.  It intentionally has **no** Python package of its own; all application
code lives in sibling projects (e.g. the Drain3 FastAPI service and the
`log-anomaly-analysis` library).  The compose stack stitches them together.

## Layout

```
demo-system/
├── docker-compose.yml        # Entry point for the stack
├── logstash/                 # Pipeline definitions and configs
├── loki/                     # Loki configuration / provisioning
├── grafana/                  # Datasources + dashboards
├── replay/                   # Log replay utilities for demo datasets
└── README.md                 # This file
```

The Drain3/Preprocessing service is packaged under `services/drain3-service`
and referenced from `docker-compose.yml`.  Adjust volume mounts and environment
variables there when wiring in new datastreams.
