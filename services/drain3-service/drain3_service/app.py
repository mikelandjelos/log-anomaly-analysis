from __future__ import annotations

from pathlib import Path
from typing import Dict

import json
import os
import time
from datetime import datetime, timezone

import httpx
import polars as pl
from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel

from log_anomaly_analysis.core.components.parsing import TemplateParserComponent
from log_anomaly_analysis.core.components.preprocessing import PreprocessorComponent
from log_anomaly_analysis.core.config.loader import load_config


class IngestRequest(BaseModel):
    message: str
    datastream: str
    timestamp: str | None = None


def build_components(config_path: Path) -> Dict[str, object]:
    cfg = load_config(str(config_path))
    preprocessor = PreprocessorComponent(cfg.preprocessing.__dict__)
    parser = TemplateParserComponent(cfg.parsing.__dict__)
    return {"preprocessor": preprocessor, "parser": parser}


app = FastAPI(title="Drain3 Service")
component_cache: Dict[str, Dict[str, object]] = {}
CONFIG_DIR = Path("/configs")
LOKI_URL = os.environ.get("LOKI_URL")


def resolve_config(stream: str) -> Path | None:
    direct = CONFIG_DIR / f"{stream}.yaml"
    if direct.exists():
        return direct
    # try case-insensitive match
    candidates = list(CONFIG_DIR.glob(f"{stream}*.yaml"))
    if candidates:
        return candidates[0]
    candidates = list(CONFIG_DIR.glob(f"{stream.upper()}*.yaml"))
    if candidates:
        return candidates[0]
    candidates = list(CONFIG_DIR.glob(f"{stream.lower()}*.yaml"))
    if candidates:
        return candidates[0]
    return None


def get_components(stream: str) -> Dict[str, object]:
    if stream not in component_cache:
        config_file = resolve_config(stream)
        if config_file is None:
            raise HTTPException(
                status_code=404, detail=f"Unknown datastream '{stream}'"
            )
        component_cache[stream] = build_components(config_file)
        logger.info(
            "Loaded parser/preprocessor for %s from %s", stream, config_file.name
        )
    return component_cache[stream]


@app.post("/ingest")
async def ingest(payload: IngestRequest):
    comps = get_components(payload.datastream)
    preprocessor: PreprocessorComponent = comps["preprocessor"]  # type: ignore[assignment]
    parser: TemplateParserComponent = comps["parser"]  # type: ignore[assignment]

    processed = preprocessor.process(pl.DataFrame({"raw_log": [payload.message]}))
    if processed.is_empty():
        return {"status": "skipped"}

    templates = parser.process(processed)
    if templates.is_empty():
        return {"status": "no_template"}

    record = templates.to_dicts()[0]
    record["datastream"] = payload.datastream
    if LOKI_URL:
        await push_to_loki(record)
    return {"status": "ok", "record": record}


@app.get("/healthz")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


async def push_to_loki(record: Dict[str, object]) -> None:
    labels: Dict[str, str] = {"datastream": str(record.get("datastream", "unknown"))}

    def _sanitize(label: str) -> str:
        cleaned = label.replace(" ", "_").replace("-", "_")
        if not cleaned:
            cleaned = "field"
        if cleaned[0].isdigit():
            cleaned = f"_{cleaned}"
        return cleaned

    for key, value in record.items():
        if key in {"datastream", "Content", "Parameters"}:
            continue
        if isinstance(value, (str, int, float, bool)) and value != "":
            labels[_sanitize(key)] = str(value)
    if "template_id" not in labels and "TemplateId" in record:
        labels["template_id"] = str(record["TemplateId"])
    if "event_template" not in labels and "EventTemplate" in record:
        labels["event_template"] = str(record["EventTemplate"])

    ts_value = record.get("Timestamp")

    def to_ns(val: object) -> str:
        if isinstance(val, (int, float)):
            return str(int(float(val) * 1e9))
        if isinstance(val, str) and val:
            candidate = val.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(candidate)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return str(int(dt.timestamp() * 1e9))
            except ValueError:
                pass
        return str(time.time_ns())

    timestamp_ns = to_ns(ts_value)

    payload = {
        "streams": [
            {
                "stream": labels,
                "values": [
                    [
                        timestamp_ns,
                        json.dumps(record, default=str),
                    ]
                ],
            }
        ]
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(LOKI_URL, json=payload)
            resp.raise_for_status()
    except Exception as exc:  # pragma: no cover - logging path
        logger.warning("Failed to push record to Loki: %s", exc)
