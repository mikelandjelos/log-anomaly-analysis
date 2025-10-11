from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import time_ns
from typing import Any, Dict

import httpx
import polars as pl
from fastapi import FastAPI, HTTPException
from log_anomaly_analysis.core.components.parsing import TemplateParserComponent
from log_anomaly_analysis.core.components.preprocessing import PreprocessorComponent
from log_anomaly_analysis.core.config.loader import load_config
from loguru import logger
from pydantic import BaseModel


class IngestRequest(BaseModel):
    message: str
    datastream: str
    timestamp: str | None = None


def build_components(config_path: Path) -> Dict[str, object]:
    cfg = load_config(str(config_path))
    preprocessor = PreprocessorComponent(cfg.preprocessing.__dict__)
    parser = TemplateParserComponent(cfg.parsing.__dict__)
    return {"preprocessor": preprocessor, "parser": parser}


LOGURU_LEVEL = os.environ.get("LOGURU_LEVEL", "WARNING").upper()
try:
    logger.remove()
except Exception:
    pass
logger.add(
    sys.stderr, level=LOGURU_LEVEL, enqueue=True, backtrace=False, diagnose=False
)

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


async def push_to_loki(record: Dict[str, Any]) -> None:
    record = dict(record)
    record.pop("LineNumber", None)

    labels: Dict[str, str] = {
        "datastream": str(record.get("datastream", "unknown")),
    }

    def _label_value(value: Any) -> str:
        text = str(value)
        text = text.replace("\\", " ")
        text = text.replace("\n", " ")
        text = text.replace('"', "'")
        text = " ".join(text.split())
        return text[:250]

    if record.get("EventTemplate"):
        labels["EventTemplate"] = _label_value(record["EventTemplate"])
    if record.get("TemplateId") is not None:
        labels["TemplateId"] = str(record["TemplateId"])

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
        return str(time_ns())

    def serialize(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        if isinstance(value, (list, tuple, set)):
            return json.dumps(list(value), default=str)
        if isinstance(value, dict):
            return json.dumps(value, default=str)
        return str(value)

    structured_metadata: Dict[str, str] = {}
    for key, value in record.items():
        if key in {"datastream", "EventTemplate", "TemplateId"}:
            continue
        serialized = serialize(value)
        if serialized is not None:
            structured_metadata[key] = serialized

    def json_default(obj: Any) -> str:
        if isinstance(obj, datetime):
            return obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(obj)

    line_payload = json.dumps(record, default=json_default)
    ts_value = record.get("Timestamp")
    timestamp_ns = to_ns(ts_value)

    entry: list[Any] = [timestamp_ns, line_payload]
    if structured_metadata:
        entry.append(structured_metadata)

    payload = {
        "streams": [
            {
                "stream": labels,
                "values": [entry],
            }
        ]
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(LOKI_URL, json=payload)
            if resp.status_code >= 400:
                logger.warning(f"Loki push failed ({resp.status_code}): {resp.text}")
            resp.raise_for_status()
    except Exception as exc:  # pragma: no cover - logging path
        logger.warning("Failed to push record to Loki: %s", exc)
