from __future__ import annotations

from pathlib import Path
from typing import Dict

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
    return {"status": "ok", "record": record}


@app.get("/healthz")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
