from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import httpx
import polars as pl
import websockets
from fastapi import FastAPI
import yaml
from log_anomaly_analysis.realtime import Accumulator, StreamingPCAScorer
from loguru import logger
from pydantic import BaseModel

CONFIG_DIR = Path("/configs")


def _bool_env(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "on"}


class Stats(BaseModel):
    total_logs: int
    total_windows: int
    total_anomalies: int
    warmed: bool
    k: int
    spe_threshold: Optional[float] = None
    t2_threshold: Optional[float] = None


LOKI_BASE_URL = os.environ.get("LOKI_BASE_URL", "http://loki:3100")
LOKI_PUSH_URL = f"{LOKI_BASE_URL.rstrip('/')}/loki/api/v1/push"
LOKI_QUERY = os.environ.get("LOKI_QUERY", '{datastream="bgl"}')
DATASTREAM = os.environ.get("DATASTREAM", "bgl")

# Push timestamp mode: "ingest" (now) or "event" (WindowEnd)
PUSH_TS_MODE = os.environ.get("PUSH_TS_MODE", "ingest").strip().lower()

TAIL_LIMIT = int(os.environ.get("LOKI_TAIL_LIMIT", "500"))
TAIL_DELAY_FOR = int(os.environ.get("LOKI_TAIL_DELAY_FOR", "1"))


app = FastAPI(title="Streaming Analytics Service")


class ServiceState:
    def __init__(self) -> None:
        acc_cfg, scorer_cfg = self._load_runtime_configs(DATASTREAM)
        self.acc = Accumulator(acc_cfg)
        self.scorer = StreamingPCAScorer(scorer_cfg)
        self.total_logs = 0
        self.total_windows = 0
        self.total_anomalies = 0
        self.stop_event = asyncio.Event()

    @staticmethod
    def _resolve_config(stream: str) -> Optional[Path]:
        direct = CONFIG_DIR / f"{stream}.yaml"
        if direct.exists():
            return direct
        # try case-insensitive / prefix match
        for pat in (f"{stream}*.yaml", f"{stream.upper()}*.yaml", f"{stream.lower()}*.yaml"):
            candidates = list(CONFIG_DIR.glob(pat))
            if candidates:
                return candidates[0]
        return None

    @classmethod
    def _load_runtime_configs(cls, stream: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
        cfg_file = cls._resolve_config(stream)
        if cfg_file and cfg_file.exists():
            try:
                with cfg_file.open("r", encoding="utf-8") as fh:
                    raw = yaml.safe_load(fh) or {}
                acc_cfg = dict(raw.get("accumulator", {}))
                scorer_cfg = dict(raw.get("scorer", {}))
                logger.info(
                    "Loaded analytics config for '%s' from %s", stream, cfg_file.name
                )
                return acc_cfg, scorer_cfg
            except Exception as exc:
                logger.warning(
                    "Failed to load analytics config for '%s' at %s: %s. Using defaults.",
                    stream,
                    cfg_file,
                    exc,
                )

        # Fallback to library defaults (aligned with notebook)
        logger.warning(
            "Analytics config for '%s' not found under %s; using defaults.",
            stream,
            CONFIG_DIR,
        )
        return {}, {}

    def stats(self) -> Stats:
        spe_th = getattr(self.scorer, "spe_threshold", float("inf"))
        t2_th = getattr(self.scorer, "_t2_threshold", None)
        if isinstance(spe_th, float) and not math.isfinite(spe_th):
            spe_th = None
        if isinstance(t2_th, float) and not math.isfinite(t2_th):
            t2_th = None
        return Stats(
            total_logs=self.total_logs,
            total_windows=self.total_windows,
            total_anomalies=self.total_anomalies,
            warmed=self.scorer.warmed,
            k=self.scorer.k,
            spe_threshold=spe_th,  # type: ignore[arg-type]
            t2_threshold=t2_th,  # type: ignore[arg-type]
        )


state = ServiceState()


@app.get("/healthz")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> Stats:
    return state.stats()


# convenience alias
@app.get("/status")
async def status_alias() -> Stats:
    return state.stats()


def _mk_ws_url(base_url: str, query: str, limit: int, delay_for: int) -> str:
    base = base_url.rstrip("/")
    # ws scheme from http(s)
    if base.startswith("https://"):
        ws_base = "wss://" + base[len("https://") :]
    elif base.startswith("http://"):
        ws_base = "ws://" + base[len("http://") :]
    else:
        ws_base = base
    params = {"query": query, "limit": str(limit), "delay_for": str(delay_for)}
    return f"{ws_base}/loki/api/v1/tail?{urlencode(params)}"


def _ns_to_dt(ns_str: str) -> datetime:
    return datetime.fromtimestamp(int(ns_str) / 1e9, tz=timezone.utc)


def _parse_event_ts(val: Any) -> Optional[datetime]:
    """Parse event-time from record field value.

    Accepts ISO strings (with or without trailing Z), numeric seconds, or datetime objects.
    Returns timezone-aware UTC datetimes, or None on failure.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(float(val), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(val, str) and val:
        candidate = val.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None
    return None


def _ts_ns_for_push(event_ts: Any) -> str:
    """Choose the sample timestamp to use when pushing to Loki.

    - ingest: use current time (avoids Loki "too old" rejections)
    - event: use the event window end time
    """
    if PUSH_TS_MODE == "event":
        ts = event_ts
        if isinstance(ts, datetime):
            return str(int(ts.timestamp() * 1e9))
        try:
            dt = datetime.fromisoformat(str(ts))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return str(int(dt.timestamp() * 1e9))
        except Exception:
            pass
    # Default: ingestion time (now)
    return str(int(datetime.now(tz=timezone.utc).timestamp() * 1e9))


async def _push_anomalies_to_loki(anom_df: pl.DataFrame) -> None:
    if anom_df.is_empty():
        return
    # Build a single stream with labels {datastream, type=anomaly}
    labels = {"datastream": DATASTREAM, "type": "anomaly"}
    values = []

    def _json_default(obj: Any) -> str:
        if isinstance(obj, datetime):
            dt = obj.astimezone(timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        return str(obj)

    for row in anom_df.iter_rows(named=True):
        payload: Dict[str, Any] = {
            "Window": row.get("Window"),
            "WindowStart": row.get("WindowStart"),
            "WindowEnd": row.get("WindowEnd"),
            "LogCount": int(row.get("LogCount") or 0),
            "AnomalyScore_SPE": float(row.get("AnomalyScore_SPE") or 0.0),
            "IsAnomaly": bool(row.get("IsAnomaly") or False),
        }
        if "AnomalyScore_T2" in anom_df.columns:
            try:
                payload["AnomalyScore_T2"] = float(row.get("AnomalyScore_T2") or 0.0)
            except Exception:
                pass
        # include thresholds if present for plotting
        if "SPE_threshold" in anom_df.columns:
            try:
                payload["SPE_threshold"] = float(row.get("SPE_threshold") or 0.0)
            except Exception:
                pass
        if "T2_threshold" in anom_df.columns:
            try:
                payload["T2_threshold"] = float(row.get("T2_threshold") or 0.0)
            except Exception:
                pass
        # Use WindowEnd as the event timestamp
        ns = _ts_ns_for_push(row.get("WindowEnd"))
        values.append([ns, json.dumps(payload, default=_json_default)])

    body = {"streams": [{"stream": labels, "values": values}]}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(LOKI_PUSH_URL, json=body)
            if resp.status_code >= 400:
                logger.warning("Loki push failed ({}): {}", resp.status_code, resp.text)
    except Exception as exc:
        logger.warning("Failed to push anomalies to Loki: {}", exc)


async def _push_scores_to_loki(score_df: pl.DataFrame) -> None:
    """Push per-window scores (timeline) to Loki.

    Labels: {datastream, type=score}
    Line JSON: Window, WindowStart, WindowEnd, LogCount, AnomalyScore_SPE, (optional AnomalyScore_T2), IsAnomaly
    Sample ts: WindowEnd (event-time)
    """
    if score_df.is_empty():
        return

    labels = {"datastream": DATASTREAM, "type": "score"}
    values = []

    def _json_default(obj: Any) -> str:
        if isinstance(obj, datetime):
            dt = obj.astimezone(timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        return str(obj)

    for row in score_df.iter_rows(named=True):
        payload: Dict[str, Any] = {
            "Window": row.get("Window"),
            "WindowStart": row.get("WindowStart"),
            "WindowEnd": row.get("WindowEnd"),
            "LogCount": int(row.get("LogCount") or 0),
            "AnomalyScore_SPE": float(row.get("AnomalyScore_SPE") or 0.0),
            "IsAnomaly": bool(row.get("IsAnomaly") or False),
        }
        # optional T2 score
        if "AnomalyScore_T2" in score_df.columns:
            try:
                payload["AnomalyScore_T2"] = float(row.get("AnomalyScore_T2") or 0.0)
            except Exception:
                pass
        # include thresholds if present for plotting
        if "SPE_threshold" in score_df.columns:
            try:
                thr = row.get("SPE_threshold")
                payload["SPE_threshold"] = float(thr) if thr is not None else None
            except Exception:
                pass
        if "T2_threshold" in score_df.columns:
            try:
                thr = row.get("T2_threshold")
                payload["T2_threshold"] = float(thr) if thr is not None else None
            except Exception:
                pass

        ns = _ts_ns_for_push(row.get("WindowEnd"))
        values.append([ns, json.dumps(payload, default=_json_default)])

    body = {"streams": [{"stream": labels, "values": values}]}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(LOKI_PUSH_URL, json=body)
            if resp.status_code >= 400:
                logger.warning(
                    "Loki push (scores) failed ({}): {}", resp.status_code, resp.text
                )
    except Exception as exc:
        logger.warning("Failed to push scores to Loki: {}", exc)


async def _tail_loop() -> None:
    ws_url = _mk_ws_url(LOKI_BASE_URL, LOKI_QUERY, TAIL_LIMIT, TAIL_DELAY_FOR)
    logger.info("Connecting to Loki tail: {}", ws_url)
    backoff = 1.0
    while not state.stop_event.is_set():
        try:
            async with websockets.connect(ws_url, max_size=8 * 1024 * 1024) as ws:
                logger.info("Tail connected")
                backoff = 1.0
                while not state.stop_event.is_set():
                    msg = await ws.recv()
                    data = json.loads(msg)
                    streams = data.get("streams", [])
                    ts_list: list[datetime] = []
                    tpl_list: list[object] = []
                    for st in streams:
                        values = st.get("values") or []
                        for ts_ns, line, *_rest in values:
                            try:
                                rec = json.loads(line)
                            except Exception:
                                rec = {"raw": line}
                            # Prefer record event-time if present to avoid relying on Loki's sample ts
                            rec_ts = (
                                rec.get("Timestamp")
                                or rec.get("original_timestamp")
                                or rec.get("OriginalTimestamp")
                            )
                            ts = _parse_event_ts(rec_ts) or _ns_to_dt(ts_ns)
                            tpl = rec.get("TemplateId")
                            if tpl is None:
                                tpl = rec.get("EventTemplate")
                            if ts is None or tpl is None:
                                continue
                            ts_list.append(ts)
                            tpl_list.append(tpl)
                            state.total_logs += 1

                    if ts_list:
                        state.acc.update_chunk(ts_list, tpl_list)
                        win_df, feat_df = state.acc.finalize_ready()
                        if not win_df.is_empty():
                            state.total_windows += len(win_df)
                        if not feat_df.is_empty():
                            cols = [str(i) for i in range(state.acc.B)]
                            try:
                                X = (
                                    feat_df.select(cols)
                                    .to_numpy()
                                    .astype("float32", copy=False)
                                )
                            except Exception:
                                X = pl.DataFrame().to_numpy()
                            if X.size > 0:
                                if not state.scorer.warmed:
                                    state.scorer.partial_fit_warmup(X)
                                else:
                                    # Score all windows in this flush
                                    # Gather thresholds once per flush
                                    spe_thr = getattr(
                                        state.scorer, "spe_threshold", None
                                    )
                                    t2_thr = getattr(
                                        state.scorer, "_t2_threshold", None
                                    )
                                    try:
                                        S, T2, yhat = state.scorer.score(X)  # type: ignore[misc]
                                        n = len(S)
                                        result_dict: Dict[str, Any] = {
                                            "Window": feat_df["Window"],
                                            "WindowStart": feat_df["WindowStart"],
                                            "WindowEnd": feat_df["WindowEnd"],
                                            "LogCount": feat_df["LogCount"],
                                            "AnomalyScore_SPE": S,
                                            "AnomalyScore_T2": T2,
                                            "SPE_threshold": (
                                                [float(spe_thr)] * n
                                                if isinstance(spe_thr, float)
                                                else [None] * n
                                            ),
                                            "T2_threshold": (
                                                [float(t2_thr)] * n
                                                if isinstance(t2_thr, float)
                                                else [None] * n
                                            ),
                                            "IsAnomaly": yhat,
                                        }
                                    except Exception:
                                        S, yhat = state.scorer.score(X)  # type: ignore[misc]
                                        n = len(S)
                                        result_dict = {
                                            "Window": feat_df["Window"],
                                            "WindowStart": feat_df["WindowStart"],
                                            "WindowEnd": feat_df["WindowEnd"],
                                            "LogCount": feat_df["LogCount"],
                                            "AnomalyScore_SPE": S,
                                            "SPE_threshold": (
                                                [float(spe_thr)] * n
                                                if isinstance(spe_thr, float)
                                                else [None] * n
                                            ),
                                            "IsAnomaly": yhat,
                                        }
                                    result_df = pl.DataFrame(result_dict)
                                    # Push timeline scores for every window after warmup
                                    await _push_scores_to_loki(result_df)
                                    # Also push anomalies-only stream for dashboards
                                    if "IsAnomaly" in result_df.columns:
                                        anom_only = result_df.filter(
                                            pl.col("IsAnomaly") == True
                                        )
                                        if not anom_only.is_empty():
                                            state.total_anomalies += int(
                                                anom_only["IsAnomaly"].sum()
                                            )
                                            await _push_anomalies_to_loki(anom_only)

        except Exception as exc:
            logger.warning("Tail connection error: {}", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


@app.on_event("startup")
async def on_start() -> None:
    loop = asyncio.get_event_loop()
    loop.create_task(_tail_loop())
    logger.info("Streaming loop started")


@app.on_event("shutdown")
async def on_stop() -> None:
    state.stop_event.set()
    logger.info("Streaming loop stopping")
