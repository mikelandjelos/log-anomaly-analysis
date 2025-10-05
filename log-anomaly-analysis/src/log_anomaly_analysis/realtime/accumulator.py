"""
Tumbling window accumulator with hashed features.

Design goals:
- Epoch-aligned fixed windows (e.g., 10m), with allowed lateness.
- Dense hashed feature vector per window (feature hashing on TemplateId/EventTemplate).
- Optional signed hashing and per-window normalization by log count.

This mirrors the notebook logic from examples/notebooks/bgl_realtime_streaming.ipynb
but removes ground-truth labels and focuses on production streaming usage.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Sequence

import numpy as np
import polars as pl


def _parse_iso8601(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        # Handle trailing 'Z'
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        return dt
    except Exception:
        return None


def _parse_duration(spec: str) -> timedelta:
    if not spec:
        return timedelta(0)
    try:
        unit = spec[-1].lower()
        val = float(spec[:-1])
    except Exception:
        # fallback: interpret as seconds
        return timedelta(seconds=float(spec))
    if unit == "s":
        return timedelta(seconds=val)
    if unit == "m":
        return timedelta(minutes=val)
    if unit == "h":
        return timedelta(hours=val)
    if unit == "d":
        return timedelta(days=val)
    return timedelta(seconds=val)


def _stable_hash(text: str, seed: int = 0) -> int:
    h = hashlib.blake2b(text.encode("utf-8"), digest_size=8, key=bytes([seed]))
    return int.from_bytes(h.digest(), "little", signed=False)


@dataclass
class AccumulatorConfig:
    window_size: str = "10m"
    allowed_lateness: str = "0s"
    hash_bins: int = 4096
    hash_signed: bool = True


class Accumulator:
    """Epoch-aligned tumbling window accumulator with hashed features.

    - update_chunk(ts_iter, template_iter): feed new parsed logs
    - finalize_ready(): close and emit windows complete per allowed lateness

    Returned dataframes:
      windowed_df: [Window, WindowStart, WindowEnd, LogCount]
      features_df: windowed metadata + string columns "0".."B-1" with counts
                   (optionally normalized by LogCount)
    """

    def __init__(self, config: AccumulatorConfig | Dict | None = None):
        if config is None:
            config = AccumulatorConfig()
        elif isinstance(config, dict):
            config = AccumulatorConfig(**config)

        self.wtd = _parse_duration(config.window_size)
        self.ltd = _parse_duration(config.allowed_lateness)
        self.B = int(config.hash_bins)
        self.signed = bool(config.hash_signed)
        # Always use raw counts; no per-window normalization here.

        self._bins: Dict[datetime, np.ndarray] = {}
        self._counts: Dict[datetime, int] = {}
        self._max_ts: datetime | None = None

    def _to_dt(self, ts: datetime | str | None) -> datetime | None:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        return _parse_iso8601(ts)

    def _win_key(self, ts: datetime) -> datetime:
        # epoch-aligned tumbling windows
        # Preserve naive vs aware consistently to avoid mixing types
        tz = ts.tzinfo
        epoch = datetime(1970, 1, 1, tzinfo=tz) if tz is not None else datetime(1970, 1, 1)
        s = (ts - epoch).total_seconds()
        size = self.wtd.total_seconds() or 1.0
        return epoch + timedelta(seconds=int(s // size) * size)

    def update_chunk(
        self,
        ts_iter: Sequence[datetime | str | None] | Iterable[datetime | str | None],
        template_iter: Sequence[object | None] | Iterable[object | None],
    ) -> None:
        for ts_raw, tpl in zip(ts_iter, template_iter):
            ts = self._to_dt(ts_raw)
            if ts is None:
                continue

            if self._max_ts is None or ts > self._max_ts:
                self._max_ts = ts

            k = self._win_key(ts)
            if k not in self._bins:
                self._bins[k] = np.zeros(self.B, dtype=np.int32)
                self._counts[k] = 0

            if tpl is not None:
                idx = _stable_hash(str(tpl), 0) % self.B
                if self.signed and (_stable_hash(str(tpl), 7) & 1):
                    self._bins[k][idx] -= 1
                else:
                    self._bins[k][idx] += 1

            self._counts[k] += 1

    def _should_close(self, k: datetime) -> bool:
        if self._max_ts is None:
            return False
        return k + self.wtd <= self._max_ts - self.ltd

    def finalize_ready(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Close and emit all windows complete per allowed lateness.

        Returns (windowed_df, features_df). Empty frames if none are ready.
        """
        ready = [k for k in self._bins.keys() if self._should_close(k)]
        if not ready:
            return pl.DataFrame(), pl.DataFrame()

        ready.sort()
        win_rows: List[Dict[str, object]] = []
        feat_rows: List[Dict[str, object]] = []

        for k in ready:
            bins = self._bins.pop(k)
            cnt = self._counts.pop(k, 0)

            row_meta = {
                "Window": k,
                "WindowStart": k,
                "WindowEnd": k + self.wtd,
                "LogCount": cnt,
            }
            win_rows.append(row_meta)

            # materialize hashed counts (raw counts)
            vec = bins.astype(np.float32, copy=False)

            row_feat = dict(row_meta)
            for i, v in enumerate(vec.tolist()):
                row_feat[str(i)] = v
            feat_rows.append(row_feat)

        windowed = pl.DataFrame(win_rows) if win_rows else pl.DataFrame()
        features = pl.DataFrame(feat_rows) if feat_rows else pl.DataFrame()
        return windowed, features

    def finalize_all(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Force-close and emit all windows regardless of lateness.

        Useful at end-of-stream to flush the remainder.
        """
        if not self._bins:
            return pl.DataFrame(), pl.DataFrame()

        ready = sorted(self._bins.keys())
        win_rows: List[Dict[str, object]] = []
        feat_rows: List[Dict[str, object]] = []

        for k in ready:
            bins = self._bins.pop(k)
            cnt = self._counts.pop(k, 0)

            meta = {
                "Window": k,
                "WindowStart": k,
                "WindowEnd": k + self.wtd,
                "LogCount": cnt,
            }
            win_rows.append(meta)

            vec = bins.astype(np.float32, copy=False)
            row = dict(meta)
            for i, v in enumerate(vec.tolist()):
                row[str(i)] = v
            feat_rows.append(row)

        windowed = pl.DataFrame(win_rows) if win_rows else pl.DataFrame()
        features = pl.DataFrame(feat_rows) if feat_rows else pl.DataFrame()
        return windowed, features
