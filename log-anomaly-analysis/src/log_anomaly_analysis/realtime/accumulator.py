"""
Tumbling window accumulator with hashed features.

Design goals:
- Epoch-aligned fixed windows (e.g., 10m), with allowed lateness.
- Dense hashed feature vector per window (feature hashing on TemplateId/EventTemplate).
- Optional signed hashing and per-window normalization by log count (frequency) or L2.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import polars as pl


def _parse_iso8601(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _parse_duration(spec: str) -> timedelta:
    if not spec:
        return timedelta(0)
    try:
        unit = spec[-1].lower()
        val = float(spec[:-1])
    except Exception:
        return timedelta(seconds=float(spec))
    return {
        "s": timedelta(seconds=val),
        "m": timedelta(minutes=val),
        "h": timedelta(hours=val),
        "d": timedelta(days=val),
    }.get(unit, timedelta(seconds=val))


def _stable_hash(text: str, seed: int = 0) -> int:
    h = hashlib.blake2b(text.encode("utf-8"), digest_size=8, key=bytes([seed]))
    return int.from_bytes(h.digest(), "little", signed=False)


@dataclass
class AccumulatorConfig:
    window_size: str = "10m"
    allowed_lateness: str = "0s"
    hash_bins: int = 4096
    hash_signed: bool = True
    # NEW:
    normalize: str = "none"  # "none" | "freq" | "l2"
    add_volume_feature: bool = False  # add __log_volume column


class Accumulator:
    """Tumbling window accumulator with hashed features.

    - update_chunk(ts_iter, template_iter): feed new parsed logs
    - finalize_ready(): close and emit windows complete per allowed lateness

    Returns (windowed_df, features_df):
      windowed_df: [Window, WindowStart, WindowEnd, LogCount]
      features_df: window metadata + string columns "0".."B-1" (normalized per config)
                   and optional "__log_volume" if add_volume_feature=True
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
        self.normalize = config.normalize.lower()
        assert self.normalize in {
            "none",
            "freq",
            "l2",
        }, "normalize must be none|freq|l2"
        self.add_volume_feature = bool(config.add_volume_feature)

        self._bins: Dict[datetime, np.ndarray] = {}
        self._counts: Dict[datetime, int] = {}
        self._max_ts: Optional[datetime] = None

    def _to_dt(self, ts: datetime | str | None) -> Optional[datetime]:
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        return _parse_iso8601(ts)

    def _win_key(self, ts: datetime) -> datetime:
        tz = ts.tzinfo
        epoch = datetime(1970, 1, 1, tzinfo=tz) if tz else datetime(1970, 1, 1)
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

    def _normalize_row(self, vec: np.ndarray, log_count: int) -> np.ndarray:
        v = vec.astype(np.float32, copy=False)
        if self.normalize == "freq":
            den = float(max(log_count, 1))
            v = v / den
        elif self.normalize == "l2":
            nrm = float(np.linalg.norm(v))
            if nrm > 0:
                v = v / nrm
        # "none": return raw counts
        return v

    def finalize_ready(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        ready = [k for k in self._bins.keys() if self._should_close(k)]
        if not ready:
            return pl.DataFrame(), pl.DataFrame()

        ready.sort()
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

            vec = self._normalize_row(bins, cnt)
            row = dict(meta)
            # hashed features
            vals = vec.tolist()
            for i, v in enumerate(vals):
                row[str(i)] = v
            # optional volume channel
            if self.add_volume_feature:
                row["__log_volume"] = float(np.log1p(cnt))

            feat_rows.append(row)

        windowed = pl.DataFrame(win_rows) if win_rows else pl.DataFrame()
        features = pl.DataFrame(feat_rows) if feat_rows else pl.DataFrame()
        return windowed, features

    def finalize_all(self) -> tuple[pl.DataFrame, pl.DataFrame]:
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

            vec = self._normalize_row(bins, cnt)
            row = dict(meta)
            for i, v in enumerate(vec.tolist()):
                row[str(i)] = v
            if self.add_volume_feature:
                row["__log_volume"] = float(np.log1p(cnt))
            feat_rows.append(row)

        windowed = pl.DataFrame(win_rows) if win_rows else pl.DataFrame()
        features = pl.DataFrame(feat_rows) if feat_rows else pl.DataFrame()
        return windowed, features
