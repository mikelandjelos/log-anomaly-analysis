"""
Windowing component with multiple strategies
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import polars as pl
from loguru import logger

from .base import BaseComponent


class WindowingComponent(BaseComponent):
    """Configurable windowing component with multiple strategies"""

    def _validate_config(self):
        if "strategy" not in self.config:
            raise ValueError("Windowing strategy must be specified")

        valid_strategies = ["fixed", "sliding", "session", "adaptive"]
        if self.config["strategy"] not in valid_strategies:
            raise ValueError(f"Invalid strategy. Must be one of: {valid_strategies}")

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Apply windowing strategy to the data"""
        if data.is_empty():
            return pl.DataFrame()

        strategy = self.config["strategy"]
        logger.info(f"Applying {strategy} windowing strategy")

        if strategy == "fixed":
            return self._fixed_time_window(data)
        elif strategy == "sliding":
            return self._sliding_time_window(data)
        elif strategy == "session":
            return self._session_window(data)
        elif strategy == "adaptive":
            return self._adaptive_volume_window(data)
        else:
            raise ValueError(f"Unknown windowing strategy: {strategy}")

    def _fixed_time_window(self, data: pl.DataFrame) -> pl.DataFrame:
        """Fixed time windows"""
        window_size = self.config.get("window_size", "5m")

        if "Timestamp" not in data.columns:
            raise ValueError("Fixed time windowing requires Timestamp column")

        windowed = (
            data.with_columns(
                [pl.col("Timestamp").dt.truncate(window_size).alias("Window")]
            )
            .group_by("Window")
            .agg(
                [
                    pl.col("EventTemplate").alias("EventTemplates"),
                    pl.col("TemplateId").alias("TemplateIds"),
                    (
                        pl.col("LogLevel").alias("LogLevels")
                        if "LogLevel" in data.columns
                        else pl.lit([]).alias("LogLevels")
                    ),
                    pl.col("Timestamp").min().alias("WindowStart"),
                    pl.col("Timestamp").max().alias("WindowEnd"),
                    pl.len().alias("LogCount"),
                ]
            )
            .sort("Window")
        )

        return windowed

    def _sliding_time_window(self, data: pl.DataFrame) -> pl.DataFrame:
        """Sliding time windows with overlap"""
        window_size = self.config.get("window_size", "5m")
        step_size = self.config.get("step_size", "1m")

        if data.is_empty():
            return pl.DataFrame()

        window_seconds = self._parse_duration(window_size)
        step_seconds = self._parse_duration(step_size)

        if window_seconds <= 0:
            raise ValueError("window_size must represent a duration greater than zero")

        if step_seconds <= 0:
            raise ValueError("step_size must represent a duration greater than zero")

        start_time: Optional[datetime] = data["Timestamp"].min()
        end_time: Optional[datetime] = data["Timestamp"].max()

        if start_time is None or end_time is None:
            return pl.DataFrame()

        current_start = start_time
        current_end_bound = end_time

        windows = []
        window_delta = timedelta(seconds=window_seconds)
        step_delta = timedelta(seconds=step_seconds)

        while current_start <= current_end_bound:
            current_end = current_start + window_delta

            window_data = data.filter(
                (pl.col("Timestamp") >= current_start)
                & (pl.col("Timestamp") < current_end)
            )

            if not window_data.is_empty():
                window_info = {
                    "Window": current_start,
                    "WindowStart": current_start,
                    "WindowEnd": current_end,
                    "EventTemplates": window_data["EventTemplate"].to_list(),
                    "TemplateIds": window_data["TemplateId"].to_list(),
                    "LogLevels": (
                        window_data["LogLevel"].to_list()
                        if "LogLevel" in window_data.columns
                        else []
                    ),
                    "LogCount": len(window_data),
                }
                windows.append(window_info)

            current_start = current_start + step_delta

        return pl.DataFrame(windows) if windows else pl.DataFrame()

    def _session_window(self, data: pl.DataFrame) -> pl.DataFrame:
        """Session-based windows using inactivity gaps"""
        session_timeout = self.config.get("session_timeout", "30m")
        timeout_seconds = self._parse_duration(session_timeout)

        if data.is_empty():
            return pl.DataFrame()

        data_with_sessions = (
            data.sort("Timestamp")
            .with_columns(
                [
                    (
                        pl.col("Timestamp").diff().dt.total_seconds() > timeout_seconds
                    ).alias("NewSession")
                ]
            )
            .with_columns(
                [pl.col("NewSession").fill_null(True).cum_sum().alias("SessionId")]
            )
        )

        sessions = (
            data_with_sessions.group_by("SessionId")
            .agg(
                [
                    pl.col("Timestamp").min().alias("WindowStart"),
                    pl.col("Timestamp").max().alias("WindowEnd"),
                    pl.len().alias("LogCount"),
                    pl.col("EventTemplate").alias("EventTemplates"),
                    pl.col("TemplateId").alias("TemplateIds"),
                    (
                        pl.col("LogLevel").alias("LogLevels")
                        if "LogLevel" in data.columns
                        else pl.lit([]).alias("LogLevels")
                    ),
                ]
            )
            .with_columns([pl.col("WindowStart").alias("Window")])
            .sort("Window")
        )

        return sessions

    def _adaptive_volume_window(self, data: pl.DataFrame) -> pl.DataFrame:
        """Adaptive windows based on log volume"""
        target_logs = self.config.get("target_logs_per_window", 100)
        min_window_seconds = self._parse_duration(
            self.config.get("min_window_size", "1m")
        )
        max_window_seconds = self._parse_duration(
            self.config.get("max_window_size", "1h")
        )

        if data.is_empty():
            return pl.DataFrame()

        data_sorted = data.sort("Timestamp")
        if data_sorted.is_empty():
            return pl.DataFrame()
        windows = []
        start_idx = 0

        while start_idx < len(data_sorted):
            end_idx = min(start_idx + target_logs, len(data_sorted))

            window_data = data_sorted[start_idx:end_idx]
            if window_data.is_empty():
                break

            window_start = window_data["Timestamp"][0]
            window_end = window_data["Timestamp"][-1]

            if window_start is None or window_end is None:
                start_idx = end_idx
                continue

            duration_seconds = (window_end - window_start).total_seconds()

            if duration_seconds < min_window_seconds and end_idx < len(data_sorted):
                target_end_time = window_start + timedelta(seconds=min_window_seconds)
                extended_data = data_sorted.filter(
                    (pl.col("Timestamp") >= window_start)
                    & (pl.col("Timestamp") <= target_end_time)
                )
                if not extended_data.is_empty():
                    window_data = extended_data
                    end_idx = start_idx + len(window_data)
            elif duration_seconds > max_window_seconds:
                target_end_time = window_start + timedelta(seconds=max_window_seconds)
                truncated_data = data_sorted.filter(
                    (pl.col("Timestamp") >= window_start)
                    & (pl.col("Timestamp") <= target_end_time)
                )
                if not truncated_data.is_empty():
                    window_data = truncated_data
                    end_idx = start_idx + len(window_data)

            window_info = {
                "Window": window_start,
                "WindowStart": window_start,
                "WindowEnd": window_data["Timestamp"].max(),
                "EventTemplates": window_data["EventTemplate"].to_list(),
                "TemplateIds": window_data["TemplateId"].to_list(),
                "LogLevels": (
                    window_data["LogLevel"].to_list()
                    if "LogLevel" in window_data.columns
                    else []
                ),
                "LogCount": len(window_data),
            }
            windows.append(window_info)

            start_idx = end_idx

        return pl.DataFrame(windows) if windows else pl.DataFrame()

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds"""
        duration_map = {
            "s": 1,
            "sec": 1,
            "second": 1,
            "seconds": 1,
            "m": 60,
            "min": 60,
            "minute": 60,
            "minutes": 60,
            "h": 3600,
            "hour": 3600,
            "hours": 3600,
            "d": 86400,
            "day": 86400,
            "days": 86400,
        }

        match = re.match(r"(\d+)\s*([a-zA-Z]+)", duration_str)
        if not match:
            raise ValueError(f"Invalid duration format: {duration_str}")

        number, unit = match.groups()
        multiplier = duration_map.get(unit.lower())
        if multiplier is None:
            raise ValueError(f"Unknown time unit: {unit}")

        return int(number) * multiplier
