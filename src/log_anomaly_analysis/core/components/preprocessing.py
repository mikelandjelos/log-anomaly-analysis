"""
Log preprocessing component
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

import polars as pl
from loguru import logger

from .base import BaseComponent


class PreprocessorComponent(BaseComponent):
    """Configurable log preprocessing component"""

    def _validate_config(self):
        required = ["log_pattern"]
        for req in required:
            if req not in self.config:
                raise ValueError(f"Missing required config: {req}")

    def __init__(self, config: Dict):
        super().__init__(config)
        self.log_pattern = re.compile(self.config["log_pattern"])
        self.timestamp_format = self.config.get("timestamp_format")
        self.strict_mode = self.config.get("strict_mode", False)
        self.require_timestamps = self.config.get("require_timestamps", True)

    def _parse_single_log(self, raw_log: str) -> Optional[Dict[str, str]]:
        """Parse a single log line"""
        raw_log = raw_log.strip()
        if not raw_log:
            return None

        matched = self.log_pattern.match(raw_log)

        if matched is None:
            if self.strict_mode:
                raise ValueError(f"Log doesn't match pattern: {raw_log}")
            return {
                "Content": raw_log,
                "Timestamp": datetime.now().isoformat(),
                "LogLevel": "UNKNOWN",
            }

        structured = matched.groupdict()

        # Convert timestamp to ISO format
        if (
            "Timestamp" in structured
            and structured["Timestamp"]
            and self.timestamp_format
        ):
            try:
                if self.timestamp_format.upper() == "UNIX":
                    dt = datetime.fromtimestamp(float(structured["Timestamp"]))
                else:
                    dt = datetime.strptime(
                        structured["Timestamp"], self.timestamp_format
                    )
                structured["Timestamp"] = dt.isoformat()
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Failed to parse timestamp {structured['Timestamp']}: {e}"
                )
                structured["Timestamp"] = None

        return structured

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Process raw log DataFrame"""
        if "raw_log" not in data.columns:
            raise ValueError("Input DataFrame must have 'raw_log' column")

        raw_logs = data["raw_log"].to_list()

        processed_logs = []
        for i, raw_log in enumerate(raw_logs):
            try:
                parsed = self._parse_single_log(raw_log)
                if parsed:
                    parsed["LineNumber"] = i + 1
                    processed_logs.append(parsed)
            except Exception as e:
                logger.warning(f"Error processing line {i+1}: {e}")

        if not processed_logs:
            return pl.DataFrame()

        df = pl.DataFrame(processed_logs)

        # Filter out logs without valid timestamps if required
        if self.require_timestamps and "Timestamp" in df.columns:
            df = df.filter(pl.col("Timestamp").is_not_null())

        # Convert timestamp column to datetime
        if "Timestamp" in df.columns:
            df = df.with_columns(
                [pl.col("Timestamp").str.to_datetime().alias("Timestamp")]
            )

        return df.sort("Timestamp") if "Timestamp" in df.columns else df
