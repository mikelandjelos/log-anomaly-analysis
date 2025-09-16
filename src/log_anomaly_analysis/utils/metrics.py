"""
Metrics and performance monitoring utilities
"""

import time
from contextlib import contextmanager
from typing import Any, Dict

import polars as pl
from loguru import logger


class MetricsCollector:
    """Collect and track pipeline metrics"""

    def __init__(self):
        self.metrics = {}
        self.timings = {}

    @contextmanager
    def timer(self, name: str):
        """Context manager for timing operations"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.timings[name] = duration
            logger.info(f"{name} completed in {duration:.2f} seconds")

    def add_metric(self, name: str, value: Any):
        """Add a custom metric"""
        self.metrics[name] = value

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        return {
            "metrics": self.metrics,
            "timings": self.timings,
            "total_time": sum(self.timings.values()),
        }


def calculate_data_quality_metrics(df: pl.DataFrame) -> Dict[str, Any]:
    """Calculate data quality metrics for a DataFrame"""

    if df.is_empty():
        return {"empty_dataset": True}

    metrics = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "null_counts": {},
        "duplicate_rows": 0,
        "memory_usage_mb": df.estimated_size("mb"),
    }

    # Calculate null counts per column
    for col in df.columns:
        null_count = df[col].null_count()
        metrics["null_counts"][col] = null_count

    # Check for duplicate rows
    try:
        unique_count = df.unique().height
        metrics["duplicate_rows"] = len(df) - unique_count
    except Exception:
        # Some columns might not be hashable
        pass

    return metrics
