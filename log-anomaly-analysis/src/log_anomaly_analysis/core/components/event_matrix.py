"""
Event count matrix generation component
"""

from typing import Dict

import polars as pl
from loguru import logger

from .base import BaseComponent


class EventMatrixComponent(BaseComponent):
    """Convert windowed data to event count matrix"""

    def _validate_config(self):
        pass

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create event count matrix from windowed data"""
        if data.is_empty():
            return pl.DataFrame()

        logger.info("Creating event count matrix from windowed data")

        # Explode the event templates for each window
        expanded_data = []

        for row in data.iter_rows(named=True):
            window = row["Window"]
            window_start = row.get("WindowStart", window)
            window_end = row.get("WindowEnd", window)
            log_count = row.get("LogCount", 0)

            for template in row["EventTemplates"]:
                expanded_data.append(
                    {
                        "Window": window,
                        "EventTemplate": template,
                        "WindowStart": window_start,
                        "WindowEnd": window_end,
                        "LogCount": log_count,
                    }
                )

        if not expanded_data:
            return pl.DataFrame()

        expanded_df = pl.DataFrame(expanded_data)

        # Create pivot table
        event_matrix = (
            expanded_df.group_by(["Window", "EventTemplate"], maintain_order=True)
            .len()
            .pivot(
                index="Window",
                on="EventTemplate",
                values="len",
                aggregate_function="sum",
            )
            .fill_null(0)
            .sort("Window")
        )

        # Add metadata columns if they exist
        if "WindowStart" in expanded_df.columns:
            metadata = expanded_df.group_by("Window", maintain_order=True).agg(
                [
                    pl.col("WindowStart").first(),
                    pl.col("WindowEnd").first(),
                    pl.col("LogCount").first(),
                ]
            )

            event_matrix = event_matrix.join(metadata, on="Window", how="left").sort(
                "Window"
            )

        logger.info(f"Created event matrix with shape: {event_matrix.shape}")
        return event_matrix
