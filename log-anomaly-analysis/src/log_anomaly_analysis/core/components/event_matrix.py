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

        if "EventTemplates" not in data.columns:
            logger.warning("Windowed data missing 'EventTemplates' column; skipping")
            return pl.DataFrame()

        logger.info("Creating event count matrix from windowed data")

        metadata_cols = [
            col
            for col in ["WindowStart", "WindowEnd", "LogCount"]
            if col in data.columns
        ]

        select_cols = ["Window", "EventTemplates", *metadata_cols]

        expanded = (
            data.select(select_cols)
            .with_columns(
                pl.col("EventTemplates").fill_null(pl.lit([], dtype=pl.List(pl.Utf8)))
            )
            .explode("EventTemplates")
            .rename({"EventTemplates": "EventTemplate"})
        )

        if expanded.is_empty():
            return pl.DataFrame()

        expanded = expanded.filter(pl.col("EventTemplate").is_not_null())
        if expanded.is_empty():
            return pl.DataFrame()

        event_matrix = (
            expanded.group_by(["Window", "EventTemplate"], maintain_order=True)
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

        if metadata_cols:
            metadata = (
                expanded.select(["Window", *metadata_cols])
                .group_by("Window", maintain_order=True)
                .agg([pl.col(col).first().alias(col) for col in metadata_cols])
            )
            event_matrix = event_matrix.join(metadata, on="Window", how="left")

        # Reorder columns: Window, event counts, metadata
        metadata_set = set(metadata_cols)
        event_cols = [
            col for col in event_matrix.columns if col not in {"Window", *metadata_set}
        ]
        ordered_cols = ["Window", *event_cols, *metadata_cols]
        event_matrix = event_matrix.select(ordered_cols)

        logger.info(f"Created event matrix with shape: {event_matrix.shape}")
        return event_matrix
