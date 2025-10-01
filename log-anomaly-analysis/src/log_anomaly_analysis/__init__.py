"""
Log Anomaly Analysis - Modular pipeline for log anomaly detection
"""

__version__ = "0.1.0"
__author__ = "Mihajlo Madic"

import polars as pl

pl.Config.set_engine_affinity(engine="streaming")

from .core.config.models import PipelineConfig
from .core.modular_pipeline import ModularPipeline
from .core.streaming_pipeline import StreamingPipeline

__all__ = [
    "ModularPipeline",
    "StreamingPipeline",
    "PipelineConfig",
]
