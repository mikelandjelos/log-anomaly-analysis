"""
Log Anomaly Analysis - Modular pipeline for log anomaly detection
"""

__version__ = "0.1.0"
__author__ = "Your Name"

from .core.config.models import PipelineConfig
from .core.pipeline import ModularPipeline

__all__ = ["ModularPipeline", "PipelineConfig"]
