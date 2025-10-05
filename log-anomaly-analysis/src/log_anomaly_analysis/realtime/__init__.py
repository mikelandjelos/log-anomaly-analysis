"""Realtime utilities for streaming anomaly detection.

Exports:
- Accumulator: tumbling window hashed-feature accumulator
- StreamingPCAScorer: incremental PCA SPE scorer with warmup + freeze
"""

from .accumulator import Accumulator
from .scorer import StreamingPCAScorer

__all__ = [
    "Accumulator",
    "StreamingPCAScorer",
]

