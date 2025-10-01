"""
Configuration data models using dataclasses
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DatasetConfig:
    """Dataset configuration"""

    type: str
    path: str
    format: str = "raw"
    columns: Optional[Dict[str, str]] = None


@dataclass
class MaskingRule:
    """Masking rule for sensitive data"""

    regex_pattern: str
    mask_with: str


@dataclass
class PreprocessingConfig:
    """Preprocessing configuration"""

    log_pattern: str
    timestamp_format: Optional[str] = None
    strict_mode: bool = False
    require_timestamps: bool = True


@dataclass
class ParsingConfig:
    """Template parsing configuration"""

    similarity_threshold: float = 0.4
    depth: int = 4
    max_children: int = 100
    max_clusters: int = 1024
    extra_delimiters: List[str] = field(default_factory=lambda: ["_"])
    masking_rules: List[Dict[str, str]] = field(default_factory=list)
    mask_prefix: str = "<:"
    mask_suffix: str = ":>"


@dataclass
class WindowingConfig:
    """Windowing configuration"""

    strategy: str = "fixed"
    window_size: Optional[str] = "5m"
    step_size: Optional[str] = None
    session_timeout: Optional[str] = None
    target_logs_per_window: Optional[int] = None
    min_window_size: Optional[str] = None
    max_window_size: Optional[str] = None


@dataclass
class AnomalyDetectionConfig:
    """Anomaly detection configuration"""

    algorithm: str = "pca_subspace"
    variance_threshold: float = 0.95
    alpha: float = 0.001
    use_tfidf: bool = False
    use_scaling: bool = True


@dataclass
class OutputConfig:
    """Output configuration"""

    directory: str = "results"
    save_intermediate: bool = True
    formats: List[str] = field(default_factory=lambda: ["parquet"])


@dataclass
class PipelineConfig:
    """Complete pipeline configuration"""

    dataset: DatasetConfig
    preprocessing: PreprocessingConfig
    parsing: ParsingConfig
    windowing: WindowingConfig
    anomaly_detection: AnomalyDetectionConfig
    output: OutputConfig
