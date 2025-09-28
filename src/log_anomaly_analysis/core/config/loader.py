"""Configuration loading utilities"""

from pathlib import Path
from typing import Any, Dict

import yaml

from .models import (
    AnomalyDetectionConfig,
    DatasetConfig,
    OutputConfig,
    ParsingConfig,
    PipelineConfig,
    PreprocessingConfig,
    WindowingConfig,
)


def load_config(config_path: str | Path) -> PipelineConfig:
    """Load configuration from YAML file"""

    config_file = Path(config_path)
    with config_file.open("r") as f:
        config_dict = yaml.safe_load(f)

    resolved = _resolve_paths(config_dict, config_file.parent)

    return PipelineConfig(
        dataset=DatasetConfig(**resolved["dataset"]),
        preprocessing=PreprocessingConfig(**resolved["preprocessing"]),
        parsing=ParsingConfig(**resolved["parsing"]),
        windowing=WindowingConfig(**resolved["windowing"]),
        anomaly_detection=AnomalyDetectionConfig(**resolved["anomaly_detection"]),
        output=OutputConfig(**resolved["output"]),
    )


def _resolve_paths(config: Dict[str, Any], base_dir: Path) -> Dict[str, Any]:
    """Resolve dataset/output paths relative to the config file location."""

    resolved = {section: values.copy() for section, values in config.items()}

    dataset_path = resolved.get("dataset", {}).get("path")
    if dataset_path and not Path(dataset_path).is_absolute():
        resolved["dataset"]["path"] = str((base_dir / dataset_path).resolve())

    output_dir = resolved.get("output", {}).get("directory")
    if output_dir and not Path(output_dir).is_absolute():
        resolved["output"]["directory"] = str((base_dir / output_dir).resolve())

    return resolved


def save_config(config: PipelineConfig, output_path: str):
    """Save configuration to YAML file"""

    config_dict = {
        "dataset": config.dataset.__dict__,
        "preprocessing": config.preprocessing.__dict__,
        "parsing": config.parsing.__dict__,
        "windowing": config.windowing.__dict__,
        "anomaly_detection": config.anomaly_detection.__dict__,
        "output": config.output.__dict__,
    }

    with open(output_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, indent=2)
