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

    dataset_cfg = resolved.get("dataset")
    if dataset_cfg is not None:
        dataset = DatasetConfig(**dataset_cfg)
    else:
        dataset = DatasetConfig(type="inline", path="")

    preprocessing_cfg = resolved.get("preprocessing", {})
    parsing_cfg = resolved.get("parsing", {})
    windowing_cfg = resolved.get("windowing", {})
    anomaly_cfg = resolved.get("anomaly_detection", {})
    output_cfg = resolved.get("output", {})

    return PipelineConfig(
        dataset=dataset,
        preprocessing=PreprocessingConfig(**preprocessing_cfg),
        parsing=ParsingConfig(**parsing_cfg),
        windowing=WindowingConfig(**windowing_cfg),
        anomaly_detection=AnomalyDetectionConfig(**anomaly_cfg),
        output=OutputConfig(**output_cfg),
    )


def _resolve_paths(config: Dict[str, Any], base_dir: Path) -> Dict[str, Any]:
    """Resolve dataset/output paths relative to the config file location."""

    resolved = {
        section: values.copy() if isinstance(values, dict) else values
        for section, values in config.items()
    }

    dataset_section = resolved.get("dataset")
    if isinstance(dataset_section, dict):
        dataset_path = dataset_section.get("path")
        if dataset_path and not Path(dataset_path).is_absolute():
            dataset_section["path"] = str((base_dir / dataset_path).resolve())

    output_section = resolved.get("output")
    if isinstance(output_section, dict):
        output_dir = output_section.get("directory")
        if output_dir and not Path(output_dir).is_absolute():
            output_section["directory"] = str((base_dir / output_dir).resolve())

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
