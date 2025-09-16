"""
Configuration loading utilities
"""

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


def load_config(config_path: str) -> PipelineConfig:
    """Load configuration from YAML file"""

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    return PipelineConfig(
        dataset=DatasetConfig(**config_dict["dataset"]),
        preprocessing=PreprocessingConfig(**config_dict["preprocessing"]),
        parsing=ParsingConfig(**config_dict["parsing"]),
        windowing=WindowingConfig(**config_dict["windowing"]),
        anomaly_detection=AnomalyDetectionConfig(**config_dict["anomaly_detection"]),
        output=OutputConfig(**config_dict["output"]),
    )


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
