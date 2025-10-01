"""
Dataset loading utilities
"""

from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger


def load_dataset(config: Dict) -> pl.DataFrame:
    """Load dataset based on configuration"""

    dataset_type = config["type"]
    dataset_path = config["path"]

    logger.info(f"Loading dataset from {dataset_path}")

    if dataset_type == "file":
        return _load_file_dataset(config)
    else:
        raise ValueError(f"Unsupported dataset type: {dataset_type}")


def _load_file_dataset(config: Dict) -> pl.DataFrame:
    """Load dataset from file"""

    file_path = Path(config["path"])
    file_format = config.get("format", "auto")

    if not file_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {file_path}")

    if file_format == "raw":
        # Read as raw text file
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line.rstrip("\n\r") for line in f if line.strip()]

        logger.info(f"Loaded {len(lines)} raw log lines")
        return pl.DataFrame({"raw_log": lines})

    elif file_format == "csv" or file_path.suffix == ".csv":
        return pl.read_csv(file_path)

    elif file_format == "parquet" or file_path.suffix == ".parquet":
        return pl.read_parquet(file_path)

    elif file_format == "json" or file_path.suffix == ".json":
        return pl.read_json(file_path)

    else:
        # Auto-detect format based on file suffix
        if file_path.suffix == ".csv":
            return pl.read_csv(file_path)
        elif file_path.suffix == ".parquet":
            return pl.read_parquet(file_path)
        elif file_path.suffix == ".json":
            return pl.read_json(file_path)
        else:
            # Default to raw text
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.rstrip("\n\r") for line in f if line.strip()]
            return pl.DataFrame({"raw_log": lines})
