"""
Main pipeline orchestrator
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import polars as pl
from loguru import logger

from ..datasets.loaders import load_dataset
from .components.anomaly_detection import AnomalyDetectorComponent
from .components.event_matrix import EventMatrixComponent
from .components.parsing import TemplateParserComponent
from .components.preprocessing import PreprocessorComponent
from .components.windowing import WindowingComponent
from .config.loader import load_config
from .config.models import PipelineConfig


class ModularPipeline:
    """Main pipeline orchestrator with configurable components"""

    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self._initialize_components()

        # Setup timestamped output directory
        base_output = Path(self.config.output.directory)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.output_dir = base_output / timestamp
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.run_timestamp = timestamp

        logger.info(f"Pipeline initialized with output directory: {self.output_dir}")

    def _initialize_components(self):
        """Initialize all pipeline components"""
        logger.info("Initializing pipeline components")

        self.preprocessor = PreprocessorComponent(self.config.preprocessing.__dict__)
        self.parser = TemplateParserComponent(self.config.parsing.__dict__)
        self.windowing = WindowingComponent(self.config.windowing.__dict__)
        self.event_matrix = EventMatrixComponent({})
        self.anomaly_detector = AnomalyDetectorComponent(
            self.config.anomaly_detection.__dict__
        )

    def run(self) -> Tuple[Dict[str, pl.DataFrame], Dict[str, Any]]:
        """Run the complete pipeline"""
        logger.info("Starting modular anomaly detection pipeline")

        results = {}

        # Step 1: Load dataset
        logger.info("Loading dataset...")
        raw_data = load_dataset(self.config.dataset.__dict__)
        logger.info(f"Loaded {len(raw_data)} raw log entries")

        # Step 2: Preprocessing
        logger.info("Preprocessing logs...")
        preprocessed_data = self.preprocessor.process(raw_data)
        logger.info(f"Preprocessed {len(preprocessed_data)} log entries")
        results["preprocessed"] = preprocessed_data

        if preprocessed_data.is_empty():
            logger.warning("No valid logs after preprocessing")
            return results, {}

        # Step 3: Template extraction
        logger.info("Extracting templates...")
        templates_data = self.parser.process(preprocessed_data)
        logger.info(f"Extracted templates for {len(templates_data)} logs")
        results["templates"] = templates_data

        if templates_data.is_empty():
            logger.warning("No templates extracted")
            return results, {}

        # Step 4: Windowing
        logger.info("Creating windows...")
        windowed_data = self.windowing.process(templates_data)
        logger.info(f"Created {len(windowed_data)} windows")
        results["windowed"] = windowed_data

        if windowed_data.is_empty():
            logger.warning("No windows created")
            return results, {}

        # Step 5: Event count matrix
        logger.info("Building event count matrix...")
        event_matrix = self.event_matrix.process(windowed_data)
        logger.info(f"Event matrix shape: {event_matrix.shape}")
        results["event_matrix"] = event_matrix

        if event_matrix.is_empty():
            logger.warning("Empty event matrix")
            return results, {}

        # Step 6: Anomaly detection
        logger.info("Detecting anomalies...")
        anomaly_results = self.anomaly_detector.process(event_matrix)
        logger.info("Anomaly detection completed")
        results["anomalies"] = anomaly_results

        # Save results
        if self.config.output.save_intermediate:
            self._save_results(results)

        # Generate summary
        summary = self._generate_summary(results)
        summary["run_timestamp"] = self.run_timestamp
        logger.info("Pipeline completed successfully!")

        return results, summary

    def _save_results(self, results: Dict[str, pl.DataFrame]):
        """Save pipeline results"""
        logger.info("Saving results...")

        for stage, data in results.items():
            if not data.is_empty():
                for fmt in self.config.output.formats:
                    prepared = self._prepare_for_format(data, fmt)
                    output_file: Optional[Path] = None
                    if fmt == "parquet":
                        output_file = self.output_dir / f"{stage}.parquet"
                        prepared.write_parquet(output_file)
                    elif fmt == "json":
                        output_file = self.output_dir / f"{stage}.json"
                        prepared.write_json(output_file)
                    elif fmt == "csv":
                        output_file = self.output_dir / f"{stage}.csv"
                        prepared.write_csv(output_file)

                    logger.info(
                        f"Saved {stage} results to '{output_file or 'Not Given'}'"
                    )

    def _prepare_for_format(self, data: pl.DataFrame, fmt: str) -> pl.DataFrame:
        """Adjust dataframe for format-specific constraints."""
        if fmt != "csv":
            return data

        transforms = []
        for column, dtype in zip(data.columns, data.dtypes):
            if dtype == pl.List:
                transforms.append(
                    pl.col(column)
                    .cast(pl.List(pl.Utf8), strict=False)
                    .list.join("|")
                    .alias(column)
                )
            elif dtype == pl.Struct:
                transforms.append(
                    pl.col(column)
                    .struct.json_encode()
                    .alias(column)
                )

        return data.with_columns(transforms) if transforms else data

    def _generate_summary(self, results: Dict[str, pl.DataFrame]) -> Dict[str, Any]:
        """Generate pipeline summary statistics"""
        summary = {
            "total_logs": len(results.get("preprocessed", pl.DataFrame())),
            "unique_templates": 0,
            "total_windows": len(results.get("windowed", pl.DataFrame())),
            "anomalous_windows": 0,
            "anomaly_rate": 0.0,
        }

        if "templates" in results and not results["templates"].is_empty():
            summary["unique_templates"] = results["templates"]["TemplateId"].n_unique()

        if "anomalies" in results and not results["anomalies"].is_empty():
            anomalies_df = results["anomalies"]
            if "IsAnomaly" in anomalies_df.columns:
                summary["anomalous_windows"] = anomalies_df["IsAnomaly"].sum()
                summary["anomaly_rate"] = summary["anomalous_windows"] / len(
                    anomalies_df
                )

        return summary
