#!/usr/bin/env python3
"""
Example of creating custom pipeline components
"""

import polars as pl

from log_anomaly_analysis.core.components.base import BaseComponent
from log_anomaly_analysis.core.pipeline import ModularPipeline


class CustomPreprocessor(BaseComponent):
    """Custom preprocessing component example"""

    def _validate_config(self):
        # Add custom validation logic
        pass

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Custom preprocessing logic"""

        # Example: Add custom timestamp parsing
        if "raw_log" in data.columns:
            # Custom log parsing logic here
            processed = []
            for log in data["raw_log"].to_list():
                # Your custom parsing logic
                processed.append({"Content": log, "CustomField": "custom_value"})

            return pl.DataFrame(processed)

        return data


class IsolationForestDetector(BaseComponent):
    """Example of alternative anomaly detection algorithm"""

    def _validate_config(self):
        pass

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Isolation Forest anomaly detection"""
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            raise ImportError("sklearn required for IsolationForest")

        # Get event columns
        metadata_cols = ["Window", "WindowStart", "WindowEnd", "LogCount"]
        event_cols = [col for col in data.columns if col not in metadata_cols]

        if not event_cols:
            return data.with_columns(
                [pl.lit(0.0).alias("AnomalyScore"), pl.lit(False).alias("IsAnomaly")]
            )

        # Apply Isolation Forest
        event_matrix = data.select(event_cols).to_numpy()

        if len(event_matrix) < 2:
            return data.with_columns(
                [pl.lit(0.0).alias("AnomalyScore"), pl.lit(False).alias("IsAnomaly")]
            )

        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_labels = iso_forest.fit_predict(event_matrix)
        anomaly_scores = -iso_forest.score_samples(
            event_matrix
        )  # Negative for higher = more anomalous

        # Add results
        result = data.with_columns(
            [
                pl.Series("AnomalyScore", anomaly_scores),
                pl.Series("IsAnomaly", anomaly_labels == -1),
            ]
        )

        return result


def main():
    """Example of using custom components"""

    # You would need to modify the pipeline to use custom components
    # This is a simplified example showing the concept

    print("Custom components example")
    print("See the custom classes above for implementation details")


if __name__ == "__main__":
    main()
