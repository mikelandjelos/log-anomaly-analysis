"""Example showing how to use metrics and logging helpers."""

from pathlib import Path

from log_anomaly_analysis.core.modular_pipeline import ModularPipeline
from log_anomaly_analysis.utils.logging import setup_logging
from log_anomaly_analysis.utils.metrics import (
    MetricsCollector,
    calculate_data_quality_metrics,
)


def main():
    # Configure logging to both console and file
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    setup_logging(level="INFO", log_file=str(logs_dir / "metrics_demo.log"))

    metrics = MetricsCollector()

    pipeline = ModularPipeline("configs/apache/basic.yaml")

    with metrics.timer("pipeline_run"):
        results, summary = pipeline.run()

    metrics.add_metric("pipeline_summary", summary)
    metrics.add_metric(
        "preprocessed_quality", calculate_data_quality_metrics(results["preprocessed"])
    )
    metrics.add_metric(
        "anomalies_quality", calculate_data_quality_metrics(results["anomalies"])
    )

    print("Collected metrics:\n", metrics.get_summary())


if __name__ == "__main__":
    main()
