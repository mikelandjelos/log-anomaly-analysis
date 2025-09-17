#!/usr/bin/env python3
"""
Basic usage example for the log anomaly analysis pipeline
"""


import polars as pl

from log_anomaly_analysis import ModularPipeline


def main():
    """Run basic example"""

    # Path to your configuration file
    config_path = "configs/apache/basic.yaml"

    # Initialize and run pipeline
    pipeline = ModularPipeline(config_path)
    results, summary = pipeline.run()

    # Print summary
    print("\n=== Pipeline Results ===")
    print(f"Total logs processed: {summary['total_logs']:,}")
    print(f"Unique templates: {summary['unique_templates']:,}")
    print(f"Total windows: {summary['total_windows']:,}")
    print(f"Anomalous windows: {summary['anomalous_windows']:,}")
    print(f"Anomaly rate: {summary['anomaly_rate']:.2%}")

    # Access specific results
    anomalies = results["anomalies"]

    # Find top anomalous windows
    if "AnomalyScore" in anomalies.columns:
        top_anomalies = (
            anomalies.filter(pl.col("IsAnomaly") == True)
            .sort("AnomalyScore", descending=True)
            .head(5)
        )

        print(f"\nTop 5 anomalous windows:")
        for i, row in enumerate(top_anomalies.iter_rows(named=True)):
            print(f"{i+1}. Window: {row['Window']}, Score: {row['AnomalyScore']:.4f}")


if __name__ == "__main__":
    main()
