"""
Integration tests for the complete pipeline
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from log_anomaly_analysis.core.pipeline import ModularPipeline


class TestPipelineIntegration:

    def test_complete_pipeline(self, sample_apache_logs, sample_config):
        """Test complete pipeline execution"""

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test log file
            log_file = temp_path / "test.log"
            with open(log_file, "w") as f:
                for log in sample_apache_logs:
                    f.write(log + "\n")

            # Create config file
            config_file = temp_path / "config.yaml"
            test_config = sample_config.copy()
            test_config["dataset"]["path"] = str(log_file)
            test_config["output"]["directory"] = str(temp_path / "results")

            with open(config_file, "w") as f:
                yaml.dump(test_config, f)

            # Run pipeline
            pipeline = ModularPipeline(str(config_file))
            results, summary = pipeline.run()

            # Verify results
            assert "preprocessed" in results, "'preprocessed' key missing in results"
            assert "templates" in results, "'templates' key missing in results"
            assert "windowed" in results, "'windowed' key missing in results"
            assert "event_matrix" in results, "'event_matrix' key missing in results"
            assert "anomalies" in results, "'anomalies' key missing in results"

            # Verify summary
            assert "total_logs" in summary, "'total_logs' key missing in summary"
            assert (
                "unique_templates" in summary
            ), "'unique_templates' key missing in summary"
            assert "total_windows" in summary, "'total_windows' key missing in summary"
            assert (
                "anomalous_windows" in summary
            ), "'anomalous_windows' key missing in summary"
            assert "anomaly_rate" in summary, "'anomaly_rate' key missing in summary"

            # Check that files were saved
            results_dir = temp_path / "results"
            assert (
                results_dir.exists()
            ), f"Results directory does not exist at {results_dir}"
            anomalies_file = results_dir / "anomalies.parquet"
            assert (
                sample_config["output"]["save_intermediate"] == anomalies_file.exists()
            ), f"Anomalies file is missing: {anomalies_file}"
