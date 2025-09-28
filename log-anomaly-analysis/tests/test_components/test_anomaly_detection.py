"""
Tests for anomaly detection component
"""

import numpy as np
import polars as pl
import pytest

from log_anomaly_analysis.core.components.anomaly_detection import (
    AnomalyDetectorComponent,
)


class TestAnomalyDetectorComponent:

    def test_initialization(self, sample_config):
        """Test component initialization"""
        config = sample_config["anomaly_detection"]
        detector = AnomalyDetectorComponent(config)

        assert detector.variance_threshold == 0.95
        assert detector.alpha == 0.001
        assert detector.use_tfidf is True
        assert detector.use_scaling is True

    def test_anomaly_detection(self, sample_config):
        """Test anomaly detection on synthetic data"""
        config = sample_config["anomaly_detection"]
        detector = AnomalyDetectorComponent(config)

        # Create synthetic event matrix
        np.random.seed(42)
        data = np.random.poisson(5, (50, 10))  # 50 windows, 10 event types

        # Add some anomalous windows
        data[45:48, :] = np.random.poisson(20, (3, 10))  # High counts

        # Convert to DataFrame with event column names
        event_cols = [f"event_{i}" for i in range(10)]
        test_data = pl.DataFrame({col: data[:, i] for i, col in enumerate(event_cols)})
        test_data = test_data.with_columns([pl.arange(len(test_data)).alias("Window")])

        result = detector.process(test_data)

        assert not result.is_empty()
        assert "AnomalyScore" in result.columns
        assert "IsAnomaly" in result.columns
        assert len(result) == len(test_data)

    def test_insufficient_data(self, sample_config):
        """Test with insufficient data"""
        config = sample_config["anomaly_detection"]
        detector = AnomalyDetectorComponent(config)

        # Single row of data
        test_data = pl.DataFrame({"event_1": [5], "event_2": [3], "Window": [0]})

        result = detector.process(test_data)

        assert "AnomalyScore" in result.columns
        assert "IsAnomaly" in result.columns
        assert result["AnomalyScore"][0] == 0.0
        assert result["IsAnomaly"][0] is False
