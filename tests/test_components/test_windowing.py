"""
Tests for windowing component
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from log_anomaly_analysis.core.components.windowing import WindowingComponent


class TestWindowingComponent:

    def test_fixed_windowing(self, sample_config):
        """Test fixed time windowing"""
        config = sample_config["windowing"]
        windowing = WindowingComponent(config)

        # Create test data with timestamps
        base_time = datetime(2005, 12, 4, 4, 47, 44)
        test_data = pl.DataFrame(
            {
                "Timestamp": [
                    base_time,
                    base_time + timedelta(minutes=2),
                    base_time + timedelta(minutes=7),
                    base_time + timedelta(minutes=12),
                ],
                "EventTemplate": ["template1", "template2", "template1", "template3"],
                "TemplateId": [1, 2, 1, 3],
                "LogLevel": ["notice", "error", "notice", "warn"],
            }
        )

        result = windowing.process(test_data)

        assert not result.is_empty()
        assert "Window" in result.columns
        assert "EventTemplates" in result.columns
        assert "LogCount" in result.columns

    def test_sliding_window_captures_final_events(self, sample_config):
        """Sliding windowing should include the tail end of the timeline"""
        config = sample_config["windowing"].copy()
        config.update({"strategy": "sliding", "window_size": "5m", "step_size": "5m"})
        windowing = WindowingComponent(config)

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        timestamps = [
            base_time,
            base_time + timedelta(minutes=2),
            base_time + timedelta(minutes=6),
            base_time + timedelta(minutes=9),
        ]

        test_data = pl.DataFrame(
            {
                "Timestamp": timestamps,
                "EventTemplate": ["a", "b", "a", "c"],
                "TemplateId": [1, 2, 1, 3],
            }
        )

        result = windowing.process(test_data)

        assert not result.is_empty()
        # Last timestamp should appear in some window
        assert result["WindowEnd"].max() >= timestamps[-1]

    def test_invalid_strategy(self):
        """Test with invalid windowing strategy"""
        config = {"strategy": "invalid_strategy"}

        with pytest.raises(ValueError, match="Invalid strategy"):
            WindowingComponent(config)

    def test_missing_strategy(self):
        """Test with missing strategy"""
        config = {}

        with pytest.raises(ValueError, match="Windowing strategy must be specified"):
            WindowingComponent(config)
