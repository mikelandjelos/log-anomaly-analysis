"""Tests for event matrix component"""

from datetime import datetime, timedelta

import polars as pl

from log_anomaly_analysis.core.components.event_matrix import EventMatrixComponent


class TestEventMatrixComponent:

    def test_event_counts_match_window_data(self):
        """Each template count should reflect occurrences per window"""
        component = EventMatrixComponent({})

        base_time = datetime(2024, 1, 1, 0, 0, 0)
        windowed = pl.DataFrame(
            {
                "Window": [base_time, base_time + timedelta(minutes=5)],
                "EventTemplates": [
                    ["A", "A", "B"],
                    ["B", "C"],
                ],
                "TemplateIds": [[1, 1, 2], [2, 3]],
                "LogCount": [3, 2],
                "WindowStart": ["2024-01-01 00:00:00", "2024-01-01 00:05:00"],
                "WindowEnd": ["2024-01-01 00:05:00", "2024-01-01 00:10:00"],
            }
        )

        matrix = component.process(windowed)

        assert not matrix.is_empty()
        assert matrix.shape[0] == 2
        first_window = matrix.filter(pl.col("Window") == base_time)
        second_window = matrix.filter(
            pl.col("Window") == base_time + timedelta(minutes=5)
        )

        assert first_window["A"][0] == 2
        assert first_window["B"][0] == 1
        assert second_window["B"][0] == 1
        assert second_window["C"][0] == 1
        # Metadata columns should pass through
        assert "LogCount" in matrix.columns

    def test_empty_input(self):
        component = EventMatrixComponent({})
        result = component.process(pl.DataFrame())
        assert result.is_empty()
