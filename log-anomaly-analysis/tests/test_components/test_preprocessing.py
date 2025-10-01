"""
Tests for preprocessing component
"""

import polars as pl
import pytest

from log_anomaly_analysis.core.components.preprocessing import PreprocessorComponent


class TestPreprocessorComponent:

    def test_initialization(self, sample_config):
        """Test component initialization"""
        config = sample_config["preprocessing"]
        preprocessor = PreprocessorComponent(config)

        assert preprocessor.log_pattern is not None
        assert preprocessor.timestamp_format == "%a %b %d %H:%M:%S %Y"
        assert preprocessor.strict_mode is False
        assert preprocessor.require_timestamps is True

    def test_single_log_parsing(self, sample_config):
        """Test parsing a single log line"""
        config = sample_config["preprocessing"]
        preprocessor = PreprocessorComponent(config)

        log_line = "[Sun Dec 04 04:47:44 2005] [notice] workerEnv.init() ok"
        result = preprocessor._parse_single_log(log_line)

        assert result is not None
        assert "Timestamp" in result
        assert "LogLevel" in result
        assert "Content" in result
        assert result["LogLevel"] == "notice"
        assert "workerEnv.init() ok" in result["Content"]

    def test_process_dataframe(self, sample_config, sample_dataframe):
        """Test processing a DataFrame"""
        config = sample_config["preprocessing"]
        preprocessor = PreprocessorComponent(config)

        result = preprocessor.process(sample_dataframe)

        assert not result.is_empty()
        assert "Timestamp" in result.columns
        assert "LogLevel" in result.columns
        assert "Content" in result.columns
        assert len(result) > 0

    def test_invalid_log_pattern(self):
        """Test with invalid log pattern"""
        config = {"log_pattern": "invalid[pattern"}

        with pytest.raises(Exception):
            PreprocessorComponent(config)

    def test_missing_required_config(self):
        """Test with missing required configuration"""
        config = {}

        with pytest.raises(ValueError, match="Missing required config: log_pattern"):
            PreprocessorComponent(config)
