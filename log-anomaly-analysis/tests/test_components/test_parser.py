"""
Tests for parsing component
"""

import polars as pl
import pytest

from log_anomaly_analysis.core.components.parsing import TemplateParserComponent


class TestTemplateParserComponent:

    def test_initialization(self, sample_config):
        """Test component initialization"""
        config = sample_config["parsing"]
        parser = TemplateParserComponent(config)

        assert parser.template_miner is not None

    def test_template_extraction(self, sample_config):
        """Test template extraction"""
        config = sample_config["parsing"]
        parser = TemplateParserComponent(config)

        # Create test data
        test_data = pl.DataFrame(
            {
                "Content": [
                    "workerEnv.init() ok /etc/httpd/conf/workers2.properties",
                    "mod_jk child workerEnv in error state 6",
                    "jk2_init() Found child 6725 in scoreboard slot 10",
                    "jk2_init() Found child 6726 in scoreboard slot 8",
                ]
            }
        )

        result = parser.process(test_data)

        assert not result.is_empty()
        assert "EventTemplate" in result.columns
        assert "TemplateId" in result.columns
        assert "Parameters" in result.columns
        assert len(result) == len(test_data)

    def test_empty_dataframe(self, sample_config):
        """Test with empty DataFrame"""
        config = sample_config["parsing"]
        parser = TemplateParserComponent(config)

        empty_df = pl.DataFrame({"Content": []})
        result = parser.process(empty_df)

        assert result.is_empty()

    def test_missing_content_column(self, sample_config):
        """Test with missing Content column"""
        config = sample_config["parsing"]
        parser = TemplateParserComponent(config)

        test_data = pl.DataFrame({"NotContent": ["test"]})

        with pytest.raises(
            ValueError, match="Input DataFrame must have 'Content' column"
        ):
            parser.process(test_data)
