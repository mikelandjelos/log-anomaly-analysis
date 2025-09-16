"""
Test configuration and fixtures
"""

from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def sample_apache_logs():
    """Sample Apache log lines for testing"""
    return [
        "[Sun Dec 04 04:47:44 2005] [notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties",
        "[Sun Dec 04 04:47:44 2005] [error] mod_jk child workerEnv in error state 6",
        "[Sun Dec 04 04:51:08 2005] [notice] jk2_init() Found child 6725 in scoreboard slot 10",
        "[Sun Dec 04 05:15:09 2005] [error] [client 222.166.160.184] Directory index forbidden by rule: /var/www/html/",
    ]


@pytest.fixture
def sample_config():
    """Sample pipeline configuration"""
    return {
        "dataset": {"type": "file", "path": "test.log", "format": "raw"},
        "preprocessing": {
            "log_pattern": r"\[(?P<Timestamp>.+?)\]\s+\[(?P<LogLevel>\w+)]\s+(?P<Content>.+)",
            "timestamp_format": "%a %b %d %H:%M:%S %Y",
            "strict_mode": False,
            "require_timestamps": True,
        },
        "parsing": {
            "similarity_threshold": 0.4,
            "depth": 4,
            "max_children": 100,
            "max_clusters": 1024,
            "extra_delimiters": ["_"],
            "masking_rules": [],
            "mask_prefix": "<:",
            "mask_suffix": ":>",
        },
        "windowing": {"strategy": "fixed", "window_size": "5m"},
        "anomaly_detection": {
            "algorithm": "pca_subspace",
            "variance_threshold": 0.95,
            "alpha": 0.001,
            "use_tfidf": True,
            "use_scaling": True,
        },
        "output": {
            "directory": "test_results",
            "save_intermediate": True,
            "formats": ["parquet"],
        },
    }


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pl.DataFrame(
        {
            "raw_log": [
                "[Sun Dec 04 04:47:44 2005] [notice] workerEnv.init() ok",
                "[Sun Dec 04 04:47:44 2005] [error] mod_jk child workerEnv in error state",
                "[Sun Dec 04 04:51:08 2005] [notice] jk2_init() Found child in scoreboard",
            ]
        }
    )
