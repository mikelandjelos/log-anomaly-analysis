"""Tests for streaming pipeline utilities."""

import copy

import polars as pl
import yaml

from log_anomaly_analysis.core.streaming_pipeline import (
    StreamingPipeline,
    StreamingPipelineState,
    StreamingResultWriter,
    stream_file_dataset,
)


def test_streaming_writer_creates_parquet_parts(tmp_path):
    writer = StreamingResultWriter(tmp_path, formats=["parquet", "csv", "json"])

    df_first = pl.DataFrame({
        "Window": ["w1"],
        "Value": [1],
        "Templates": [["A", "B"]],
    })
    df_second = pl.DataFrame({
        "Window": ["w2"],
        "Value": [2],
        "Templates": [["C"]],
    })

    writer.append("anomalies", df_first)
    writer.append("anomalies", df_second)

    parquet_dir = tmp_path / "anomalies"
    assert parquet_dir.exists(), "Parquet directory not created"
    assert sorted(p.name for p in parquet_dir.iterdir()) == [
        "part-00000.parquet",
        "part-00001.parquet",
    ]

    csv_path = tmp_path / "anomalies.csv"
    assert csv_path.exists(), "CSV output missing"
    csv_contents = csv_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(csv_contents) == 3, "CSV should contain one header and two rows"
    assert "['A','B']" in csv_contents[1], "List column should be serialized"

    json_path = tmp_path / "anomalies.json"
    assert json_path.exists(), "JSON output missing"
    json_lines = json_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(json_lines) == 2, "JSON output should contain two records"


def test_streaming_pipeline_flushes_remainder(sample_apache_logs, sample_config, tmp_path):
    log_path = tmp_path / "apache.log"
    log_path.write_text("\n".join(sample_apache_logs), encoding="utf-8")

    config = copy.deepcopy(sample_config)
    config["dataset"]["path"] = str(log_path)
    config["output"]["directory"] = str(tmp_path / "results")
    config["output"]["formats"] = ["csv"]

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    pipeline = StreamingPipeline(str(config_path))

    chunks = [sample_apache_logs[:2], sample_apache_logs[2:]]
    state = pipeline.process_stream(chunks, state=StreamingPipelineState(), writer=None)

    assert state.template_remainder.is_empty(), "Remainder should be cleared"
    assert state.visualization.anomalies, "Anomaly results should be accumulated"
    assert state.visualization.event_matrices, "Event matrix results should be accumulated"

    dataset_chunks = list(stream_file_dataset(pipeline.config, chunk_size=2))
    assert len(dataset_chunks) >= 2, "Chunk helper should yield multiple chunks"
