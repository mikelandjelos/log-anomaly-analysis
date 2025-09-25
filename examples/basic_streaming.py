"""Quick-start streaming example.

Run with `poetry run python examples/basic_streaming.py`.

Configuration defaults to ``configs/apache/streaming_basic.yaml`` which already
points at the LogHub Apache full dataset and a results directory under
``data/results/apache_streaming``.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from log_anomaly_analysis.core.streaming_pipeline import (
    StreamingPipeline,
    StreamingPipelineState,
    StreamingResultWriter,
    stream_file_dataset,
)

# Tune these to experiment with chunking, state persistence, etc.
REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "configs" / "apache" / "streaming_basic.yaml"
CHUNK_SIZE = 1_000
STATE_IN: Path | None = None
STATE_OUT: Path | None = None
OVERRIDE_OUTPUT_DIR: Path | None = None


def main() -> None:
    pipeline = StreamingPipeline(str(CONFIG_PATH))

    if OVERRIDE_OUTPUT_DIR is not None:
        OVERRIDE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        pipeline.config.output.directory = str(OVERRIDE_OUTPUT_DIR)
        pipeline.output_dir = OVERRIDE_OUTPUT_DIR / pipeline.run_timestamp
        pipeline.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Overriding output directory -> {}", pipeline.output_dir)

    writer = StreamingResultWriter(pipeline.output_dir, pipeline.config.output.formats)

    if STATE_IN and STATE_IN.exists():
        logger.info("Loading streaming state from {}", STATE_IN)
        state = StreamingPipelineState.load(STATE_IN)
    else:
        state = StreamingPipelineState()

    logger.info(
        "Processing dataset {} in chunks of {}",
        pipeline.config.dataset.path,
        CHUNK_SIZE,
    )
    chunk_iterator = stream_file_dataset(pipeline.config, chunk_size=CHUNK_SIZE)
    state = pipeline.process_stream(chunk_iterator, state=state, writer=writer)

    pipeline.flush_visualizations(state)

    if STATE_OUT is not None:
        logger.info("Saving streaming state to {}", STATE_OUT)
        STATE_OUT.parent.mkdir(parents=True, exist_ok=True)
        state.save(STATE_OUT)

    logger.info("Streaming run complete -> {}", pipeline.output_dir)
    logger.info("Processed {} chunks", state.chunk_index)
    logger.info(
        "Visualization artifacts saved under {}",
        pipeline.output_dir / "visual_ready",
    )


if __name__ == "__main__":
    main()
