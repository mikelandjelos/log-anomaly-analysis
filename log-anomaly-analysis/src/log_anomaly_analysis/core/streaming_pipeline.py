"""Streaming pipeline utilities"""

from __future__ import annotations

import pickle
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import polars as pl
from loguru import logger

from .components.anomaly_detection import AnomalyDetectorComponent
from .components.event_matrix import EventMatrixComponent
from .components.parsing import TemplateParserComponent
from .components.preprocessing import PreprocessorComponent
from .components.windowing import WindowingComponent
from .config.loader import load_config
from .config.models import PipelineConfig


@dataclass
class StreamingPipelineState:
    """Holds incremental state between streaming batches."""

    template_remainder: pl.DataFrame = field(default_factory=pl.DataFrame)
    chunk_index: int = 0
    total_logs: int = 0
    total_windows: int = 0
    total_anomalies: int = 0

    def save(self, path: Union[str, Path]) -> None:
        with open(path, "wb") as fh:
            pickle.dump(self, fh)

    @staticmethod
    def load(path: Union[str, Path]) -> "StreamingPipelineState":
        with open(path, "rb") as fh:
            return pickle.load(fh)


class StreamingResultWriter:
    """Append-only writer for streaming outputs."""

    def __init__(self, base_dir: Path, formats: Sequence[str]):
        self.base_dir = base_dir
        self.formats = list(formats)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._csv_header_written: Dict[Path, bool] = {}
        self._parquet_part_index: Dict[str, int] = defaultdict(int)

    def append(self, stage: str, data: pl.DataFrame) -> None:
        if data is None or data.is_empty():
            return
        for fmt in self.formats:
            prepared = data
            if fmt == "parquet":
                part_dir = self.base_dir / stage
                part_dir.mkdir(parents=True, exist_ok=True)
                part_index = self._parquet_part_index[stage]
                path = part_dir / f"part-{part_index:05d}.parquet"
                prepared.write_parquet(path, compression="zstd")
                self._parquet_part_index[stage] += 1
            elif fmt == "json":
                raise ValueError(f"`json` format not supported when streaming")
                path = self.base_dir / f"{stage}.json"
                with path.open("a", encoding="utf-8") as results_file:
                    prepared.write_ndjson(file=results_file)
            elif fmt == "csv":
                raise ValueError(f"`csv` format not supported when streaming")
                path = self.base_dir / f"{stage}.csv"
                header_written = self._csv_header_written.get(path, False)
                path_exists = path.exists()
                mode = "a" if path_exists else "w"
                should_write_header = not header_written
                if path_exists and should_write_header:
                    should_write_header = path.stat().st_size == 0
                prepared = self._prepare_for_csv(prepared)
                with path.open(mode, encoding="utf-8") as results_file:
                    prepared.write_csv(
                        file=results_file,
                        include_header=should_write_header,
                    )
                if not header_written:
                    self._csv_header_written[path] = True

    @staticmethod
    def _prepare_for_csv(data: pl.DataFrame) -> pl.DataFrame:
        transforms = []
        for column, dtype in zip(data.columns, data.dtypes):
            if dtype == pl.List:
                transforms.append(
                    pl.col(column)
                    .map_elements(
                        lambda v: (
                            "[" + ",".join(f"'{item}'" for item in v) + "]"
                            if v is not None and len(v) > 0
                            else "[]"
                        ),
                        return_dtype=pl.Utf8,
                    )
                    .alias(column)
                )
            elif dtype == pl.Struct:
                transforms.append(pl.col(column).struct.json_encode().alias(column))

        return data.with_columns(transforms) if transforms else data


class StreamingPipeline:
    """Incremental pipeline that processes logs in batches."""

    def __init__(self, config_path: str | Path):
        self.config: PipelineConfig = load_config(config_path)
        self._initialize_components()

        base_output = Path(self.config.output.directory)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.output_dir = base_output / timestamp
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.run_timestamp = timestamp

        logger.info(f"Streaming pipeline output directory: {self.output_dir}")

    def _initialize_components(self) -> None:
        logger.info("Initializing streaming pipeline components")
        self.preprocessor = PreprocessorComponent(self.config.preprocessing.__dict__)
        self.parser = TemplateParserComponent(self.config.parsing.__dict__)
        self.windowing = WindowingComponent(self.config.windowing.__dict__)
        self.event_matrix = EventMatrixComponent({})
        self.anomaly_detector = AnomalyDetectorComponent(
            self.config.anomaly_detection.__dict__
        )

        if self.config.windowing.strategy != "fixed":
            logger.warning(
                "StreamingPipeline currently optimised for fixed windows; other strategies may yield partial duplicates."
            )

    def process_chunk(
        self,
        raw_chunk: Union[Sequence[str], pl.DataFrame],
        state: Optional[StreamingPipelineState] = None,
        writer: Optional[StreamingResultWriter] = None,
    ) -> Tuple[Dict[str, pl.DataFrame], StreamingPipelineState]:
        if state is None:
            state = StreamingPipelineState()

        if isinstance(raw_chunk, pl.DataFrame):
            chunk_df = raw_chunk
        else:
            chunk_df = pl.DataFrame({"raw_log": list(raw_chunk)})

        if chunk_df.is_empty():
            return {}, state

        preprocessed = self.preprocessor.process(chunk_df)
        if preprocessed.is_empty():
            return {}, state

        state.total_logs += len(preprocessed)

        parsed_chunk = self.parser.process(preprocessed)

        if not state.template_remainder.is_empty():
            templates_combined = pl.concat(
                [state.template_remainder, parsed_chunk], how="vertical_relaxed"
            )
        else:
            templates_combined = parsed_chunk

        if templates_combined.is_empty():
            return {}, state

        templates_combined = templates_combined.sort("Timestamp")
        windowed = self.windowing.process(templates_combined)

        if windowed.is_empty():
            state.template_remainder = templates_combined
            return {}, state

        window_col = "Window"
        strategy = self.config.windowing.strategy

        if strategy == "fixed":
            unique_windows = windowed.select(window_col).to_series().to_list()

            if not unique_windows:
                state.template_remainder = templates_combined
                return {}, state

            last_window_value = unique_windows[-1]
            completed_windowed = windowed.filter(
                pl.col(window_col) != last_window_value
            )

            window_size = self.config.windowing.window_size or "5m"
            template_windows = templates_combined.with_columns(
                pl.col("Timestamp").dt.truncate(window_size).alias("_Window")
            )
            state.template_remainder = template_windows.filter(
                pl.col("_Window") == last_window_value
            ).drop("_Window")

            if completed_windowed.is_empty():
                return {}, state
        else:
            completed_windowed = windowed
            state.template_remainder = pl.DataFrame()

        event_matrix = self.event_matrix.process(completed_windowed)
        if event_matrix.is_empty():
            return {}, state

        anomalies = self.anomaly_detector.process(event_matrix)
        self._update_metrics(state, event_matrix, anomalies)

        results = {
            "windowed": completed_windowed,
            "event_matrix": event_matrix,
            "anomalies": anomalies,
        }

        if writer is not None:
            for stage, df in results.items():
                writer.append(stage, df)

        state.chunk_index += 1

        return results, state

    def process_stream(
        self,
        chunk_iterator: Iterable[Union[Sequence[str], pl.DataFrame]],
        state: Optional[StreamingPipelineState] = None,
        writer: Optional[StreamingResultWriter] = None,
    ) -> StreamingPipelineState:
        if state is None:
            state = StreamingPipelineState()

        start_time = perf_counter()

        for raw_chunk in chunk_iterator:
            _, state = self.process_chunk(raw_chunk, state=state, writer=writer)

        self._flush_remainder(state, writer)

        duration = perf_counter() - start_time
        throughput = state.total_logs / duration if duration > 0 else 0.0
        logger.info(
            f"Streaming summary: {state.total_logs} logs -> {state.total_windows} windows ({state.total_anomalies} anomalies) in {duration:.2f}s ({throughput:.1f} logs/s)",
        )

        return state

    def flush_visualizations(
        self,
        state: StreamingPipelineState,
        directory_name: str = "visual_ready",
    ) -> None:
        return

    def _flush_remainder(
        self,
        state: StreamingPipelineState,
        writer: Optional[StreamingResultWriter],
    ) -> None:
        if state.template_remainder.is_empty():
            return

        logger.info("Flushing remaining templates into final window")
        remainder_sorted = state.template_remainder.sort("Timestamp")
        windowed = self.windowing.process(remainder_sorted)
        if windowed.is_empty():
            state.template_remainder = pl.DataFrame()
            return

        event_matrix = self.event_matrix.process(windowed)
        if event_matrix.is_empty():
            state.template_remainder = pl.DataFrame()
            return

        anomalies = self.anomaly_detector.process(event_matrix)
        self._update_metrics(state, event_matrix, anomalies)

        if writer is not None:
            writer.append("windowed", windowed)
            writer.append("event_matrix", event_matrix)
            writer.append("anomalies", anomalies)

        state.template_remainder = pl.DataFrame()

    @staticmethod
    def _update_metrics(
        state: StreamingPipelineState,
        event_matrix: pl.DataFrame,
        anomalies: pl.DataFrame,
    ) -> None:
        state.total_windows += len(event_matrix)
        if "IsAnomaly" in anomalies.columns:
            try:
                anomaly_count = int(anomalies["IsAnomaly"].sum())
            except TypeError:
                anomaly_count = int(anomalies["IsAnomaly"].cast(pl.Int64).sum())
            state.total_anomalies += anomaly_count

    @staticmethod
    def _prepare_anomalies_for_visuals(data: pl.DataFrame) -> pl.DataFrame:
        target_columns = [
            "Window",
            "WindowStart",
            "WindowEnd",
            "LogCount",
            "AnomalyScore",
            "IsAnomaly",
        ]

        if data.is_empty():
            return pl.DataFrame({col: [] for col in target_columns})

        expressions = []
        for column in target_columns:
            if column in data.columns:
                expressions.append(pl.col(column).alias(column))
            else:
                expressions.append(pl.lit(None).alias(column))

        return data.select(expressions)


def stream_file_dataset(
    config: Union[PipelineConfig, Dict[str, Any]], chunk_size: int
) -> Iterator[Sequence[str]]:
    """Helper to chunk raw datasets according to config."""

    if isinstance(config, PipelineConfig):
        dataset_type = config.dataset.type
        dataset_path = config.dataset.path
        dataset_format = config.dataset.format or "raw"
    else:
        dataset_type = config["dataset"]["type"]
        dataset_path = config["dataset"]["path"]
        dataset_format = config["dataset"].get("format", "raw")

    if dataset_type != "file" or dataset_format != "raw":
        raise NotImplementedError(
            "Streaming only implemented for raw file datasets at the moment"
        )

    path = Path(dataset_path)
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        chunk: List[str] = []
        for line in fh:
            line = line.rstrip("\n\r")
            if not line:
                continue
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
