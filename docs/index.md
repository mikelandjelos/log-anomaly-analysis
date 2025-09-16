# Log Anomaly Analysis

A modular, configurable pipeline for log anomaly detection using multiple windowing strategies and machine learning techniques.

## Overview

This project provides a comprehensive solution for detecting anomalies in system logs through:

- **Modular Architecture**: Each pipeline component is independently configurable and replaceable
- **Multiple Windowing Strategies**: Fixed, sliding, session-based, and adaptive volume windows
- **High Performance**: Built with Polars for fast data processing of large log files
- **Extensible Design**: Easy to add new algorithms, data sources, and visualization techniques

## Key Features

### Preprocessing
- Configurable log pattern matching with regex
- Automatic timestamp parsing and normalization
- Flexible handling of malformed log entries

### Template Extraction
- Drain3 algorithm for automatic log template discovery
- Configurable similarity thresholds and masking rules
- Support for custom delimiters and pattern recognition

### Windowing Strategies
- **Fixed Time Windows**: Regular intervals (e.g., every 5 minutes)
- **Sliding Windows**: Overlapping time periods for trend analysis
- **Session Windows**: Activity-based grouping with timeout detection
- **Adaptive Windows**: Volume-based windowing that adjusts to log density

### Anomaly Detection
- PCA-based subspace anomaly detection
- TF-IDF preprocessing for feature normalization
- Chi-squared threshold calculation for statistical significance

### Visualization
- Interactive and static plot generation
- Comprehensive dashboards with multiple view types
- t-SNE dimensionality reduction for pattern visualization

## Quick Start

```bash
# Install dependencies
poetry install

# Run basic analysis
poetry run log-anomaly configs/apache/basic.yaml

# Create visualizations
poetry run log-anomaly-visualize data/results/apache_basic/
```

## Configuration

All pipeline behavior is controlled through YAML configuration files:

```yaml
dataset:
  type: "file"
  path: "data/raw/Apache_2k.log"
  format: "raw"

preprocessing:
  log_pattern: '\[(?P<Timestamp>.+?)\]\s+\[(?P<LogLevel>\w+)]\s+(?P<Content>.+)'
  timestamp_format: "%a %b %d %H:%M:%S %Y"

windowing:
  strategy: "fixed"
  window_size: "5m"

anomaly_detection:
  variance_threshold: 0.95
  alpha: 0.001
```

## Architecture

The pipeline consists of six main components:

1. **Dataset Loader**: Handles various input formats (raw text, CSV, Parquet)
2. **Preprocessor**: Parses raw logs into structured format
3. **Template Parser**: Extracts recurring patterns using Drain3
4. **Windowing**: Groups logs by time or session
5. **Event Matrix**: Converts templates to numerical features
6. **Anomaly Detector**: Identifies outliers using PCA subspace method

Each component is independently configurable and can be replaced with custom implementations.