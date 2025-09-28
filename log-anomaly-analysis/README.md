# Log Anomaly Analysis

A modular, configurable pipeline for log anomaly detection using multiple windowing strategies and machine learning techniques.

## Features

- **Modular Architecture**: Easily swappable components for each pipeline stage
- **Multiple Windowing Strategies**: Fixed, sliding, session-based, and adaptive volume windows
- **High Performance**: Built with Polars for fast data processing
- **Configurable**: YAML-based configuration for all components
- **Extensible**: Easy to add new algorithms and data sources
- **Rich CLI**: Command-line interface with progress bars and colored output

## Installation

### Using Poetry (Recommended)

```bash
git clone https://github.com/yourusername/log-anomaly-analysis
cd log-anomaly-analysis
poetry install
```

### Using pip

```bash
pip install log-anomaly-analysis
```

## Quick Start

```bash
# Activate the poetry environment
poetry shell

# Run with Apache logs using basic configuration
log-anomaly configs/apache/basic.yaml --data-path data/raw/Apache_2k.log

# Run with sliding windows
log-anomaly configs/apache/sliding.yaml --data-path data/raw/Apache_2k.log

# Visualize results
log-anomaly-visualize data/results/apache_basic_results/
```

## Configuration

Create a YAML configuration file:

```yaml
dataset:
  type: "file"
  path: "Apache_2k.log"
  format: "raw"

preprocessing:
  log_pattern: '\\[(?P<Timestamp>.+?)\\]\\s+\\[(?P<LogLevel>\\w+)]\\s+(?P<Content>.+)'
  timestamp_format: "%a %b %d %H:%M:%S %Y"

windowing:
  strategy: "fixed"
  window_size: "5m"

anomaly_detection:
  variance_threshold: 0.95
  alpha: 0.001
```

## Development

```bash
# Install development dependencies
poetry install --with dev,visualization,docs

# Run tests
poetry run pytest

# Format code
poetry run black src/ tests/
poetry run isort src/ tests/

# Type checking
poetry run mypy src/

# Install pre-commit hooks
poetry run pre-commit install
```

### Project structure explanation

```bash
log-anomaly-analysis/
├── .gitignore
├── .pre-commit-config.yaml        # Code quality hooks
├── pyproject.toml                 # Poetry configuration
├── README.md                      # Project documentation
├── src/log_anomaly_analysis/      # Main source code
│   ├── __init__.py
│   ├── cli.py                     # Command-line interface
│   ├── core/                      # Core pipeline components
│   │   ├── __init__.py
│   │   ├── pipeline.py            # Main orchestrator
│   │   ├── components/            # Modular components
│   │   │   ├── __init__.py
│   │   │   ├── base.py                 # Base component class
│   │   │   ├── preprocessing.py        # Log preprocessing
│   │   │   ├── parsing.py              # Drain3 template extraction
│   │   │   ├── windowing.py            # Multiple windowing strategies
│   │   │   ├── event_matrix.py         # Event count matrix creation
│   │   │   └── anomaly_detection.py    # PCA anomaly detection
│   │   └── config/                     # Configuration management
│   │       ├── __init__.py
│   │       ├── models.py         # Dataclass models
│   │       └── loader.py         # YAML config loading
│   ├── datasets/                 # Dataset loading utilities
│   │   ├── __init__.py
│   │   └── loaders.py
│   ├── utils/                    # Utility functions
│   │   ├── __init__.py
│   │   ├── logging.py            # Loguru setup
│   │   └── metrics.py            # Performance monitoring
│   └── visualization/            # Plotting and dashboards
│       ├── __init__.py
│       ├── plots.py              # Visualization functions
│       └── dashboard.py          # Interactive dashboards
├── configs/                   # Configuration files
│   ├── apache/                # Apache-specific configs
│   │   ├── basic.yaml         # Fixed time windows
│   │   ├── sliding.yaml       # Sliding windows
│   │   ├── session.yaml       # Session-based windows
│   │   └── adaptive.yaml      # Adaptive volume windows
│   └── templates/
│       └── default.yaml         # Default configuration template
├── tests/                     # Test suite
│   ├── __init__.py
│   ├── conftest.py            # Test fixtures
│   ├── test_components/       # Component tests
│   │   ├── __init__.py
│   │   ├── test_preprocessing.py
│   │   ├── test_parsing.py
│   │   ├── test_windowing.py
│   │   └── test_anomaly_detection.py
│   ├── test_integration/        # Integration tests
│   │   ├── __init__.py
│   │   └── test_pipeline.py
│   └── fixtures/                # Test data
│       ├── sample_logs.txt
│       └── test_configs.yaml
├── examples/                     # Usage examples
│   ├── basic_usage.py
│   ├── custom_components.py
│   └── notebooks/               # Jupyter notebooks
│       ├── apache_analysis.ipynb
│       └── windowing_comparison.ipynb
├── docs/                        # Documentation
│   ├── index.md
│   └── installation.md
└── data/                       # Data directories
    ├── raw/.gitkeep            # Raw log files (datasets)
    ├── processed/.gitkeep      # Intermediate data
    └── results/.gitkeep        # Final results (excluded from git)
```

## License

MIT License - see LICENSE file for details.
