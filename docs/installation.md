# Installation Guide

## Requirements

- Python 3.9 or higher
- Poetry (recommended) or pip

## Using Poetry (Recommended)

Poetry provides better dependency management and virtual environment handling.

### Install Poetry

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### Clone and Install

```bash
git clone https://github.com/yourusername/log-anomaly-analysis
cd log-anomaly-analysis
poetry install
```

### Install Optional Dependencies

```bash
# For visualization features
poetry install --with visualization

# For development tools
poetry install --with dev

# For documentation
poetry install --with docs

# Install everything
poetry install --with dev,visualization,docs
```

## Using pip

```bash
git clone https://github.com/yourusername/log-anomaly-analysis
cd log-anomaly-analysis
pip install -e .
```

## Verify Installation

```bash
# Using Poetry
poetry run log-anomaly --help

# Using pip
log-anomaly --help
```

## Development Setup

For development work, install pre-commit hooks:

```bash
poetry install --with dev
poetry run pre-commit install
```

This will automatically run code formatting and linting on each commit.

## Docker Setup (Optional)

Create a Dockerfile for containerized deployment:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

# Copy application code
COPY src/ ./src/
COPY configs/ ./configs/

# Set entrypoint
ENTRYPOINT ["python", "-m", "log_anomaly_analysis.cli"]
```

Build and run:

```bash
docker build -t log-anomaly-analysis .
docker run -v $(pwd)/data:/app/data log-anomaly-analysis configs/apache/basic.yaml
```
