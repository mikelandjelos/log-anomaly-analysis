FROM python:3.11-slim

WORKDIR /app

ENV POETRY_VERSION=1.8.3
RUN pip install "poetry==$POETRY_VERSION"

COPY pyproject.toml README.md /app/
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --only main

COPY drain3_service /app/drain3_service

CMD ["uvicorn", "drain3_service.app:app", "--host", "0.0.0.0", "--port", "8000"]
