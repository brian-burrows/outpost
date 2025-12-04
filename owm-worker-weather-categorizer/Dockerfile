# Use Python 3.13 slim
FROM python:3.13-slim-bookworm

# Set working directory
WORKDIR /app

# Install system dependencies for building packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install UV package manager
RUN pip install --upgrade pip setuptools wheel uv

# Copy lockfile and pyproject.toml
COPY ./owm-worker-weather-categorizer/uv.lock /app/
COPY ./owm-worker-weather-categorizer/pyproject.toml /app/
COPY ./owm-worker-weather-categorizer/dist/ /app/dist
RUN uv sync --locked --no-dev --find-links file:///app/dist/


# Copy project source
COPY ./owm-worker-weather-categorizer/worker_categorization.py /app
COPY ./owm-worker-weather-categorizer/src /app/src
RUN uv pip install -e .

# Non-root user
RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot \
    && chown -R nonroot:nonroot /app

USER nonroot

EXPOSE 8000

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "/app/worker_categorization.py"]