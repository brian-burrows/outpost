FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel uv

COPY uv.lock pyproject.toml /app/

RUN uv sync --locked --no-dev

COPY . /app
RUN uv sync --locked --no-dev

RUN groupadd --system --gid 999 nonroot \
    && useradd --system --gid 999 --uid 999 --create-home nonroot \
    && chown -R nonroot:nonroot /app

USER nonroot

EXPOSE 8000

ENV PATH="/app/.venv/bin:$PATH"

CMD ["/app/.venv/bin/gunicorn", "main:app", \
    "--workers", "1", \
    "--bind", "0.0.0.0:8000", \
    "--access-logfile", "-", \
    "--log-level", "debug", \
    "--worker-class", "uvicorn.workers.UvicornWorker"]
