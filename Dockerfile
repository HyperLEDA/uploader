FROM python:3.13-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY uploader ./uploader
RUN uv sync --frozen --no-dev

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
COPY . .
