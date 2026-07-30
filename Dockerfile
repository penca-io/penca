# Multi-stage build for the Penca Python client.
# Build: docker build -t penca-client .
# Test:  docker run penca-client python -c "from penca_client import PencaClient; print('ok')"

FROM python:3.10-slim AS build

# Pinned, not `:latest`: an unpinned tag lets a rebuild pick up a new uv
# without a repo change, which is how the toolchain drifts silently between
# CI and local. CHA-542.
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv

WORKDIR /app

# Copy workspace definition files first for layer caching.
COPY pyproject.toml uv.lock .python-version ./
COPY packages/penca-client/pyproject.toml packages/penca-client/pyproject.toml
COPY packages/penca-proto/pyproject.toml packages/penca-proto/pyproject.toml

# Copy source code.
COPY packages/ packages/

# Install dependencies (no dev deps, locked versions).
RUN uv sync --frozen --no-dev

# --- Final stage: runtime only ---
FROM python:3.10-slim

WORKDIR /app
COPY --from=build /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python"]
