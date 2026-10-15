# syntax=docker/dockerfile:1
# isel batch image. Build on the target host from one commit (see DESIGN §12):
#   GIT_SHA=<sha> docker compose -f deploy/compose.compute.yaml build --build-arg GIT_SHA=<sha> isel
# `--target test` runs the unit tests without network access.

ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.7.3@sha256:87a04222b228501907f487b338ca6fc1514a93369bfce6930eb06c8d576e58a4

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS base
# LightGBM needs the OpenMP runtime, which slim images lack.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

FROM base AS builder
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
# Dependencies first: this layer is reused until uv.lock changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project --no-editable
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM builder AS test
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable
COPY config ./config
COPY tests ./tests
# No network: proves DuckDB, tldextract and LightGBM need no downloads at run time.
RUN --network=none /opt/venv/bin/python -m pytest -q -p no:cacheprovider

FROM base AS runtime
ARG GIT_SHA=unknown
COPY --from=builder /opt/venv /opt/venv
COPY config /app/config
ENV PATH=/opt/venv/bin:$PATH \
    ISEL_CODE_VERSION=${GIT_SHA}
LABEL org.opencontainers.image.title="isel" \
      org.opencontainers.image.revision=${GIT_SHA}
# Same uid:gid as the host account, so files in the mounted work dir stay ours.
USER 1014:1015
WORKDIR /work
ENTRYPOINT ["isel"]
