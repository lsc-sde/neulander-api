FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_SYSTEM_PYTHON=1

COPY ./pyproject.toml ./neulander-api/pyproject.toml
COPY --from=neulander-core . ./neulander-core

RUN uv pip install --system -r ./neulander-api/pyproject.toml

COPY . ./neulander-api

WORKDIR /neulander-api
RUN uv pip install --system -e .

EXPOSE 8000
CMD fastapi run src/neulander_api/api.py --port 8000
