FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY scripts/ ./scripts/

COPY datasets/ ./datasets/

CMD ["uv", "run", "scripts/etl_insecta.py"]