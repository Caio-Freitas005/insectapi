FROM python:3.13-slim

# Install UV
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

WORKDIR /app

# Copy and install dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy Workdirs
COPY api/ ./api/
COPY scripts/ ./scripts/

# Fallback for Render Port
ENV PORT=8000

# Command to start API 
# Consider Render PORT if running on internet or 8000 if locally
CMD ["sh", "-c", "exec uv run uvicorn api.main:app --host 0.0.0.0 --port $PORT"]