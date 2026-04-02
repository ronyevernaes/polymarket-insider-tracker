FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies in a cached layer (skip the project itself — src/ not copied yet)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application source and migration files
COPY README.md ./
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Install the project package now that src/ is present
RUN uv sync --frozen --no-dev

# Run DB migrations then start the tracker
CMD ["sh", "-c", "uv run alembic upgrade head && uv run python -m polymarket_insider_tracker"]
