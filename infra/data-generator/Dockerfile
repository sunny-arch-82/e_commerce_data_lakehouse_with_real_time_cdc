FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY . ./

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD pg_isready -h postgres -p 5432 || exit 1

CMD ["python", "-u", "-m", "src.generator"]
