FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies in a virtualenv for smaller final image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml .
# Install dependencies only
RUN pip install --no-cache-dir .

FROM python:3.11-slim

WORKDIR /app

# Copy the virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY . .

# Install the app itself into the venv
RUN pip install --no-cache-dir -e .

EXPOSE 8000

# Set production env vars
ENV PYTHONUNBUFFERED=1

CMD uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}
