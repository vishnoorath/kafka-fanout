FROM python:3.11-slim

# Avoid bytecode + interactive prompts; route stdout/stderr straight to
# the docker logs (so uvicorn output is visible without buffering).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install OS-level deps that some of our wheels expect (gcc for any
# sdist fallback; curl is handy for the healthcheck).
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc curl libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first so the layer caches when only source changes.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the application source.
COPY app ./app

# Create a non-root user for runtime safety.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# The SQLite file lives next to the working directory; mount a volume
# here to persist it across container restarts.
VOLUME ["/app/data"]
WORKDIR /app/data
ENV DATABASE_URL=sqlite+aiosqlite:///./kafka_fanout.db

EXPOSE 8000

# `app` is the FastAPI module; uvicorn's `app.main:app` lives one
# directory up under /app, so we set PYTHONPATH explicitly.
ENV PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
