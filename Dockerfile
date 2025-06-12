# Use lightweight Python base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Environment variables (fallback defaults only — final ones should be loaded via .env or --env-file)
ENV PYTHONUNBUFFERED=1 \
    MAX_RETRIES=3 \
    RETRY_DELAY=600 \
    BATCH_SIZE=125 \
    BATCH_INTERVAL=21600 \
    BATCH_TIMEOUT=21600 \
    MAIL_SEND_ENABLED=False \
    FORCE_DRAFTS=True \
    YOUR_DOMAIN=abc-amega.com \
    MS_GRAPH_TIMEOUT=60 \
    ADD_EMAIL_FOOTER=true \
    TIME_FILTER_HOURS=24 \
    EMAIL_FETCH_TOP=1000 \
    LOG_LEVEL=INFO \
    LOG_DIR=/app/logs

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Ensure logs folder exists
RUN mkdir -p /app/logs

# Expose port (if you're running a Flask/FastAPI server)
EXPOSE 5000

# Health check (adjust if endpoint differs)
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Start the application
CMD ["python", "main.py"]
