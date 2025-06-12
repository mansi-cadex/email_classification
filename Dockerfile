# Use Python 3.11 slim as base image
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /app

# Install system dependencies required for compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Create application structure matching your exact code
RUN mkdir -p src logs && \
    chmod 755 src logs

# Copy all application files in correct structure
COPY main.py .
COPY loop.py .
COPY src/ ./src/

# Ensure src module has __init__.py
RUN touch src/__init__.py

# Make sure main.py is executable
RUN chmod +x main.py

# Set environment variables based on ALL your files
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    LOG_LEVEL=INFO \
    LOG_DIR=/app/logs \
    BATCH_SIZE=125 \
    BATCH_INTERVAL=21600 \
    MAX_RETRIES=3 \
    RETRY_DELAY=600 \
    BATCH_TIMEOUT=21600 \
    MAIL_SEND_ENABLED=False \
    FORCE_DRAFTS=True \
    SFTP_ENABLED=True \
    MS_GRAPH_TIMEOUT=60 \
    ADD_EMAIL_FOOTER=true \
    YOUR_DOMAIN=abc-amega.com \
    TIME_FILTER_HOURS=24 \
    EMAIL_FETCH_TOP=1000 \
    SFTP_PORT=22

# Expose port for health check (Flask app in main.py)
EXPOSE 5000

# Health check endpoint with proper timing for initialization
HEALTHCHECK --interval=30s --timeout=30s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Run the application
CMD ["python", "main.py"]