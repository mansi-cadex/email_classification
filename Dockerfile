# Use lightweight Python base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for the application
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    openssh-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Environment variables (fallback defaults - actual values loaded from .env)
# These values perfectly match your .env configuration
ENV PYTHONUNBUFFERED=1 \
    # Core batch processing (aligned with .env)
    BATCH_SIZE=30 \
    BATCH_INTERVAL=6000 \
    BATCH_TIMEOUT=4200 \
    MAX_RETRIES=3 \
    RETRY_DELAY=600 \
    # Email configuration (aligned with .env)
    MAIL_SEND_ENABLED=False \
    FORCE_DRAFTS=True \
    ADD_EMAIL_FOOTER=true \
    YOUR_DOMAIN=abc-amega.com \
    # System configuration (aligned with .env)
    MS_GRAPH_TIMEOUT=60 \
    TIME_FILTER_HOURS=24 \
    EMAIL_FETCH_TOP=1000 \
    LOG_LEVEL=INFO \
    LOG_DIR=/app/logs \
    # SFTP configuration (aligned with .env)
    SFTP_ENABLED=True \
    SFTP_PORT=22

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Ensure logs folder exists with proper permissions
RUN mkdir -p /app/logs && chmod 755 /app/logs

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port for health checks and monitoring
EXPOSE 5000

# Health check optimized for 3-email automation
# Increased start-period to account for 3-account initialization
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Add labels for better container management
LABEL maintainer="ABC/AMEGA Development Team" \
      description="Email Classification System with 3-Email Automation" \
      version="1.0" \
      email-accounts="3" \
      batch-size="30"

# Start the application
CMD ["python", "main.py"]