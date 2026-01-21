FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
# Note: Java is required by PySpark (used for Azure Synapse Spark API types)
# Spark execution happens remotely in Azure Synapse pools, not locally
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    procps \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create a wrapper script to set JAVA_HOME dynamically based on architecture
# This works for both amd64 and arm64 architectures
RUN echo '#!/bin/bash\n\
JAVA_DIR=$(find /usr/lib/jvm -maxdepth 1 -type d -name "java-21-openjdk-*" | head -1)\n\
if [ -n "$JAVA_DIR" ]; then\n\
    export JAVA_HOME="$JAVA_DIR"\n\
fi\n\
exec "$@"' > /usr/local/bin/run-with-java.sh && \
    chmod +x /usr/local/bin/run-with-java.sh

# Default JAVA_HOME (will be overridden by wrapper script)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./
COPY src_data ./src_data

# Create logs directory
RUN mkdir -p logs

# Expose API port
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Run the application using uvicorn with dynamic JAVA_HOME
CMD ["/usr/local/bin/run-with-java.sh", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

