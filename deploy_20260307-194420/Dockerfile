FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install minimal system deps (no Java - Spark runs in Azure Synapse only)
RUN apt-get update && apt-get install -y \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

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
    CMD curl -f http://localhost:8000/health || exit 1

# Run the API (Spark runs in Azure Synapse, not locally)
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

