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
ENV LANGFUSE_SECRET_KEY=sk-lf-f708968e-5023-41f6-a4b4-da6b9c6f01c8
ENV LANGFUSE_PUBLIC_KEY=pk-lf-b3bc8526-3a13-44ad-be08-78de2f1f89ea
ENV LANGFUSE_BASE_URL=https://langfuse.7sg.ai

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the API (Spark runs in Azure Synapse, not locally)
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

