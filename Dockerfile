FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# OpenTelemetry auto-instrumentation for Langfuse tracing
RUN pip install --no-cache-dir opentelemetry-distro opentelemetry-exporter-otlp-proto-http opentelemetry-instrumentation-fastapi openinference-instrumentation-openai

COPY *.py ./
COPY src_data ./src_data
COPY profile_data ./profile_data 2>/dev/null || true

RUN mkdir -p logs

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["opentelemetry-instrument", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8080"]
