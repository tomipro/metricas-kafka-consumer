FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates gcc g++ librdkafka-dev curl && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/consumer.py /app/consumer.py
CMD ["python", "/app/consumer.py"]
