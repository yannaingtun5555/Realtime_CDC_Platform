FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Kafka binaries (to use kafka-topics.sh)
RUN curl -fsSL https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz | tar -xz -C /opt && \
    mv /opt/kafka_2.13-3.4.0 /opt/kafka

ENV PATH="/opt/kafka/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY automation.py .

CMD ["python", "-u", "automation.py"]