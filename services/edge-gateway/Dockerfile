# Edge Gateway Service
# Stadium-deployed event gateway for real-time sports data

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

# WAL storage volume
VOLUME /data/wal

ENV PYTHONUNBUFFERED=1
ENV GATEWAY_ID=gw-parc-des-princes-01
ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
ENV WAL_DIR=/data/wal
ENV LOG_LEVEL=INFO

EXPOSE 8080
EXPOSE 9090

CMD ["python", "-m", "src.main"]
