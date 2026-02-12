# ============================================================
# Deployment Guide — Real-Time Sports Data System
# ============================================================

## Prerequisites

- Docker + Docker Compose
- Kubernetes cluster (for production)
- AWS/GCP account (for managed services)

## Local Development

### Quick Start

```bash
# Clone the repository
git clone git@github.com:org/realtime-sportdata.git
cd realtime-sportdata

# Start the full stack
docker-compose up -d

# Verify services are healthy
docker-compose ps

# View logs
docker-compose logs -f edge-gateway
```

### Service Ports (Local)

| Service | Port | URL |
|---|---|---|
| Edge Gateway (Health) | 8080 | http://localhost:8080/health |
| Edge Gateway (Metrics) | 9090 | http://localhost:9090/metrics |
| WebSocket Gateway | 8081 | ws://localhost:8081/ws/matches/{id} |
| REST API | 8082 | http://localhost:8082/docs |
| Kafka Broker 1 | 9092 | localhost:9092 |
| Schema Registry | 8081 | http://localhost:8081/subjects |
| Redis | 6379 | localhost:6379 |
| PostgreSQL | 5432 | localhost:5432 |
| ClickHouse (HTTP) | 8123 | http://localhost:8123 |
| Prometheus | 9091 | http://localhost:9091 |
| Grafana | 3000 | http://localhost:3000 (admin/admin) |
| Jaeger UI | 16686 | http://localhost:16686 |

## Production Deployment (Kubernetes)

### 1. Create Namespace

```bash
kubectl create namespace sportdata
```

### 2. Deploy Config & Secrets

```bash
kubectl apply -f infrastructure/kubernetes/configmaps/
kubectl apply -f infrastructure/kubernetes/secrets/
```

### 3. Deploy Data Stores

```bash
# Redis (or use ElastiCache/Memorystore)
kubectl apply -f infrastructure/kubernetes/redis.yaml

# PostgreSQL (or use RDS/Cloud SQL)
kubectl apply -f infrastructure/kubernetes/postgres.yaml
```

### 4. Deploy Kafka

```bash
# Using Strimzi Kafka operator (recommended)
kubectl apply -f infrastructure/kubernetes/kafka/
```

### 5. Deploy Application Services

```bash
kubectl apply -f infrastructure/kubernetes/edge-gateway.yaml
kubectl apply -f infrastructure/kubernetes/websocket-gateway.yaml
kubectl apply -f infrastructure/kubernetes/rest-api.yaml
kubectl apply -f infrastructure/kubernetes/push-notification.yaml
```

### 6. Deploy Flink Jobs

```bash
# Using Flink Kubernetes operator
kubectl apply -f infrastructure/kubernetes/flink/
```

### 7. Deploy Observability

```bash
kubectl apply -f infrastructure/kubernetes/prometheus.yaml
kubectl apply -f infrastructure/kubernetes/grafana.yaml
kubectl apply -f infrastructure/kubernetes/jaeger.yaml
```

## Scaling Guidelines

| Component | Scaling Strategy | Key Metric |
|---|---|---|
| Edge Gateway | 1 per stadium, 2 replicas for HA | WAL pending events |
| Kafka | 3+ brokers, 12 partitions per topic | Consumer lag |
| Flink Enrichment | Match parallelism to Kafka partitions | Checkpoint duration |
| WebSocket Gateway | HPA on connection count | Active connections |
| Push Notification | HPA on Kafka consumer lag | Notification queue depth |
| REST API | HPA on CPU | Request latency P99 |
| Redis | Cluster mode, 3+ nodes | Memory usage |
| PostgreSQL | Read replicas for queries | Connection count |
| ClickHouse | Sharding by match_id | Query latency |

## Monitoring Checklist

- [ ] Grafana pipeline dashboard shows green
- [ ] Kafka consumer lag < 100 for all groups
- [ ] WAL pending events = 0
- [ ] E2E latency P99 < 3 seconds
- [ ] Push notification delivery rate > 99%
- [ ] No errors in DLQ topic
