# ============================================================
# Runbook — Incident Response Procedures
# ============================================================

## 🟥 P1 — Kafka Cluster Down

### Symptoms
- Events accumulating in Edge Gateway WAL
- Consumer lag spiking to infinity
- Grafana alerts: `kafka_under_replicated_partitions > 0`

### Response
1. Check Kafka broker status: `kubectl get pods -l app=kafka`
2. Check Zookeeper: `kubectl exec -it zookeeper-0 -- zkCli.sh stat`
3. Verify network connectivity between brokers
4. Check disk space on broker nodes
5. If single broker: restart pod, wait for ISR catch-up
6. If cluster-wide: escalate to infrastructure team

### Impact
- WAL buffers events at stadium (capacity: ~10,000 events / 1GB)
- WebSocket clients receive stale data
- Notifications delayed until recovery
- Betting markets should auto-suspend on data feed loss

---

## 🟧 P2 — High End-to-End Latency (> 3s)

### Symptoms
- Grafana: `E2E latency P99 > 3s`
- User complaints about delayed notifications

### Response
1. Check per-stage latency breakdown in Grafana
2. `occurred_at → received_at` (gateway latency)
3. `received_at → published_at` (Kafka latency)
4. Flink checkpoint duration
5. If gateway: check source connectivity, WAL size
6. If Kafka: check consumer lag, broker CPU
7. If Flink: check backpressure, state size
8. If notification: check FCM/APNs response times

---

## 🟧 P2 — Flink Job Crashed

### Symptoms
- Consumer lag growing on `match-events-raw`
- No new events on `match-events-enriched`
- Flink dashboard: job in FAILED state

### Response
1. Check Flink TaskManager logs
2. Verify last successful checkpoint
3. Restart from latest checkpoint:
   ```bash
   flink run -s <checkpoint-path> enrichment-job.jar
   ```
4. Monitor consumer lag until it returns to normal
5. Verify match state accuracy after recovery

### The Good News
Flink checkpoints include all keyed state (scores, cards, etc.).
Recovery from checkpoint means no data loss and correct state.

---

## 🟨 P3 — DLQ Events Growing

### Symptoms
- `match-events-dlq` has messages
- Grafana: `dlq_events_total > 0`

### Response
1. Read DLQ messages to understand failure reason
2. Common causes:
   - Schema validation failure → check schema version compatibility
   - Serialization error → check Avro schema registry
   - Processing exception → check Flink logs

### Investigation
```bash
# Read DLQ messages
kafka-console-consumer --bootstrap-server kafka-1:9092 \
  --topic match-events-dlq --from-beginning --max-messages 10
```

---

## 🟨 P3 — Push Notification Delivery Rate < 99%

### Symptoms
- FCM/APNs error rates increasing
- Users reporting missed notifications

### Response
1. Check FCM dashboard for service status
2. Check APNs status at https://developer.apple.com/system-status/
3. Verify device tokens are not stale
4. Check rate limiting — are we hitting FCM quotas?
5. Check push dispatcher logs for error patterns

---

## 🟩 P4 — Stadium Network Flap

### Symptoms
- Gateway WAL growing
- Events delayed but not lost

### Response
1. Check WAL size: `gateway_wal_pending_events` metric
2. If < 5,000: wait for network recovery (self-healing)
3. If > 5,000: investigate network, consider 4G failover
4. After recovery: monitor WAL drain rate
5. Verify no events lost (compare sequence numbers)

### Design Note
The WAL is specifically designed for this case. A 30-second
network blip during a Champions League final would buffer
~50 events (plenty of capacity). Recovery is automatic.
