"""
Prometheus metrics server for the Edge Gateway.
"""

import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server

logger = structlog.get_logger(__name__)

# --- Counters ---
EVENTS_RECEIVED = Counter(
    "gateway_events_received_total",
    "Total events received from all sources",
    ["source", "event_type"],
)

EVENTS_PRODUCED = Counter(
    "gateway_events_produced_total",
    "Total events produced to Kafka",
    ["topic", "event_type"],
)

EVENTS_DEDUPLICATED = Counter(
    "gateway_events_deduplicated_total",
    "Total duplicate events dropped",
    ["event_type"],
)

EVENTS_VALIDATION_FAILED = Counter(
    "gateway_events_validation_failed_total",
    "Total events that failed schema validation",
    ["event_type"],
)

# --- Histograms ---
EVENT_PROCESSING_LATENCY = Histogram(
    "gateway_event_processing_seconds",
    "Time to process an event through the gateway pipeline",
    ["event_type"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

KAFKA_PRODUCE_LATENCY = Histogram(
    "gateway_kafka_produce_seconds",
    "Time to produce event to Kafka (including ack)",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0],
)

# --- Gauges ---
WAL_PENDING_EVENTS = Gauge(
    "gateway_wal_pending_events",
    "Number of events pending in the Write-Ahead Log",
)

ACTIVE_SOURCES = Gauge(
    "gateway_active_sources",
    "Number of active event sources",
)

DEDUP_CACHE_SIZE = Gauge(
    "gateway_dedup_cache_size",
    "Current size of the deduplication cache",
)


class MetricsServer:
    """Prometheus metrics HTTP server."""

    def __init__(self, config: dict):
        self.port = config.get("port", 9090)

    async def start(self):
        start_http_server(self.port)
        logger.info("metrics_server_started", port=self.port)

    async def stop(self):
        pass  # prometheus_client handles cleanup
