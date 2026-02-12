"""
Kafka Event Producer — Exactly-once production from edge to cloud.

Wraps confluent-kafka with:
- Idempotent producer (no duplicates even on retry)
- Delivery callbacks for monitoring
- Graceful flush on shutdown
"""

import json
import structlog
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError

logger = structlog.get_logger(__name__)


class EventProducer:
    """
    Kafka producer with exactly-once semantics for the stadium-to-cloud path.

    Configuration from architecture:
    - acks=all (wait for all replicas)
    - enable.idempotence=true (no duplicates on retry)
    - compression.type=lz4 (fast compression)
    - linger.ms=5 (minimal batching for low latency)
    """

    def __init__(self, kafka_config: dict, gateway_id: str):
        self.gateway_id = gateway_id

        producer_conf = {
            "bootstrap.servers": kafka_config["bootstrap_servers"],
            "client.id": f"edge-gateway-{gateway_id}",
            "acks": kafka_config.get("acks", "all"),
            "enable.idempotence": kafka_config.get("enable_idempotence", True),
            "compression.type": kafka_config.get("compression_type", "lz4"),
            "linger.ms": kafka_config.get("linger_ms", 5),
            "batch.size": kafka_config.get("batch_size", 16384),
            "max.in.flight.requests.per.connection": kafka_config.get("max_in_flight", 5),
            "retries": kafka_config.get("retries", 2147483647),
            "delivery.timeout.ms": kafka_config.get("delivery_timeout_ms", 120000),
        }

        self._producer = Producer(producer_conf)
        self._delivery_count = 0
        self._error_count = 0

        logger.info(
            "kafka_producer_initialized",
            gateway_id=gateway_id,
            servers=kafka_config["bootstrap_servers"],
        )

    async def produce(self, topic: str, key: str, value: dict):
        """
        Produce an event to Kafka.

        Before producing, stamp the event with:
        - published_at timestamp (processing time)
        - stadium_gateway_id
        """
        # Stamp with processing-time timestamp and gateway ID
        value["published_at"] = datetime.now(timezone.utc).isoformat()
        value.setdefault("source", {})["stadium_gateway_id"] = self.gateway_id

        serialized = json.dumps(value).encode("utf-8")

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=serialized,
            callback=self._delivery_callback,
        )

        # Trigger delivery report callbacks
        self._producer.poll(0)

    def _delivery_callback(self, err, msg):
        """Callback invoked when Kafka confirms or rejects a message."""
        if err:
            self._error_count += 1
            logger.error(
                "kafka_delivery_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                error=str(err),
            )
        else:
            self._delivery_count += 1
            logger.debug(
                "kafka_delivery_confirmed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    async def flush(self, timeout: float = 30.0):
        """Flush all pending messages before shutdown."""
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(
                "kafka_flush_incomplete",
                remaining_messages=remaining,
            )
        else:
            logger.info(
                "kafka_flush_completed",
                delivered=self._delivery_count,
                errors=self._error_count,
            )

    @property
    def stats(self) -> dict:
        return {
            "delivered": self._delivery_count,
            "errors": self._error_count,
        }
