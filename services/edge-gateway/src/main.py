"""
Edge Gateway — Main Entry Point

Stadium-deployed service that:
1. Receives signals from Hawk-Eye, Opta, referee comms, GPS sensors
2. Correlates and deduplicates signals into canonical events
3. Validates against JSON schema
4. Buffers in Write-Ahead Log for network resilience
5. Produces to Kafka with exactly-once semantics
"""

import asyncio
import signal
import structlog
from pathlib import Path

from .ingestion.source_manager import SourceManager
from .correlation.event_correlator import EventCorrelator
from .deduplication.dedup_engine import DeduplicationEngine
from .validation.schema_validator import SchemaValidator
from .wal.write_ahead_log import WriteAheadLog
from .producer.kafka_producer import EventProducer
from .api.health import HealthServer
from .config import load_config
from .metrics import MetricsServer

logger = structlog.get_logger(__name__)


class EdgeGateway:
    """
    Stadium Edge Gateway — the single point of truth that normalizes
    disparate signals into canonical events before publishing to Kafka.

    Runs at the venue. Provides local buffering (WAL) in case of
    network partitions — events are durably written to local disk
    and forwarded once connectivity resumes.
    """

    def __init__(self, config_path: str = "config/gateway.yaml"):
        self.config = load_config(config_path)
        self.gateway_id = self.config["gateway"]["id"]
        self.running = False

        # Core components
        self.source_manager = SourceManager(self.config["sources"])
        self.correlator = EventCorrelator(self.config["correlation"])
        self.dedup_engine = DeduplicationEngine(
            dedup_fields=self.config["correlation"]["dedup_fields"]
        )
        self.validator = SchemaValidator(
            schema_dir=self.config["validation"]["schema_dir"]
        )
        self.wal = WriteAheadLog(self.config["wal"])
        self.producer = EventProducer(self.config["kafka"], self.gateway_id)

        # Infrastructure
        self.health_server = HealthServer(self.config["health"])
        self.metrics_server = MetricsServer(self.config["metrics"])

        logger.info(
            "edge_gateway_initialized",
            gateway_id=self.gateway_id,
            stadium=self.config["gateway"]["stadium"],
        )

    async def start(self):
        """Start all gateway components."""
        self.running = True

        # Start infrastructure
        await self.health_server.start()
        await self.metrics_server.start()

        # Initialize WAL — replay any buffered events from previous crash
        pending_events = await self.wal.initialize()
        if pending_events:
            logger.info(
                "wal_replay_started",
                pending_count=len(pending_events),
            )
            for event in pending_events:
                await self._forward_to_kafka(event)
            logger.info("wal_replay_completed")

        # Start event sources
        await self.source_manager.start()

        # Main processing loop
        await self._processing_loop()

    async def _processing_loop(self):
        """
        Main loop: receive signals → correlate → deduplicate →
        validate → WAL → produce to Kafka.
        """
        logger.info("processing_loop_started")

        while self.running:
            try:
                # 1. Receive raw signals from all sources
                raw_signals = await self.source_manager.receive(timeout_ms=100)

                if not raw_signals:
                    continue

                # 2. Correlate signals into canonical events
                #    (fuses Hawk-Eye binary + Opta rich data into single event)
                canonical_events = self.correlator.correlate(raw_signals)

                for event in canonical_events:
                    # 3. Deduplicate using idempotency key
                    if self.dedup_engine.is_duplicate(event):
                        logger.debug(
                            "duplicate_event_dropped",
                            event_id=event.get("event_id"),
                            idempotency_key=event.get("metadata", {}).get(
                                "idempotency_key"
                            ),
                        )
                        continue

                    # 4. Validate against JSON schema
                    validation_result = self.validator.validate(event)
                    if not validation_result.is_valid:
                        logger.warning(
                            "schema_validation_failed",
                            event_type=event.get("event_type"),
                            errors=validation_result.errors,
                        )
                        continue

                    # 5. Write to WAL first (survive network failures)
                    await self.wal.append(event)

                    # 6. Produce to Kafka
                    await self._forward_to_kafka(event)

            except Exception as e:
                logger.error("processing_loop_error", error=str(e))
                await asyncio.sleep(0.1)

    async def _forward_to_kafka(self, event: dict):
        """Produce event to Kafka and acknowledge in WAL."""
        try:
            await self.producer.produce(
                topic="match-events-raw",
                key=event["match_id"],
                value=event,
            )
            # Mark as successfully forwarded in WAL
            await self.wal.acknowledge(event["event_id"])

            logger.info(
                "event_produced",
                event_id=event["event_id"],
                event_type=event["event_type"],
                match_id=event["match_id"],
                sequence=event.get("sequence_number"),
            )

        except Exception as e:
            # Event remains in WAL for retry
            logger.error(
                "kafka_produce_failed",
                event_id=event["event_id"],
                error=str(e),
            )

    async def stop(self):
        """Graceful shutdown."""
        logger.info("gateway_shutdown_started")
        self.running = False
        await self.source_manager.stop()
        await self.producer.flush()
        await self.wal.close()
        await self.health_server.stop()
        await self.metrics_server.stop()
        logger.info("gateway_shutdown_completed")


async def main():
    gateway = EdgeGateway()

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(gateway.stop()))

    await gateway.start()


if __name__ == "__main__":
    asyncio.run(main())
