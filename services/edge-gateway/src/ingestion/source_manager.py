"""
Source Manager — Manages connections to all event sources.

Receives signals from:
- Hawk-Eye (GLT): Binary goal detection via gRPC
- Opta (Stats Perform): Rich event data via TCP
- Referee Communications: Confirmation signals via gRPC
- GPS/IMU Sensors (EPTS): Player tracking via UDP
"""

import asyncio
import structlog
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from enum import Enum

logger = structlog.get_logger(__name__)


class SourceSystem(str, Enum):
    HAWK_EYE = "HAWK_EYE"
    OPTA = "OPTA"
    REFEREE_COMMS = "REFEREE_COMMS"
    MANUAL_OPERATOR = "MANUAL_OPERATOR"
    VAR = "VAR"
    GPS_SENSORS = "GPS_SENSORS"


@dataclass
class RawSignal:
    """Raw signal received from an event source before correlation."""
    source: SourceSystem
    signal_type: str           # e.g., "GOAL_DETECTED", "EVENT_CODED"
    timestamp: datetime
    payload: dict
    confidence: float = 1.0
    operator_id: Optional[str] = None
    raw_bytes: Optional[bytes] = None


class SourceManager:
    """
    Manages connections to all event sources at the stadium.
    Each source runs its own listener coroutine and pushes
    signals into a shared async queue.
    """

    def __init__(self, sources_config: dict):
        self.config = sources_config
        self.signal_queue: asyncio.Queue[RawSignal] = asyncio.Queue(maxsize=10000)
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self):
        """Start all configured source listeners."""
        self._running = True

        if self.config.get("hawk_eye", {}).get("enabled", False):
            task = asyncio.create_task(self._listen_hawk_eye())
            self._tasks.append(task)
            logger.info("source_started", source="hawk_eye")

        if self.config.get("opta", {}).get("enabled", False):
            task = asyncio.create_task(self._listen_opta())
            self._tasks.append(task)
            logger.info("source_started", source="opta")

        if self.config.get("referee_comms", {}).get("enabled", False):
            task = asyncio.create_task(self._listen_referee_comms())
            self._tasks.append(task)
            logger.info("source_started", source="referee_comms")

        if self.config.get("gps_sensors", {}).get("enabled", False):
            task = asyncio.create_task(self._listen_gps_sensors())
            self._tasks.append(task)
            logger.info("source_started", source="gps_sensors")

        logger.info("all_sources_started", count=len(self._tasks))

    async def receive(self, timeout_ms: int = 100) -> list[RawSignal]:
        """
        Collect all available signals from all sources.
        Returns a batch of signals received within the timeout window.
        """
        signals = []
        deadline = asyncio.get_event_loop().time() + (timeout_ms / 1000)

        while asyncio.get_event_loop().time() < deadline:
            try:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                signal = await asyncio.wait_for(
                    self.signal_queue.get(),
                    timeout=remaining,
                )
                signals.append(signal)
            except asyncio.TimeoutError:
                break

        return signals

    async def stop(self):
        """Stop all source listeners."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("all_sources_stopped")

    # --- Source-specific listeners ---

    async def _listen_hawk_eye(self):
        """
        Listen to Hawk-Eye GLT feed.
        Hawk-Eye provides binary goal detection (ball crossed line)
        with <500ms latency. Signal is simple but authoritative.
        """
        endpoint = self.config["hawk_eye"]["endpoint"]
        logger.info("hawk_eye_connecting", endpoint=endpoint)

        while self._running:
            try:
                # In production: gRPC client connection to Hawk-Eye system
                # Simulated: await self._grpc_connect(endpoint)
                await asyncio.sleep(0.05)  # Poll interval

                # When goal detected, Hawk-Eye sends binary signal
                # payload = await hawk_eye_client.receive()
                # if payload.goal_detected:
                #     signal = RawSignal(
                #         source=SourceSystem.HAWK_EYE,
                #         signal_type="GOAL_LINE_CROSSED",
                #         timestamp=datetime.now(timezone.utc),
                #         payload={"goal_detected": True, "camera_ids": [...]},
                #         confidence=0.99,
                #     )
                #     await self.signal_queue.put(signal)

            except Exception as e:
                logger.error("hawk_eye_error", error=str(e))
                await asyncio.sleep(1)

    async def _listen_opta(self):
        """
        Listen to Opta/Stats Perform live feed.
        Human operator codes each event with rich detail:
        scorer, assister, minute, coordinates, goal type.
        Latency: 3-8 seconds but very detailed.
        """
        endpoint = self.config["opta"]["endpoint"]
        logger.info("opta_connecting", endpoint=endpoint)

        while self._running:
            try:
                # In production: TCP socket to Opta feed server
                # Simulated: await self._tcp_connect(endpoint)
                await asyncio.sleep(0.1)

                # When event coded by operator:
                # raw_data = await opta_client.receive()
                # signal = RawSignal(
                #     source=SourceSystem.OPTA,
                #     signal_type=raw_data["type"],  # "GOAL_SCORED", etc.
                #     timestamp=datetime.now(timezone.utc),
                #     payload=raw_data,
                #     confidence=0.95,
                #     operator_id=raw_data.get("operator_id"),
                # )
                # await self.signal_queue.put(signal)

            except Exception as e:
                logger.error("opta_error", error=str(e))
                await asyncio.sleep(1)

    async def _listen_referee_comms(self):
        """
        Listen to referee communication system.
        Provides official confirmation signals from the match referee
        and VAR room. Latency: 1-3 seconds.
        """
        endpoint = self.config["referee_comms"]["endpoint"]
        logger.info("referee_comms_connecting", endpoint=endpoint)

        while self._running:
            try:
                await asyncio.sleep(0.1)
                # In production: gRPC stream from referee comms system
            except Exception as e:
                logger.error("referee_comms_error", error=str(e))
                await asyncio.sleep(1)

    async def _listen_gps_sensors(self):
        """
        Listen to EPTS (Electronic Performance and Tracking System).
        Player GPS/IMU data at 25Hz. Used for position corroboration,
        not primary event detection.
        """
        endpoint = self.config["gps_sensors"]["endpoint"]
        logger.info("gps_sensors_connecting", endpoint=endpoint)

        while self._running:
            try:
                await asyncio.sleep(0.04)  # ~25Hz
                # In production: UDP receiver for EPTS data
            except Exception as e:
                logger.error("gps_sensors_error", error=str(e))
                await asyncio.sleep(1)
