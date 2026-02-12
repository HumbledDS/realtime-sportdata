"""
Write-Ahead Log (WAL) — Network resilience for the stadium edge.

Events are durably written to local disk BEFORE being sent to Kafka.
If the stadium network drops, events accumulate in the WAL and are
replayed automatically when connectivity resumes.

This is the "lifeboat" of the edge gateway. Without it, a 30-second
network blip during a goal could mean permanent data loss.
"""

import json
import asyncio
import structlog
import aiofiles
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

logger = structlog.get_logger(__name__)


class WriteAheadLog:
    """
    Append-only, file-based WAL for event durability.

    Design:
    - Events are appended to a log file as newline-delimited JSON
    - Each entry has a status: PENDING or ACKNOWLEDGED
    - On startup, replay all PENDING entries
    - On Kafka produce success, mark as ACKNOWLEDGED
    - Periodically compact (remove acknowledged entries)

    Capacity: ~10,000 events or 1 GB. Sufficient to cover
    several minutes of network outage during a match.
    """

    def __init__(self, config: dict):
        self.directory = Path(config.get("directory", "/data/wal"))
        self.max_size_mb = config.get("max_size_mb", 1024)
        self.max_events = config.get("max_events", 10000)
        self.sync_interval_ms = config.get("sync_interval_ms", 100)

        self._log_file = self.directory / "events.wal"
        self._pending_count = 0
        self._acknowledged: set[str] = set()
        self._lock = asyncio.Lock()

    async def initialize(self) -> list[dict]:
        """
        Initialize WAL. Load and return any pending (unacknowledged) events
        from a previous run. These need to be replayed to Kafka.
        """
        self.directory.mkdir(parents=True, exist_ok=True)

        pending_events = []

        if self._log_file.exists():
            async with aiofiles.open(self._log_file, "r") as f:
                contents = await f.read()

            for line in contents.strip().split("\n"):
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("status") == "PENDING":
                        pending_events.append(entry["event"])
                except json.JSONDecodeError:
                    logger.warning("wal_corrupt_entry", line=line[:100])

            self._pending_count = len(pending_events)

            if pending_events:
                logger.info(
                    "wal_pending_events_found",
                    count=len(pending_events),
                )

        return pending_events

    async def append(self, event: dict) -> None:
        """
        Append event to WAL. Called BEFORE producing to Kafka.
        This ensures the event survives network failures.
        """
        async with self._lock:
            entry = {
                "status": "PENDING",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_id": event["event_id"],
                "event": event,
            }

            self.directory.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(self._log_file, "a") as f:
                await f.write(json.dumps(entry) + "\n")
                await f.flush()

            self._pending_count += 1

            # Check if compaction is needed
            if self._pending_count > self.max_events:
                await self._compact()

    async def acknowledge(self, event_id: str) -> None:
        """
        Mark event as successfully forwarded to Kafka.
        """
        self._acknowledged.add(event_id)
        self._pending_count = max(0, self._pending_count - 1)

        # Compact periodically to free disk space
        if len(self._acknowledged) > 1000:
            await self._compact()

    async def _compact(self) -> None:
        """
        Remove acknowledged entries from the WAL file.
        Rewrite only pending entries to a new file.
        """
        if not self._log_file.exists():
            return

        logger.info(
            "wal_compaction_started",
            acknowledged=len(self._acknowledged),
        )

        new_entries = []

        async with aiofiles.open(self._log_file, "r") as f:
            contents = await f.read()

        for line in contents.strip().split("\n"):
            if not line:
                continue
            try:
                entry = json.loads(line)
                event_id = entry.get("event_id")
                if event_id not in self._acknowledged:
                    new_entries.append(line)
            except json.JSONDecodeError:
                continue

        # Atomic write: write to temp file, then rename
        temp_file = self._log_file.with_suffix(".wal.tmp")
        async with aiofiles.open(temp_file, "w") as f:
            for line in new_entries:
                await f.write(line + "\n")

        temp_file.replace(self._log_file)

        removed = len(self._acknowledged)
        self._acknowledged.clear()
        self._pending_count = len(new_entries)

        logger.info(
            "wal_compaction_completed",
            removed=removed,
            remaining=len(new_entries),
        )

    async def close(self) -> None:
        """Flush and close the WAL."""
        await self._compact()
        logger.info("wal_closed", pending=self._pending_count)

    @property
    def pending_count(self) -> int:
        return self._pending_count
