"""
Deduplication Engine — Prevents duplicate events from entering the pipeline.

Uses content-based idempotency keys to detect when the same physical event
(e.g., a goal) is reported by multiple sources. The idempotency key is
derived from: hash(match_id + event_type + match_minute + player_id).
"""

import time
import structlog
from collections import OrderedDict

logger = structlog.get_logger(__name__)


class DeduplicationEngine:
    """
    In-memory deduplication using an LRU cache of idempotency keys.

    Strategy: Before processing an event, check if its idempotency_key
    has been seen within the TTL window. If yes, drop it silently.

    The LRU cache bounds memory usage. Old keys are evicted when
    the cache exceeds max_size.
    """

    def __init__(
        self,
        dedup_fields: list[str],
        max_size: int = 100_000,
        ttl_seconds: int = 600,
    ):
        self.dedup_fields = dedup_fields
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._seen: OrderedDict[str, float] = OrderedDict()

    def is_duplicate(self, event: dict) -> bool:
        """
        Check if event is a duplicate based on its idempotency key.
        Returns True if duplicate (should be dropped).
        """
        idempotency_key = event.get("metadata", {}).get("idempotency_key")
        if not idempotency_key:
            # No idempotency key — cannot deduplicate, let it through
            return False

        now = time.monotonic()
        self._evict_expired(now)

        if idempotency_key in self._seen:
            # Duplicate found — check if within TTL
            first_seen = self._seen[idempotency_key]
            if now - first_seen < self.ttl_seconds:
                logger.debug(
                    "duplicate_detected",
                    idempotency_key=idempotency_key,
                    event_type=event.get("event_type"),
                    age_seconds=round(now - first_seen, 2),
                )
                return True
            else:
                # TTL expired, treat as new
                del self._seen[idempotency_key]

        # Mark as seen
        self._seen[idempotency_key] = now

        # Evict oldest if over capacity
        while len(self._seen) > self.max_size:
            self._seen.popitem(last=False)

        return False

    def _evict_expired(self, now: float):
        """Remove entries older than TTL."""
        expired_keys = []
        for key, timestamp in self._seen.items():
            if now - timestamp > self.ttl_seconds:
                expired_keys.append(key)
            else:
                break  # OrderedDict is ordered by insertion time

        for key in expired_keys:
            del self._seen[key]

    def clear(self):
        """Clear all deduplication state."""
        self._seen.clear()

    @property
    def size(self) -> int:
        """Current number of tracked idempotency keys."""
        return len(self._seen)
