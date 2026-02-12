"""
Event Correlator — Fuses multiple signals from different sources
into a single canonical event.

Example: Hawk-Eye detects ball crossing the line at t=0 (binary signal),
then Opta operator codes the goal details at t=4s (rich data).
The correlator merges both into one GoalScored event.
"""

import structlog
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict
from uuid import uuid4

from uuid7 import uuid7

logger = structlog.get_logger(__name__)


class EventCorrelator:
    """
    Correlates signals from multiple sources within a time window
    to produce a single canonical event.

    Strategy:
    - Keep a sliding window of recent signals per match
    - When a new signal arrives, check if it correlates with existing signals
    - If correlation found, merge into a richer event
    - If no correlation within window, emit standalone event

    Correlation key: (match_id, event_type_category, approximate_minute)
    """

    def __init__(self, config: dict):
        self.window_ms = config.get("window_ms", 5000)
        self.min_confidence = config.get("min_confidence", 0.8)

        # Pending signals awaiting correlation
        # Key: (match_id, event_category, minute)
        # Value: list of RawSignal
        self._pending: dict[tuple, list] = defaultdict(list)

        # Sequence counter per match
        self._sequence_counters: dict[str, int] = defaultdict(int)

    def correlate(self, signals: list) -> list[dict]:
        """
        Process a batch of raw signals from various sources
        and produce canonical events.
        """
        canonical_events = []
        now = datetime.now(timezone.utc)

        for signal in signals:
            correlation_key = self._make_correlation_key(signal)
            self._pending[correlation_key].append(signal)

        # Check all pending groups for signals ready to emit
        expired_keys = []
        for key, pending_signals in self._pending.items():
            # Check if we have enough confidence to emit
            best_signal = max(pending_signals, key=lambda s: s.confidence)

            # Emit if:
            # 1. High-confidence signal (e.g., confirmed by referee), OR
            # 2. Multiple corroborating sources, OR
            # 3. Window expired with at least one signal
            has_high_confidence = best_signal.confidence >= self.min_confidence
            has_multiple_sources = len(set(s.source for s in pending_signals)) >= 2
            window_expired = (now - pending_signals[0].timestamp).total_seconds() * 1000 > self.window_ms

            if has_high_confidence or has_multiple_sources or window_expired:
                event = self._merge_signals(key, pending_signals)
                if event:
                    canonical_events.append(event)
                expired_keys.append(key)

        # Clean up emitted groups
        for key in expired_keys:
            del self._pending[key]

        return canonical_events

    def _make_correlation_key(self, signal) -> tuple:
        """
        Create a correlation key from a raw signal.
        Signals with the same key are considered to be about the same event.
        """
        match_id = signal.payload.get("match_id", "unknown")
        event_category = self._categorize_signal(signal.signal_type)
        minute = signal.payload.get("match_minute", 0)
        return (match_id, event_category, minute)

    def _categorize_signal(self, signal_type: str) -> str:
        """Map specific signal types to broader categories for correlation."""
        categories = {
            "GOAL_LINE_CROSSED": "GOAL",
            "GOAL_SCORED": "GOAL",
            "GOAL_DETECTED": "GOAL",
            "GOAL_CONFIRMED": "GOAL",
            "PENALTY_AWARDED": "PENALTY",
            "PENALTY_SIGNAL": "PENALTY",
            "CARD_SHOWN": "CARD",
            "YELLOW_CARD": "CARD",
            "RED_CARD": "CARD",
            "SUBSTITUTION": "SUBSTITUTION",
            "VAR_REVIEW": "VAR",
        }
        return categories.get(signal_type, signal_type)

    def _merge_signals(self, key: tuple, signals: list) -> Optional[dict]:
        """
        Merge multiple corroborating signals into a single canonical event.
        Priority: Opta (richest data) > Referee (authoritative) > Hawk-Eye (fastest)
        """
        match_id, event_category, minute = key

        # Find the richest signal (Opta provides most detail)
        source_priority = {"OPTA": 0, "MANUAL_OPERATOR": 1, "REFEREE_COMMS": 2, "HAWK_EYE": 3, "VAR": 0}
        sorted_signals = sorted(
            signals,
            key=lambda s: source_priority.get(s.source.value, 99),
        )
        primary = sorted_signals[0]

        # Increment sequence counter
        self._sequence_counters[match_id] += 1
        seq = self._sequence_counters[match_id]

        # Generate canonical event
        event_type = self._resolve_event_type(event_category, primary)
        event_id = str(uuid7())
        now = datetime.now(timezone.utc)

        canonical_event = {
            "event_id": event_id,
            "event_type": event_type,
            "event_version": 1,
            "match_id": match_id,
            "occurred_at": min(s.timestamp for s in signals).isoformat(),
            "received_at": now.isoformat(),
            "sequence_number": seq,
            "source": {
                "system": primary.source.value,
                "operator_id": primary.operator_id,
                "confidence": max(s.confidence for s in signals),
                "stadium_gateway_id": None,  # Set by gateway
                "corroborating_sources": list(set(s.source.value for s in signals)),
            },
            "data": primary.payload.get("data", {}),
            "metadata": {
                "idempotency_key": self._compute_idempotency_key(
                    match_id, event_type, minute, primary.payload
                ),
                "correlation_id": f"corr-{event_id[:8]}-{event_category.lower()}-{minute}",
                "causation_id": primary.payload.get("causation_id"),
                "ttl_seconds": 300,
            },
        }

        logger.info(
            "event_correlated",
            event_id=event_id,
            event_type=event_type,
            match_id=match_id,
            sources_count=len(signals),
            sources=list(set(s.source.value for s in signals)),
        )

        return canonical_event

    def _resolve_event_type(self, category: str, primary_signal) -> str:
        """Map category back to specific event type."""
        type_map = {
            "GOAL": "GOAL_SCORED",
            "PENALTY": "PENALTY_AWARDED",
            "CARD": primary_signal.payload.get("card_type", "YELLOW_CARD"),
            "SUBSTITUTION": "SUBSTITUTION",
            "VAR": "VAR_REVIEW_STARTED",
        }
        return type_map.get(category, primary_signal.signal_type)

    def _compute_idempotency_key(
        self, match_id: str, event_type: str, minute: int, payload: dict
    ) -> str:
        """
        Compute content-based idempotency key.
        hash(match_id + event_type + match_minute + player_id)
        """
        import hashlib

        player_id = payload.get("data", {}).get("scorer", {}).get("player_id", "")
        if not player_id:
            player_id = payload.get("data", {}).get("player", {}).get("player_id", "")

        content = f"{match_id}:{event_type}:{minute}:{player_id}"
        return f"sha256:{hashlib.sha256(content.encode()).hexdigest()[:32]}"
