# ============================================================
# Event Catalog — Complete Reference
# ============================================================
# Every event type in the real-time sports data system,
# with schemas, routing, and processing details.

## Event Types Overview

| Event Type | Priority | Notification | Odds Impact | Schema File |
|---|---|---|---|---|
| GOAL_SCORED | 🟥 CRITICAL | ✅ Yes | ✅ Suspend | `goal-scored.json` |
| GOAL_DISALLOWED | 🟥 CRITICAL | ✅ Yes | ✅ Resume | `goal-disallowed.json` |
| PENALTY_AWARDED | 🟧 HIGH | ✅ Yes | ✅ Suspend | `penalty-awarded.json` |
| RED_CARD | 🟧 HIGH | ✅ Yes | ✅ Suspend | `red-card.json` |
| YELLOW_CARD | 🟨 NORMAL | ❌ No | ⚠️ Minor | `yellow-card.json` |
| SUBSTITUTION | 🟩 LOW | ❌ No | ⚠️ Minor | `substitution.json` |
| VAR_REVIEW_STARTED | 🟧 HIGH | ✅ Yes | ⚠️ Pending | `var-review.json` |
| VAR_DECISION | 🟧 HIGH | ✅ Yes | ✅ Variable | `var-review.json` |
| KICK_OFF | 🟨 NORMAL | ❌ No | ✅ Open | `match-state.json` |
| HALF_TIME | 🟨 NORMAL | ✅ Yes | ✅ Adjust | `match-state.json` |
| FULL_TIME | 🟨 NORMAL | ✅ Yes | ✅ Settle | `match-state.json` |
| INJURY_STOPPAGE | 🟩 LOW | ❌ No | ❌ No | `match-state.json` |
| MATCH_ABANDONED | 🟥 CRITICAL | ✅ Yes | ✅ Void | `match-state.json` |

## Event Flow

```
Stadium Sources → Edge Gateway → Kafka (raw) → Flink Enrichment → Kafka (enriched)
                                                        ↓
                                              ┌─────────┼──────────┐
                                              │         │          │
                                        Notifications  Odds    Analytics
                                              ↓         ↓          ↓
                                         FCM/APNs   Betting   ClickHouse
                                              ↓                    ↓
                                          📱 Phone            📊 Dashboard
```

## Event Schema Structure

Every event follows the base schema pattern:

```json
{
  "event_id": "UUIDv7 (time-ordered)",
  "event_type": "GOAL_SCORED",
  "event_version": 1,
  "match_id": "match-uuid",
  "occurred_at": "2025-03-15T20:37:12.450Z",  // Event time (on pitch)
  "received_at": "2025-03-15T20:37:12.823Z",  // Ingestion time (gateway)
  "published_at": "2025-03-15T20:37:13.001Z", // Processing time (Kafka)
  "sequence_number": 42,
  "source": { "system": "OPTA", "confidence": 0.95 },
  "data": { ... },                             // Event-specific payload
  "metadata": {
    "idempotency_key": "sha256:a3f2b8c1...",
    "correlation_id": "corr-xxxxx-goal-37",
    "causation_id": null,
    "ttl_seconds": 300
  }
}
```

## Triple Timestamp System

| Timestamp | Set By | Purpose |
|---|---|---|
| `occurred_at` | Stadium source | When it *physically happened* on the pitch |
| `received_at` | Edge Gateway | When the gateway first processed it |
| `published_at` | Kafka Producer | When Kafka durably stored it |

## Latency Budget

| Stage | Budget | Cumulative |
|---|---|---|
| Physical Event → Hawk-Eye | 200ms | 200ms |
| Hawk-Eye → Gateway | 100ms | 300ms |
| Gateway processing (dedup + validate + WAL) | 50ms | 350ms |
| Gateway → Kafka | 100ms | 450ms |
| Kafka → Flink Enrichment | 200ms | 650ms |
| Flink Enrichment processing | 150ms | 800ms |
| Enriched → Notification Router | 200ms | 1000ms |
| Notification routing + templating | 100ms | 1100ms |
| FCM/APNs delivery | 500-1500ms | 1600-2600ms |
| **Total: pitch → phone** | | **< 3 seconds** |

## Delivery Guarantees

| Consumer | Guarantee | Rationale |
|---|---|---|
| Flink Enrichment | Exactly-once | Core data path, must not lose or duplicate |
| Notification Router | At-least-once | Duplicate push OK, missed push not OK |
| Odds Engine | Exactly-once | Duplicate bet settlements are catastrophic |
| Analytics/ClickHouse | At-least-once | Deduped on insert via ReplacingMergeTree |
| WebSocket Gateway | At-most-once | Stale/missing events handled by client reconnect |

## Idempotency Key Generation

```python
# Formula: hash(match_id + event_type + match_minute + player_id)
idempotency_key = f"sha256:{sha256(f'{match_id}:{event_type}:{minute}:{player_id}')[:32]}"
```

Purpose: Two signals about the same physical event (e.g., Hawk-Eye + Opta both
reporting a goal at minute 37 by player X) should produce the same idempotency
key AND be correlated into a single canonical event.
