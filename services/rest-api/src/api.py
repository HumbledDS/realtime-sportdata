"""
REST API — Public HTTP API for match events, scores, and stats.

Architecture role: Serves cached event data for clients that cannot 
use WebSocket or for one-time data fetches. Data served from Redis
(hot tier) with PostgreSQL fallback (warm tier).
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


app = FastAPI(
    title="Real-Time Sports Data API",
    description="Public API for live match events, scores, and statistics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# === Matches ===

@app.get("/api/v1/matches")
async def list_live_matches(
    competition_id: Optional[str] = Query(None, description="Filter by competition"),
    status: Optional[str] = Query(None, description="Filter by status: LIVE, UPCOMING, FINISHED"),
):
    """
    List all currently live matches, or filter by competition/status.
    Data sourced from Redis hot tier.
    """
    # In production: redis.hgetall("matches:live")
    return {
        "matches": [],
        "count": 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/matches/{match_id}")
async def get_match(match_id: str = Path(..., description="Match ID")):
    """
    Get full match details including current score, events, and statistics.
    """
    # In production: redis.hget(f"match:{match_id}", "state")
    return {
        "match_id": match_id,
        "status": "LIVE",
        "score": {"home": 0, "away": 0},
        "events": [],
        "statistics": {},
    }


@app.get("/api/v1/matches/{match_id}/events")
async def list_match_events(
    match_id: str = Path(...),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    since_sequence: Optional[int] = Query(None, description="Get events after this seq number"),
    limit: int = Query(50, ge=1, le=200),
):
    """
    List events for a match, optionally filtered by type.
    Supports polling with since_sequence for clients that cannot use WebSocket.
    """
    return {
        "match_id": match_id,
        "events": [],
        "next_sequence": 0,
    }


@app.get("/api/v1/matches/{match_id}/timeline")
async def get_match_timeline(match_id: str = Path(...)):
    """
    Get the match timeline: a chronological list of key events
    (goals, cards, substitutions) suitable for rendering a match summary.
    """
    return {
        "match_id": match_id,
        "timeline": [],
    }


# === Statistics ===

@app.get("/api/v1/matches/{match_id}/stats")
async def get_match_stats(match_id: str = Path(...)):
    """
    Get live match statistics: possession, shots, corners, xG, etc.
    Data sourced from ClickHouse analytics tier.
    """
    return {
        "match_id": match_id,
        "statistics": {
            "possession": {"home": 50, "away": 50},
            "shots": {"home": 0, "away": 0},
            "shots_on_target": {"home": 0, "away": 0},
            "corners": {"home": 0, "away": 0},
            "fouls": {"home": 0, "away": 0},
            "xG": {"home": 0.0, "away": 0.0},
        },
    }


# === Health ===

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "rest-api"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)
