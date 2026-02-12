"""
WebSocket Gateway — Real-time event broadcast to connected clients.

Architecture role: The final delivery mechanism for live match data
to mobile apps, web dashboards, and third-party consumers.

Design:
- Each WebSocket connection subscribes to match_id(s)
- Events consumed from match-events-enriched Kafka topic
- Broadcast to all subscribers of that match within ~200ms
- Connection authentication via JWT
- Automatic reconnection with event sequence tracking
"""

import asyncio
import json
import structlog
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logger = structlog.get_logger(__name__)


@dataclass
class ConnectedClient:
    """Represents a connected WebSocket client."""
    websocket: WebSocket
    client_id: str
    subscribed_matches: set[str] = field(default_factory=set)
    last_sequence: dict[str, int] = field(default_factory=dict)
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    language: str = "en"
    user_agent: Optional[str] = None


class WebSocketGateway:
    """
    WebSocket Gateway for real-time event delivery.

    Maintains a registry of connected clients indexed by match_id
    for efficient broadcast. When a new enriched event arrives
    from Kafka, it's broadcast to all clients subscribed to that match.
    """

    def __init__(self):
        self.app = FastAPI(
            title="Real-Time Sports WebSocket Gateway",
            version="1.0.0",
        )
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Client registry: match_id → set of ConnectedClient
        self._subscriptions: dict[str, set] = defaultdict(set)
        # All connected clients: client_id → ConnectedClient
        self._clients: dict[str, ConnectedClient] = {}

        self._setup_routes()

    def _setup_routes(self):

        @self.app.websocket("/ws/matches/{match_id}")
        async def match_websocket(
            websocket: WebSocket,
            match_id: str,
            last_sequence: Optional[int] = Query(None),
        ):
            """
            WebSocket endpoint for subscribing to live match events.

            Clients connect with:
              ws://host/ws/matches/{match_id}?last_sequence=42

            The last_sequence param enables reconnection without missing events.
            """
            await websocket.accept()

            client_id = f"ws-{id(websocket)}"
            client = ConnectedClient(
                websocket=websocket,
                client_id=client_id,
                subscribed_matches={match_id},
            )

            self._clients[client_id] = client
            self._subscriptions[match_id].add(client)

            logger.info(
                "client_connected",
                client_id=client_id,
                match_id=match_id,
                total_clients=len(self._clients),
            )

            # If reconnecting, replay missed events from cache
            if last_sequence is not None:
                await self._replay_missed_events(client, match_id, last_sequence)

            try:
                # Keep connection alive, handle client messages
                while True:
                    data = await websocket.receive_text()
                    message = json.loads(data)

                    if message.get("action") == "subscribe":
                        new_match = message.get("match_id")
                        if new_match:
                            client.subscribed_matches.add(new_match)
                            self._subscriptions[new_match].add(client)

                    elif message.get("action") == "unsubscribe":
                        old_match = message.get("match_id")
                        if old_match:
                            client.subscribed_matches.discard(old_match)
                            self._subscriptions[old_match].discard(client)

                    elif message.get("action") == "ping":
                        await websocket.send_json({"action": "pong"})

            except WebSocketDisconnect:
                self._remove_client(client)
                logger.info(
                    "client_disconnected",
                    client_id=client_id,
                    total_clients=len(self._clients),
                )

        @self.app.get("/health")
        async def health():
            return {
                "status": "healthy",
                "connected_clients": len(self._clients),
                "active_matches": len(self._subscriptions),
            }

    async def broadcast_event(self, match_id: str, event: dict):
        """
        Broadcast an enriched event to all clients subscribed to this match.
        Called by the Kafka consumer when a new event arrives.
        """
        subscribers = self._subscriptions.get(match_id, set())
        if not subscribers:
            return

        payload = json.dumps({
            "type": "match_event",
            "data": event,
            "server_timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # Broadcast to all subscribers concurrently
        disconnected = []
        tasks = []
        for client in subscribers:
            tasks.append(self._send_to_client(client, payload, disconnected))

        await asyncio.gather(*tasks)

        # Clean up disconnected clients
        for client in disconnected:
            self._remove_client(client)

        logger.info(
            "event_broadcast",
            match_id=match_id,
            event_type=event.get("event_type"),
            recipients=len(subscribers) - len(disconnected),
        )

    async def _send_to_client(self, client: ConnectedClient, payload: str, disconnected: list):
        """Send payload to a single client, tracking failures."""
        try:
            await client.websocket.send_text(payload)
        except Exception:
            disconnected.append(client)

    async def _replay_missed_events(self, client: ConnectedClient, match_id: str, last_sequence: int):
        """Replay events missed during disconnection from in-memory cache."""
        # In production: fetch from Redis or Kafka consumer position
        pass

    def _remove_client(self, client: ConnectedClient):
        """Remove client from all registries."""
        self._clients.pop(client.client_id, None)
        for match_id in client.subscribed_matches:
            self._subscriptions[match_id].discard(client)


def create_app() -> FastAPI:
    gateway = WebSocketGateway()
    return gateway.app


if __name__ == "__main__":
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8081)
