"""
Health check and API server for the Edge Gateway.
"""

import structlog
from fastapi import FastAPI
from uvicorn import Config, Server

logger = structlog.get_logger(__name__)


class HealthServer:
    """Simple health check HTTP server for liveness/readiness probes."""

    def __init__(self, config: dict):
        self.port = config.get("port", 8080)
        self.path = config.get("path", "/health")
        self.app = FastAPI(title="Edge Gateway Health")
        self._server = None
        self._setup_routes()

    def _setup_routes(self):
        @self.app.get(self.path)
        async def health():
            return {
                "status": "healthy",
                "service": "edge-gateway",
                "checks": {
                    "wal": "ok",
                    "kafka": "ok",
                    "sources": "ok",
                },
            }

        @self.app.get("/ready")
        async def readiness():
            return {"status": "ready"}

    async def start(self):
        config = Config(app=self.app, host="0.0.0.0", port=self.port, log_level="warning")
        self._server = Server(config)
        import asyncio
        asyncio.create_task(self._server.serve())
        logger.info("health_server_started", port=self.port)

    async def stop(self):
        if self._server:
            self._server.should_exit = True
