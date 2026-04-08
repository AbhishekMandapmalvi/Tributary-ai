"""Real-time pipeline dashboard.

Runs a FastAPI server with WebSocket that streams pipeline events
to a browser-based dashboard. The dashboard shows live document
processing status, stage timing, and failure details.

Usage:
    from tributary.dashboard.server import DashboardServer

    dashboard = DashboardServer(port=8765)
    pipeline = Pipeline(..., on_event=dashboard.on_event)

    # Start dashboard in background, then run pipeline
    await dashboard.start()
    await pipeline.run()
    await dashboard.stop()
"""
from __future__ import annotations
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from tributary.pipeline.events import PipelineEvent
import uvicorn
import structlog

logger = structlog.get_logger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"


class DashboardServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.host = host
        self.port = port
        self._connections: list[WebSocket] = []
        self._event_history: list[dict] = []
        self._server_task: asyncio.Task | None = None
        self._app = self._create_app()

    def _create_app(self) -> FastAPI:
        app = FastAPI(title="Tributary Dashboard")

        @app.get("/", response_class=HTMLResponse)
        async def index():
            html_path = _STATIC_DIR / "dashboard.html"
            return html_path.read_text()

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            self._connections.append(ws)
            try:
                # Send event history to new connections
                for event in self._event_history:
                    await ws.send_json(event)
                # Keep connection alive
                while True:
                    await ws.receive_text()
            except WebSocketDisconnect:
                self._connections.remove(ws)

        return app

    async def on_event(self, event: PipelineEvent) -> None:
        """Async event callback — pass to Pipeline(on_event=dashboard.on_event)."""
        data = {k: v for k, v in asdict(event).items() if v is not None}
        self._event_history.append(data)

        disconnected = []
        for ws in self._connections:
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self._connections.remove(ws)

    async def start(self) -> None:
        """Start the dashboard server in the background."""
        config = uvicorn.Config(
            self._app,
            host=self.host,
            port=self.port,
            log_level="warning",
        )
        server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(server.serve())
        # Wait briefly for server to start
        await asyncio.sleep(0.3)
        logger.info("Dashboard started", url=f"http://{self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop the dashboard server."""
        if self._server_task:
            self._server_task.cancel()
            try:
                await self._server_task
            except asyncio.CancelledError:
                pass
        for ws in self._connections:
            try:
                await ws.close()
            except Exception:
                pass
        self._connections.clear()
        logger.info("Dashboard stopped")
