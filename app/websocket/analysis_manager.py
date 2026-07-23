from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket


logger = logging.getLogger(__name__)


class AnalysisWebSocketManager:
    """Keep active dashboard websocket connections and broadcast update hints."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._loop = asyncio.get_running_loop()
        if websocket not in self._connections:
            self._connections.append(websocket)
        await websocket.send_json({"type": "CONNECTED"})

    def disconnect(self, websocket: WebSocket) -> None:
        try:
            self._connections.remove(websocket)
        except ValueError:
            pass

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self._connections:
            return

        disconnected: list[WebSocket] = []
        for websocket in tuple(self._connections):
            try:
                await websocket.send_json(message)
            except Exception:
                disconnected.append(websocket)

        for websocket in disconnected:
            self.disconnect(websocket)

    def broadcast_from_thread(self, message: dict[str, Any]) -> None:
        loop = self._loop
        if loop is None or not loop.is_running():
            logger.warning(
                "Skipped process analysis websocket broadcast: "
                "event_loop_unavailable type=%s",
                message.get("type"),
            )
            return
        if not self._connections:
            logger.warning(
                "Skipped process analysis websocket broadcast: "
                "active_connections=0 type=%s",
                message.get("type"),
            )
            return

        future = asyncio.run_coroutine_threadsafe(self.broadcast(message), loop)
        future.add_done_callback(self._log_broadcast_failure)

    @staticmethod
    def _log_broadcast_failure(future: asyncio.Future[Any]) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Failed to broadcast process analysis websocket update.")


analysis_websocket_manager = AnalysisWebSocketManager()
