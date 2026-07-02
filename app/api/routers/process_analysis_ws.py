from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.websocket.analysis_manager import analysis_websocket_manager


router = APIRouter(tags=["process-analysis-websocket"])


@router.websocket("/ws/process-analysis")
async def process_analysis_websocket(websocket: WebSocket) -> None:
    await analysis_websocket_manager.connect(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            if message.lower() == "ping":
                await websocket.send_json({"type": "PONG"})
    except WebSocketDisconnect:
        analysis_websocket_manager.disconnect(websocket)
    except Exception:
        analysis_websocket_manager.disconnect(websocket)
        raise
