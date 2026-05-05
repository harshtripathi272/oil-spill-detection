import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from app.database import get_db
from app.services.dashboard_service import DashboardService
from app.services.system_service import build_system_health
from app.models.incident import Incident

router = APIRouter()

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

@router.websocket("/updates")
async def websocket_updates(websocket: WebSocket, db=Depends(get_db)):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(6)
            stats = DashboardService(db).get_dashboard_stats()
            health = build_system_health(db)
            incidents = db.query(Incident).order_by(Incident.detection_time.desc()).limit(10).all()
            alert_items = [
                {
                    "id": incident.id,
                    "level": "critical" if incident.confidence_score and incident.confidence_score > 0.85 else "warning" if incident.confidence_score and incident.confidence_score > 0.65 else "low",
                    "message": f"Incident {incident.id} update: status={incident.status}",
                    "latitude": incident.latitude,
                    "longitude": incident.longitude,
                    "confidence": incident.confidence_score,
                    "status": incident.status,
                }
                for incident in incidents
            ]

            await manager.broadcast({
                "type": "dashboard_update",
                "stats": stats.model_dump() if hasattr(stats, "model_dump") else dict(stats),
                "system_health": health.model_dump() if hasattr(health, "model_dump") else dict(health),
                "alerts": alert_items,
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
