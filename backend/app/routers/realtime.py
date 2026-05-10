import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from fastapi.encoders import jsonable_encoder
from app.database import get_db
from app.services.dashboard_service import DashboardService
from app.services.system_service import build_system_health
from app.models.incident import Incident
from app.models.predictions import Prediction

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
async def websocket_updates(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We fetch a fresh DB session for each update iteration to avoid 
            # holding a connection open indefinitely (connection pool exhaustion).
            from app.database import SessionLocal
            db = SessionLocal()
            try:
                service = DashboardService(db)
                stats = service.get_dashboard_stats()
                health = build_system_health(db)
                incidents = db.query(Incident).order_by(Incident.detection_time.desc()).limit(10).all()
                predictions = db.query(Prediction).order_by(Prediction.created_at.desc()).limit(5).all()
                
                alert_items = [
                    {
                        "id": incident.id,
                        "level": "critical" if incident.confidence_score and incident.confidence_score > 0.85 else "warning" if incident.confidence_score and incident.confidence_score > 0.65 else "low",
                        "message": f"Incident {incident.id} update: status={incident.status}",
                        "latitude": incident.latitude,
                        "longitude": incident.longitude,
                        "confidence": incident.confidence_score,
                        "status": incident.status,
                        "detection_time": incident.detection_time.isoformat() if incident.detection_time else None
                    }
                    for incident in incidents
                ]

                prediction_items = [
                    {
                        "id": prediction.id,
                        "incident_id": prediction.incident_id,
                        "prediction": prediction.prediction,
                        "confidence": prediction.confidence,
                        "prediction_image_path": prediction.prediction_image_path,
                        "bbox_coordinates": prediction.bbox_coordinates,
                        "created_at": prediction.created_at.isoformat() if prediction.created_at else None
                    }
                    for prediction in predictions
                ]

                # Send ONLY to this connection to avoid N*N message explosion
                await websocket.send_json(jsonable_encoder({
                    "type": "dashboard_update",
                    "stats": stats,
                    "system_health": health,
                    "alerts": alert_items,
                    "recent_predictions": prediction_items,
                }))
            finally:
                db.close()
                
            await asyncio.sleep(10) # Reduced frequency to 10s to keep system snappy
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)
