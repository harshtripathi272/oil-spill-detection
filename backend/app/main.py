from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from app.config import settings
from app.database import engine
from app.models import Base
from app.routers import dashboard, incidents, metrics, system, alerts, realtime, users, logs, pipeline, predictions, analytics, vessels
import os

# Repo root: backend/app/main.py -> ../.. = oil-spill-detection project root
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Oil Spill Detection Backend",
    description="Backend API for Oil Spill Detection System Dashboard",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    redirect_slashes=False
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(dashboard.router, prefix="/api/v1/dashboard", tags=["dashboard"])
app.include_router(incidents.router, prefix="/api/v1/incidents", tags=["incidents"])
app.include_router(metrics.router, prefix="/api/v1/metrics", tags=["metrics"])
app.include_router(system.router, prefix="/api/v1/system", tags=["system"])
app.include_router(alerts.router, prefix="/api/v1/alerts", tags=["alerts"])
app.include_router(users.router, prefix="/api/v1/users", tags=["users"])
app.include_router(logs.router, prefix="/api/v1/logs", tags=["logs"])
app.include_router(pipeline.router, prefix="/api/v1/pipeline", tags=["pipeline"])
app.include_router(predictions.router, prefix="/api/v1/predictions", tags=["predictions"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(vessels.router, prefix="/api/v1/vessels", tags=["vessels"])
app.include_router(realtime.router, prefix="/ws", tags=["realtime"])

# Mount SAR / prediction images (ensure dirs exist so empty installs do not break mounts)
sar_dir = os.path.join(_REPO_ROOT, "sentinel_data", "preprocessed")
prediction_dir = os.path.join(_REPO_ROOT, "sentinel_data", "predictions")
os.makedirs(sar_dir, exist_ok=True)
os.makedirs(prediction_dir, exist_ok=True)
app.mount("/sar-images", StaticFiles(directory=sar_dir), name="sar-images")
app.mount("/prediction-images", StaticFiles(directory=prediction_dir), name="prediction-images")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "oil-spill-detection-backend"}

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Oil Spill Detection Backend API", "version": "1.0.0"}