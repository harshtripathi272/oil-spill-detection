from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import engine
from app.models import Base
from app.routers import dashboard, incidents, metrics, system

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Oil Spill Detection Backend",
    description="Backend API for Oil Spill Detection System Dashboard",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
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

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "oil-spill-detection-backend"}

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Oil Spill Detection Backend API", "version": "1.0.0"}