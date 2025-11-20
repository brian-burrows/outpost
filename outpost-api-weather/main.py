import logging

from fastapi import FastAPI
from outpost.endpoints import cities#, weather
from outpost.endpoints.system import router as system_router
from outpost.core.database import close_db_connections

LOG_LEVEL = logging.DEBUG
logger = logging.getLogger(__name__)
logging.basicConfig(filename='ingestion-worker.log', encoding='utf-8', level=LOG_LEVEL)

app = FastAPI(
    title="Outpost Weather API",
    description="API Gateway for weather ingestion and categorization services.",
    version="1.0.0"
)

# Include all application routers
app.include_router(system_router, tags=["System", "Monitoring"])
app.include_router(cities.router, tags=["Cities"])
# app.include_router(weather.router, tags=["Weather"])


@app.on_event("startup")
async def startup_event():
    """Initializes external services (databases, caches, etc.)"""
    logger.info("Application startup initiated, loading configurations...")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleans up resources by gracefully closing all connection pools."""
    logger.info("Application shutdown initiated, cleaning up database connections...")
    await close_db_connections()
    logger.info("All connection pools closed.")