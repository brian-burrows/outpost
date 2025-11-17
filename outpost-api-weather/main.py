import logging

from fastapi import FastAPI
from outpost.endpoints.system import router as system_router

LOG_LEVEL = logging.DEBUG
logger = logging.getLogger(__name__)
logging.basicConfig(filename='ingestion-worker.log', encoding='utf-8', level=LOG_LEVEL)

app = FastAPI(
    title="Outpost Weather API",
    description="API Gateway for weather ingestion and categorization services.",
    version="1.0.0"
)

app.include_router(system_router, tags=["System", "Monitoring"])

@app.on_event("startup")
async def startup_event():
    """Initializes external services (databases, caches, etc.)"""
    # https://fastapi.tiangolo.com/advanced/events/ for details on what goes here
    logger.info("Application startup initiated, loading configurations...")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleans up resources by gracefully closing all connection pools."""
    # https://fastapi.tiangolo.com/advanced/events/ for details on what goes here
    logger.info("Application shutdown initiated, cleaning up resources...")
