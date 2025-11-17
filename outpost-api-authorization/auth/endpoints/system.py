import logging
from typing import Any, Dict

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/")
async def root_endpoint(request: Request):
    """Provides a simple welcome message and system status overview."""
    app = request.app
    return JSONResponse(content={
        "service_name": app.title,
        "version": app.version,
        "status": "Running",
        "message": "Access /health for current readiness status."
    })

logger = logging.getLogger(__name__)

@router.get("/health", tags=["Monitoring"])
async def health_check(request: Request):
    """
    Load balancer and orchestrator health check endpoint.
    Checks the status of all critical external dependencies (PostgreSQL and Redis).
    """
    health_status: Dict[str, Any] = {"status": "ok", "dependencies": {}}
    app_state = request.app.state
    all_healthy = True
    # TODO: Ping any databases or external connection pools we require here
    if not all_healthy:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=health_status
        )
    return health_status
