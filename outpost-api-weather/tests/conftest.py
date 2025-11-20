from typing import AsyncGenerator
import pytest
import pytest_asyncio
import docker
import asyncpg 
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy import text 
import asyncio
from main import app
from outpost.core.database import get_read_conn, get_write_conn

POSTGRES_USER = "testuser"
POSTGRES_PASSWORD = "testpass"
POSTGRES_DB = "testdb"
POSTGRES_IMAGE = "postgres:16-alpine"

CREATE_DDL_STATEMENTS = [
    "DROP TABLE IF EXISTS forecast_weather_data CASCADE;",
    "DROP TABLE IF EXISTS historical_weather_data CASCADE;",
    "DROP TABLE IF EXISTS city CASCADE;",
    """
    CREATE TABLE city (
        city_id SERIAL PRIMARY KEY,
        city_name TEXT NOT NULL,
        latitude_deg REAL NOT NULL,
        longitude_deg REAL NOT NULL
    );
    """,
    """
    CREATE TABLE forecast_weather_data (
        city_id INTEGER NOT NULL REFERENCES city(city_id),
        temperature_deg_c REAL NOT NULL,
        wind_speed_mps REAL NOT NULL,
        rain_fall_total_mm REAL NOT NULL,
        aggregation_level TEXT NOT NULL,
        forecast_generated_at_ts_utc TIMESTAMP WITH TIME ZONE NOT NULL, 
        forecast_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
        PRIMARY KEY (city_id, forecast_generated_at_ts_utc, forecast_timestamp_utc)
    );
    """,
    """
    CREATE TABLE historical_weather_data (
        city_id INTEGER NOT NULL REFERENCES city(city_id),
        temperature_deg_c REAL NOT NULL,
        wind_speed_mps REAL NOT NULL,
        rain_fall_total_mm REAL NOT NULL,
        aggregation_level TEXT NOT NULL,
        measured_at_ts_utc TIMESTAMP WITH TIME ZONE NOT NULL,
        PRIMARY KEY (city_id, measured_at_ts_utc)
    );
    """,
]

@pytest.fixture(scope="session")
def anyio_backend():
    """Tells pytest to use the asyncio backend for asynchronous tests."""
    return "asyncio"

@pytest_asyncio.fixture(scope="session")
async def postgres_container_details() -> AsyncGenerator[dict, None]:
    """
    Sets up a PostgreSQL container using the official docker-py client for the entire test session.
    Manages the container lifecycle (start, readiness check, stop).
    """
    container = None
    # The docker-py client is synchronous, so we run initialization on a thread.
    docker_client = await asyncio.to_thread(docker.from_env)
    try:
        # 1. Start the container on a dedicated thread
        container = await asyncio.to_thread(
            docker_client.containers.run,
            POSTGRES_IMAGE,
            detach=True,
            environment={
                "POSTGRES_USER": POSTGRES_USER,
                "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
                "POSTGRES_DB": POSTGRES_DB,
            },
            ports={"5432/tcp": None}, # Docker assigns a random host port
            remove=True # Ensure container is cleaned up even on crash
        )
        # Explicitly reload the container object to get fresh network attributes
        await asyncio.to_thread(container.reload)
        # Get the dynamically mapped port and construct connection details
        ports_attr = container.attrs["NetworkSettings"]["Ports"]
        if "5432/tcp" not in ports_attr or not ports_attr["5432/tcp"]:
            raise Exception("Docker failed to map port 5432/tcp. Check Docker daemon status.")
        mapped_port = ports_attr["5432/tcp"][0]["HostPort"]
        container_host = "127.0.0.1"
        container_port = int(mapped_port)
        # Wait for PostgreSQL service to be ready (max 30 seconds)
        print(f"\nWaiting for PostgreSQL container to start on {container_host}:{container_port}...")
        max_retries = 30
        for i in range(1, max_retries + 1):
            try:
                conn = await asyncpg.connect(
                    user=POSTGRES_USER, 
                    password=POSTGRES_PASSWORD, 
                    database=POSTGRES_DB, 
                    host=container_host, 
                    port=container_port
                )
                await conn.close()
                print(f"PostgreSQL is ready after {i} second(s)!")
                break
            except Exception as e:
                if i == max_retries:
                     raise TimeoutError(f"PostgreSQL container failed to start in {max_retries} seconds: {e}")
                await asyncio.sleep(1)
        POSTGRES_URI = (
            f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{container_host}:{container_port}/{POSTGRES_DB}"
        )
        yield {"uri": POSTGRES_URI}
    finally:
        if container:
            print(f"\nStopping and removing container {container.short_id}...")
            await asyncio.to_thread(container.stop, timeout=5)


@pytest_asyncio.fixture(scope="function")
async def test_db_engine(postgres_container_details: dict) -> AsyncGenerator[AsyncEngine, None]:
    """
    Creates a PostgreSQL database engine using the running container's URL and 
    ensures a clean schema for each test function.
    """
    POSTGRES_URI = postgres_container_details["uri"]
    engine = create_async_engine(POSTGRES_URI)
    async with engine.begin() as conn:
        for statement in CREATE_DDL_STATEMENTS:
            if statement.strip():
                await conn.execute(text(statement))
    yield engine
    await engine.dispose()

@pytest.fixture(scope="function")
def override_db_dependencies(test_db_engine: AsyncEngine):
    """
    Patches the database dependency functions (get_read_conn, get_write_conn) 
    to yield connections from the PostgreSQL engine created by the test fixture.
    """
    async def get_test_conn_generator() -> AsyncGenerator[AsyncConnection, None]:
        async with test_db_engine.connect() as conn:
            yield conn
    app.dependency_overrides[get_read_conn] = get_test_conn_generator
    app.dependency_overrides[get_write_conn] = get_test_conn_generator
    yield
    app.dependency_overrides = {}

@pytest_asyncio.fixture(scope="function")
async def test_client(
    override_db_dependencies,
    test_db_engine,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Compound fixture that yields the FastAPI client ready for integration testing 
    against a fully functional PostgreSQL database.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://localhost") as client:
        yield client