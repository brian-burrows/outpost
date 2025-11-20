from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from typing import AsyncGenerator

READ_DATABASE_URL = "postgresql+asyncpg://readonly_user:password@read-replica-host:5432/weatherdb"
WRITE_DATABASE_URL = "postgresql+asyncpg://write_user:password@master-host:5432/weatherdb"

write_engine: AsyncEngine = create_async_engine(WRITE_DATABASE_URL, pool_size=5)
read_engine: AsyncEngine = create_async_engine(READ_DATABASE_URL, pool_size=20)

async def get_read_conn() -> AsyncGenerator[AsyncConnection, None]:
    """Dependency for Read-Only Core operations (SELECTs)."""
    async with read_engine.connect() as conn:
        yield conn
        # Connection released/closed upon exit

async def get_write_conn() -> AsyncGenerator[AsyncConnection, None]:
    """Dependency for Write-Only Core operations (INSERTs/UPDATEs/DELETEs).
    
    Here we return a bare connection object. For writes, it is the responsibility
    of the endpoint to create and close a transaction. The following pattern is
    recommended:

    trans = conn.begin()
    conn.exetute(sql_statment, params)
    trans.commit()
    """
    async with write_engine.connect() as conn:
        yield conn
        # Transaction released/closed upon exit

async def close_db_connections():
    """
    Called during application shutdown.
    Gracefully closes all connection pools for both read and write engines.
    """
    await read_engine.dispose()
    await write_engine.dispose()