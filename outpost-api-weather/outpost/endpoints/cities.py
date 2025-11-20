from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
import logging

from ..core.database import get_read_conn, get_write_conn 

LOGGER = logging.getLogger(__name__)

router = APIRouter(
    prefix="/cities",
    tags=["cities"],
)

class CityCreate(BaseModel):
    """Defines the expected shape of the input data for creating a city."""
    city_id: int
    city_name: str
    latitude_deg: float
    longitude_deg: float

@router.get("/{city_id}")
async def get_city_detail(
    city_id: int, 
    conn: AsyncConnection = Depends(get_read_conn) # Uses Read Replica
):
    """Retrieves city details using the read-only connection and raw SQL."""
    stmt = text("""
        SELECT city_id, city_name, latitude_deg, longitude_deg
        FROM city
        WHERE city_id = :city_id
    """)
    result = await conn.execute(stmt, {"city_id": city_id})
    city = result.mappings().first()
    if city is None:
        raise HTTPException(status_code=404, detail="City not found")
    return dict(city)


@router.post("/")
async def create_new_city(
    city_data: CityCreate,
    conn: AsyncConnection = Depends(get_write_conn)
):
    """Creates a new city record using the write connection and raw SQL."""
    stmt = text("""
        INSERT INTO city (city_id, city_name, latitude_deg, longitude_deg)
        VALUES (:city_id, :city_name, :latitude_deg, :longitude_deg)
    """)
    trans = await conn.begin()
    await conn.execute(stmt, city_data.model_dump())
    await trans.commit()
    return {"message": "City created", "id": city_data.city_id}