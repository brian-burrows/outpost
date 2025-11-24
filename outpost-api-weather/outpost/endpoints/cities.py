import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..core.database import get_read_conn, get_write_conn

LOGGER = logging.getLogger(__name__)
MAX_GET_CITIES_PAGE_SIZE = 1000

router = APIRouter(
    prefix="/cities",
    tags=["cities"],
)

class CityCreate(BaseModel):
    """Defines the expected shape of the input data for creating a city."""
    city_name: str
    state_name: str
    latitude_deg: float
    longitude_deg: float

class CityDetail(CityCreate):
    city_id: int

class PaginatedCityDetails(BaseModel):
    total_count: int
    limit: int 
    page: int 
    cities: list[CityDetail]

@router.get("/", response_model = PaginatedCityDetails)
async def list_city_details(
    conn: AsyncConnection = Depends(get_read_conn),
    limit: int = 100,
    page: int = 0,
):
    """Retrieves city details for all cities in the database"""
    if limit > MAX_GET_CITIES_PAGE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
            detail=f"Requested limit exceeds the maximum allowed limit of {MAX_GET_CITIES_PAGE_SIZE}."
        )
    count_stmt = text("SELECT COUNT(*) FROM cities")
    total_count_result = await conn.execute(count_stmt)
    total_count = total_count_result.scalar_one()
    kws = dict(
        total_count=total_count,
        limit=limit,
        page=page,
    )
    offset = limit * page 
    if offset > total_count:
        return PaginatedCityDetails(**kws, cities=[])
    stmt = text("""
        SELECT city_id, city_name, state_name, latitude_deg, longitude_deg
        FROM cities
        ORDER BY city_id
        LIMIT :limit
        OFFSET :offset
    """)
    result = await conn.execute(stmt, {"limit": limit, "offset": limit * page})
    cities = result.mappings().all()
    return PaginatedCityDetails(**kws, cities=[dict(c) for c in cities])
    
@router.get("/{city_id}", response_model = CityDetail)
async def get_city_detail(
    city_id: int, 
    conn: AsyncConnection = Depends(get_read_conn)
):
    """Retrieves city details for a particular `city_id`"""
    stmt = text("""
        SELECT city_id, city_name, state_name, latitude_deg, longitude_deg
        FROM cities
        WHERE city_id = :city_id
    """)
    result = await conn.execute(stmt, {"city_id": city_id})
    city = result.mappings().first()
    if city is None:
        raise HTTPException(status_code=404, detail="City not found")
    return dict(city)


@router.post("/", response_model = CityDetail, status_code=status.HTTP_201_CREATED)
async def create_new_city(
    city_data: CityCreate,
    conn: AsyncConnection = Depends(get_write_conn)
):
    """Creates a new city record using the write connection and raw SQL."""
    stmt = text("""
        INSERT INTO cities (city_name, state_name, latitude_deg, longitude_deg)
        VALUES (:city_name, :state_name, :latitude_deg, :longitude_deg)
        RETURNING city_id, city_name, state_name, latitude_deg, longitude_deg
    """)
    trans = await conn.begin()
    result = await conn.execute(stmt, city_data.model_dump())
    await trans.commit()
    new_city = result.mappings().first()
    if new_city is None:
        # If the insert failed to return the row, raise a server error
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail="City created but failed to retrieve details.")
    return new_city