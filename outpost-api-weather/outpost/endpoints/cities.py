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

class WeatherClassification(BaseModel):
    city_id: int
    class_label: str

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

@router.post(
    "/{city_id}/classification",
    response_model=WeatherClassification,
    status_code=status.HTTP_201_CREATED
)
async def create_or_update_weather_classification(
    city_id: int,
    classification_data: WeatherClassification,
    conn: AsyncConnection = Depends(get_write_conn)
):
    """
    Creates or updates the weather classification for a specific city.
    Uses INSERT ... ON CONFLICT DO UPDATE (common in PostgreSQL)
    """
    if city_id != classification_data.city_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="City ID in the path must match the city_id in the request body."
        )

    # Note: This query uses PostgreSQL's ON CONFLICT syntax, which is not Portable
    stmt = text("""
        INSERT INTO weather_classifications (city_id, class_label)
        VALUES (:city_id, :class_label)
        ON CONFLICT (city_id) DO UPDATE 
        SET class_label = EXCLUDED.class_label
        RETURNING city_id, class_label
    """)

    trans = await conn.begin()
    result = await conn.execute(stmt, classification_data.model_dump())
    await trans.commit()
    
    new_classification = result.mappings().first()
    if new_classification is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Failed to create or update weather classification."
        )

    return new_classification

@router.get(
    "/{city_id}/classification",
    response_model=WeatherClassification
)
async def get_weather_classification(
    city_id: int, 
    conn: AsyncConnection = Depends(get_read_conn)
):
    """Retrieves the weather classification for a particular `city_id`"""
    stmt = text("""
        SELECT city_id, class_label
        FROM weather_classifications
        WHERE city_id = :city_id
    """)
    result = await conn.execute(stmt, {"city_id": city_id})
    classification = result.mappings().first()

    if classification is None:
        raise HTTPException(status_code=404, detail=f"Weather classification not found for city_id {city_id}")
        
    return classification