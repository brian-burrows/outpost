SET statement_timeout TO 0;
SET client_encoding TO 'UTF8';
SET standard_conforming_strings TO on;
SET check_function_bodies TO false;
SET client_min_messages TO warning;

-- DROP TABLES
DROP TABLE IF EXISTS open_weather_map_historical_weather_data CASCADE;
DROP TABLE IF EXISTS open_weather_map_forecast_weather_data CASCADE;
DROP TABLE IF EXISTS cities CASCADE;

-- --- 1. CITIES TABLE (Using SERIAL Primary Key for city_id) ---
CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    latitude_deg DECIMAL(9, 6) NOT NULL,
    longitude_deg DECIMAL(9, 6) NOT NULL,
    CONSTRAINT VALID_LATITUDE_BOUNDS CHECK (latitude_deg BETWEEN -90 AND 90),
    CONSTRAINT VALID_LONGITUDE_BOUNDS CHECK (longitude_deg BETWEEN -180 AND 180),
    -- Keep the natural key unique to prevent duplicate cities
    CONSTRAINT cities_unique_name_state UNIQUE (city_name, state_name)
);

-- --- 2. FORECAST WEATHER DATA TABLE (Using city_id FOREIGN KEY) ---
CREATE TABLE open_weather_map_forecast_weather_data (
    city_id INTEGER NOT NULL,
    forecast_generated_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_deg_c DECIMAL(5, 2),
    rain_total_mm DECIMAL(6, 1),
    PRIMARY KEY (forecast_generated_ts, city_id, ts),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_total_mm IS NULL OR rain_total_mm >= 0),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id) ON DELETE CASCADE
) PARTITION BY RANGE (forecast_generated_ts);

-- --- 3. HISTORICAL WEATHER DATA TABLE (Using city_id FOREIGN KEY) ---
CREATE TABLE open_weather_map_historical_weather_data (
    city_id INTEGER NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_deg_c DECIMAL(5, 2),
    rain_total_mm DECIMAL(6, 1),
    PRIMARY KEY (ts, city_id),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_total_mm IS NULL OR rain_total_mm >= 0),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id) ON DELETE CASCADE
) PARTITION BY RANGE (ts);

-- --- 4. INSERT DATA (Including city_id based on insertion order) ---
INSERT INTO cities (city_name, state_name, latitude_deg, longitude_deg) VALUES 
    ('colorado-springs', 'colorado', 38.83, -104.82), -- city_id 1
    ('denver', 'colorado', 39.74, -104.99),        -- city_id 2
    ('crested-butte', 'colorado', 38.87, -106.99), -- city_id 3
    ('fruita', 'colorado', 39.14, -108.73),         -- city_id 4
    ('durango', 'colorado', 37.27, -107.88),        -- city_id 5
    ('moab', 'utah', 38.57, -109.55),               -- city_id 6
    ('saint-george', 'utah', 37.10, -113.57),      -- city_id 7
    ('park-city', 'utah', 40.65, -111.50),          -- city_id 8
    ('hurricane', 'utah', 37.18, -113.40),          -- city_id 9
    ('sedona', 'arizona', 34.87, -111.76),          -- city_id 10
    ('flagstaff', 'arizona', 35.20, -111.65),       -- city_id 11
    ('prescott', 'arizona', 34.55, -112.45),        -- city_id 12
    ('phoenix', 'arizona', 33.45, -112.07),         -- city_id 13
    ('boulder-city', 'nevada', 35.95, -114.83),     -- city_id 14
    ('las-vegas', 'nevada', 36.17, -115.14),        -- city_id 15
    ('reno', 'nevada', 39.53, -119.82);             -- city_id 16

-- VIEW CREATION (Updated to use city_id joins if necessary, but the original view logic uses city/state name)

CREATE MATERIALIZED VIEW IF NOT EXISTS current_weather_data AS 
WITH latest_forecast_run AS (
    SELECT city_id, MAX(forecast_generated_ts) AS latest_gen_ts
    FROM open_weather_map_forecast_weather_data
    GROUP BY city_id
),
most_recent_forecast AS (
    SELECT 
        f.city_id, 
        c.city_name, 
        c.state_name, 
        f.ts, 
        f.temperature_deg_c, 
        f.rain_total_mm, 
        'FORECAST' AS data_source
    FROM open_weather_map_forecast_weather_data f
    JOIN latest_forecast_run l ON f.city_id = l.city_id AND f.forecast_generated_ts = l.latest_gen_ts
    JOIN cities c ON f.city_id = c.city_id
)
(
    SELECT 
        c.city_name, 
        c.state_name, 
        h.ts, 
        h.temperature_deg_c, 
        h.rain_total_mm, 
        'HISTORICAL' AS data_source
    FROM open_weather_map_historical_weather_data h
    JOIN cities c ON h.city_id = c.city_id
    WHERE h.ts >= (NOW() - INTERVAL '3 days')
) UNION ALL (
    SELECT city_name, state_name, ts, temperature_deg_c, rain_total_mm, data_source
    FROM most_recent_forecast
    WHERE ts > NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS current_weather_data_unique_idx
ON current_weather_data (city_name, state_name, ts);

-- ROLE CREATION
-- TODO: Remove this for production
DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'weather_app') THEN
        EXECUTE 'CREATE ROLE weather_app WITH LOGIN PASSWORD ''weather_app_password''';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE outpost_weather_db TO weather_app;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA public
TO weather_app;

ALTER DEFAULT PRIVILEGES FOR ROLE outpost_postgres_super_user IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO weather_app;