SET statement_timeout TO 0;
SET client_encoding TO 'UTF8';
SET standard_conforming_strings TO on;
SET check_function_bodies TO false;
SET client_min_messages TO warning;

DROP TABLE IF EXISTS historical_weather_data CASCADE;
DROP TABLE IF EXISTS forecast_weather_data CASCADE;
DROP TABLE IF EXISTS cities CASCADE;

CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    latitude_deg DECIMAL(9, 6) NOT NULL,
    longitude_deg DECIMAL(9, 6) NOT NULL,
    CONSTRAINT VALID_LATITUDE_BOUNDS CHECK (latitude_deg BETWEEN -90 AND 90),
    CONSTRAINT VALID_LONGITUDE_BOUNDS CHECK (longitude_deg BETWEEN -180 AND 180),
    CONSTRAINT cities_unique_name_state UNIQUE (city_name, state_name)
);

CREATE TABLE forecast_weather_data (
    city_id INTEGER NOT NULL,
    temperature_deg_c REAL NOT NULL,
    rain_fall_total_mm REAL NOT NULL,
    wind_speed_mps REAL NOT NULL,
    aggregation_level VARCHAR(100) NOT NULL,
    forecast_generated_at_ts_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    forecast_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (forecast_generated_at_ts_utc, city_id, forecast_timestamp_utc),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_fall_total_mm IS NULL OR rain_fall_total_mm >= 0),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id) ON DELETE CASCADE
); -- PARTITION BY RANGE (forecast_generated_at_ts_utc); TODO: Need automated tool to partition

CREATE TABLE historical_weather_data (
    city_id INTEGER NOT NULL,
    temperature_deg_c REAL NOT NULL,
    wind_speed_mps REAL NOT NULL,
    rain_fall_total_mm REAL NOT NULL,
    aggregation_level TEXT NOT NULL,
    measured_at_ts_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (measured_at_ts_utc, city_id),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_fall_total_mm IS NULL OR rain_fall_total_mm >= 0),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id) ON DELETE CASCADE
); -- PARTITION BY RANGE (measured_at_ts_utc); TODO: Need automated tool to partition

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

CREATE TABLE weather_classifications (
    city_id INTEGER PRIMARY KEY,
    class_label VARCHAR(50) NOT NULL,
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id) ON DELETE CASCADE
);

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