SET statement_timeout TO 0;
SET client_encoding TO 'UTF8';
SET standard_conforming_strings TO on;
SET check_function_bodies TO false;
SET client_min_messages TO warning;

DROP TABLE IF EXISTS open_weather_map_historical_weather_data CASCADE;
DROP TABLE IF EXISTS open_weather_map_forecast_weather_data CASCADE;
DROP TABLE IF EXISTS cities CASCADE;

CREATE TABLE cities (
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    latitude_deg DECIMAL(9, 6) NOT NULL,
    longitude_deg DECIMAL(9, 6) NOT NULL,
    CONSTRAINT VALID_LATITUDE_BOUNDS CHECK (latitude_deg BETWEEN -90 AND 90),
    CONSTRAINT VALID_LONGITUDE_BOUNDS CHECK (longitude_deg BETWEEN -180 AND 180),
    PRIMARY KEY (city_name, state_name)
);

CREATE TABLE open_weather_map_historical_weather_data (
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_degc DECIMAL(5, 2),
    rain_total_mm DECIMAL(6, 1),
    PRIMARY KEY (ts, city_name, state_name),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_total_mm IS NULL OR rain_total_mm >= 0),
    FOREIGN KEY (city_name, state_name)
        REFERENCES cities (city_name, state_name) ON DELETE CASCADE
) PARTITION BY RANGE (ts);

CREATE TABLE open_weather_map_forecast_weather_data (
    city_name VARCHAR(100) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    forecast_generated_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature_degc DECIMAL(5, 2),
    rain_total_mm DECIMAL(6, 1),
    PRIMARY KEY (forecast_generated_ts, city_name, state_name, ts),
    CONSTRAINT POSITIVE_RAIN_TOTAL CHECK (rain_total_mm IS NULL OR rain_total_mm >= 0),
    FOREIGN KEY (city_name, state_name)
        REFERENCES cities (city_name, state_name) ON DELETE CASCADE
) PARTITION BY RANGE (forecast_generated_ts);

INSERT INTO cities (city_name, state_name, latitude_deg, longitude_deg) VALUES 
    ('colorado-springs', 'colorado', 38.83, -104.82), 
    ('denver', 'colorado', 39.74, -104.99),        
    ('crested-butte', 'colorado', 38.87, -106.99), 
    ('fruita', 'colorado', 39.14, -108.73),         
    ('durango', 'colorado', 37.27, -107.88), 
    ('moab', 'utah', 38.57, -109.55), 
    ('saint-george', 'utah', 37.10, -113.57),
    ('park-city', 'utah', 40.65, -111.50),
    ('hurricane', 'utah', 37.18, -113.40),
    ('sedona', 'arizona', 34.87, -111.76), 
    ('flagstaff', 'arizona', 35.20, -111.65), 
    ('prescott', 'arizona', 34.55, -112.45),
    ('phoenix', 'arizona', 33.45, -112.07),
    ('boulder-city', 'nevada', 35.95, -114.83),
    ('las-vegas', 'nevada', 36.17, -115.14),
    ('reno', 'nevada', 39.53, -119.82);

-- VIEW CREATION

-- `REFRESH MATERIALIZED VIEW CONCURRENTLY` to be run by application code
CREATE MATERIALIZED VIEW IF NOT EXISTS current_weather_data AS 
WITH latest_forecast_run AS (
    SELECT city_name, state_name, MAX(forecast_generated_ts) AS latest_gen_ts
    FROM open_weather_map_forecast_weather_data
    GROUP BY city_name, state_name
),
most_recent_forecast AS (
    SELECT f.city_name, f.state_name, f.ts, f.temperature_degc, f.rain_total_mm, 'FORECAST' AS data_source
    FROM open_weather_map_forecast_weather_data f
    JOIN latest_forecast_run l
    ON f.city_name = l.city_name AND f.state_name = l.state_name AND f.forecast_generated_ts = l.latest_gen_ts
)
(
    SELECT city_name, state_name, ts, temperature_degc, rain_total_mm, 'HISTORICAL' AS data_source
    FROM open_weather_map_historical_weather_data
    WHERE ts >= (NOW() - INTERVAL '3 days')
) UNION ALL (
    SELECT city_name, state_name, ts, temperature_degc, rain_total_mm, data_source
    FROM most_recent_forecast
    WHERE ts > NOW()
);
-- 
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