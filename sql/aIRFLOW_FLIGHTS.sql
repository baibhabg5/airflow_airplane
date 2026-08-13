
CREATE DATABASE IF NOT EXISTS FLIGHTS;
USE DATABASE FLIGHTS;

CREATE SCHEMA IF NOT EXISTS KPI;
USE SCHEMA KPI;

'''CREATE TABLE IF NOT EXISTS FLIGHT_KPIS (
        window_start TIMESTAMP,
        origin_country TEXT,
        total_flights INT,
        avg_velocity FLOAT,
        on_ground INT,
        Load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (window_start, origin_country)
);'''
CREATE TABLE IF NOT EXISTS COUNTRY_SUMMARY (
    window_start TIMESTAMP, origin_country STRING,
    total_flights INTEGER,unique_callsigns INTEGER,
    avg_speed_kmh FLOAT,max_speed_kmh FLOAT,
    avg_geo_altitude FLOAT,avg_baro_altitude FLOAT,
    avg_altitude_difference FLOAT,grounded_aircraft INTEGER,
    airborne_aircraft INTEGER,avg_position_age_seconds FLOAT,
    squawk_available INTEGER,spi_enabled INTEGER,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, origin_country)
);
CREATE TABLE IF NOT EXISTS ALTITUDE_ANALYSIS (
    window_start TIMESTAMP,altitude_category STRING,
    aircraft_count INTEGER,avg_geo_altitude FLOAT,
    avg_baro_altitude FLOAT,avg_speed_kmh FLOAT,
    max_speed_kmh FLOAT,avg_vertical_rate FLOAT,
    avg_altitude_difference FLOAT,avg_position_age_seconds FLOAT,
    airborne_aircraft INTEGER,grounded_aircraft INTEGER,
    avg_heading_degree FLOAT,load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, altitude_category)
);
CREATE TABLE IF NOT EXISTS MOVEMENT_ANALYSIS (
    window_start TIMESTAMP,climb_status STRING,
    aircraft_count INTEGER,avg_speed_kmh FLOAT,
    max_speed_kmh FLOAT, avg_geo_altitude FLOAT,
    avg_baro_altitude FLOAT, avg_altitude_difference FLOAT,
    avg_vertical_rate FLOAT, avg_position_age_seconds FLOAT,
    unique_countries INTEGER, airborne_aircraft INTEGER,
    squawk_available INTEGER, load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, climb_status)
);
CREATE TABLE IF NOT EXISTS POSITION_SOURCE_ANALYSIS (
    window_start TIMESTAMP,position_source_name STRING,
    aircraft_count INTEGER,unique_countries INTEGER,
    avg_speed_kmh FLOAT, avg_geo_altitude FLOAT,
    avg_baro_altitude FLOAT,avg_altitude_difference FLOAT,
    avg_position_age_seconds FLOAT,grounded_aircraft INTEGER,
    airborne_aircraft INTEGER,squawk_available INTEGER,
    spi_enabled INTEGER, load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start, position_source_name)
);
CREATE TABLE IF NOT EXISTS OPERATIONAL_QUALITY (
    window_start TIMESTAMP,total_aircraft INTEGER,
    unique_aircraft INTEGER,unique_callsigns INTEGER,
    countries_observed INTEGER,avg_speed_kmh FLOAT,
    max_speed_kmh FLOAT,avg_geo_altitude FLOAT,
    avg_altitude_difference FLOAT,avg_position_age_seconds FLOAT,
    grounded_aircraft INTEGER,airborne_aircraft INTEGER,
    callsigns_available INTEGER,squawk_available INTEGER,
    spi_enabled INTEGER,position_sources INTEGER,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start)
);
CREATE TABLE IF NOT EXISTS GEOGRAPHICAL_DISTRIBUTION (
    window_start TIMESTAMP,latitude_grid INTEGER,
    longitude_grid INTEGER,aircraft_count INTEGER,
    unique_countries INTEGER,unique_callsigns INTEGER,
    avg_speed_kmh FLOAT,max_speed_kmh FLOAT,
    avg_geo_altitude FLOAT,avg_baro_altitude FLOAT,
    avg_altitude_difference FLOAT,avg_vertical_rate FLOAT,
    avg_position_age_seconds FLOAT,grounded_aircraft INTEGER,
    airborne_aircraft INTEGER,squawk_available INTEGER,
    spi_enabled INTEGER,load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start,latitude_grid,longitude_grid)
);
CREATE TABLE IF NOT EXISTS TIME_ANALYSIS (
    window_start TIMESTAMP,hour INTEGER,
    aircraft_count INTEGER,unique_countries INTEGER,
    unique_callsigns INTEGER,avg_speed_kmh FLOAT,
    max_speed_kmh FLOAT,avg_geo_altitude FLOAT,
    avg_baro_altitude FLOAT,avg_vertical_rate FLOAT,
    avg_altitude_difference FLOAT,avg_position_age_seconds FLOAT,
    grounded_aircraft INTEGER,airborne_aircraft INTEGER,
    squawk_available INTEGER,spi_enabled INTEGER,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start,hour)
);
CREATE TABLE IF NOT EXISTS HEADING_ANALYSIS (
    window_start TIMESTAMP,heading STRING,
    aircraft_count INTEGER,unique_countries INTEGER,
    avg_speed_kmh FLOAT,max_speed_kmh FLOAT,
    avg_geo_altitude FLOAT,avg_vertical_rate FLOAT,
    avg_altitude_difference FLOAT,avg_position_age_seconds FLOAT,
    avg_true_track FLOAT,airborne_aircraft INTEGER,
    squawk_available INTEGER,load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (window_start,heading)
);
SHOW TRANSACTIONS;
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
ORDER BY START_TIME DESC
LIMIT 20;