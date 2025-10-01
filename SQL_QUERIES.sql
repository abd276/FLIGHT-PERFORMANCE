SET SQL_SAFE_UPDATES = 0;
-- Control Server 
SET GLOBAL net_write_timeout=1200;
SET GLOBAL net_read_timeout=1200;
SET GLOBAL wait_timeout=1200;
SET GLOBAL interactive_timeout=1200;

-- Phase 1

-- Creating database and giving prviledges
CREATE DATABASE IF NOT EXISTS flightdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'flightuser'@'localhost' IDENTIFIED BY 'admin';
GRANT ALL PRIVILEGES ON flightdb.* TO 'flightuser'@'localhost';
FLUSH PRIVILEGES;
USE flightdb;

-- airlines
CREATE TABLE IF NOT EXISTS airlines (
  iata_code VARCHAR(10) PRIMARY KEY,
  airline VARCHAR(255)
);

-- airports
CREATE TABLE IF NOT EXISTS airports (
  iata_code VARCHAR(10) PRIMARY KEY,
  airport VARCHAR(255),
  city VARCHAR(100),
  state VARCHAR(100),
  country VARCHAR(100),
  latitude DOUBLE,
  longitude DOUBLE
);

-- flights (import raw first; we'll transform times/dates later)
CREATE TABLE IF NOT EXISTS flights (
  year SMALLINT,
  month TINYINT,
  day TINYINT,
  day_of_week TINYINT,
  airline VARCHAR(10),
  flight_number INT,
  tail_number VARCHAR(10),
  origin_airport VARCHAR(10),
  destination_airport VARCHAR(10),
  scheduled_departure VARCHAR(10),   -- keep raw HHMM as text for initial import
  departure_time VARCHAR(10),
  departure_delay DECIMAL(7,2),
  taxi_out VARCHAR(10),
  wheels_off VARCHAR(10),
  scheduled_time VARCHAR(10),
  elapsed_time VARCHAR(10),
  air_time VARCHAR(10),
  distance INT,
  wheels_on VARCHAR(10),
  taxi_in VARCHAR(10),
  scheduled_arrival VARCHAR(10),
  arrival_time VARCHAR(10),
  arrival_delay DECIMAL(7,2),
  diverted INT,
  cancelled TINYINT,
  cancellation_reason CHAR(1),
  air_system_delay DECIMAL(7,2),
  security_delay DECIMAL(7,2),
  airline_delay DECIMAL(7,2),
  late_aircraft_delay DECIMAL(7,2),
  weather_delay DECIMAL(7,2),
  -- derived columns to be created / filled next
  flight_date DATE,
  scheduled_departure_time TIME,
  scheduled_arrival_time TIME
);

-- Phase 2
-- check data imported or no
select * from airlines;
select * from airports;
select * from flights LIMIT 10;

-- count number of rows
USE flightdb;
SELECT COUNT(*) AS no_of_rows FROM flights;
SELECT COUNT(*) FROM airlines;
SELECT COUNT(*) FROM airports;

-- sample rows
SELECT * FROM flights LIMIT 5;
-- check nulls and basic stats
SELECT AVG(departure_delay) FROM flights WHERE departure_delay;
SELECT airline, COUNT(*) cnt FROM flights GROUP BY airline ORDER BY cnt DESC LIMIT 10;

-- time and date handling
-- Update flights format
UPDATE flights
SET flight_date = STR_TO_DATE(CONCAT(year, '-', month, '-', day), '%Y-%m-%d');

-- Convert scheduled_departure (HHMM) into TIME
UPDATE flights
SET scheduled_departure_time = STR_TO_DATE(LPAD(scheduled_departure, 4, '0'), '%H%i'),
    scheduled_arrival_time   = STR_TO_DATE(LPAD(scheduled_arrival, 4, '0'), '%H%i');

-- missing values
-- Replace NULL delays with 0
-- Update 1,00,000 rows at a time
UPDATE flights
SET 
    departure_delay      = IFNULL(departure_delay, 0),
    arrival_delay        = IFNULL(arrival_delay, 0),
    air_system_delay     = IFNULL(air_system_delay, 0),
    security_delay       = IFNULL(security_delay, 0),
    airline_delay        = IFNULL(airline_delay, 0),
    late_aircraft_delay  = IFNULL(late_aircraft_delay, 0),
    weather_delay        = IFNULL(weather_delay, 0)
LIMIT 100000;
    
-- data enrichment
ALTER TABLE flights MODIFY cancellation_reason VARCHAR(50);

UPDATE flights
SET cancellation_reason = CASE cancellation_reason
    WHEN 'A' THEN 'Airline/Carrier'
    WHEN 'B' THEN 'Weather'
    WHEN 'C' THEN 'National Air System'
    WHEN 'D' THEN 'Security'
    WHEN NULL THEN 'NA'
END;

-- unified 
CREATE OR REPLACE VIEW flights_enriched AS
SELECT 
    f.*, 
    a.AIRLINE AS airline_name,
    ao.city AS origin_city, ao.state AS origin_state, ao.country AS origin_country,
    ad.city AS dest_city, ad.state AS dest_state, ad.country AS dest_country
FROM flights f
JOIN airlines a ON f.airline = a.iata_code
JOIN airports ao ON f.origin_airport = ao.iata_code
JOIN airports ad ON f.destination_airport = ad.iata_code;

-- Check for any remaining NULLs in key fields
SELECT COUNT(*) AS null_airline FROM flights WHERE airline IS NULL;
SELECT COUNT(*) AS null_origin FROM flights WHERE origin_airport IS NULL;

-- Ensure dates are valid
SELECT MIN(flight_date), MAX(flight_date) FROM flights;

-- Phase 3

-- Integration
-- Flights ↔ Airlines
SELECT f.airline AS flights_airline, a.airline
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LIMIT 10;

-- Flights ↔ Airports (origin)
SELECT f.origin_airport, ap.city, ap.state
FROM flights f
LEFT JOIN airports ap ON f.origin_airport = ap.iata_code
LIMIT 10;

-- Transformation
-- Flight duration reliability
ALTER TABLE flights ADD COLUMN on_time_status VARCHAR(50);

UPDATE flights
SET on_time_status = CASE
    WHEN arrival_delay <= 0 THEN 'On-Time'
    WHEN arrival_delay BETWEEN 1 AND 15 THEN 'Slight Delay'
    ELSE 'Delayed'
END
LIMIT 100000;
SELECT COUNT(*) FROM flights WHERE on_time_status IS NULL;

-- Flight distance categories
ALTER TABLE flights ADD COLUMN distance_category VARCHAR(20);

UPDATE flights
SET distance_category = CASE
    WHEN distance < 500 THEN 'Short Haul'
    WHEN distance BETWEEN 500 AND 1500 THEN 'Medium Haul'
    ELSE 'Long Haul'
END;
SELECT COUNT(*) FROM flights WHERE distance_category IS NULL;

-- Exploratory analysis
-- top 5 airlines by number of flights
SELECT a.airline, COUNT(*) AS total_flights
FROM flights f
JOIN airlines a ON f.airline = a.iata_code
GROUP BY a.airline
ORDER BY total_flights DESC
LIMIT 5;

-- airports with the worst average delays
SELECT ap.city, ap.state, AVG(f.arrival_delay) AS avg_arrival_delay
FROM flights f
JOIN airports ap ON f.origin_airport = ap.iata_code
WHERE f.arrival_delay IS NOT NULL
GROUP BY ap.city, ap.state
ORDER BY avg_arrival_delay DESC
LIMIT 10;

-- flights cancelled per airlines
SELECT a.airline, 
       COUNT(*) AS total_flights,
       SUM(f.cancelled) AS cancelled_flights,
       ROUND((SUM(f.cancelled) / COUNT(*)) * 100, 2) AS cancel_rate
FROM flights f
JOIN airlines a ON f.airline = a.iata_code
GROUP BY a.airline
ORDER BY cancel_rate DESC;

-- delay reason contribution
SELECT
  AVG(air_system_delay) AS avg_air_system,
  AVG(security_delay) AS avg_security,
  AVG(airline_delay) AS avg_airline,
  AVG(late_aircraft_delay) AS avg_late_aircraft,
  AVG(weather_delay) AS avg_weather
FROM flights;

-- Data integration checks
SELECT DISTINCT f.airline
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
WHERE a.iata_code IS NULL;

SELECT DISTINCT f.origin_airport
FROM flights f
LEFT JOIN airports ap ON f.origin_airport = ap.iata_code
WHERE ap.iata_code IS NULL;


-- Export for Power BI
CREATE OR REPLACE VIEW flight_analysis AS
SELECT
    f.*,
    a.airline AS airline_name,
    ap.city AS origin_city,
    ap.state AS origin_state,
    ap2.city AS dest_city,
    ap2.state AS dest_state
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LEFT JOIN airports ap ON f.origin_airport = ap.iata_code
LEFT JOIN airports ap2 ON f.destination_airport = ap2.iata_code;





