-- Schema DDL for Neon Postgres Serving Layer
-- Table: airport_congestion_weather_mart

CREATE TABLE IF NOT EXISTS airport_congestion_weather_mart (
    event_date DATE NOT NULL,
    airport_code VARCHAR(10) NOT NULL,
    airport_name VARCHAR(100),
    total_unique_aircraft INT DEFAULT 0,
    inbound_approaches INT DEFAULT 0,
    outbound_departures INT DEFAULT 0,
    airborne_holding INT DEFAULT 0,
    ground_movements INT DEFAULT 0,
    avg_speed_knots NUMERIC(6, 2),
    avg_temp_c NUMERIC(5, 2),
    max_wind_gust_kmh NUMERIC(6, 2),
    total_precipitation_mm NUMERIC(6, 2),
    adverse_weather_events INT DEFAULT 0,
    congestion_index NUMERIC(6, 2),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_date, airport_code)
);

CREATE INDEX IF NOT EXISTS idx_airport_event_date 
ON airport_congestion_weather_mart (event_date DESC);

CREATE INDEX IF NOT EXISTS idx_congestion_index 
ON airport_congestion_weather_mart (congestion_index DESC);
