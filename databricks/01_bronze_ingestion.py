# Databricks Notebook: 01_bronze_ingestion
# Description: Ingests raw flight state vectors (OpenSky) and weather forecasts (Open-Meteo) into Bronze Delta tables.

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, LongType, ArrayType
)
from pyspark.sql.functions import current_timestamp, lit

# Initialize Spark (pre-existing in Databricks)
spark = SparkSession.builder.getOrCreate()

# Base paths (Databricks File System or Unity Catalog Volume)
BRONZE_BASE_PATH = "/tmp/lakehouse/bronze"

# Target Major Airports (lat, lon, icao)
AIRPORTS = {
    "JFK": {"name": "New York JFK", "lat": 40.6413, "lon": -73.7781, "bbox": (40.0, 41.5, -74.5, -73.0)},
    "ORD": {"name": "Chicago O'Hare", "lat": 41.9742, "lon": -87.9073, "bbox": (41.2, 42.5, -88.5, -87.2)},
    "LHR": {"name": "London Heathrow", "lat": 51.4700, "lon": -0.4543, "bbox": (51.0, 52.0, -1.0, 0.2)},
    "HND": {"name": "Tokyo Haneda", "lat": 35.5494, "lon": 139.7798, "bbox": (35.0, 36.0, 139.0, 140.5)},
    "SIN": {"name": "Singapore Changi", "lat": 1.3644, "lon": 103.9915, "bbox": (1.0, 2.0, 103.5, 104.5)}
}

# -------------------------------------------------------------------------
# 1. Ingest OpenSky Flight States (Bronze)
# -------------------------------------------------------------------------
def fetch_flight_states():
    """Fetch live flight vectors from OpenSky Network."""
    url = "https://opensky-network.org/api/states/all"
    headers = {"User-Agent": "Aviation-ETL-Pipeline/1.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        payload = response.json()
        
        timestamp = payload.get("time", int(datetime.now(timezone.utc).timestamp()))
        states = payload.get("states", [])
        
        if not states:
            print("No flight states received.")
            return

        # OpenSky state vector schema
        # [icao24, callsign, origin_country, time_position, last_contact,
        #  longitude, latitude, baro_altitude, on_ground, velocity,
        #  true_track, vertical_rate, sensors, geo_altitude, squawk, spi, position_source]
        flight_rows = []
        for s in states:
            flight_rows.append({
                "icao24": s[0],
                "callsign": s[1].strip() if s[1] else None,
                "origin_country": s[2],
                "time_position": s[3],
                "last_contact": s[4],
                "longitude": float(s[5]) if s[5] is not None else None,
                "latitude": float(s[6]) if s[6] is not None else None,
                "baro_altitude": float(s[7]) if s[7] is not None else None,
                "on_ground": bool(s[8]),
                "velocity": float(s[9]) if s[9] is not None else None,
                "true_track": float(s[10]) if s[10] is not None else None,
                "vertical_rate": float(s[11]) if s[11] is not None else None,
                "geo_altitude": float(s[13]) if s[13] is not None else None,
                "squawk": s[14],
                "spi": bool(s[15]),
                "position_source": s[16]
            })

        df = spark.createDataFrame(flight_rows)
        df_bronze = df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("snapshot_time", lit(timestamp))

        # Append to Bronze Delta Lake
        target_path = f"{BRONZE_BASE_PATH}/flight_states"
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(target_path)

        print(f"Successfully appended {len(flight_rows)} flight records to {target_path}")

    except Exception as e:
        print(f"Error ingesting flight data: {e}")
        raise

# -------------------------------------------------------------------------
# 2. Ingest Airport Weather Forecasts & Conditions (Bronze)
# -------------------------------------------------------------------------
def fetch_airport_weather():
    """Fetch current & hourly weather conditions for target airports via Open-Meteo."""
    weather_records = []
    
    for iata, info in AIRPORTS.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": info["lat"],
            "longitude": info["lon"],
            "current": "temperature_2m,relative_humidity_2m,precipitation,rain,wind_speed_10m,wind_gusts_10m,surface_pressure",
            "hourly": "temperature_2m,precipitation_probability,visibility,wind_speed_10m",
            "timezone": "UTC"
        }
        
        try:
            res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            data = res.json()
            
            current = data.get("current", {})
            weather_records.append({
                "airport_code": iata,
                "airport_name": info["name"],
                "latitude": info["lat"],
                "longitude": info["lon"],
                "observed_at": current.get("time"),
                "temperature": current.get("temperature_2m"),
                "humidity": current.get("relative_humidity_2m"),
                "precipitation": current.get("precipitation"),
                "rain": current.get("rain"),
                "wind_speed": current.get("wind_speed_10m"),
                "wind_gusts": current.get("wind_gusts_10m"),
                "surface_pressure": current.get("surface_pressure"),
                "raw_payload": json.dumps(data)
            })
        except Exception as err:
            print(f"Failed to fetch weather for {iata}: {err}")

    if weather_records:
        df_weather = spark.createDataFrame(weather_records) \
                          .withColumn("ingestion_timestamp", current_timestamp())
        
        target_path = f"{BRONZE_BASE_PATH}/airport_weather"
        df_weather.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(target_path)
            
        print(f"Successfully appended {len(weather_records)} weather records to {target_path}")

# Run Ingestion
if __name__ == "__main__":
    fetch_flight_states()
    fetch_airport_weather()
