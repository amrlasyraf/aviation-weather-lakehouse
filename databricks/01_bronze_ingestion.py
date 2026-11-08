# Databricks Notebook: 01_bronze_ingestion
# Description: Ingests raw flight state vectors (OpenSky) and weather forecasts (Open-Meteo) into Unity Catalog Bronze Delta tables.

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# Ensure we use default catalog/schema
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

AIRPORTS = {
    "JFK": {"name": "New York JFK", "lat": 40.6413, "lon": -73.7781},
    "ORD": {"name": "Chicago O'Hare", "lat": 41.9742, "lon": -87.9073},
    "LHR": {"name": "London Heathrow", "lat": 51.4700, "lon": -0.4543},
    "HND": {"name": "Tokyo Haneda", "lat": 35.5494, "lon": 139.7798},
    "SIN": {"name": "Singapore Changi", "lat": 1.3644, "lon": 103.9915}
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

        # Write to Unity Catalog Managed Delta Table
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("bronze_flight_states")

        print(f"Successfully appended {len(flight_rows)} flight records to table bronze_flight_states")

    except Exception as e:
        print(f"Error ingesting flight data: {e}")
        raise

# -------------------------------------------------------------------------
# 2. Ingest Airport Weather Forecasts & Conditions (Bronze)
# -------------------------------------------------------------------------
def fetch_airport_weather():
    """Fetch current conditions for target airports via Open-Meteo."""
    weather_records = []
    
    for iata, info in AIRPORTS.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": info["lat"],
            "longitude": info["lon"],
            "current": "temperature_2m,relative_humidity_2m,precipitation,rain,wind_speed_10m,wind_gusts_10m,surface_pressure",
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
        
        # Write to Unity Catalog Managed Delta Table
        df_weather.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("bronze_airport_weather")
            
        print(f"Successfully appended {len(weather_records)} weather records to table bronze_airport_weather")

if __name__ == "__main__":
    fetch_flight_states()
    fetch_airport_weather()
