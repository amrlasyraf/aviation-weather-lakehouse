# Databricks Notebook: 02_silver_transform
# Description: Cleans, deduplicates, and enriches flight and weather data in Unity Catalog. 

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, row_number, when, round,
    radians, sin, cos, atan2, sqrt, lit
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

# -------------------------------------------------------------------------
# 1. Clean & Deduplicate Airport Weather
# -------------------------------------------------------------------------
def process_silver_weather():
    df_raw = spark.read.table("bronze_airport_weather")
    
    window_spec = Window.partitionBy("airport_code", "observed_at").orderBy(col("ingestion_timestamp").desc())
    
    df_clean = df_raw \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn", "raw_payload") \
        .withColumn("event_date", to_date(col("observed_at"))) \
        .withColumn("is_adverse_weather", 
            (col("wind_gusts") > 40.0) | (col("precipitation") > 5.0) | (col("wind_speed") > 30.0)
        )
        
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("airport_code") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_airport_weather")
        
    print("Silver weather processing complete.")
    return df_clean

# -------------------------------------------------------------------------
# 2. Clean Flight States & Compute Airport Proximity
# -------------------------------------------------------------------------
def process_silver_flights(df_weather_silver):
    df_flights_raw = spark.read.table("bronze_flight_states")
    
    window_spec = Window.partitionBy("icao24", "snapshot_time").orderBy(col("ingestion_timestamp").desc())
    
    df_valid_flights = df_flights_raw \
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull() & col("icao24").isNotNull()) \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .withColumn("event_date", to_date(col("ingestion_timestamp")))

    airports_df = df_weather_silver.select(
        col("airport_code"), 
        col("airport_name"), 
        col("latitude").alias("airport_lat"), 
        col("longitude").alias("airport_lon")
    ).distinct()

    joined = df_valid_flights.crossJoin(airports_df)

    # Haversine Distance in km
    R = 6371.0
    dlat = radians(col("latitude") - col("airport_lat"))
    dlon = radians(col("longitude") - col("airport_lon"))
    lat1 = radians(col("airport_lat"))
    lat2 = radians(col("latitude"))

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(lit(1) - a))
    distance_km = round(lit(R) * c, 2)

    df_proximity = joined.withColumn("distance_km", distance_km) \
        .filter(col("distance_km") <= 120.0)

    df_silver_enriched = df_proximity.withColumn("airspace_status", 
        when(col("on_ground") == True, "TAXI_OR_GROUND")
        .when((col("baro_altitude") < 3000) & (col("vertical_rate") < -1.0), "APPROACHING")
        .when((col("baro_altitude") < 3000) & (col("vertical_rate") > 1.0), "DEPARTING")
        .when(col("baro_altitude") >= 3000, "EN_ROUTE_HOLDING")
        .otherwise("UNKNOWN")
    )

    df_silver_enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("airport_code") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver_flights_near_airports")

    print("Silver flight spatial enrichment complete.")

if __name__ == "__main__":
    df_weather = process_silver_weather()
    process_silver_flights(df_weather)
