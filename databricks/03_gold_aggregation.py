# Databricks Notebook: 03_gold_aggregation
# Description: Generates business-ready Gold analytical marts aggregating airport congestion,
# flight traffic volumes, and adverse weather impact correlations.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, max, min, sum, 
    round, when, current_timestamp
)

spark = SparkSession.builder.getOrCreate()

SILVER_BASE = "/tmp/lakehouse/silver"
GOLD_BASE = "/tmp/lakehouse/gold"

def generate_gold_marts():
    df_flights = spark.read.format("delta").load(f"{SILVER_BASE}/flights_near_airports")
    df_weather = spark.read.format("delta").load(f"{SILVER_BASE}/airport_weather")

    # ---------------------------------------------------------------------
    # 1. Airport Traffic Aggregates
    # ---------------------------------------------------------------------
    traffic_summary = df_flights.groupBy("event_date", "airport_code", "airport_name").agg(
        countDistinct("icao24").alias("total_unique_aircraft"),
        count(when(col("airspace_status") == "APPROACHING", 1)).alias("inbound_approaches"),
        count(when(col("airspace_status") == "DEPARTING", 1)).alias("outbound_departures"),
        count(when(col("airspace_status") == "EN_ROUTE_HOLDING", 1)).alias("airborne_holding"),
        count(when(col("airspace_status") == "TAXI_OR_GROUND", 1)).alias("ground_movements"),
        round(avg("velocity"), 2).alias("avg_speed_knots")
    )

    # ---------------------------------------------------------------------
    # 2. Weather Aggregates
    # ---------------------------------------------------------------------
    weather_summary = df_weather.groupBy("event_date", "airport_code").agg(
        round(avg("temperature"), 2).alias("avg_temp_c"),
        round(max("wind_gusts"), 2).alias("max_wind_gust_kmh"),
        round(sum("precipitation"), 2).alias("total_precipitation_mm"),
        sum(when(col("is_adverse_weather") == True, 1).otherwise(0)).alias("adverse_weather_events")
    )

    # ---------------------------------------------------------------------
    # 3. Gold Mart: Airport Congestion & Weather Impact
    # ---------------------------------------------------------------------
    gold_mart = traffic_summary.join(
        weather_summary, 
        on=["event_date", "airport_code"], 
        how="inner"
    ).withColumn(
        # Congestion Index: ratio of holding + approaching aircraft per operational capacity baseline
        "congestion_index", 
        round((col("airborne_holding") * 2.0 + col("inbound_approaches")) / 10.0, 2)
    ).withColumn("updated_at", current_timestamp())

    # Save to Gold Delta Lake
    gold_mart.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/airport_congestion_weather_mart")

    print("Gold mart generation complete.")
    gold_mart.show(10, truncate=False)

if __name__ == "__main__":
    generate_gold_marts()
