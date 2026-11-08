import { NextResponse } from "next/server";
import { neon } from "@neondatabase/serverless";

export const dynamic = "force-dynamic";

export async function GET() {
  const databaseUrl = process.env.DATABASE_URL || process.env.NEON_DATABASE_URL;

  if (!databaseUrl) {
    // Return sample mock data if Neon DB is not yet hooked up
    return NextResponse.json({
      status: "mock",
      source: "demo_mode",
      data: [
        {
          event_date: "2026-11-08",
          airport_code: "JFK",
          airport_name: "New York JFK",
          total_unique_aircraft: 142,
          inbound_approaches: 38,
          outbound_departures: 45,
          airborne_holding: 12,
          ground_movements: 47,
          avg_speed_knots: 182.4,
          avg_temp_c: 12.5,
          max_wind_gust_kmh: 48.2,
          total_precipitation_mm: 6.8,
          adverse_weather_events: 3,
          congestion_index: 6.2,
          updated_at: new Date().toISOString(),
        },
        {
          event_date: "2026-11-08",
          airport_code: "ORD",
          airport_name: "Chicago O'Hare",
          total_unique_aircraft: 168,
          inbound_approaches: 52,
          outbound_departures: 61,
          airborne_holding: 18,
          ground_movements: 37,
          avg_speed_knots: 195.1,
          avg_temp_c: 8.1,
          max_wind_gust_kmh: 55.4,
          total_precipitation_mm: 14.2,
          adverse_weather_events: 5,
          congestion_index: 8.8,
          updated_at: new Date().toISOString(),
        },
        {
          event_date: "2026-11-08",
          airport_code: "LHR",
          airport_name: "London Heathrow",
          total_unique_aircraft: 129,
          inbound_approaches: 34,
          outbound_departures: 39,
          airborne_holding: 9,
          ground_movements: 47,
          avg_speed_knots: 175.0,
          avg_temp_c: 11.0,
          max_wind_gust_kmh: 22.0,
          total_precipitation_mm: 0.2,
          adverse_weather_events: 0,
          congestion_index: 5.2,
          updated_at: new Date().toISOString(),
        },
        {
          event_date: "2026-11-08",
          airport_code: "SIN",
          airport_name: "Singapore Changi",
          total_unique_aircraft: 115,
          inbound_approaches: 28,
          outbound_departures: 31,
          airborne_holding: 4,
          ground_movements: 52,
          avg_speed_knots: 168.9,
          avg_temp_c: 29.3,
          max_wind_gust_kmh: 18.5,
          total_precipitation_mm: 1.5,
          adverse_weather_events: 1,
          congestion_index: 3.6,
          updated_at: new Date().toISOString(),
        },
        {
          event_date: "2026-11-08",
          airport_code: "HND",
          airport_name: "Tokyo Haneda",
          total_unique_aircraft: 98,
          inbound_approaches: 22,
          outbound_departures: 26,
          airborne_holding: 3,
          ground_movements: 47,
          avg_speed_knots: 172.3,
          avg_temp_c: 16.4,
          max_wind_gust_kmh: 15.0,
          total_precipitation_mm: 0.0,
          adverse_weather_events: 0,
          congestion_index: 2.8,
          updated_at: new Date().toISOString(),
        }
      ]
    });
  }

  try {
    const sql = neon(databaseUrl);
    const rows = await sql`
      SELECT 
        event_date,
        airport_code,
        airport_name,
        total_unique_aircraft,
        inbound_approaches,
        outbound_departures,
        airborne_holding,
        ground_movements,
        avg_speed_knots,
        avg_temp_c,
        max_wind_gust_kmh,
        total_precipitation_mm,
        adverse_weather_events,
        congestion_index,
        updated_at
      FROM airport_congestion_weather_mart
      ORDER BY event_date DESC, congestion_index DESC
      LIMIT 100;
    `;

    return NextResponse.json({
      status: "live",
      source: "neon_postgres",
      data: rows
    });
  } catch (error: any) {
    return NextResponse.json(
      { error: "Failed to fetch from Neon Postgres", details: error.message },
      { status: 500 }
    );
  }
}
