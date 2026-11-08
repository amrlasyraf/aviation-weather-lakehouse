import { NextResponse } from "next/server";
import { neon } from "@neondatabase/serverless";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  const databaseUrl = process.env.DATABASE_URL || process.env.NEON_DATABASE_URL;

  if (!databaseUrl) {
    return NextResponse.json({
      status: "mock",
      source: "demo_mode",
      data: []
    });
  }

  try {
    const sql = neon(databaseUrl);
    const rows = await sql(
      `SELECT 
        TO_CHAR(event_date, 'YYYY-MM-DD') AS event_date,
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
        TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at
      FROM airport_congestion_weather_mart
      ORDER BY event_date DESC, congestion_index DESC
      LIMIT 100`
    );

    return NextResponse.json({
      status: "live",
      source: "neon_postgres",
      data: rows
    }, {
      headers: {
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0"
      }
    });
  } catch (error: any) {
    return NextResponse.json(
      { error: "Failed to fetch from Neon Postgres", details: error.message },
      { status: 500 }
    );
  }
}
