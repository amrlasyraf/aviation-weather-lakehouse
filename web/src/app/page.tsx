"use client";

import { useEffect, useState } from "react";
import { Plane, CloudRain, Wind, Compass, Database, RefreshCw, AlertTriangle, Layers, CheckCircle2 } from "lucide-react";

interface AirportMetric {
  event_date: string;
  airport_code: string;
  airport_name: string;
  total_unique_aircraft: number;
  inbound_approaches: number;
  outbound_departures: number;
  airborne_holding: number;
  ground_movements: number;
  avg_speed_knots: number;
  avg_temp_c: number;
  max_wind_gust_kmh: number;
  total_precipitation_mm: number;
  adverse_weather_events: number;
  congestion_index: number;
  updated_at: string;
}

export default function Dashboard() {
  const [metrics, setMetrics] = useState<AirportMetric[]>([]);
  const [sourceStatus, setSourceStatus] = useState<string>("loading");
  const [loading, setLoading] = useState<boolean>(true);

  const fetchMetrics = async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/metrics");
      const json = await res.json();
      if (json.data) {
        setMetrics(json.data);
        setSourceStatus(json.status === "live" ? "Live Neon Postgres" : "Mock (Set DATABASE_URL in Vercel)");
      }
    } catch (err) {
      console.error("Error fetching metrics:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
  }, []);

  const totalAircraft = metrics.reduce((acc, curr) => acc + (Number(curr.total_unique_aircraft) || 0), 0);
  const totalHolding = metrics.reduce((acc, curr) => acc + (Number(curr.airborne_holding) || 0), 0);
  const maxGust = metrics.length ? Math.max(...metrics.map(m => Number(m.max_wind_gust_kmh) || 0)) : 0;
  const adverseAirports = metrics.filter(m => Number(m.adverse_weather_events) > 0).length;

  return (
    <main className="min-h-screen p-6 md:p-12 max-w-7xl mx-auto space-y-8">
      {/* Top Banner & Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 border-b border-slate-800 pb-6">
        <div>
          <div className="flex items-center gap-3">
            <Plane className="w-8 h-8 text-sky-400" />
            <h1 className="text-2xl md:text-3xl font-bold tracking-tight text-white">
              Aviation & Weather Observability Lakehouse
            </h1>
          </div>
          <p className="text-sm text-slate-400 mt-1">
            Databricks Medallion ETL (Bronze → Silver → Gold) • Neon Postgres Serving • Vercel Edge
          </p>
        </div>

        <div className="flex items-center gap-3">
          <span className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium bg-slate-900 border border-slate-700 text-slate-300">
            <span className={`w-2 h-2 rounded-full ${sourceStatus.includes("Live") ? "bg-emerald-400" : "bg-amber-400"}`} />
            {sourceStatus}
          </span>
          <button
            onClick={fetchMetrics}
            disabled={loading}
            className="flex items-center gap-2 px-3.5 py-1.5 bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white rounded-lg text-xs font-semibold transition"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-slate-400 text-xs uppercase font-medium">
            <span>Aircraft Tracked</span>
            <Plane className="w-4 h-4 text-sky-400" />
          </div>
          <div className="text-3xl font-bold text-white">{totalAircraft}</div>
          <p className="text-xs text-slate-500">Across monitored terminal airspaces</p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-slate-400 text-xs uppercase font-medium">
            <span>Airborne Holding</span>
            <Compass className="w-4 h-4 text-amber-400" />
          </div>
          <div className="text-3xl font-bold text-amber-300">{totalHolding}</div>
          <p className="text-xs text-slate-500">Aircraft in delay or holding patterns</p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-slate-400 text-xs uppercase font-medium">
            <span>Peak Wind Gust</span>
            <Wind className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="text-3xl font-bold text-emerald-300">{maxGust} <span className="text-sm font-normal text-slate-400">km/h</span></div>
          <p className="text-xs text-slate-500">Maximum recorded across hubs</p>
        </div>

        <div className="p-5 bg-slate-900/80 border border-slate-800 rounded-xl space-y-2">
          <div className="flex items-center justify-between text-slate-400 text-xs uppercase font-medium">
            <span>Adverse Weather Zones</span>
            <AlertTriangle className="w-4 h-4 text-rose-400" />
          </div>
          <div className="text-3xl font-bold text-rose-300">{adverseAirports} <span className="text-sm font-normal text-slate-400">Airports</span></div>
          <p className="text-xs text-slate-500">Exceeding wind/rain thresholds</p>
        </div>
      </div>

      {/* Architecture Lineage Pipeline */}
      <div className="p-5 bg-slate-900/60 border border-slate-800 rounded-xl space-y-3">
        <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-wider text-slate-400">
          <Layers className="w-4 h-4 text-indigo-400" />
          Data Lineage & Pipeline Stages
        </div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-xs">
          <div className="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg">
            <span className="text-amber-400 font-bold block mb-1">1. Bronze (Raw)</span>
            <p className="text-slate-400">OpenSky Network live state vectors + Open-Meteo forecast JSON stored in append-only Delta Lake.</p>
          </div>
          <div className="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg">
            <span className="text-slate-300 font-bold block mb-1">2. Silver (Cleaned)</span>
            <p className="text-slate-400">Deduplicated on (icao24, timestamp). Haversine spatial join matches aircraft within 120km airspace.</p>
          </div>
          <div className="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg">
            <span className="text-yellow-400 font-bold block mb-1">3. Gold (Mart)</span>
            <p className="text-slate-400">Computes Congestion Index and aggregates inbound/outbound/holding counts against adverse weather.</p>
          </div>
          <div className="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg">
            <span className="text-emerald-400 font-bold block mb-1">4. Serving (Neon + Vercel)</span>
            <p className="text-slate-400">Idempotent UPSERT to Neon Serverless Postgres. Cached Edge API renders to this Vercel UI.</p>
          </div>
        </div>
      </div>

      {/* Main Table: Airport Metrics */}
      <div className="border border-slate-800 rounded-xl overflow-hidden bg-slate-900/70 shadow-xl">
        <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between">
          <h2 className="text-base font-semibold text-white">Airport Congestion & Weather Correlation Mart</h2>
          <span className="text-xs text-slate-400">Source: Gold Table (airport_congestion_weather_mart)</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm text-slate-300">
            <thead className="bg-slate-950/70 text-xs uppercase tracking-wider text-slate-400 border-b border-slate-800">
              <tr>
                <th className="px-6 py-3.5">Airport</th>
                <th className="px-4 py-3.5">Congestion Index</th>
                <th className="px-4 py-3.5">Traffic (App / Dep / Hold / Gnd)</th>
                <th className="px-4 py-3.5">Temp</th>
                <th className="px-4 py-3.5">Wind Gust</th>
                <th className="px-4 py-3.5">Precipitation</th>
                <th className="px-6 py-3.5">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {metrics.map((row) => {
                const cong = Number(row.congestion_index) || 0;
                const isHighCongestion = cong >= 7.0;
                const isMedCongestion = cong >= 4.0 && cong < 7.0;

                return (
                  <tr key={row.airport_code} className="hover:bg-slate-800/40 transition">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="font-bold text-white">{row.airport_code}</div>
                      <div className="text-xs text-slate-400">{row.airport_name}</div>
                    </td>

                    <td className="px-4 py-4 whitespace-nowrap">
                      <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-semibold ${
                        isHighCongestion
                          ? "bg-rose-500/20 text-rose-300 border border-rose-500/30"
                          : isMedCongestion
                          ? "bg-amber-500/20 text-amber-300 border border-amber-500/30"
                          : "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30"
                      }`}>
                        {cong.toFixed(1)} / 10
                      </span>
                    </td>

                    <td className="px-4 py-4 whitespace-nowrap text-xs font-mono">
                      <span className="text-sky-300">{row.inbound_approaches}</span> / {" "}
                      <span className="text-indigo-300">{row.outbound_departures}</span> / {" "}
                      <span className="text-amber-300">{row.airborne_holding}</span> / {" "}
                      <span className="text-slate-400">{row.ground_movements}</span>
                    </td>

                    <td className="px-4 py-4 whitespace-nowrap">
                      {row.avg_temp_c != null ? `${row.avg_temp_c}°C` : "—"}
                    </td>

                    <td className="px-4 py-4 whitespace-nowrap">
                      {row.max_wind_gust_kmh != null ? `${row.max_wind_gust_kmh} km/h` : "—"}
                    </td>

                    <td className="px-4 py-4 whitespace-nowrap">
                      {row.total_precipitation_mm != null ? `${row.total_precipitation_mm} mm` : "—"}
                    </td>

                    <td className="px-6 py-4 whitespace-nowrap">
                      {Number(row.adverse_weather_events) > 0 ? (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-rose-950/80 text-rose-300 border border-rose-800">
                          <AlertTriangle className="w-3 h-3" /> Adverse Weather
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-emerald-950/80 text-emerald-300 border border-emerald-800">
                          <CheckCircle2 className="w-3 h-3" /> Nominal
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
