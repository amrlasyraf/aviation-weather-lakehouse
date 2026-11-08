# Aviation Disruptions & Weather Impact Lakehouse

[![Live Demo](https://img.shields.io/badge/Live%20Demo-Vercel-black?style=for-the-badge&logo=vercel)](https://aviation-weather-lakehouse.vercel.app)
[![Engine](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Storage](https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD8?style=for-the-badge&logo=apache)](https://delta.io)
[![Database](https://img.shields.io/badge/Neon-Serverless%20Postgres-00E599?style=for-the-badge&logo=postgresql)](https://neon.tech)

An end-to-end Data Engineering pipeline implementing **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks Delta Lake (Unity Catalog)**, synced via idempotent UPSERTs over HTTP to **Neon Serverless Postgres**, and served on a **Vercel Edge Dashboard**.

🌐 **Live Dashboard:** [aviation-weather-lakehouse.vercel.app](https://aviation-weather-lakehouse.vercel.app)

---

## 🏛️ Architecture Overview

```text
[OpenSky Network API] (Live Flight States)    [Open-Meteo API] (Airport Weather)
                 \                                     /
                  \                                   /
       ┌─────────────────────────────────────────────────────────┐
       │             Databricks Lakehouse (PySpark)              │
       │                                                         │
       │  1. Bronze (Raw):   bronze_flight_states                │
       │                     bronze_airport_weather              │
       │                                                         │
       │  2. Silver (Clean): silver_flights_near_airports        │
       │                     (Haversine proximity <120km)        │
       │                     silver_airport_weather              │
       │                                                         │
       │  3. Gold (Mart):    gold_airport_congestion_weather_mart│
       │                     (Congestion Index & Weather KPIs)   │
       └────────────────────────────┬────────────────────────────┘
                                    │
                     HTTP SQL Proxy / Idempotent UPSERT
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │  Neon Serverless Postgres   │
                     │  (Gold Analytical Serving)  │
                     └──────────────┬──────────────┘
                                    │
                             SQL over HTTPS
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │       Vercel Dashboard      │
                     │ (Next.js 14 + Tailwind CSS) │
                     └─────────────────────────────┘
```

---

## 📂 Repository Structure

```text
ETL1/
├── databricks/
│   ├── 01_bronze_ingestion.py      # Extracts OpenSky & Open-Meteo APIs into Bronze Delta tables
│   ├── 02_silver_transform.py      # Deduplication, Haversine spatial join (<120km terminal airspace)
│   ├── 03_gold_aggregation.py      # Computes Congestion Index and adverse weather KPI mart
│   └── 04_gold_to_neon.py          # Idempotent batch UPSERT to Neon Postgres via HTTP proxy
├── sql/
│   └── neon_schema.sql             # Gold schema DDL and indexes for Neon Postgres
├── web/                            # Next.js 14 Dashboard deployed to Vercel
├── .env.example                    # Environment variable template
├── .gitignore                      # Safe ignore rules (prevents credential leaks)
└── README.md
```

---

## 💡 Key Data Engineering Highlights

- **Unity Catalog & Lakehouse Architecture:** Raw streaming/batch snapshots are preserved in append-only Bronze Delta tables, cleansed and enriched with geospatial dimensions in Silver, and aggregated into business-ready KPIs in Gold.
- **Geospatial Processing at Scale:** Employs the **Haversine formula** directly in PySpark to calculate real-time proximity between high-frequency aircraft coordinates and international airport terminal airspace boundaries (<120km radius).
- **Idempotent Data Sync:** Sync to Neon Postgres uses parameterized `INSERT INTO ... ON CONFLICT (event_date, airport_code) DO UPDATE`, guaranteeing idempotency and preventing duplicate rows during pipeline retries or backfilling.
- **Zero-Dependency Serverless Ingestion:** The sync stage utilizes Neon's native Serverless HTTP SQL API using pure Python (`requests`), avoiding external C-driver compile issues and minimizing cluster overhead.

---

## 🚀 Setup & Deployment

### 1. Database (Neon Postgres)
1. Create a serverless project on [neon.tech](https://neon.tech).
2. Execute [`sql/neon_schema.sql`](sql/neon_schema.sql) in the Neon SQL Editor.

### 2. Databricks Workflow
1. Import `databricks/` notebooks into your workspace under `/Users/<email>/aviation-weather-lakehouse`.
2. Create a secret scope and store your Neon database connection string:
   ```bash
   databricks secrets create-scope aviation_scope
   databricks secrets put-secret aviation_scope neon_db_url --string-value "<NEON_CONNECTION_STRING>"
   ```
3. Create a multi-task workflow orchestrating `01` → `02` → `03` → `04` on a recurring schedule.

### 3. Vercel Dashboard
1. Import repository on [vercel.com](https://vercel.com) with Root Directory set to `web`.
2. Add environment variable:
   * `DATABASE_URL` = your Neon connection string.
3. Deploy.
