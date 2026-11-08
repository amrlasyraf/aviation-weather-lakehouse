# Aviation Disruptions & Weather Impact Lakehouse

An end-to-end Data Engineering pipeline implementing **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks Delta Lake**, synced via idempotent UPSERTs to **Neon Serverless Postgres**, and served on a **Vercel Edge Dashboard**.

---

## 🏛️ Architecture Overview

```text
[OpenSky API] (Live Flight States)    [Open-Meteo API] (Airport Forecasts)
               \                               /
                \                             /
        ┌──────────────────────────────────────────────┐
        │        Databricks Lakehouse (PySpark)        │
        │                                              │
        │  1. Bronze: Raw append-only Delta tables     │
        │  2. Silver: Spatial Haversine join (<120km), │
        │             deduplicated on (icao24, time)   │
        │  3. Gold:   Aggregated Congestion Index &    │
        │             weather impact analytical mart   │
        └──────────────────────┬───────────────────────┘
                               │
                Idempotent UPSERT (psycopg2)
                               │
                               ▼
               ┌───────────────────────────────┐
               │    Neon Serverless Postgres   │
               │  (Gold analytical serving)    │
               └───────────────┬───────────────┘
                               │
                       SQL over HTTPS
                               │
                               ▼
               ┌───────────────────────────────┐
               │       Vercel Dashboard        │
               │  (Next.js 14 + Tailwind CSS)  │
               └───────────────────────────────┘
```

---

## 📂 Repository Structure

```text
ETL1/
├── databricks/
│   ├── 01_bronze_ingestion.py      # Extracts OpenSky & Open-Meteo APIs to Bronze Delta
│   ├── 02_silver_transform.py      # Deduplication, Haversine spatial join (<120km)
│   ├── 03_gold_aggregation.py      # Congestion Index and adverse weather KPI mart
│   └── 04_gold_to_neon.py          # Idempotent batch UPSERT to Neon Postgres
├── sql/
│   └── neon_schema.sql             # Gold schema DDL and indexes for Neon
├── web/                            # Next.js 14 Dashboard ready for Vercel
├── .env.example                    # Environment variable template
└── README.md
```

---

## 🚀 Quickstart Guide

### 1. Database Setup (Neon Postgres)
1. Create a free serverless Postgres database at [neon.tech](https://neon.tech).
2. Open the Neon SQL Editor and execute the DDL in `sql/neon_schema.sql`.
3. Copy your database connection string:
   ```text
   postgresql://<user>:<password>@<ep-id>.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```

### 2. Databricks Ingestion & Processing
1. Import the files in `databricks/` into your Databricks workspace.
2. In Databricks, install the `psycopg2-binary` library on your cluster (or run `%pip install psycopg2-binary` at top of notebook 04).
3. Set your Neon connection string in Databricks secrets or environment variable:
   ```python
   %env NEON_DATABASE_URL=postgresql://<user>:<password>@<ep-id>.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
4. Schedule the pipeline:
   - Create a multi-task Databricks Workflow or schedule `01_bronze_ingestion` → `02_silver_transform` → `03_gold_aggregation` → `04_gold_to_neon` daily or hourly.

### 3. Vercel Dashboard Deployment
1. Navigate to the `web/` folder.
2. Push this repo to GitHub and import it on [vercel.com](https://vercel.com).
3. Set the Root Directory to `web`.
4. Add the Environment Variable in Vercel:
   - `DATABASE_URL` = your Neon connection string.
5. Deploy. The dashboard will query live Gold metrics directly from Neon.

---

## 💡 Key Data Engineering Highlights

- **Idempotency:** Silver and Gold layers utilize partition overwrites and deterministic deduplication. The sync to Neon uses `INSERT INTO ... ON CONFLICT (event_date, airport_code) DO UPDATE`, allowing safe pipeline re-runs without data duplication.
- **Geospatial Processing:** Silver transformation uses the **Haversine formula** in distributed PySpark to calculate real-time proximity between live aircraft positions and major international airport coordinates.
- **Medallion Lakehouse Separation:** Raw API dumps are preserved in Bronze for auditability, enriched in Silver, and condensed in Gold, maintaining minimal storage footprint on the serverless serving database.
