# Earthquake Risk Data Platform

A data engineering platform that enables a multinational construction firm to evaluate seismic risk at future building locations. The platform ingests live earthquake data from a public API, transforms it into a dimensional model in a cloud data warehouse, and exposes it through a risk dashboard for project leaders.

## Business Context

Project leaders need answers to questions such as:

- Which regions are most frequently hit by earthquakes?
- How intense are the earthquakes near a proposed construction site?
- What is the risk level for a given location over time?

## Architecture

```
USGS Earthquake API  (live data, updated continuously)
        │
        ▼
 Dagster (daily ETL job @ 06:00 UTC)
   • extract_earthquakes  — calls USGS REST API, fetches up to 1 000 events
   • transform_earthquakes — flattens GeoJSON into tabular rows
   • load_earthquakes     — upserts into PostgreSQL raw staging table
        │
        ▼
PostgreSQL RDS (AWS)   raw_earthquakes staging table
        │
        ▼
Airbyte Cloud          "Postgres RAW → Snowflake DWH" sync
        │
        ▼
Snowflake (DWH)
  RAW.PUBLIC.raw_earthquakes          ← Airbyte lands data here
        │
        ▼
dbt (warehouse project)
  STAGING.EARTHQUAKES.stg_earthquakes ← cleans and casts raw data
        │
        ├── MARTS.EARTHQUAKES.dim_date
        ├── MARTS.EARTHQUAKES.dim_location
        ├── MARTS.EARTHQUAKES.dim_source
        ├── MARTS.EARTHQUAKES.fact_earthquakes
        └── MARTS.EARTHQUAKES.fct_earthquake_daily_stats
        │
        ▼
Dashboard (connected to Snowflake MARTS schema)
```

![Architecture diagram](project_2_architecture.png)

## Repository Structure

```
project-2/
├── README.md                        ← you are here
├── project-2-plan.md                ← project objectives and business context
├── project_2_architecture.png       ← architecture diagram
└── etl-project/
    ├── .env.template                ← environment variable template
    ├── dagster/                     ← orchestration layer (Dagster)
    │   ├── README.md
    │   └── analytics/
    │       ├── ops/                 ← extract, transform, load operations
    │       ├── jobs/                ← earthquakes_etl job
    │       ├── schedules/           ← daily 06:00 UTC schedule
    │       ├── assets/              ← Airbyte and dbt assets
    │       ├── resources/           ← PostgreSQL resource
    │       └── definitions.py       ← Dagster entry point
    └── dbt/                         ← transformation layer (dbt + Snowflake)
        ├── dbt_project.yml
        ├── profiles.yml
        └── models/
            ├── sources/             ← source definition for raw_earthquakes
            ├── staging/earthquakes/ ← stg_earthquakes (cleansing and casting)
            └── marts/earthquakes/   ← dimensional model and aggregated facts
```

## Data Models

### Staging

| Model             | Description                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------- |
| `stg_earthquakes` | Cleans raw GeoJSON data: converts epoch timestamps, parses coordinate arrays, normalises column names |

### Marts — Dimensions

| Model          | Grain                                                       | Key                       |
| -------------- | ----------------------------------------------------------- | ------------------------- |
| `dim_date`     | One row per calendar day                                    | `date_day`                |
| `dim_location` | One row per unique (latitude, longitude, depth) combination | `location_id` (surrogate) |
| `dim_source`   | One row per unique (network, event code) combination        | `source_id` (surrogate)   |

### Marts — Facts

| Model                        | Grain                             | Key measures                                                            |
| ---------------------------- | --------------------------------- | ----------------------------------------------------------------------- |
| `fact_earthquakes`           | One row per earthquake event      | magnitude, significance, tsunami flag, risk_index                       |
| `fct_earthquake_daily_stats` | One row per (date, location) pair | event count, avg/max/min magnitude, activity score, daily location rank |

### Transformation techniques used

| Technique         | Where applied                                                                                                                   |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Data type casting | `stg_earthquakes` — epoch → timestamp, JSON string → float coordinates                                                          |
| Renaming          | All models — descriptive column aliases                                                                                         |
| Joins             | `fact_earthquakes`, `fct_earthquake_daily_stats` — LEFT JOIN to dimension tables                                                |
| Filtering         | `fact_earthquakes` (WHERE mag/id not null), `fct_earthquake_daily_stats` (WHERE mag/event_time not null)                        |
| Grouping          | `fct_earthquake_daily_stats` — GROUP BY date, location                                                                          |
| Aggregation       | `fct_earthquake_daily_stats` — COUNT, AVG, MAX, MIN, SUM                                                                        |
| Window function   | `fct_earthquake_daily_stats` — RANK() OVER (PARTITION BY event_date)                                                            |
| Calculation       | `fact_earthquakes` — risk_index = mag × sig / 1000; `fct_earthquake_daily_stats` — activity_score = avg_magnitude × event_count |
| Sorting           | `fct_earthquake_daily_stats` — ORDER BY event_date DESC, activity_score DESC                                                    |

## Prerequisites

- Python 3.11+
- conda or a Python virtual environment manager
- Access credentials for: PostgreSQL RDS, Snowflake, Airbyte Cloud (see `.env.template`)

## Installation

### 1. Clone the repository

```bash
git clone <repo-url>
cd project-2/etl-project
```

### 2. Create environment file

```bash
cp .env.template .env
# Fill in credentials for Postgres, Snowflake, and Airbyte
```

### 3. Set up the Dagster environment

```bash
conda create -n dagster python=3.11 -y
conda activate dagster
cd dagster
pip install -e ".[dev]"
```

### 4. Set up the dbt environment

```bash
conda create -n dbt python=3.11 -y
conda activate dbt
pip install dbt-snowflake dbt-utils
cd dbt
dbt deps   # install dbt packages
```

## Running the Pipeline

### Load environment variables through CMD

### Start Dagster (orchestration UI)

```bash
cd dagster
dagster dev
```

Open `http://127.0.0.1:3000`, navigate to **Jobs → earthquakes_etl**, and click **Materialize** to trigger a manual run, or wait for the daily schedule at 06:00 UTC.

### Run dbt transformations manually

```bash
conda activate dbt
cd dbt
dbt run        # build all models
dbt test       # run all data quality tests
dbt docs generate && dbt docs serve   # browse lineage in browser
```

## Data Quality Tests

Tests are defined in `.yml` files alongside each model. Current coverage:

| Model                        | Tests                                                                                                                                                               |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `raw_earthquakes` (source)   | `id`: not_null, unique                                                                                                                                              |
| `dim_date`                   | `date_day`: not_null, unique                                                                                                                                        |
| `dim_location`               | `location_id`: not_null, unique                                                                                                                                     |
| `dim_source`                 | `source_id`: not_null, unique                                                                                                                                       |
| `fact_earthquakes`           | `id`: not_null, unique; `magnitude`, `significance`, `risk_index`, `event_time`: not_null; `location_id` and `source_id`: referential integrity to dimension tables |
| `fct_earthquake_daily_stats` | `event_date`, `latitude`, `longitude`, `event_count`, `avg_magnitude`, `activity_score`: not_null                                                                   |

## Scheduling

The Dagster schedule `earthquakes_schedule` runs the full ETL pipeline daily at **06:00 UTC** using the cron expression `0 6 * * *`. Airbyte then syncs the new rows from PostgreSQL to Snowflake, after which the dbt asset materialises automatically via Dagster's asset automation.

## Cloud Services

| Service       | Provider           | Purpose                                   |
| ------------- | ------------------ | ----------------------------------------- |
| PostgreSQL    | AWS RDS            | Raw landing zone                          |
| Airbyte Cloud | Airbyte            | Postgres → Snowflake sync                 |
| Snowflake     | Snowflake Cloud    | Data warehouse and transformation runtime |
| Dagster Cloud | Dagster            | Orchestration, scheduling, monitoring     |
| Dashboard     | Preset / Snowflake | End-user risk visualisation               |
