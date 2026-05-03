# Dagster ETL Pipeline

This module handles the extraction and loading of raw seismic data using Dagster as the orchestration layer. It is the first stage in a broader data pipeline that moves data from a public API into a data warehouse for analysis.

## Architecture overview

```
USGS Earthquake API
        |
        v
  Dagster (ETL)          <-- this module
        |
        v
  PostgreSQL (RDS)       Raw staging layer
        |
        v
   Airbyte               Moves data from Postgres to Snowflake
        |
        v
  Snowflake (DWH)        Data warehouse
        |
        v
     dbt                 Transforms raw data into a dimensional model
```

## Data source

We use the **USGS Earthquake Hazards Program API**, a free and public REST API that provides real-time and historical earthquake data in GeoJSON format.

- Endpoint: `https://earthquake.usgs.gov/fdsnws/event/1/query`
- No API key required
- We pull up to 1000 events per run, filtered by magnitude >= 1.0, ordered by most recent

## Project structure

```
dagster/
  analytics/
    __init__.py       Package init
    ops.py            Individual units of work (extract, transform, load)
    jobs.py           Wires ops together into a complete pipeline
    schedules.py      Defines when jobs run automatically
    definitions.py    Entry point that registers everything with Dagster
  setup.py            Package dependencies
```

## The ETL pipeline

The pipeline is made up of three ops that run sequentially:

**1. extract_earthquakes**
Calls the USGS API and returns a list of raw GeoJSON features. Each feature represents one seismic event and contains a `properties` block with measurements and a `geometry` block with coordinates.

**2. transform_earthquakes**
Flattens the nested GeoJSON structure into a list of flat dictionaries, one per event. Field names are normalized to match the target schema in Postgres. Coordinates are serialized as a JSON string since they come as an array `[longitude, latitude, depth_km]`.

**3. load_earthquakes**
Creates the `raw_earthquakes` table in Postgres if it does not exist, truncates it, and bulk-inserts the transformed records using pandas `to_sql`. The table is truncated on every run so it always reflects the latest snapshot from the API.

## Raw table schema

The `raw_earthquakes` table in Postgres holds the data exactly as it comes from the API, with no transformations applied beyond flattening. This is the staging layer — dbt handles all further transformations downstream in Snowflake.

| Column | Type | Description |
|---|---|---|
| id | TEXT | Unique event identifier |
| mag | FLOAT | Magnitude |
| place | TEXT | Human-readable location |
| time | BIGINT | Origin time (ms since epoch) |
| updated | BIGINT | Last updated (ms since epoch) |
| tz | FLOAT | Timezone offset at event location |
| url | TEXT | USGS event page |
| detail | TEXT | URL to full GeoJSON detail |
| felt | FLOAT | Number of felt reports |
| cdi | FLOAT | Max reported intensity (DYFI) |
| mmi | FLOAT | Max instrumental intensity (ShakeMap) |
| alert | TEXT | PAGER alert level |
| status | TEXT | Review status (automatic / reviewed) |
| tsunami | INTEGER | 1 if tsunami message was issued |
| sig | INTEGER | Significance score (0-1000) |
| net | TEXT | Authoring network |
| code | TEXT | Network event code |
| ids | TEXT | All associated event IDs |
| sources | TEXT | Contributing networks |
| types | TEXT | Available product types |
| nst | FLOAT | Number of stations used |
| dmin | FLOAT | Distance to nearest station (degrees) |
| rms | FLOAT | RMS travel time residual (seconds) |
| gap | FLOAT | Largest azimuthal gap (degrees) |
| magtype | TEXT | Magnitude calculation method |
| type | TEXT | Event type (earthquake, quarry blast, etc.) |
| title | TEXT | Display title |
| coordinates | TEXT | [longitude, latitude, depth_km] as JSON |

## Schedule

The pipeline runs automatically every day at 6:00 AM via a Dagster schedule. It can also be triggered manually at any time from the Dagster UI.

## Setup

**Requirements:** Python 3.13, conda

```bash
conda create -n dagster python=3.13 -y
conda activate dagster
cd dagster
pip install -e ".[dev]"
```

Create a `.env` file at the project root using `.env.template` as reference and fill in the Postgres connection credentials.

**Load environment variables (PowerShell):**
```powershell
Get-Content ".env" | Where-Object { $_ -notmatch '^\s*#' -and $_ -match '=' } | ForEach-Object { $parts = $_ -split '=', 2; [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process') }
```

**Run Dagster:**
```bash
dagster dev
```

Open `http://127.0.0.1:3000` in the browser, go to Jobs, select `earthquakes_etl`, and launch a run from the Launchpad.
