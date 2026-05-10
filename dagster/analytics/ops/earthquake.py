import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone
from analytics.resources.postgresql import PostgresqlDatabaseResource
from sqlalchemy import create_engine, text
from dagster import op, get_dagster_logger
from dotenv import load_dotenv
from analytics.ops.common import upsert_to_database
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, create_engine, BigInteger, DateTime

load_dotenv()


def get_pg_engine():
    # Reads connection parameters from environment variables.
    # All values default to a local Postgres instance if not set.
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    db = os.getenv("DB_NAME", "postgres")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        connect_args={
            "sslmode": "verify-full",
            "sslrootcert": os.path.join(os.path.dirname(__file__), "..", "..", "global-bundle.pem"), # TODO: Esto hacerlo dinámico
        },
    )


@op
def extract_earthquakes():
    """
    Hits the USGS Earthquake Hazards API and pulls up to 1000 events
    with magnitude >= 1.0, ordered by most recent first.

    The API returns GeoJSON, so we grab the 'features' list which contains
    one entry per earthquake event.

    Docs: https://earthquake.usgs.gov/fdsnws/event/1/
    """
    logger = get_dagster_logger()
    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
        "?format=geojson&limit=1000&orderby=time&minmagnitude=1.0"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    features = response.json()["features"]
    logger.info(f"Extracted {len(features)} earthquake records")
    return features


@op
def transform_earthquakes(features):
    """
    Flattens the nested GeoJSON structure into a list of plain dictionaries.

    Each feature has two main sections we care about:
      - properties: all the seismic measurements and metadata
      - geometry.coordinates: [longitude, latitude, depth]

    We serialize coordinates as a JSON string so it can be stored as TEXT
    in Postgres without needing a separate column for each value.
    """
    rows = []
    for f in features:
        p = f["properties"]
        coords = f["geometry"]["coordinates"] if f.get("geometry") else []
        rows.append({
            "id":          f["id"],
            "mag":         p.get("mag"),        # Magnitude of the event
            "place":       p.get("place"),      # Human-readable location description
            "time":        p.get("time"),        # Origin time in milliseconds since epoch
            "updated":     p.get("updated"),    # Last update time in milliseconds since epoch
            "tz":          p.get("tz"),          # Timezone offset at the event location
            "url":         p.get("url"),         # USGS event page
            "detail":      p.get("detail"),     # URL to the full GeoJSON detail feed
            "felt":        p.get("felt"),        # Number of "felt" reports submitted
            "cdi":         p.get("cdi"),         # Maximum reported intensity (DYFI)
            "mmi":         p.get("mmi"),         # Maximum estimated instrumental intensity (ShakeMap)
            "alert":       p.get("alert"),      # PAGER alert level (green/yellow/orange/red)
            "status":      p.get("status"),     # Review status: automatic or reviewed
            "tsunami":     p.get("tsunami"),    # 1 if a tsunami message was issued, 0 otherwise
            "sig":         p.get("sig"),         # Significance score (0-1000)
            "net":         p.get("net"),         # Network that authored the preferred solution
            "code":        p.get("code"),       # Event code assigned by the network
            "ids":         p.get("ids"),         # Comma-separated list of event IDs
            "sources":     p.get("sources"),    # Comma-separated list of contributing networks
            "types":       p.get("types"),      # Comma-separated list of available product types
            "nst":         p.get("nst"),         # Number of seismic stations used
            "dmin":        p.get("dmin"),       # Horizontal distance to nearest station (degrees)
            "rms":         p.get("rms"),         # RMS travel time residual (seconds)
            "gap":         p.get("gap"),         # Largest azimuthal gap between stations (degrees)
            "magtype":     p.get("magType"),    # Method used to calculate magnitude (ml, mb, mw, etc.)
            "type":        p.get("type"),        # Type of seismic event (earthquake, quarry blast, etc.)
            "title":       p.get("title"),      # Full display title combining magnitude and place
            "coordinates": json.dumps(coords), # [longitude, latitude, depth_km] as JSON string
        })
    return rows


@op
def load_earthquakes(data, postgres_conn: PostgresqlDatabaseResource):

    metadata = MetaData()
    table = Table(
        "raw_earthquakes",
        metadata,
        Column("id", String, primary_key=True, nullable=False),
        Column("mag", Float),
        Column("place", String),
        Column("time", BigInteger),
        Column("updated", BigInteger),
        Column("tz", Float),
        Column("url", String),
        Column("detail", String),
        Column("felt", Float),
        Column("cdi", Float),
        Column("mmi", Float),
        Column("alert", String),
        Column("status", String),
        Column("tsunami", Integer),
        Column("sig", Integer),
        Column("net", String),
        Column("code", String),
        Column("ids", String),
        Column("sources", String),
        Column("types", String),
        Column("nst", Float),
        Column("dmin", Float),
        Column("rms", Float),
        Column("gap", Float),
        Column("magtype", String),
        Column("type", String),
        Column("title", String),
        Column("coordinates", String),
        Column("inserted_at", DateTime),
    )
    engine = postgres_conn.get_engine()
    upsert_to_database(
        engine=engine,
        data=data,
        table=table,
        metadata=metadata
    )