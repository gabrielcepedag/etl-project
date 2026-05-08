from dagster import job
from analytics.ops.earthquake import extract_earthquakes, transform_earthquakes, load_earthquakes


@job
def earthquakes_etl():
    """
    Full extract-transform-load pipeline for USGS earthquake data.

    Execution order:
      1. extract_earthquakes  - pulls raw GeoJSON from the USGS API
      2. transform_earthquakes - flattens nested fields into tabular rows
      3. load_earthquakes      - writes the result to raw_earthquakes in Postgres
    """
    load_earthquakes(transform_earthquakes(extract_earthquakes()))
