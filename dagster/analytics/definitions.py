from dagster import Definitions
from analytics.jobs import earthquakes_etl
from analytics.schedules import earthquakes_schedule

# The Definitions object is the entry point for Dagster.
# Everything registered here (jobs, schedules, sensors, assets) becomes
# visible and executable in the Dagster UI.
defs = Definitions(
    jobs=[earthquakes_etl],
    schedules=[earthquakes_schedule],
)
