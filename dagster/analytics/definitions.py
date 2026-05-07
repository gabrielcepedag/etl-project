from dagster import Definitions
from analytics.jobs import earthquakes_etl
from analytics.schedules import earthquakes_schedule
from analytics.assets.airbyte import earthquakes_airbyte_assets, airbyte_workspace

defs = Definitions(
    assets=[*earthquakes_airbyte_assets],
    jobs=[earthquakes_etl],
    schedules=[earthquakes_schedule],
    resources={
        "airbyte": airbyte_workspace
    }
)