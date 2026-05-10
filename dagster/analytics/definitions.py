from dagster import Definitions, EnvVar
from analytics.jobs.jobs import earthquakes_etl
from analytics.schedules.schedules import earthquakes_schedule
from analytics.assets.airbyte import earthquakes_airbyte_assets, airbyte_workspace
from analytics.assets.dbt import dbt_warehouse_resource, dbt_warehouse
from analytics.resources.postgresql import PostgresqlDatabaseResource
from dotenv import load_dotenv
import os

load_dotenv()

defs = Definitions(
    assets=[*earthquakes_airbyte_assets, dbt_warehouse],
    jobs=[earthquakes_etl],
    schedules=[earthquakes_schedule],
    resources={
        "airbyte": airbyte_workspace,
        "dbt_warehouse_resource": dbt_warehouse_resource,
         "postgres_conn": PostgresqlDatabaseResource(
            host_name=os.environ["DB_HOST"],
            database_name=os.environ["DB_NAME"],
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            port=os.environ["DB_PORT"],
        )
    }
)