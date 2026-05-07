from dagster_airbyte import AirbyteCloudWorkspace, build_airbyte_assets_definitions, DagsterAirbyteTranslator, AirbyteConnectionTableProps
import dagster as dg
from dagster import AssetSpec, AutomationCondition, AssetKey

class CustomDagsterAirbyrteTranslator(DagsterAirbyteTranslator):
    def get_asset_spec(self, props: AirbyteConnectionTableProps) -> AssetSpec:
        default_spec = super().get_asset_spec(props)
        return default_spec.replace_attributes(
            group_name="airbyte_assets",
            automation_condition=AutomationCondition.on_cron("* * * * *")
        )

airbyte_workspace = AirbyteCloudWorkspace(
    workspace_id=dg.EnvVar("AIRBYTE_CLOUD_WORKSPACE_ID"),
    client_id=dg.EnvVar("AIRBYTE_CLOUD_CLIENT_ID"),
    client_secret=dg.EnvVar("AIRBYTE_CLOUD_CLIENT_SECRET"),
)

earthquakes_airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_workspace,
    dagster_airbyte_translator=CustomDagsterAirbyrteTranslator(),
    connection_selector_fn=lambda connection: connection.name in ["Postgres RAW → DWH Raw"]
)