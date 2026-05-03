from dagster import ScheduleDefinition
from analytics.jobs import earthquakes_etl

# Triggers the earthquakes_etl job every day at 6:00 AM.
# The cron expression "0 6 * * *" means: minute 0, hour 6, any day/month/weekday.
earthquakes_schedule = ScheduleDefinition(job=earthquakes_etl, cron_schedule="0 6 * * *")
