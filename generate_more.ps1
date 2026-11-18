# Simulate NEW credit applications arriving: publishes -MB more megabytes of events to Kafka, then stops.
# The stream-bronze service ingests them; the next Airflow DAG run sees the new data and retrains
# (check_new_data) if gold grew by at least MIN_NEW_DATA_RATIO.
#   .\generate_more.ps1 -MB 10
param([double]$MB = 10)
Set-Location -Path $PSScriptRoot
docker compose run --rm --no-deps -e EXTRA_DATA_MB=$MB generator
