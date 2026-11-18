# Stop the WHOLE project with one command:  .\stop.ps1
#   default : stops and removes containers, KEEPS data (Kafka, MinIO, Postgres volumes)
#   -Purge  : also deletes all volumes (lake, models, metadata) - asks for confirmation unless -Yes
param(
    [switch]$Purge,
    [switch]$Yes
)
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Docker is not running - nothing to stop." -ForegroundColor Yellow; exit 0 }

if ($Purge -and -not $Yes) {
    $a = Read-Host "This DELETES all data (Kafka, MinIO lake, MLflow models, Airflow/MLflow DB). Type YES to continue"
    if ($a -ne 'YES') { Write-Host "Cancelled."; exit 0 }
}

Write-Host "==> Removing one-off Spark job containers started by Airflow" -ForegroundColor Cyan
$jobs = docker ps -aq --filter "name=mlops_job_"
if ($jobs) { docker rm -f $jobs | Out-Null }

Write-Host "==> Stopping all services" -ForegroundColor Cyan
if ($Purge) { docker compose down -v --remove-orphans } else { docker compose down --remove-orphans }

Write-Host ""
if ($Purge) { Write-Host "Project stopped and all data deleted." -ForegroundColor Green }
else { Write-Host "Project stopped. Data kept; start again with .\start.ps1 (streaming resumes from its checkpoints)." -ForegroundColor Green }
