# Start the WHOLE project with one command:  .\start.ps1
#   -NoGenerator : start everything except the Kafka data generator
#   -Rebuild     : rebuild the custom images without cache (after changing Dockerfiles)
param(
    [switch]$NoGenerator,
    [switch]$Rebuild,
    [int]$TimeoutSec = 600
)
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot

function Step($m) { Write-Host "`n==> $m" -ForegroundColor Cyan }
function Fail($m) { Write-Host $m -ForegroundColor Red; exit 1 }

Step "Checking Docker"
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker is not running. Start Docker Desktop and retry." }
if (-not (Test-Path .env)) {
    if (Test-Path .env.example) { Copy-Item .env.example .env; Write-Host "  .env created from .env.example" }
    else { Fail ".env file is missing." }
}

Step "Checking images (pull only if missing)"
& "$PSScriptRoot\scripts\ensure_images.ps1"
if ($LASTEXITCODE -ne 0) { Fail "Image check failed." }

Step "Building custom images (cached layers are reused)"
if ($Rebuild) { docker compose build --no-cache spark-master airflow } else { docker compose build spark-master airflow }
if ($LASTEXITCODE -ne 0) { Fail "docker compose build failed." }

Step "Starting all services"
# the Airflow dvc_snapshot task bind-mounts the project folder (git + DVC repo); give it the host path
$env:HOST_PROJECT_DIR = $PSScriptRoot
docker compose up -d
if ($LASTEXITCODE -ne 0) { Fail "docker compose up failed." }
if ($NoGenerator) { docker compose stop generator | Out-Null }

Step "Waiting for services to become healthy (timeout ${TimeoutSec}s)"
$names = 'mlops-kafka', 'mlops-minio', 'mlops-postgres', 'mlops-spark-master', 'mlops-mlflow', 'mlops-airflow', 'mlops-api', 'mlops-ui'
$deadline = (Get-Date).AddSeconds($TimeoutSec)
do {
    $pending = @()
    foreach ($n in $names) {
        $s = docker inspect -f '{{.State.Health.Status}}' $n 2>$null
        if ($s -ne 'healthy') { $pending += "$n($s)" }
    }
    if ($pending.Count -gt 0) { Write-Host "  waiting: $($pending -join ', ')"; Start-Sleep -Seconds 10 }
} while ($pending.Count -gt 0 -and (Get-Date) -lt $deadline)

if ($pending.Count -gt 0) {
    Write-Host "Some services are not healthy yet: $($pending -join ', ')" -ForegroundColor Yellow
    Write-Host "Check logs with: docker compose logs <service>" -ForegroundColor Yellow
}

# read host ports from .env
$p = @{}
Get-Content .env | Where-Object { $_ -match '^\s*[A-Z_]+=' } | ForEach-Object { $k, $v = $_ -split '=', 2; $p[$k.Trim()] = $v.Trim() }

Step "Status"
docker compose ps --format "table {{.Name}}\t{{.Status}}"
Write-Host ""
Write-Host "  Spark UI      http://localhost:$($p.SPARK_UI_PORT)"
Write-Host "  MinIO console http://localhost:$($p.MINIO_CONSOLE_PORT)   ($($p.MINIO_ROOT_USER) / $($p.MINIO_ROOT_PASSWORD))"
Write-Host "  MLflow        http://localhost:$($p.MLFLOW_PORT)"
Write-Host "  Airflow       http://localhost:$($p.AIRFLOW_PORT)   ($($p.AIRFLOW_ADMIN_USER) / $($p.AIRFLOW_ADMIN_PASSWORD))"
Write-Host "  API docs      http://localhost:$($p.API_PORT)/docs"
Write-Host "  Web UI        http://localhost:$($p.UI_PORT)"
Write-Host "  Kafka UI      http://localhost:$($p.KAFKA_UI_PORT)"
Write-Host ""
Write-Host "Stop everything with: .\stop.ps1" -ForegroundColor Green
