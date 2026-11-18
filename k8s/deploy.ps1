# Deploy the credit-risk serving layer (API + UI with the champion model) to minikube.
#   .\k8s\deploy.ps1                 export champion, build image, deploy (Compose stack keeps running)
#   .\k8s\deploy.ps1 -StopCompose    same, but stops the Docker Compose stack before starting minikube
#                                    (recommended with ~8 GB for Docker: the two together do not fit)
# Requires the Compose stack to be running for the export step (MLflow).
param(
    [switch]$StopCompose,
    [int]$MinikubeMemoryMB = 3072,
    [int]$MinikubeCpus = 2
)
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
Set-Location -Path $root

function Step($m) { Write-Host "`n==> $m" -ForegroundColor Cyan }
function Fail($m) { Write-Host $m -ForegroundColor Red; exit 1 }

Step "Checking MLflow (Compose stack) is reachable"
try { Invoke-WebRequest http://localhost:5000/health -UseBasicParsing -TimeoutSec 10 | Out-Null }
catch { Fail "MLflow is not reachable on http://localhost:5000. Start the stack first: .\start.ps1" }

Step "Building the project image mlops-spark:1.0 (cached layers are reused)"
docker compose build spark-master
if ($LASTEXITCODE -ne 0) { Fail "docker compose build failed." }

Step "Exporting the champion model from MLflow"
New-Item -ItemType Directory -Force -Path "$root\k8s\build" | Out-Null
docker run --rm --network mlops-net --user 0:0 -e MLFLOW_TRACKING_URI=http://mlflow:5000 `
    -v "${root}\k8s:/k8s" --entrypoint python3 mlops-spark:1.0 /k8s/export_model.py
if ($LASTEXITCODE -ne 0) { Fail "Model export failed (is there a champion? run the Airflow DAG first)." }
$info = Get-Content "$root\k8s\build\model_info.json" | ConvertFrom-Json
$tag = "v$($info.version)"
$image = "credit-risk-serving:$tag"
Write-Host "  champion: version $($info.version), $($info.algorithm), auc=$($info.auc)"

Step "Building image $image"
docker build -f "$root\k8s\Dockerfile.serving" --build-arg MODEL_VERSION=$($info.version) -t $image "$root\k8s\build"
if ($LASTEXITCODE -ne 0) { Fail "docker build failed." }

if ($StopCompose) {
    Step "Stopping the Docker Compose stack to free memory (data is kept)"
    & "$root\stop.ps1"
}

Step "Starting minikube (if needed)"
$status = minikube status --format '{{.Host}}' 2>$null
if ($status -ne 'Running') {
    minikube start --memory=$MinikubeMemoryMB --cpus=$MinikubeCpus
    if ($LASTEXITCODE -ne 0) { Fail "minikube start failed." }
} else { Write-Host "  minikube already running" }

Step "Loading $image into minikube (only if missing)"
$present = minikube image ls 2>$null | Select-String -SimpleMatch "credit-risk-serving:$tag"
if ($present) { Write-Host "  [skip] $image already in minikube" }
else {
    minikube image load $image
    if ($LASTEXITCODE -ne 0) { Fail "minikube image load failed." }
}

Step "Applying manifests"
(Get-Content "$root\k8s\credit-risk.yaml" -Raw) -replace '__IMAGE_TAG__', $tag | kubectl apply -f -
if ($LASTEXITCODE -ne 0) { Fail "kubectl apply failed." }

Step "Waiting for the deployments (the API loads Spark + the model, ~1-3 min)"
kubectl -n credit-risk rollout status deployment/credit-risk-ui --timeout=300s
kubectl -n credit-risk rollout status deployment/credit-risk-api --timeout=900s
kubectl -n credit-risk get pods -o wide

Write-Host ""
Write-Host "Deployed model $tag ($($info.algorithm)) to namespace credit-risk." -ForegroundColor Green
Write-Host "Open the UI / API from Windows with:  .\k8s\port-forward.ps1" -ForegroundColor Green
Write-Host "   UI  -> http://localhost:8601        API -> http://localhost:8100/docs"
