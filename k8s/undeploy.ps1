# Remove the Kubernetes deployment.
#   .\k8s\undeploy.ps1                   stop port-forwards + delete the credit-risk namespace
#   .\k8s\undeploy.ps1 -KeepDeployment   only stop the port-forwards
#   .\k8s\undeploy.ps1 -StopMinikube     also stop minikube (frees its memory for the Compose stack)
param([switch]$KeepDeployment, [switch]$StopMinikube)
$ErrorActionPreference = 'Continue'

$pidFile = Join-Path $PSScriptRoot 'build\port-forward.pids'
if (Test-Path $pidFile) {
    Get-Content $pidFile | ForEach-Object { if ($_) { Stop-Process -Id ([int]$_) -Force -ErrorAction SilentlyContinue } }
    Remove-Item $pidFile -Force
    Write-Host "port-forwards stopped"
}
if (-not $KeepDeployment) {
    kubectl delete namespace credit-risk --ignore-not-found
}
if ($StopMinikube) {
    minikube stop
}
