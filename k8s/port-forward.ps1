# Make the Kubernetes services reachable from Windows (minikube with the docker driver does not expose NodePorts
# directly on Windows). Runs in the background; .\k8s\undeploy.ps1 stops it.
#   UI  -> http://localhost:8601      API -> http://localhost:8100/docs
# Ports differ from the Compose ones (8501 / 8000) so both can run at the same time.
$pidFile = Join-Path $PSScriptRoot 'build\port-forward.pids'
New-Item -ItemType Directory -Force -Path (Split-Path $pidFile) | Out-Null

$ui = Start-Process kubectl -ArgumentList '-n', 'credit-risk', 'port-forward', 'svc/credit-risk-ui', '8601:8501' -WindowStyle Hidden -PassThru
$api = Start-Process kubectl -ArgumentList '-n', 'credit-risk', 'port-forward', 'svc/credit-risk-api', '8100:8000' -WindowStyle Hidden -PassThru
"$($ui.Id)`n$($api.Id)" | Set-Content $pidFile

Start-Sleep -Seconds 3
Write-Host "UI  -> http://localhost:8601"
Write-Host "API -> http://localhost:8100/docs"
Write-Host "(stop with .\k8s\undeploy.ps1 -KeepDeployment, or remove everything with .\k8s\undeploy.ps1)"
