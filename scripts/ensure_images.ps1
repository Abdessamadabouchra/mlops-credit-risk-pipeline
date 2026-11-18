# Pull ONLY the images that are missing locally (docker image inspect first).
# The list lives in scripts\images.txt. Images built by us (mlops-spark, mlops-airflow) are handled by 'docker compose build'.
$ErrorActionPreference = 'Continue'
$list = Join-Path $PSScriptRoot 'images.txt'

foreach ($img in (Get-Content $list | Where-Object { $_.Trim() -and -not $_.StartsWith('#') })) {
    $img = $img.Trim()
    docker image inspect $img *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [skip] $img already exists locally"
    } else {
        Write-Host "  [pull] $img not found locally, pulling..." -ForegroundColor Yellow
        docker pull $img
        if ($LASTEXITCODE -ne 0) { Write-Host "  [fail] could not pull $img" -ForegroundColor Red; exit 1 }
    }
}
exit 0
