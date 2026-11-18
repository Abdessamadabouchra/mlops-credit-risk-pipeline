# Version the current gold dataset with DVC (stored in MinIO bucket 'dvc'). Requires the stack to be running.
Set-Location -Path $PSScriptRoot
docker compose run --rm dvc
