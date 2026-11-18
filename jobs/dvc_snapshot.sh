#!/bin/bash
# Version the gold snapshot with DVC (remote = MinIO bucket 'dvc'). Runs inside the 'dvc' compose service,
# which mounts the project folder at /project.
set -euo pipefail
if [ ! -f /project/docker-compose.yml ]; then
  echo "ERROR: project folder is not mounted at /project (start the stack with start.ps1 so HOST_PROJECT_DIR is set)" >&2
  exit 1
fi
cd /project

git config --global --add safe.directory /project
git config --global user.email "mlops@example.com"
git config --global user.name "mlops-pipeline"
[ -d .git ] || git init -q
[ -d .dvc ] || dvc init -q

if ! dvc remote list | grep -q '^minio'; then
  dvc remote add -d minio s3://dvc/store
  dvc remote modify minio endpointurl "$MINIO_ENDPOINT"
fi
# credentials stay out of git (.dvc/config.local is git-ignored by dvc)
dvc remote modify --local minio access_key_id "$MINIO_ROOT_USER"
dvc remote modify --local minio secret_access_key "$MINIO_ROOT_PASSWORD"

/opt/jobs/submit.sh export_gold.py 2 1g 1g

dvc add data/gold_snapshot
dvc push
# add only the DVC metadata that exists (one missing path would make 'git add' reject the whole list)
for f in .dvc/config .dvc/.gitignore .dvcignore data/gold_snapshot.dvc data/.gitignore; do
  [ -e "$f" ] && git add "$f"
done
if git diff --cached --quiet; then
  echo "nothing new to commit (data unchanged)"
else
  git commit -q -m "data: gold snapshot $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "committed: $(git log --oneline -1)"
fi

# LAST line = the hash only: Airflow pushes it to XCom and train.py logs it as the MLflow tag dvc_data_hash
grep -m1 'md5:' data/gold_snapshot.dvc | awk '{print $3}'
