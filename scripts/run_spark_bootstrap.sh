#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(git rev-parse --show-toplevel)"
spark_image="${SPARK_IMAGE:-mba-spark:4.0.1}"
dataset_dir="$(readlink -f "${INSTACART_DATA_DIR:-$repo_dir/InstacartMBA}")"

if [[ "$spark_image" == "mba-spark:4.0.1" ]] && ! docker image inspect "$spark_image" >/dev/null 2>&1; then
  docker build --tag "$spark_image" --file "$repo_dir/infra/Dockerfile.spark" "$repo_dir"
fi

docker_args=(
  --rm
  --user "$(id -u):$(id -g)"
  --env HOME=/tmp
  --env SPARK_USER=mba-runner
  --env SPARK_LOCAL_DIRS=/tmp/spark
  --volume /etc/passwd:/etc/passwd:ro
  --volume /etc/group:/etc/group:ro
  --volume "$repo_dir:/workspace"
  --workdir /workspace
)
if [[ -d "$dataset_dir" ]]; then
  docker_args+=(--volume "$dataset_dir:/workspace/InstacartMBA:ro")
fi

docker run "${docker_args[@]}" "$spark_image" \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  pipeline/spark_bootstrap.py "$@"
