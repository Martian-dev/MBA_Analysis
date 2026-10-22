#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(git rev-parse --show-toplevel)
spark_image="${SPARK_IMAGE:-mba-spark-runtime:4.0.1}"
mkdir -p "$repo_dir/.runtime/spark-ivy"
if ! docker image inspect "$spark_image" >/dev/null 2>&1; then
  docker build --tag "$spark_image" --file "$repo_dir/infra/Dockerfile.spark" "$repo_dir"
fi
exec docker run --rm --network host \
  --user "$(id -u):$(id -g)" \
  --env HOME=/tmp --env SPARK_USER=mba-runner --env PYTHONPATH=/workspace \
  --env "ADAPTIVE_REDIS_URL=${ADAPTIVE_REDIS_URL:-${REDIS_URL:-redis://localhost:6379/0}}" \
  --env "KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --volume /etc/passwd:/etc/passwd:ro --volume /etc/group:/etc/group:ro \
  --volume "$repo_dir:/workspace" --workdir /workspace \
  "$spark_image" /opt/spark/bin/spark-submit --master 'local[2]' \
  --conf spark.jars.ivy=/workspace/.runtime/spark-ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  pipeline/adaptive_streaming.py "$@"
