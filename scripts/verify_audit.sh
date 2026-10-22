#!/usr/bin/env bash
# Full regression and real service acceptance against isolated Redis state.
set -euo pipefail
repo_dir=$(git rev-parse --show-toplevel)
cd "$repo_dir"
verification_redis="mba-verification-redis-$$"
cleanup() { docker stop "$verification_redis" >/dev/null 2>&1 || true; }
trap cleanup EXIT
verification_port=$(uv run python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
docker run --rm -d --name "$verification_redis" --publish "127.0.0.1:$verification_port:6379" \
  redis:7.4-alpine redis-server --appendonly yes --appendfsync always >/dev/null
export REDIS_TEST_URL="redis://127.0.0.1:$verification_port/15"
export REDIS_TEST_CONTAINER="$verification_redis"
export KAFKA_TEST_BOOTSTRAP="${KAFKA_TEST_BOOTSTRAP:-localhost:9092}"
export SPARK_TEST_IMAGE="${SPARK_TEST_IMAGE:-mba-spark-runtime:4.0.1}"
if ! docker image inspect "$SPARK_TEST_IMAGE" >/dev/null 2>&1; then
  docker build --tag "$SPARK_TEST_IMAGE" --file infra/Dockerfile.spark .
fi
uv run pytest -q
node tests/frontend/product_page_recommendation.mjs
for script in app/static/js/*.js; do node --check "$script"; done
git diff --check
