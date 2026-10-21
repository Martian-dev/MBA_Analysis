#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(git rev-parse --show-toplevel)"
cd "$repo_dir"

uv sync
uv run pytest -q
uv run python -m pipeline.adaptive_replay

for script in app/static/js/*.js; do
  node --check "$script"
done

if [[ "${RUN_SPARK_ACCEPTANCE:-0}" == "1" ]]; then
  run_id="verification-mini-$(date -u +%Y%m%dT%H%M%SZ)"
  ./scripts/run_spark_bootstrap.sh \
    --input-dir tests/fixtures/instacart-mini \
    --mode sample --sample-orders 5 \
    --min-support 0.4 --min-confidence 0.5 \
    --num-partitions 1 \
    --als-rank 2 --als-max-iter 2 --als-top-k 2 \
    --run-id "$run_id"
fi

echo "Final deliverable verification passed."
