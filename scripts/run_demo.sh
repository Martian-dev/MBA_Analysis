#!/usr/bin/env bash

set -euo pipefail

compose=(docker compose -f infra/docker-compose.yml)
consumer_log=$(mktemp)

cleanup() {
  if [[ "${KEEP_INFRA:-0}" != "1" ]]; then
    "${compose[@]}" down >/dev/null 2>&1 || true
  fi
  rm -f "$consumer_log"
}
trap cleanup EXIT INT TERM

echo "Starting Kafka and Redis..."
"${compose[@]}" up -d >/dev/null

echo "Waiting for Redis..."
for _ in {1..30}; do
  if "${compose[@]}" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 1
done

echo "Creating Kafka topics..."
topics_ready=0
for _ in {1..30}; do
  if uv run python -m pipeline.topics >/dev/null 2>&1; then
    topics_ready=1
    break
  fi
  sleep 1
done
if [[ "$topics_ready" != "1" ]]; then
  echo "Kafka did not become ready in time." >&2
  exit 1
fi

echo "Loading the current rule snapshot into Redis..."
uv run python -m pipeline.load_rules

group_id="mba-demo-$(date +%s)"
echo "Starting the reference engine ($group_id)..."
uv run python -m pipeline.consumer --group-id "$group_id" --max-events 3 >"$consumer_log" 2>&1 &
consumer_pid=$!
sleep 2

echo "Publishing the seeded hesitation scenario..."
uv run python -m pipeline.producer
wait "$consumer_pid"

echo
echo "Coupon decision emitted by the stream engine:"
cat "$consumer_log"
echo
if [[ "${KEEP_INFRA:-0}" == "1" ]]; then
  echo "Infrastructure is still running (KEEP_INFRA=1)."
else
  echo "Demo complete; Kafka and Redis have been stopped."
fi
