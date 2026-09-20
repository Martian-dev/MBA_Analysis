#!/usr/bin/env bash

set -euo pipefail

compose=(docker compose -f infra/docker-compose.yml)
log_dir=$(mktemp -d)
consumer_pid=""
simulator_pid=""
flask_pid=""

cleanup() {
  set +e
  [[ -n "$consumer_pid" ]] && kill "$consumer_pid" 2>/dev/null
  [[ -n "$simulator_pid" ]] && kill "$simulator_pid" 2>/dev/null
  [[ -n "$flask_pid" ]] && kill "$flask_pid" 2>/dev/null
  wait "$consumer_pid" "$simulator_pid" "$flask_pid" 2>/dev/null
  if [[ "${KEEP_INFRA:-0}" != "1" ]]; then
    "${compose[@]}" down >/dev/null 2>&1
  fi
  rm -rf "$log_dir"
}
trap cleanup EXIT INT TERM

echo "Starting Kafka and Redis..."
"${compose[@]}" up -d >/dev/null

echo "Waiting for Redis and Kafka..."
for _ in {1..30}; do
  redis_ok=0
  kafka_ok=0
  "${compose[@]}" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG && redis_ok=1
  uv run python -m pipeline.topics >/dev/null 2>&1 && kafka_ok=1
  if [[ "$redis_ok" == "1" && "$kafka_ok" == "1" ]]; then
    break
  fi
  sleep 1
done
if [[ "$redis_ok" != "1" || "$kafka_ok" != "1" ]]; then
  echo "The local infrastructure did not become ready in time." >&2
  exit 1
fi

echo "Loading the current rule snapshot into Redis..."
uv run python -m pipeline.load_rules >"$log_dir/rules.log"

group_id="mba-live-$(date +%s)"
echo "Starting the stream consumer ($group_id)..."
uv run python -m pipeline.consumer --group-id "$group_id" >"$log_dir/consumer.log" 2>&1 &
consumer_pid=$!

echo "Starting the continuous clickstream simulator..."
uv run python -m pipeline.live_simulator --interval 2 >"$log_dir/simulator.log" 2>&1 &
simulator_pid=$!

echo
echo "Signal room: http://127.0.0.1:5000/viewer"
echo "Storefront:  http://127.0.0.1:5000/"
echo "The simulator is continuously sending events. Press Ctrl-C to stop."
echo

uv run flask --app app.main run --host 0.0.0.0 --port 5000
