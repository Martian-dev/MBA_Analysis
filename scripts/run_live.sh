#!/usr/bin/env bash

set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"

compose=(docker compose -f infra/docker-compose.yml)
runtime_dir="$project_dir/.runtime/live"
mkdir -p "$runtime_dir"
consumer_pid=""
simulator_pid=""
flask_pid=""
feedback_pid=""
spark_pid=""
miner_pid=""
order_events_pid=""
export LIVE_RULE_MINING_ENABLED="${LIVE_RULE_MINING_ENABLED:-1}"
export LIVE_RULES_ALLOW_SYNTHETIC="${LIVE_RULES_ALLOW_SYNTHETIC:-1}"

cleanup() {
  set +e
  for pid in "$consumer_pid" "$simulator_pid" "$flask_pid" "$feedback_pid" "$spark_pid" "$miner_pid" "$order_events_pid"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  for pid in "$consumer_pid" "$simulator_pid" "$flask_pid" "$feedback_pid" "$spark_pid" "$miner_pid" "$order_events_pid"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
  if [[ "${KEEP_INFRA:-0}" != "1" ]]; then
    "${compose[@]}" down >/dev/null 2>&1
  fi
}
if (echo >/dev/tcp/127.0.0.1/5000) 2>/dev/null; then
  echo "Port 5000 is already in use. Stop the existing viewer before starting a fresh demo." >&2
  exit 1
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Starting Kafka and Redis..."
"${compose[@]}" up -d >/dev/null

echo "Waiting for Redis and Kafka..."
redis_ok=0
kafka_ok=0
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

if [[ "${RESET_DEMO:-0}" == "1" ]]; then
  echo "Resetting demo state by explicit request..."
  uv run python -m pipeline.reset_demo
fi

echo "Loading the current rule snapshot into Redis..."
uv run python -m pipeline.load_rules | tee "$runtime_dir/rules.log"

group_id="${CONSUMER_GROUP:-mba-durable-engine-v2}"
echo "Starting the stream consumer ($group_id)..."
uv run python -m pipeline.consumer --group-id "$group_id" \
  > >(tee "$runtime_dir/consumer.log") 2>&1 &
consumer_pid=$!

echo "Starting adaptive feedback ingestion and outcome settlement..."
uv run python -m pipeline.feedback_worker >"$runtime_dir/feedback.log" 2>&1 &
feedback_pid=$!
echo "Starting durable confirmed-order event delivery..."
uv run python -m pipeline.order_event_worker >"$runtime_dir/order-events.log" 2>&1 &
order_events_pid=$!

if [[ "$LIVE_RULE_MINING_ENABLED" == "1" ]]; then
  echo "Starting validated live association mining (storefront and synthetic cohorts stay separate)..."
  uv run python -m pipeline.live_rule_worker >"$runtime_dir/rule-miner.log" 2>&1 &
  miner_pid=$!
fi

if [[ "${RUN_STRUCTURED_STREAMING:-0}" == "1" ]]; then
  bash scripts/run_adaptive_stream.sh >"$runtime_dir/spark-stream.log" 2>&1 &
  spark_pid=$!
fi

echo "Starting the continuous clickstream simulator..."
uv run python -m pipeline.live_simulator --interval "${SIMULATOR_INTERVAL:-2}" \
  --warmup-orders "${SIMULATOR_WARMUP_ORDERS:-300}" \
  > >(tee "$runtime_dir/simulator.log") 2>&1 &
simulator_pid=$!

echo "Starting the viewer..."
ADAPTIVE_REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}" \
  uv run flask --app app.main run --host 127.0.0.1 --port 5000 \
  >"$runtime_dir/flask.log" 2>&1 &
flask_pid=$!

for _ in {1..30}; do
  if curl -fsS http://127.0.0.1:5000/api/activity >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS http://127.0.0.1:5000/api/activity >/dev/null 2>&1; then
  echo "The viewer did not become ready. Check $runtime_dir/flask.log" >&2
  exit 1
fi

echo
operator_token=$(uv run python -c 'from app.identity import operator_token; print(operator_token())')
echo "Private signal room: http://127.0.0.1:5000/viewer?token=$operator_token"
echo "Storefront:  http://127.0.0.1:5000/"
echo "Runtime logs: $runtime_dir"
echo "Randomized clickstream is live; decisions appear in the signal room. Press Ctrl-C to stop."
echo

while true; do
  if ! kill -0 "$order_events_pid" 2>/dev/null; then
    echo "Confirmed-order delivery stopped. Check $runtime_dir/order-events.log" >&2
    exit 1
  fi
  if [[ -n "$miner_pid" ]] && ! kill -0 "$miner_pid" 2>/dev/null; then
    echo "Live rule mining stopped. Check $runtime_dir/rule-miner.log" >&2
    exit 1
  fi
  if [[ -n "$spark_pid" ]] && ! kill -0 "$spark_pid" 2>/dev/null; then
    echo "Structured Streaming stopped. Check $runtime_dir/spark-stream.log" >&2
    exit 1
  fi
  if ! kill -0 "$feedback_pid" 2>/dev/null; then
    echo "The adaptive feedback worker stopped. Check $runtime_dir/feedback.log" >&2
    exit 1
  fi
  if ! kill -0 "$consumer_pid" 2>/dev/null; then
    echo "The stream consumer stopped. Check $runtime_dir/consumer.log" >&2
    exit 1
  fi
  if ! kill -0 "$simulator_pid" 2>/dev/null; then
    echo "The clickstream simulator stopped. Check $runtime_dir/simulator.log" >&2
    exit 1
  fi
  if ! kill -0 "$flask_pid" 2>/dev/null; then
    echo "The viewer stopped. Check $runtime_dir/flask.log" >&2
    exit 1
  fi
  sleep 2
done
