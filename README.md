# Contextual Market Basket Analytics

This repository is evolving from a Flask market-basket demo into a hybrid batch + streaming analytics project.

## What to run

- `./scripts/run_spark_bootstrap.sh` — reproducible Spark 4 historical bootstrap in Docker; writes validated, versioned FP-Growth and ALS artifacts.
- `./start.sh` — one-shot launcher: verifies dependencies, starts Kafka/Redis, resets ephemeral demo state, loads the MBA rule artifact, starts the randomized clickstream, consumer, and viewer.
- `bash scripts/run_live.sh` — recommended: continuous simulated clickstream plus the live Signal Room at `/viewer`.
- `bash scripts/run_demo.sh` — finite three-event terminal demonstration; exits on its own.
- `uv run --no-sync flask --app app.main run` — marketplace and Signal Room only; live infrastructure must already be running for streaming events.
- `uv run --no-sync python -m pipeline.adaptive_replay` — deterministic proof that rewarded live feedback changes the next recommendation ranking.
- `uv run --no-sync python -m pipeline.adaptive_streaming --checkpoint-location .runtime/checkpoints/adaptive-features` — Spark Structured Streaming feature updater; it and Flask share Redis adaptive state when `ADAPTIVE_REDIS_URL` is set.
- `./scripts/verify_final_deliverables.sh` — install the core project, run the full test suite and adaptive replay, and syntax-check every storefront script. Set `RUN_SPARK_ACCEPTANCE=1` to include the Docker Spark artifact seam.

The heavyweight GPU semantic-clustering dependencies are intentionally outside
the core marketplace environment. Install them only for the clustering
notebook with `uv sync --group semantic`. Spark uses its pinned Docker runner;
see [`docs/spark_bootstrap.md`](docs/spark_bootstrap.md). Measured final-run
results are recorded in
[`docs/final-implementation-evidence.md`](docs/final-implementation-evidence.md).

## Browse the grocery marketplace

The storefront at <http://127.0.0.1:5000/> reads the local Instacart catalog
metadata but exposes only products referenced by the active `RULES_PATH`
association-rule snapshot. This keeps every listed product inside the analysis
pipeline's current scope. It supports server-side search, filters, and
pagination; deep-linked product pages at `/product/<product_id>`; a persistent
browser basket; live coupon application; and a simulated checkout at
`/checkout`. Confirmed demo orders are appended locally to
`.runtime/marketplace/orders.jsonl` and can be reopened only by the shopper
identity that created them.

The Instacart source has no price data. Every INR amount shown by the market is
stable but illustrative, and the checkout does not charge a payment method,
reserve inventory, or schedule delivery. Set `INSTACART_DATA_DIR` to override
the source-data directory and `ORDER_STORE_PATH` through Flask configuration
when embedding or testing the application.

The browser publishes product views and real dwell time, searches,
recommendation impressions and clicks, cart changes, coupon lifecycle events,
and checkout events through `POST /api/events`. When Kafka or Redis is offline,
catalog, basket, and checkout behavior remain usable locally; live offers and
Signal Room evidence resume when the stream stack is available.

## Build the historical prior with Spark

The replacement batch job reads all six Instacart tables with explicit
schemas, validates the selected data, preserves basket arrays, mines rules
with Spark MLlib FP-Growth, and exports implicit-feedback ALS candidates:

```bash
./scripts/run_spark_bootstrap.sh --mode sample --sample-orders 10000
```

See [`docs/spark_bootstrap.md`](docs/spark_bootstrap.md) for full-data tuning,
artifact contracts, the deterministic acceptance fixture, and the command for
publishing a versioned Spark rule snapshot into Redis.

## Run the first vertical slice

Prerequisites: Docker Compose and `uv`.

For a fresh end-to-end run, use the root launcher:

```bash
./start.sh
```

Then open <http://127.0.0.1:5000/viewer>. The launcher continuously samples
real products and qualifying association pairs from `RULES_PATH` (falling back
to `app/rules.csv`), so the stream produces a mix of coupon decisions and
suppressed decisions instead of replaying a fixed scenario. It clears the
demo's Kafka topics plus Redis activity, coupon, and adaptive runtime state on
startup; the rule artifact and source data are untouched. Press `Ctrl-C` to stop the application and local
infrastructure. Set `KEEP_INFRA=1` to leave Kafka and Redis running, or
`SIMULATOR_INTERVAL=1` to increase the event rate.

Runtime logs are retained under `.runtime/live/` for troubleshooting.

The storefront at <http://127.0.0.1:5000/> publishes keyed shopper events and
consumes only coupon decisions for that browser's shopper/session. Add a
product, open a suggested pairing for at least 45 seconds, then navigate away
to publish measured dwell time. A qualifying offer appears in the basket
drawer and the Signal Room shows its rule and dwell evidence. The engine also
requires minimum lift, confidence, and support thresholds, so dwell time alone
is not enough. Prices are explicitly illustrative because Instacart contains
purchase history rather than retail prices.

For a presentation script, see [`docs/demo_walkthrough.md`](docs/demo_walkthrough.md).

For the shortest demonstration, run:

```bash
bash scripts/run_demo.sh
```

Set `KEEP_INFRA=1` if you want Kafka and Redis left running after the output so you can inspect them manually.

For the continuously pumping feed and live operations viewer, run:

```bash
bash scripts/run_live.sh
```

Open <http://127.0.0.1:5000/viewer>. The viewer shows the Redis activity log through server-sent events, while the simulator continuously publishes shopper events into Kafka. Press `Ctrl-C` in the terminal to stop the Flask server, simulator, consumer, and (unless `KEEP_INFRA=1`) the Docker services.

The viewer's cards deliberately show both sides of the decision: issued offers include the supporting cart item, dwell time, lift, and confidence; suppressed views show why an offer was withheld (short view, product already in cart, or no qualifying rule).

```bash
uv sync
docker compose -f infra/docker-compose.yml up -d
uv run python -m pipeline.topics
uv run python -m pipeline.load_rules
```

In one terminal, start the reference stream engine:

```bash
uv run python -m pipeline.consumer --group-id mba-demo-$(date +%s) --max-events 3
```

In a second terminal, publish the deterministic hesitation scenario:

```bash
uv run python -m pipeline.producer
```

The producer deterministically selects the strongest pair in `RULES_PATH` that
passes the configured lift, confidence, and support thresholds. The consumer
should print one `coupon_issued` JSON event for its consequent after a seeded
47-second dwell.

Stop local infrastructure with:

```bash
docker compose -f infra/docker-compose.yml down
```

## Historical rule source

`app/rules.csv` remains the legacy fallback used by the zero-setup demo. For a
measured run, point the complete stack at a successful Spark output; the same
artifact is then loaded into Redis, sampled by the simulator, and used as the
marketplace's versioned cold-start prior:

```bash
RULES_PATH=data/processed/bootstrap/<run-id>/rules_serving.csv ./start.sh
```

The marketplace reads the Spark `run_id` column as its `snapshot_version`.
After that cold start, browser events update the adaptive ranking used by later
requests. The legacy CSV should not be presented as a newly measured result.

The architecture, event contract, milestones, and unresolved decisions are in [`docs/architecture.md`](docs/architecture.md) and [`.scratch/realtime-contextual-mba/map.md`](.scratch/realtime-contextual-mba/map.md).

## Live adaptive ranking

`pipeline.adaptive.AdaptiveRankingService` is the application-facing adaptive
decision seam. It accepts a versioned list of historical candidate dictionaries
and returns ranked dictionaries containing both `impression_id` and
`policy_version`. The ranking combines the historical prior with live session,
shopper, product-response, and cart-pair features. A deterministic LinUCB policy
learns only after feedback can be matched to an already-recorded impression.

The supported feedback events are recommendation clicks, cart additions and
removals, coupon applications, and completed checkouts. Local replay retains
the latest 50,000 event IDs; the live Redis adapter uses a 24-hour deduplication
window. Coupon quality,
supporting-cart, already-in-cart, and frequency
rules run before the bandit and cannot be overridden by a learned score. If the
learning-state adapter is unavailable, candidates remain usable in historical
prior order and the response identifies `historical_fallback` mode.

Run the evidence replay:

```bash
uv run --no-sync python -m pipeline.adaptive_replay
```

It uses the committed `pipeline/fixtures/adaptive_replay.json` fixture. The
expected first order is `bananas, berries`; after a recorded click on berries,
the second order is `berries, bananas`.

The Spark entry point parses Kafka JSON with an explicit schema, derives an
event-time timestamp, applies a configurable watermark, and requires a durable
checkpoint location. PySpark is deliberately imported only by the streaming
entry point, so routine Flask startup and unit tests do not require Spark. The
live launcher configures Flask to use the shared Redis feature, impression,
LinUCB, sequence, and event-deduplication adapters. A separately deployed Spark
worker uses the same namespace. Browser events retain display names but are
normalized to numeric catalog IDs before shared adaptive state is updated.
