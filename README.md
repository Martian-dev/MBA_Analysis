# Contextual Market Basket Analytics

This repository is evolving from a Flask market-basket demo into a hybrid batch + streaming analytics project.

## What to run

- `./start.sh` — one-shot launcher: verifies dependencies, starts Kafka/Redis, resets ephemeral demo state, loads the MBA rule artifact, starts the randomized clickstream, consumer, and viewer.
- `bash scripts/run_live.sh` — recommended: continuous simulated clickstream plus the live Signal Room at `/viewer`.
- `bash scripts/run_demo.sh` — finite three-event terminal demonstration; exits on its own.
- `uv run flask --app app.main run` — storefront and viewer only; live infrastructure must already be running for viewer data.

## Run the first vertical slice

Prerequisites: Docker Compose and `uv`.

For a fresh end-to-end run, use the root launcher:

```bash
./start.sh
```

Then open <http://127.0.0.1:5000/viewer>. The launcher continuously samples real products and qualifying association pairs from `app/rules.csv`, so the stream produces a mix of coupon decisions and suppressed decisions instead of replaying a fixed scenario. It clears only the demo's Kafka topics and Redis activity counters on startup; the rule artifact and source data are untouched. Press `Ctrl-C` to stop the application and local infrastructure. Set `KEEP_INFRA=1` to leave Kafka and Redis running, or `SIMULATOR_INTERVAL=1` to increase the event rate.

Runtime logs are retained under `.runtime/live/` for troubleshooting.

The storefront at <http://127.0.0.1:5000/> now publishes its own keyed shopper events and consumes only coupon decisions for that browser's customer/session. Its Coupon desk shows the issued percentage, an illustrative savings amount, coupon ID, supporting cart item, dwell time, lift, and confidence. Add a product, choose a recommendation, and use **Test 55s hesitation** to run the targeted decision loop for that session; then use **Add … & apply …% off** to demonstrate the customer-facing basket change. The engine also requires minimum lift, confidence, and support thresholds, so dwell time alone is not enough. Prices are explicitly illustrative because the Instacart source contains purchase history, not retail prices; production pricing should come from the catalog service.

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

The consumer should print one `coupon_issued` JSON event for `Corn Tortillas`: the shopper has `Organic Hass Avocado` in the cart, dwells for 47 seconds, and the current rule snapshot contains a qualifying association.

Stop local infrastructure with:

```bash
docker compose -f infra/docker-compose.yml down
```

## Important limitation

`app/rules.csv` is a temporary artifact from the original prototype. Its old notebook flattened multi-item rules incorrectly. The new loader deduplicates directed pairs for deterministic demonstration only; the replacement Spark batch job must be built before treating the rule snapshot as a valid analytical result.

The architecture, event contract, milestones, and unresolved decisions are in [`docs/architecture.md`](docs/architecture.md) and [`.scratch/realtime-contextual-mba/map.md`](.scratch/realtime-contextual-mba/map.md).
