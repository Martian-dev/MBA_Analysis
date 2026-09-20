# Contextual Market Basket Analytics

This repository is evolving from a Flask market-basket demo into a hybrid batch + streaming analytics project.

## What to run

- `bash scripts/run_live.sh` — recommended: continuous simulated clickstream plus the live Signal Room at `/viewer`.
- `bash scripts/run_demo.sh` — finite three-event terminal demonstration; exits on its own.
- `uv run flask --app app.main run` — storefront and viewer only; live infrastructure must already be running for viewer data.

## Run the first vertical slice

Prerequisites: Docker Compose and `uv`.

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
