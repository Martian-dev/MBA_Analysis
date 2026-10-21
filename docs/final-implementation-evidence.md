# Final implementation evidence

This document records what was executed for the final deliverables on
2026-10-21. It complements the acceptance criteria in
[`final-deliverables-spec.md`](final-deliverables-spec.md).

## Marketplace

The Flask marketplace now exposes all 49,688 local catalog products with
search, department and aisle filters, pagination, product detail pages, a
persistent browser basket, quantity controls, live coupon handling, simulated
checkout, and owner-scoped order lookup. Catalog metadata comes from the local
Instacart files. INR prices, availability, payment, and fulfilment are clearly
labelled as illustrative.

Recommendation APIs rank historical rule or aisle candidates through the
adaptive service when shopper and session identity are present. Each decision
returns a snapshot version, policy version, policy mode, and impression ID.
The browser carries that impression ID into recommendation feedback. Product,
cart, coupon, search, and checkout events share the same product-name policy
keys, with numeric catalog IDs retained as metadata.

Browser acceptance was exercised in Chromium at desktop and 375 px widths:

- catalog load, search for `banana`, and product-detail navigation;
- add to basket, open the persistent drawer, and continue to checkout;
- complete the delivery form and create a demo order;
- load the owner-scoped order confirmation;
- verify no horizontal overflow at 375 px;
- visually inspect the catalog, mobile market, and product page.

Result: `2 passed (5.1s)`. The run also caught and led to a fix for slow offline
telemetry: Kafka publication now uses a short fail-fast circuit, so an absent
live stack does not delay local shopping or checkout.

## Spark historical bootstrap

The bounded real-data command was:

```bash
./scripts/run_spark_bootstrap.sh \
  --mode sample --sample-orders 1000 \
  --min-support 0.005 --min-confidence 0.05 \
  --num-partitions 2 \
  --als-rank 8 --als-max-iter 3 --als-top-k 10 \
  --run-id final-bounded-real-20261021
```

Spark 4.0.1 completed the run in 39.564 seconds. It validated all six source
tables with zero violations, selected 1,000 orders, constructed 978 non-empty
baskets with 9,963 transactions, mined 298 association rules, exported 286
one-item directional rules, and trained implicit ALS across 973 users and
4,517 products. ALS exported 9,730 candidates. The committed machine-readable
summary is [`evidence/spark-bounded-real-20261021.json`](evidence/spark-bounded-real-20261021.json).

The deterministic miniature acceptance run independently produced five
baskets, two directional rules, four ALS candidates, and zero validation
violations. Generated Parquet, newline JSON, serving CSV, validation, and full
manifest artifacts remain under the ignored `data/processed/bootstrap/<run-id>`
directory and can be regenerated with the commands in
[`spark_bootstrap.md`](spark_bootstrap.md).

## Live adaptive analytics

The deterministic replay command was:

```bash
uv run python -m pipeline.adaptive_replay
```

The initial historical ranking was `bananas, berries`. After a recorded click
reward on the second displayed candidate, the later ranking became
`berries, bananas`. The response reported `feedback_status=learned`, reward
`0.4`, policy `linucb-v1`, snapshot `bootstrap-replay-v1`, and distinct recorded
impression IDs.

The same adaptive decision seam is used by the marketplace HTTP APIs. The test
suite proves through HTTP that feedback on the second product-page candidate
changes the subsequent ranking and moves the policy from
`historical_cold_start` to `adaptive_linucb`.

## Verification summary

```text
uv sync                                      passed
uv run pytest -q                             44 passed, 1 skipped
Docker Spark fixture artifact seam           passed
Docker Spark bounded real-data run           passed
Adaptive deterministic replay                ranking changed
Chromium desktop/mobile shopper journey      2 passed
JavaScript syntax checks                      passed
git diff --check                             passed
```

The single skipped host test is the JVM-dependent Spark CLI test. The exact
same public CLI/artifact seam was executed successfully in the pinned Docker
Spark environment.

## Boundaries

- Checkout creates a local demonstration record; it does not process payment,
  reserve inventory, fulfil an order, or send delivery notifications.
- Adaptive state and impressions are process-local in this reference build.
  Independently scaled Flask and streaming workers require a shared durable
  state adapter.
- The bounded run is measured evidence, not a full-dataset benchmark. Use the
  documented full-mode command when final infrastructure capacity is known.
