# Final implementation evidence

This document records what was executed for the final deliverables on
2026-10-21. It complements the acceptance criteria in
[`final-deliverables-spec.md`](final-deliverables-spec.md).

## Marketplace

The Flask marketplace reads all 49,688 local catalog records but exposes only
the products referenced by the active association-rule snapshot, with
search, department and aisle filters, pagination, product detail pages, a
persistent browser basket, quantity controls, live coupon handling, simulated
checkout, and owner-scoped order lookup. Catalog metadata comes from the local
Instacart files. INR prices, availability, payment, and fulfilment are clearly
labelled as illustrative.

Recommendation APIs rank historical rule or aisle candidates through the
adaptive service when shopper and session identity are present. Each decision
returns a snapshot version, policy version, policy mode, and impression ID.
The browser carries that impression ID into recommendation feedback. Numeric
catalog IDs are the canonical identity used by historical rules, live stream
normalization, adaptive features, cart state, and checkout; product names are
retained for display and coupon-rule evidence.

Setting `RULES_PATH` to a Spark run's `rules_serving.csv` makes that measured
artifact the marketplace, Redis loader, and simulator's shared historical
prior. Its `run_id` becomes the adaptive `snapshot_version`; the legacy
`app/rules.csv` is only the zero-setup fallback.

Browser acceptance was exercised in Chromium at desktop and 375 px widths:

- catalog load, search for `banana`, and product-detail navigation;
- add to basket, open the persistent drawer, and continue to checkout;
- complete the delivery form and create a demo order;
- load the owner-scoped order confirmation;
- verify no horizontal overflow at 375 px;
- verify the basket modal traps focus, suppresses background shortcuts, closes
  with Escape, and restores focus to its opener;
- visually inspect the catalog, mobile market, and product page.

The final run used the canonical-ID Spark artifact as `RULES_PATH` and completed
as `3 passed (5.0s)`. An earlier run also caught and led to a fix for slow offline
telemetry: Kafka publication now uses a short fail-fast circuit, so an absent
live stack does not delay local shopping or checkout.

The live Kafka/Redis demonstration issued an `Organic Raspberries` → `Bag of
Organic Bananas` coupon from the same Spark snapshot. The Redis coupon ledger
accepted its first checkout claim and rejected the second, proving one-time
server-authoritative redemption.

## Spark historical bootstrap

The bounded real-data command was:

```bash
./scripts/run_spark_bootstrap.sh \
  --mode sample --sample-orders 1000 \
  --min-support 0.005 --min-confidence 0.05 \
  --num-partitions 2 \
  --als-rank 8 --als-max-iter 3 --als-top-k 10 \
  --run-id final-bounded-real-canonical-20261021
```

Spark 4.0.1 completed the run in 38.267 seconds. It validated all six source
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

The deployed adapter persists features, recorded impressions, LinUCB matrices,
decision sequencing, and event claims in Redis. A cross-instance acceptance
test records a marketplace impression in one service, sends catalog-ID feedback
through `AdaptiveForeachBatch` in a second, and observes the changed ranking
from a third service instance.
The same proof was repeated against Redis 7.4: the independent workers returned
`101, 202` → `feedback=learned` → `202, 101` in `adaptive_linucb` mode.
Shared feedback writes the model update, feature counters, retention expiries,
and final event claim in one Redis transaction under a distributed lock. A
forced pre-commit failure left no partial update and the same event then retried
successfully. Malformed JSON and invalid timestamps remain in the Spark stream
and reach the JSONL dead-letter sink instead of disappearing at parse time.

## Verification summary

```text
uv sync                                      passed
uv run pytest -q                             62 passed, 1 skipped
Docker Spark fixture artifact seam           passed
Docker Spark bounded real-data run           passed
Adaptive deterministic replay                ranking changed
Chromium desktop/mobile shopper journey      3 passed
JavaScript syntax checks                      passed
git diff --check                             passed
```

The single skipped host test is the JVM-dependent Spark CLI test. The exact
same public CLI/artifact seam was executed successfully in the pinned Docker
Spark environment.

## Boundaries

- Checkout creates a local demonstration record; it does not process payment,
  reserve inventory, fulfil an order, or send delivery notifications.
- Live adaptive features, impressions, LinUCB matrices, decision sequences,
  and 24-hour event claims are shared through Redis across Flask and Spark
  workers. The in-memory adapter remains available for deterministic replay.
- This is a single-node reference deployment; production still needs Redis
  durability, access control, monitoring, and a deliberate retention policy.
- The bounded run is measured evidence, not a full-dataset benchmark. Use the
  documented full-mode command when final infrastructure capacity is known.
