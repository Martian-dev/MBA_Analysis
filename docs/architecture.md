# Real-Time Contextual Market Basket Analysis

## Destination

Build a reproducible hybrid pipeline that mines historical purchase associations, serves a validated rule snapshot, consumes clickstream events, and emits a coupon-issued event when a shopper hesitates on a product associated with something already in the cart.

## Current milestone: vertical slice

The first milestone intentionally proves the decision loop without pretending that the current CSV is a production data lake:

```text
rules.csv -> Redis rule snapshot
                    \
clickstream producer -> Kafka -> stateful coupon engine -> Kafka coupon-issued event
```

The local stack is Docker-backed:

- Apache Kafka 4.x in KRaft mode
- Redis 7.x
- Host-side Python producer, rule loader, and consumer

Kafka 4.0 removed ZooKeeper support and runs in KRaft mode, so this project does not add a ZooKeeper container. The future Spark deployment can use the same Kafka event contract. Spark's `FPGrowth` API preserves antecedent and consequent arrays; the batch job must not flatten a rule to its first item.

## Target architecture

```text
Instacart CSVs
    |
    v
HDFS landing zone (Parquet, partitioned by source/table and ingestion date)
    |
    v
Spark batch FP-Growth -> validated one-item directional rules -> Redis rule snapshot
                                                               |
Clickstream events -> Kafka -> Spark Structured Streaming state -> coupon policy
                                                               |
                                                               v
                                                  coupon-issued Kafka topic / API
```

## Event contract

All events are JSON objects with these fields:

| Field | Meaning |
|---|---|
| `event_id` | Globally unique event identifier |
| `event_type` | `product_view_started`, `product_view_ended`, `cart_item_added`, or `cart_item_removed` |
| `event_time` | ISO-8601 UTC timestamp |
| `user_id` | Shopper identity used for stream state |
| `session_id` | Session identity used to isolate carts and views |
| `product_id` | Canonical product identity; current demo uses representative names |
| `dwell_seconds` | Required for `product_view_ended`; optional when it can be derived later |
| `quantity` | Optional quantity for cart events, defaulting to one |

Coupon output events add `coupon_id`, `discount_percent`, `supporting_cart_item`, `lift`, `confidence`, `support`, and a `reason` object containing the dwell threshold and observed dwell time.

The clickstream topic is partitioned by `user_id` so all state-mutating events for one shopper are ordered within a partition.

## Live observability

The reference consumer writes a bounded activity stream to Redis. Flask exposes the initial snapshot at `/api/activity` and follows new entries at `/api/activity/stream` using Server-Sent Events. The operations viewer at `/viewer` renders the stream as an event tape, counters, and an evidence card for the latest coupon decision. `pipeline/live_simulator.py` produces a continuous, explicitly synthetic feed with eligible and suppressed scenarios so the dashboard demonstrates both branches of the policy.

## Initial policy

The demo defaults are deliberately explicit and configurable:

- Dwell threshold: 45 seconds
- Minimum lift: 1.5
- Minimum confidence: 0.10
- Minimum support: 0.012 (1.2% of the mined transaction sample)
- Discount: 5 percent
- A coupon is issued only when the viewed product is not already in the cart.
- The strongest qualifying supporting cart item wins.
- A user/product pair receives at most one coupon decision per engine lifetime.

The storefront publishes events for its own browser-scoped `user_id` and `session_id`, and filters coupon activity back to that same pair. The Signal Room intentionally remains global for operators. Dwell time alone is not sufficient: the association must also pass the rule-quality thresholds. This is a demonstration policy, not evidence that a 5% discount increases conversion. Evaluation and guardrails are separate work.

## Batch rules: correctness requirements

The existing notebook has a correctness defect: it converts every antecedent and consequent itemset to `list(x)[0]`. The replacement Spark job must either:

1. retain itemsets as arrays and implement itemset-aware decisions, or
2. explicitly filter to one-item antecedents and one-item consequents before publishing a rule snapshot.

For the first coupon policy, option 2 keeps the serving contract small and auditable. Rules should be deduplicated by `(antecedent, consequent)` using the strongest retained metric row, with the batch run ID and thresholds stored alongside the snapshot.

## Delivery phases

1. **Contract and vertical slice** — the runnable Kafka/Redis/Python path in this repository.
2. **Data landing** — download and validate the Instacart tables, write immutable Parquet to HDFS, and document the exact source version.
3. **Distributed batch mining** — Spark FP-Growth with explicit one-item rule filtering, quality checks, and Redis publication.
4. **Distributed streaming** — Spark Structured Streaming with Kafka offsets, watermark/state policy, checkpointing, and a Redis lookup strategy.
5. **Product integration** — expose coupon decisions to the Flask UI through a testable API and render only server-approved offers.
6. **Evaluation and operations** — replay tests, latency/throughput measurements, coupon guardrails, observability, and a reproducible course demonstration.

## Parallel delivery tracks

The build proceeds along two tracks that meet at the rule snapshot and event contract:

| Historical intelligence | Live activation and observability |
|---|---|
| HDFS landing and Parquet validation | Continuous clickstream simulator |
| Spark MLlib FP-Growth | Kafka topics and replay fixtures |
| Versioned Redis rule publication | Stateful decision engine |
| Batch quality and runtime metrics | Signal Room viewer and coupon delivery |

The live track is intentionally usable against the provisional rule artifact while the historical track replaces that artifact. Both tracks must converge on product IDs, a versioned rule schema, and replayable acceptance scenarios.

## Non-goals for the first slice

- No real payment, checkout, or discount redemption.
- No claim of causal uplift from coupons.
- No full HDFS/Spark cluster until the event and rule contracts are exercised end to end.
- No use of the existing flattened rule artifact as a final scientific result.

## Primary references

- [Apache Kafka 4.0 release announcement](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/)
- [Spark PySpark FPGrowth API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.fpm.FPGrowth.html)
- [Spark Structured Streaming + Kafka integration](https://spark.apache.org/docs/latest/streaming-kafka-integration)
- [Redis data types](https://redis.io/docs/latest/develop/data-types/)
