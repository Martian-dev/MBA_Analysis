# Map: Real-Time Contextual Market Basket Analysis

## Destination

A reproducible, demonstrable hybrid pipeline where historical Instacart purchases produce validated association rules, Kafka carries clickstream events, a stateful stream processor detects purchase hesitation, Redis serves rules at low latency, and coupon-issued events are emitted with an audit trail.

## Notes

- Local Markdown issue tracker under `.scratch/`.
- Domain vocabulary lives in [`CONTEXT.md`](../../CONTEXT.md).
- The first runnable milestone is allowed to execute a narrow vertical slice before HDFS/Spark scale-out.
- Coupon outputs are mock events only; no real discount is granted.
- Use official Apache/Redis documentation when pinning versions or semantics.
- The implementation currently uses a provisional event contract and coupon policy so the vertical slice can run; the open tickets remain the acceptance points for those decisions.
- Work advances in two parallel tracks: historical intelligence (HDFS/Spark/rule publication) and live activation (continuous Kafka simulation/stream decisions/Signal Room). They converge on the event and rule contracts.

## Decisions so far

No tickets have been resolved yet; the entries below are the active frontier.

## Not yet specified

- Which exact Kaggle downloads and licenses will be used, and how the data will be made available for a clean run.
- Whether final rule serving will use one-item rules only or itemset-aware rules.
- HDFS layout, replication settings, Parquet schema, and retention.
- Spark batch thresholds, partition sizing, and benchmark target.
- Spark streaming state store, watermark, checkpoint, and Redis lookup design.
- Coupon frequency caps, experiment/control design, and business guardrails.
- Production authentication, privacy retention, and deletion requirements.

## Out of scope

- Real payment or coupon redemption.
- A claim that the heuristic causes incremental conversion without an experiment.
