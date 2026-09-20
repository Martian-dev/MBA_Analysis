# Event contract and coupon policy

Type: grilling
Status: open
Blocked by: none

## Question

Which clickstream event schema and coupon eligibility policy should be treated as the stable contract between the producer, Redis rule snapshot, stream processor, Flask integration, and future Spark implementation?

The current provisional answer is documented in [`docs/architecture.md`](../../../docs/architecture.md): four event types, canonical `product_id`, a 45-second dwell threshold, minimum lift 1.5, a 5% mock discount, strongest supporting cart item wins, and one coupon per user/product per engine lifetime. This ticket remains open until the business semantics are explicitly accepted.
