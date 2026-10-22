# Live demo walkthrough

Use the storefront and Signal Room together. They are two views of the same stream, not two separate demos.

## Start

```bash
./start.sh
```

Open both pages:

- storefront: <http://127.0.0.1:5000/>
- Signal Room: <http://127.0.0.1:5000/viewer>

## Presentation flow

1. Search the rule-covered catalog, open a product, and point out its real Instacart
   aisle/department metadata and explicitly illustrative INR price.
2. Add the product to the persistent basket and open the basket drawer. Its
   pairings begin with the configured historical snapshot; later clicks and
   cart actions feed the adaptive ranker for this browser identity.
3. Open one of the basket's recommended pairings. Leave the product page open
   for at least 45 seconds, then navigate away. The page publishes real
   `product_view_started` and `product_view_ended` events with measured dwell
   time for the browser's shopper/session ID.
4. If the pair passes the configured rule thresholds, the basket drawer shows
   the live offer with:
   - the target product;
   - the percentage discount;
   - the coupon ID;
   - the supporting cart item;
   - observed dwell time, lift, and confidence in the operator view.
5. Apply the coupon, adjust quantities, and continue through the simulated
   checkout. Show the saved order confirmation and the clear no-payment notice.
6. Switch to the Signal Room. Show the same session's evidence and explain that
   the operator surface receives consumer activity through SSE. Its Available
   coupons ledger shows each unredeemed offer with the session that owns it.
7. Use the random simulator's suppressed events to explain that the engine
   records negative decisions instead of issuing an offer to every shopper.

For a compressed terminal-only coupon proof, run `bash scripts/run_demo.sh`.
That command publishes the seeded 47-second scenario directly; the storefront
itself never fabricates dwell time.

## Be precise about the demo

The simulator is synthetic, but it samples real product names and qualifying
association pairs from `RULES_PATH` (the legacy fallback is `app/rules.csv`).
For final evidence, launch with a Spark-generated `rules_serving.csv`; its run
ID becomes the marketplace snapshot version. The storefront prices are labelled
illustrative INR amounts because Instacart contains order history, not retail
prices. The browser storefront is scoped to its own shopper/session; simulator
offers remain visible to operators rather than every shopper.

The shopper-facing offer is a local demonstration of the `coupon.issued`
decision. Coupon frequency, browser storage, and checkout redemption are scoped
to the shopper session. Checkout claims the offer once from the server-side
Redis ledger and applies the illustrative saving, but does not call a payment
service or create a real retail discount.
