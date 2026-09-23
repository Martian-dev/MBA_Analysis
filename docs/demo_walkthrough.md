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

1. On the storefront, point out the `Cart-aware`, `Dwell-aware`, and `Lift-checked` labels. Explain that the product cards are backed by the current MBA rule artifact.
2. Add any product to the basket. The recommendation row demonstrates a batch association lookup from the cart item.
3. On one recommended product, click **Test 55s hesitation**. This publishes a `product_view_started` and `product_view_ended` pair for this browser's own shopper/session ID. The button compresses the wait for the presentation; it does not bypass the decision engine.
4. The storefront's **Coupon desk** will show the offer only if the rule-quality checks pass:
   - the target product;
   - the percentage discount;
   - the illustrative savings amount;
   - the coupon ID;
   - the supporting cart item;
   - observed dwell time;
   - lift and confidence.
5. Click **Add … & apply …% off**. The product is added to the basket, the item is marked `coupon applied`, and the summary shows the offer savings and demo total.
6. Switch to the Signal Room. Show that the same coupon ID appears in the event tape and the decision card, alongside the event evidence. Point out that the dashboard receives consumer activity through SSE.
7. Use the random simulator's `Short view`, `Already in cart`, or `No rule match` events to explain that the engine makes and records negative decisions rather than issuing a coupon to every shopper.

## Be precise about the demo

The feed is synthetic, but it samples real product names and qualifying association pairs from `app/rules.csv`. The coupon percentage and rule metrics are real values from the current rule artifact. The storefront prices are labelled illustrative INR amounts because the Instacart dataset contains order history, not retail prices; the monetary savings is therefore a presentation aid, not a historical price claim. The browser storefront is scoped to its own customer/session; the randomized simulator's offers remain visible to operators in the Signal Room, not to every storefront visitor.

The customer-facing offer is currently a local demonstration of the `coupon.issued` decision. It does not redeem a real discount or call a payment service.
