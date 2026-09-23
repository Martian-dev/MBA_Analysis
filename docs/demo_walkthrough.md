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
3. Wait for the randomized stream to issue a coupon. The storefront's **Coupon desk** will show:
   - the target product;
   - the percentage discount;
   - the illustrative savings amount;
   - the coupon ID;
   - the supporting cart item;
   - observed dwell time;
   - lift and confidence.
4. Click **Add … & apply …% off**. The product is added to the basket, the item is marked `coupon applied`, and the summary shows the offer savings and demo total.
5. Switch to the Signal Room. Show that the same coupon ID appears in the event tape and the decision card, alongside the event evidence. Point out that the dashboard receives consumer activity through SSE.
6. Wait for a `Short view`, `Already in cart`, or `No rule match` event. Use it to explain that the engine makes and records negative decisions rather than issuing a coupon to every shopper.

## Be precise about the demo

The feed is synthetic, but it samples real product names and qualifying association pairs from `app/rules.csv`. The coupon percentage and rule metrics are real values from the current rule artifact. The storefront prices are labelled illustrative because the Instacart dataset contains order history, not retail prices; the monetary savings is therefore a presentation aid, not a historical price claim.

The customer-facing offer is currently a local demonstration of the `coupon.issued` decision. It does not redeem a real discount or call a payment service.
