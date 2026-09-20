# Contextual Market Basket Analytics

This context combines historical purchase associations with live shopper behavior to decide when a targeted coupon is appropriate.

## Shopper behavior

**Shopper**:
A user or anonymous visitor whose cart and clickstream are being observed for a session.
_Avoid_: Customer, account (unless an authenticated identity is available)

**Cart**:
The set of products currently selected by a shopper in the active session.
_Avoid_: Order (an order is a completed purchase)

**Clickstream event**:
An immutable observation of a shopper action, such as starting a product view, ending a product view, or changing the cart.
_Avoid_: Log line, request

**Dwell time**:
The elapsed time a shopper spends viewing a product before leaving the product view.
_Avoid_: Session duration (which covers the whole session)

**Hesitation signal**:
A product-view event whose dwell time exceeds the configured threshold while the product is not yet in the shopper's cart.
_Avoid_: Intent score (the first implementation is a rule, not a probabilistic score)

## Market-basket analysis

**Association rule**:
A directional relationship from an antecedent itemset to a consequent itemset, measured with support, confidence, and lift.
_Avoid_: Recommendation (a recommendation is a product decision made from one or more rules)

**Rule snapshot**:
The versioned batch output of association-rule mining that is made available to the streaming decision service.
_Avoid_: Model (the snapshot contains rules; the mining job may use a model)

**Supporting cart item**:
An item already in the cart whose rule points to the product currently showing a hesitation signal.
_Avoid_: Related item (too vague for a coupon decision)

## Couponing

**Coupon decision**:
An auditable decision to issue or suppress a targeted offer for a shopper and product.
_Avoid_: Discount event (the decision may be a suppression as well as an issue)

**Coupon issued event**:
The immutable output event stating that a coupon decision issued a particular discount for a product.
_Avoid_: Webhook (a webhook is one possible delivery mechanism)
