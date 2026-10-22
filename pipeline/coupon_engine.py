"""Reference stateful coupon policy for the first vertical slice."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from collections import OrderedDict
from typing import Any, Protocol

from .contracts import validate_event
from .experiment import arm_for


class RuleLookup(Protocol):
    def lookup(self, antecedent: str, consequent: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class CouponClaim:
    user_id: str
    session_id: str
    product_id: str


class CouponClaimStore(Protocol):
    def claim(self, coupon_claim: CouponClaim) -> bool: ...

    def release(self, coupon_claim: CouponClaim) -> None: ...


class InMemoryCouponClaimStore:
    def __init__(self, max_claims: int = 10_000) -> None:
        self.max_claims = max_claims
        self._claims: OrderedDict[CouponClaim, None] = OrderedDict()

    def claim(self, coupon_claim: CouponClaim) -> bool:
        if coupon_claim in self._claims:
            return False
        self._claims[coupon_claim] = None
        self._claims.move_to_end(coupon_claim)
        if len(self._claims) > self.max_claims:
            self._claims.popitem(last=False)
        return True

    def release(self, coupon_claim: CouponClaim) -> None:
        self._claims.pop(coupon_claim, None)


@dataclass
class UserState:
    cart: dict[str, int] = field(default_factory=dict)
    active_views: dict[str, str] = field(default_factory=dict)


class CouponEngine:
    def __init__(
        self,
        rules: RuleLookup,
        dwell_threshold_seconds: float = 45.0,
        min_lift: float = 1.5,
        min_confidence: float = 0.10,
        min_support: float = 0.012,
        discount_percent: int = 5,
        claims: CouponClaimStore | None = None,
        holdout_fraction: float = 0.0,
    ):
        self.rules = rules
        self.dwell_threshold_seconds = dwell_threshold_seconds
        self.min_lift = min_lift
        self.min_confidence = min_confidence
        self.min_support = min_support
        self.discount_percent = discount_percent
        self.claims = claims if claims is not None else InMemoryCouponClaimStore()
        self.holdout_fraction = holdout_fraction
        self._users: OrderedDict[tuple[str, str], UserState] = OrderedDict()

    def process(self, event: dict[str, Any]) -> dict[str, Any] | None:
        decision, _ = self.evaluate(event)
        return decision

    def release_claim(self, decision: dict[str, Any]) -> None:
        """Allow an uncommitted coupon delivery to be retried."""
        self.claims.release(CouponClaim(
            str(decision["user_id"]),
            str(decision["session_id"]),
            str(decision["product_id"]),
        ))

    def evaluate(self, event: dict[str, Any], state: UserState | None = None) -> tuple[dict[str, Any] | None, str]:
        """Apply a shopper event and return the observable decision reason."""
        event = validate_event(event)
        event_type = event["event_type"]
        if event_type not in {
            "cart_item_added",
            "cart_item_removed",
            "cart_synchronized",
            "product_view_started",
            "product_view_ended",
        }:
            return None, "ignored"

        session_key = (event["user_id"], event["session_id"])
        if state is None:
            state = self._users.setdefault(session_key, UserState())
            self._users.move_to_end(session_key)
            if len(self._users) > 500:
                self._users.popitem(last=False)
        if event_type == 'cart_synchronized':
            state.cart = {item['product_id']: item['quantity'] for item in event['cart']}
            return None, 'cart_updated'
        product = event["product_id"]

        if event_type == "cart_item_added":
            state.cart[product] = event.get("cart_quantity", min(99, state.cart.get(product, 0) + event.get("quantity", 1)))
            if not state.cart[product]:
                state.cart.pop(product, None)
            return None, "cart_updated"
        if event_type == "cart_item_removed":
            remaining = event.get("cart_quantity", max(0, state.cart.get(product, 0) - event.get("quantity", 1)))
            if remaining:
                state.cart[product] = remaining
            else:
                state.cart.pop(product, None)
            return None, "cart_updated"
        if event_type == "product_view_started":
            state.active_views[product] = event["event_time"]
            return None, "view_started"
        if event_type != "product_view_ended":
            return None, "ignored"

        started = state.active_views.get(product)
        if started is None:
            return None, "missing_view_start"
        elapsed = (datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
                   - datetime.fromisoformat(started.replace("Z", "+00:00"))).total_seconds()
        if elapsed < 0:
            return None, "out_of_order_view"
        # A claim of a long view cannot exceed its matching event-time span.
        dwell = min(float(event["dwell_seconds"]), elapsed)
        if dwell < self.dwell_threshold_seconds:
            return None, "below_dwell_threshold"
        if product in state.cart:
            return None, "product_in_cart"
        best: tuple[float, float, float, str, dict[str, Any]] | None = None
        saw_rule = False
        rejected_for_confidence = False
        rejected_for_support = False
        for cart_item in state.cart:
            metrics = self.rules.lookup(cart_item, product)
            if not metrics or float(metrics["lift"]) < self.min_lift:
                continue
            saw_rule = True
            if float(metrics.get("confidence", 0)) < self.min_confidence:
                rejected_for_confidence = True
                continue
            if float(metrics.get("support", 0)) < self.min_support:
                rejected_for_support = True
                continue
            candidate = (
                float(metrics["lift"]),
                float(metrics.get("confidence", 0)),
                float(metrics.get("support", 0)),
                cart_item,
                metrics,
            )
            if best is None or candidate[0] > best[0]:
                best = candidate
        if best is None:
            if rejected_for_confidence:
                return None, "below_confidence_threshold"
            if rejected_for_support:
                return None, "below_support_threshold"
            if saw_rule:
                return None, "no_qualifying_rule"
            return None, "no_qualifying_rule"

        if arm_for(event['user_id'], self.holdout_fraction) == 'control':
            return None, 'experiment_control'
        coupon_claim = CouponClaim(event["user_id"], event["session_id"], product)
        if not self.claims.claim(coupon_claim):
            return None, "coupon_already_issued"

        lift, _, _, supporting_item, metrics = best
        return {
            "event_type": "coupon_issued",
            "event_time": event["event_time"],
            "event_id": f"coupon:{event['event_id']}:{product}",
            "coupon_id": f"coupon:{event['event_id']}:{product}",
            "user_id": event["user_id"],
            "session_id": event["session_id"],
            "product_id": product,
            "discount_percent": self.discount_percent,
            "supporting_cart_item": supporting_item,
            "lift": lift,
            "confidence": float(metrics["confidence"]),
            "support": float(metrics["support"]),
            "reason": {
                "dwell_seconds": dwell,
                "dwell_threshold_seconds": self.dwell_threshold_seconds,
                "minimum_lift": self.min_lift,
                "minimum_confidence": self.min_confidence,
                "minimum_support": self.min_support,
            },
        }, "coupon_issued"
