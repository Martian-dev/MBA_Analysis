"""Reference stateful coupon policy for the first vertical slice."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import OrderedDict
from typing import Any, Protocol

from .contracts import validate_event


class RuleLookup(Protocol):
    def lookup(self, antecedent: str, consequent: str) -> dict[str, Any] | None: ...


@dataclass
class UserState:
    cart: set[str] = field(default_factory=set)
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
    ):
        self.rules = rules
        self.dwell_threshold_seconds = dwell_threshold_seconds
        self.min_lift = min_lift
        self.min_confidence = min_confidence
        self.min_support = min_support
        self.discount_percent = discount_percent
        self._users: OrderedDict[tuple[str, str], UserState] = OrderedDict()
        # Coupon frequency is customer-scoped rather than session-scoped. A
        # new session for the same shopper must not reset the offer guardrail.
        self._issued_products: OrderedDict[tuple[str, str], None] = OrderedDict()

    def process(self, event: dict[str, Any]) -> dict[str, Any] | None:
        decision, _ = self.evaluate(event)
        return decision

    def evaluate(self, event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """Apply a shopper event and return the observable decision reason."""
        event = validate_event(event)
        session_key = (event["user_id"], event["session_id"])
        state = self._users.setdefault(session_key, UserState())
        self._users.move_to_end(session_key)
        if len(self._users) > 500:
            self._users.popitem(last=False)
        event_type = event["event_type"]
        product = event["product_id"]

        if event_type == "cart_item_added":
            state.cart.add(product)
            return None, "cart_updated"
        if event_type == "cart_item_removed":
            state.cart.discard(product)
            return None, "cart_updated"
        if event_type == "product_view_started":
            state.active_views[product] = event["event_time"]
            return None, "view_started"
        if event_type != "product_view_ended":
            return None, "ignored"

        state.active_views.pop(product, None)
        dwell = float(event["dwell_seconds"])
        if dwell < self.dwell_threshold_seconds:
            return None, "below_dwell_threshold"
        if product in state.cart:
            return None, "product_in_cart"
        if (event["user_id"], product) in self._issued_products:
            return None, "coupon_already_issued"

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

        lift, _, _, supporting_item, metrics = best
        customer_product = (event["user_id"], product)
        self._issued_products[customer_product] = None
        self._issued_products.move_to_end(customer_product)
        if len(self._issued_products) > 10_000:
            self._issued_products.popitem(last=False)
        return {
            "event_type": "coupon_issued",
            "event_time": event["event_time"],
            "event_id": f"coupon:{event['event_id']}:{product}",
            "coupon_id": f"coupon:{event['user_id']}:{event['session_id']}:{product}",
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
