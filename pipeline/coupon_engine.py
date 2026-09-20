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
    coupon_products: set[str] = field(default_factory=set)


class CouponEngine:
    def __init__(
        self,
        rules: RuleLookup,
        dwell_threshold_seconds: float = 45.0,
        min_lift: float = 1.5,
        discount_percent: int = 5,
    ):
        self.rules = rules
        self.dwell_threshold_seconds = dwell_threshold_seconds
        self.min_lift = min_lift
        self.discount_percent = discount_percent
        self._users: OrderedDict[tuple[str, str], UserState] = OrderedDict()

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
        if product in state.coupon_products:
            return None, "coupon_already_issued"

        best: tuple[float, str, dict[str, Any]] | None = None
        for cart_item in state.cart:
            metrics = self.rules.lookup(cart_item, product)
            if not metrics or float(metrics["lift"]) < self.min_lift:
                continue
            candidate = (float(metrics["lift"]), cart_item, metrics)
            if best is None or candidate[0] > best[0]:
                best = candidate
        if best is None:
            return None, "no_qualifying_rule"

        lift, supporting_item, metrics = best
        state.coupon_products.add(product)
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
            },
        }, "coupon_issued"
