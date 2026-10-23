"""Event contracts shared by the producer and coupon decision engine."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any

EVENT_TYPES = frozenset(
    {
        "product_view_started",
        "product_view_ended",
        "cart_item_added",
        "cart_item_removed",
        "cart_synchronized",
        "basket_opened",
        "search_performed",
        "recommendation_impression",
        "recommendation_clicked",
        "coupon_applied",
        "checkout_started",
        "checkout_completed",
    }
)

REQUIRED_FIELDS = frozenset(
    {"event_id", "event_type", "event_time", "user_id", "session_id"}
)

PRODUCT_EVENT_TYPES = frozenset(
    {
        "product_view_started",
        "product_view_ended",
        "cart_item_added",
        "cart_item_removed",
        "recommendation_impression",
        "recommendation_clicked",
        "coupon_applied",
    }
)
CHECKOUT_EVENT_TYPES = frozenset({"checkout_started", "checkout_completed"})


def utc_now_iso() -> str:
    """Return a UTC timestamp with an explicit timezone."""

    return datetime.now(timezone.utc).isoformat()


def validate_event(event: dict[str, Any]) -> dict[str, Any]:
    """Validate and return a clickstream event."""

    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")

    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"missing event fields: {', '.join(sorted(missing))}")

    event_type = event["event_type"]
    if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type}")

    for field in ("event_id", "user_id", "session_id"):
        if not isinstance(event[field], str) or not event[field].strip() or len(event[field]) > 256:
            raise ValueError(f"{field} must be a non-empty string")

    if event_type in PRODUCT_EVENT_TYPES:
        product_id = event.get("product_id")
        if not isinstance(product_id, str) or not product_id.strip() or len(product_id) > 512:
            raise ValueError("product_id must be a non-empty string")

    if event_type == "search_performed":
        query = event.get("query")
        if not isinstance(query, str) or not query.strip():
            raise ValueError("search_performed requires a non-empty query")

    if event_type == 'cart_synchronized':
        cart = event.get('cart')
        if not isinstance(cart, list) or len(cart) > 500:
            raise ValueError('cart_synchronized requires a bounded cart list')
        seen = set()
        for item in cart:
            if (not isinstance(item, dict) or not isinstance(item.get('product_id'), str)
                    or not item['product_id'].strip() or item['product_id'] in seen
                    or isinstance(item.get('quantity'), bool) or not isinstance(item.get('quantity'), int)
                    or not 1 <= item['quantity'] <= 99):
                raise ValueError('Invalid or duplicate cart snapshot item')
            seen.add(item['product_id'])

    if "product_ids" in event:
        values = event["product_ids"]
        if not isinstance(values, list) or len(values) > 500 or any(
            not isinstance(value, str) or not value.strip() for value in values
        ):
            raise ValueError("product_ids must be a bounded list of non-empty strings")

    for field in ("catalog_product_id",):
        if field in event and (
            isinstance(event[field], bool) or not isinstance(event[field], (int, str))
            or not str(event[field]).isdigit() or int(event[field]) < 1
        ):
            raise ValueError(f"{field} must be a positive catalog identifier")
    if "catalog_product_ids" in event and (
        not isinstance(event["catalog_product_ids"], list)
        or len(event["catalog_product_ids"]) > 500
        or any(isinstance(value, bool) or not isinstance(value, (int, str))
               or not str(value).isdigit() or int(value) < 1
               for value in event["catalog_product_ids"])
    ):
        raise ValueError("catalog_product_ids must be a bounded list of positive identifiers")

    if event_type in CHECKOUT_EVENT_TYPES:
        order_id = event.get("order_id")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("checkout events require a non-empty order_id")
        product_ids = event.get("product_ids")
        if product_ids is not None and (
            not isinstance(product_ids, list)
            or any(
                not isinstance(product_id, str) or not product_id.strip()
                for product_id in product_ids
            )
        ):
            raise ValueError("checkout product_ids must be a list of non-empty strings")

    try:
        parsed_time = datetime.fromisoformat(str(event["event_time"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time must be ISO-8601") from exc
    if parsed_time.tzinfo is None:
        raise ValueError("event_time must include a timezone")

    if event_type == "product_view_ended":
        dwell = event.get("dwell_seconds")
        if (isinstance(dwell, bool) or not isinstance(dwell, (int, float))
                or not 0 <= dwell <= 86_400 or not math.isfinite(dwell)):
            raise ValueError("product_view_ended requires a non-negative dwell_seconds")

    if event_type in {"cart_item_added", "cart_item_removed"}:
        quantity = event.get("quantity", 1)
        if isinstance(quantity, bool) or not isinstance(quantity, int) or not 1 <= quantity <= 99:
            raise ValueError("cart events require a positive integer quantity")
        remaining = event.get("cart_quantity")
        if remaining is not None and (
            isinstance(remaining, bool) or not isinstance(remaining, int) or not 0 <= remaining <= 99
        ):
            raise ValueError("cart_quantity must be an integer between 0 and 99")

    return event
