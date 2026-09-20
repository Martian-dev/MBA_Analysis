"""Event contracts shared by the producer and coupon decision engine."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

EVENT_TYPES = frozenset(
    {
        "product_view_started",
        "product_view_ended",
        "cart_item_added",
        "cart_item_removed",
    }
)

REQUIRED_FIELDS = frozenset(
    {"event_id", "event_type", "event_time", "user_id", "session_id", "product_id"}
)


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
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type}")

    for field in ("event_id", "user_id", "session_id", "product_id"):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ValueError(f"{field} must be a non-empty string")

    try:
        parsed_time = datetime.fromisoformat(str(event["event_time"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time must be ISO-8601") from exc
    if parsed_time.tzinfo is None:
        raise ValueError("event_time must include a timezone")

    if event_type == "product_view_ended":
        dwell = event.get("dwell_seconds")
        if isinstance(dwell, bool) or not isinstance(dwell, (int, float)) or dwell < 0:
            raise ValueError("product_view_ended requires a non-negative dwell_seconds")

    if event_type in {"cart_item_added", "cart_item_removed"}:
        quantity = event.get("quantity", 1)
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 1:
            raise ValueError("cart events require a positive integer quantity")

    return event
