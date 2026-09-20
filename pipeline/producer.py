"""Deterministic clickstream producer for the local hesitation scenario."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from .serialization import JsonSerializer
from .topics import EVENTS_TOPIC


def event(event_type: str, product_id: str, user_id: str, session_id: str, **extra: object) -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        **extra,
    }


def hesitation_scenario() -> list[dict[str, object]]:
    user_id = "demo-user-1"
    session_id = "demo-session-1"
    return [
        event("cart_item_added", "Organic Hass Avocado", user_id, session_id),
        event("product_view_started", "Corn Tortillas", user_id, session_id),
        event("product_view_ended", "Corn Tortillas", user_id, session_id, dwell_seconds=47),
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--pause-seconds", type=float, default=0.25)
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=JsonSerializer(),
    )
    try:
        for payload in hesitation_scenario():
            # Key by shopper so all events for one shopper stay on one Kafka
            # partition and the stateful consumer observes them in order.
            producer.send(
                EVENTS_TOPIC,
                key=payload["user_id"].encode("utf-8"),
                value=payload,
            ).get(timeout=10)
            print(json.dumps(payload))
            time.sleep(args.pause_seconds)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
