"""Deterministic clickstream producer for the local hesitation scenario."""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

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


def qualifying_pair(
    rules_path: str,
    *,
    min_lift: float,
    min_confidence: float,
    min_support: float,
) -> tuple[str, str]:
    with open(rules_path, newline="", encoding="utf-8") as handle:
        rows = [
            row
            for row in csv.DictReader(handle)
            if float(row["lift"]) >= min_lift
            and float(row["confidence"]) >= min_confidence
            and float(row["support"]) >= min_support
        ]
    if not rows:
        raise ValueError(f"No qualifying hesitation pair in {rules_path}")
    selected = max(
        rows,
        key=lambda row: (
            float(row["lift"]),
            float(row["confidence"]),
            float(row["support"]),
            row["antecedents"],
            row["consequents"],
        ),
    )
    return selected["antecedents"], selected["consequents"]


def hesitation_scenario(
    antecedent: str = "Organic Hass Avocado",
    consequent: str = "Corn Tortillas",
) -> list[dict[str, object]]:
    user_id = "demo-user-1"
    session_id = "demo-session-1"
    trace = [
        event("cart_item_added", antecedent, user_id, session_id),
        event("product_view_started", consequent, user_id, session_id),
        event("product_view_ended", consequent, user_id, session_id, dwell_seconds=47),
    ]
    trace[1]['event_time'] = (datetime.fromisoformat(trace[2]['event_time']) - timedelta(seconds=47)).isoformat()
    return trace


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--pause-seconds", type=float, default=0.25)
    parser.add_argument("--rules", default=os.getenv("RULES_PATH", "app/rules.csv"))
    args = parser.parse_args()
    antecedent, consequent = qualifying_pair(
        args.rules,
        min_lift=float(os.getenv("MIN_LIFT", "1.5")),
        min_confidence=float(os.getenv("MIN_CONFIDENCE", "0.10")),
        min_support=float(os.getenv("MIN_SUPPORT", "0.012")),
    )
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=JsonSerializer(),
    )
    try:
        for payload in hesitation_scenario(antecedent, consequent):
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
