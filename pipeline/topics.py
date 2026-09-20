"""Kafka topic creation for the local demo."""

from __future__ import annotations

import os

from kafka.admin import KafkaAdminClient, NewTopic

EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "clickstream.events")
COUPONS_TOPIC = os.getenv("COUPONS_TOPIC", "coupon.issued")


def ensure_topics(bootstrap_servers: str = "localhost:9092") -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="mba-topic-admin")
    try:
        existing = set(admin.list_topics())
        wanted = [
            NewTopic(name=name, num_partitions=3, replication_factor=1)
            for name in (EVENTS_TOPIC, COUPONS_TOPIC)
            if name not in existing
        ]
        if wanted:
            admin.create_topics(wanted)
    finally:
        admin.close()


if __name__ == "__main__":
    ensure_topics(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    print(f"Kafka topics ready: {EVENTS_TOPIC}, {COUPONS_TOPIC}")
