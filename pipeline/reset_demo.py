"""Reset only ephemeral Kafka and Redis state used by the live demonstration."""

from __future__ import annotations

import argparse
import os
import time

import redis
from kafka.admin import KafkaAdminClient

from .observability import COUNTS_KEY, STREAM_KEY
from .topics import COUPONS_TOPIC, EVENTS_TOPIC, ensure_topics


def reset_kafka(bootstrap_servers: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="mba-demo-reset")
    try:
        existing = set(admin.list_topics())
        targets = [topic for topic in (EVENTS_TOPIC, COUPONS_TOPIC) if topic in existing]
        if targets:
            admin.delete_topics(targets)
    finally:
        admin.close()

    # Topic deletion is asynchronous. Do not recreate until the broker has
    # removed both names, otherwise old records may remain visible.
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        probe = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="mba-demo-reset-probe")
        try:
            if not (set(probe.list_topics()) & {EVENTS_TOPIC, COUPONS_TOPIC}):
                break
        finally:
            probe.close()
        time.sleep(0.25)
    else:
        raise RuntimeError("Kafka did not delete the demo topics in time")

    ensure_topics(bootstrap_servers)


def reset_redis(redis_url: str) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.delete(STREAM_KEY, COUNTS_KEY)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )
    args = parser.parse_args()
    reset_redis(args.redis_url)
    reset_kafka(args.bootstrap_servers)
    print(f"Fresh demo state ready: {EVENTS_TOPIC}, {COUPONS_TOPIC}, Redis activity log")


if __name__ == "__main__":
    main()
