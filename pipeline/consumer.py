"""Kafka consumer that applies the reference coupon policy."""

from __future__ import annotations

import argparse
import json
import os

import redis
from kafka import KafkaConsumer, KafkaProducer

from .coupon_engine import CouponEngine
from .observability import ActivityLog
from .rules import RedisRuleLookup
from .serialization import JsonDeserializer, JsonSerializer
from .topics import COUPONS_TOPIC, EVENTS_TOPIC


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--group-id", default=os.getenv("CONSUMER_GROUP", "mba-reference-engine"))
    parser.add_argument("--max-events", type=int, default=0, help="stop after this many events; 0 means run forever")
    args = parser.parse_args()

    redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    redis_client.ping()
    activity = ActivityLog(redis_client)
    engine = CouponEngine(
        RedisRuleLookup(redis_client),
        dwell_threshold_seconds=float(os.getenv("DWELL_THRESHOLD_SECONDS", "45")),
        min_lift=float(os.getenv("MIN_LIFT", "1.5")),
        discount_percent=int(os.getenv("DISCOUNT_PERCENT", "5")),
    )
    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=JsonDeserializer(),
    )
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=JsonSerializer(),
    )
    processed = 0
    try:
        for message in consumer:
            processed += 1
            decision, reason = engine.evaluate(message.value)
            activity.record("event_received", {**message.value, "reason": reason})
            if decision:
                producer.send(COUPONS_TOPIC, value=decision).get(timeout=10)
                activity.record("coupon_issued", decision)
                print(json.dumps(decision), flush=True)
            elif message.value["event_type"] == "product_view_ended":
                activity.record("coupon_suppressed", {**message.value, "reason": reason})
            consumer.commit()
            if args.max_events and processed >= args.max_events:
                break
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
