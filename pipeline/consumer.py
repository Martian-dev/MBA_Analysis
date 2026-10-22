"""Recoverable coupon consumer with durable state, outbox, and rejection log."""
from __future__ import annotations

import argparse
import json
import os

import redis
from kafka import KafkaConsumer, KafkaProducer

from .durable_coupon import DurableCouponProcessor
from .observability import ActivityLog
from .rules import RedisRuleLookup
from .serialization import JsonSerializer
from .topics import COUPONS_TOPIC, EVENTS_TOPIC
from .kafka_worker import consume


def decode_event(raw):
    from .contracts import validate_event
    try:
        return validate_event(json.loads(raw))
    except (UnicodeDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid clickstream event: {exc}") from exc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--group-id', default=os.getenv('CONSUMER_GROUP', 'mba-reference-engine'))
    parser.add_argument('--max-events', type=int, default=0)
    args = parser.parse_args()
    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    processor = DurableCouponProcessor(client, RedisRuleLookup(client),
        dwell_threshold_seconds=float(os.getenv('DWELL_THRESHOLD_SECONDS', '45')),
        min_lift=float(os.getenv('MIN_LIFT', '1.5')),
        min_confidence=float(os.getenv('MIN_CONFIDENCE', '.10')),
        min_support=float(os.getenv('MIN_SUPPORT', '.012')),
        discount_percent=int(os.getenv('DISCOUNT_PERCENT', '5')),
        holdout_fraction=float(os.getenv('COUPON_HOLDOUT_FRACTION', '0')))
    consumer = KafkaConsumer(EVENTS_TOPIC, bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id, auto_offset_reset='earliest', enable_auto_commit=False)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, value_serializer=JsonSerializer())
    def publish(decision):
        producer.send(COUPONS_TOPIC, key=decision['user_id'].encode(), value=decision).get(timeout=10)
        print(json.dumps(decision), flush=True)
    def handle(message):
        try:
            payload = decode_event(message.value)
        except ValueError as exc:
            ActivityLog(client).record('event_rejected', {
                'reason': str(exc), 'topic': message.topic,
                'partition': message.partition, 'offset': message.offset,
                'raw': repr(message.value)[:2048],
            })
        else:
            processor.process(payload)
    try:
        consume(consumer, handle, idle=lambda: processor.drain(publish, max_items=1), max_events=args.max_events)
        processor.drain(publish)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close(timeout=5)
        consumer.close()


if __name__ == '__main__':
    main()
