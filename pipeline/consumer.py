"""Recoverable coupon consumer with durable state, outbox, and rejection log."""
from __future__ import annotations

import argparse
import json
import os
import time

import redis
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from .durable_coupon import DurableCouponProcessor
from .observability import ActivityLog
from .rules import RedisRuleLookup
from .serialization import JsonSerializer
from .topics import COUPONS_TOPIC, EVENTS_TOPIC


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
    processed = 0
    try:
        while True:
            try:
                processor.drain(publish)
                batches = consumer.poll(timeout_ms=1000, max_records=100)
                for partition, messages in batches.items():
                    for message in messages:
                        while True:
                            try:
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
                                    processor.drain(publish)
                                consumer.commit({TopicPartition(message.topic, message.partition):
                                    OffsetAndMetadata(message.offset + 1, '', -1)})
                                break
                            except (redis.RedisError, KafkaError):
                                time.sleep(1)
                        processed += 1
                        if args.max_events and processed >= args.max_events:
                            return
            except (redis.RedisError, KafkaError):
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close(timeout=5)
        consumer.close()


if __name__ == '__main__':
    main()
