"""Replay-safe live feature ingestion and timed zero-reward settlement."""
import json
import os
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata
from redis import Redis, RedisError

from .adaptive_redis import create_redis_adaptive_service
from .adaptive_streaming import _canonical_adaptive_payload
from .consumer import decode_event
from .observability import ActivityLog
from .topics import EVENTS_TOPIC


def main():
    client = Redis.from_url(os.getenv('ADAPTIVE_REDIS_URL', os.getenv('REDIS_URL', 'redis://localhost:6379/0')), decode_responses=True)
    service = create_redis_adaptive_service(client)
    consumer = KafkaConsumer(EVENTS_TOPIC, bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id='mba-adaptive-feedback-v2', enable_auto_commit=False, auto_offset_reset='earliest')
    try:
        while True:
            try:
                service.settle_expired()
                for partition, messages in consumer.poll(timeout_ms=1000, max_records=100).items():
                    for message in messages:
                        while True:
                            try:
                                try:
                                    payload = _canonical_adaptive_payload(decode_event(message.value))
                                except (ValueError, TypeError) as exc:
                                    ActivityLog(client).record('adaptive_event_rejected', {'reason': str(exc), 'offset': message.offset})
                                else:
                                    result = service.observe_event(payload)
                                    if result.status == 'state_unavailable':
                                        raise RedisError('Feedback remains retryable')
                                consumer.commit({TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, '', -1)})
                                break
                            except (RedisError, KafkaError):
                                time.sleep(1)
            except (RedisError, KafkaError):
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
