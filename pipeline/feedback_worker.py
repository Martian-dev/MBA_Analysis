"""Replay-safe live feature ingestion and timed zero-reward settlement."""
import os

from kafka import KafkaConsumer
from redis import Redis, RedisError

from .adaptive_redis import create_redis_adaptive_service
from .adaptive_streaming import _canonical_adaptive_payload
from .consumer import decode_event
from .observability import ActivityLog
from .topics import EVENTS_TOPIC
from .kafka_worker import consume


def main():
    client = Redis.from_url(os.getenv('ADAPTIVE_REDIS_URL', os.getenv('REDIS_URL', 'redis://localhost:6379/0')), decode_responses=True)
    service = create_redis_adaptive_service(client)
    consumer = KafkaConsumer(EVENTS_TOPIC, bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id='mba-adaptive-feedback-v2', enable_auto_commit=False, auto_offset_reset='earliest')
    def handle(message):
        try:
            payload = _canonical_adaptive_payload(decode_event(message.value))
        except (ValueError, TypeError) as exc:
            ActivityLog(client).record('adaptive_event_rejected', {'reason': str(exc), 'offset': message.offset})
        else:
            result = service.observe_event(payload)
            if result.status == 'state_unavailable':
                raise RedisError('Feedback remains retryable')
    try:
        consume(consumer, handle, idle=service.settle_expired)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
