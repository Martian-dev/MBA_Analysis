"""Poll-bounded retries for durable, replay-safe record handlers."""
import time

from kafka import TopicPartition
from kafka.errors import CommitFailedError, KafkaError
from kafka.structs import OffsetAndMetadata
from redis import RedisError


def consume(consumer, handle, *, idle=lambda: None, max_events=0, clock=time.monotonic, sleep=time.sleep):
    processed = 0
    while True:
        try:
            batches = consumer.poll(timeout_ms=1000, max_records=1)
        except KafkaError:
            sleep(1); continue
        for partition, messages in batches.items():
            for message in messages:
                deadline = clock() + consumer.config.get('max_poll_interval_ms', 300_000) / 2000
                while True:
                    try:
                        handle(message)
                        consumer.commit({TopicPartition(message.topic, message.partition):
                            OffsetAndMetadata(message.offset + 1, '', -1)})
                        processed += 1
                        if max_events and processed >= max_events:
                            return
                        break
                    except (RedisError, KafkaError) as exc:
                        if isinstance(exc, CommitFailedError) or clock() >= deadline:
                            # Return to poll so group membership can recover. The
                            # durable handler safely tolerates this record again.
                            if partition in consumer.assignment():
                                consumer.seek(partition, message.offset)
                            break
                        sleep(1)
        try:
            idle()
        except (RedisError, KafkaError):
            # Poll even during an extended backend outage; do not strand the group.
            sleep(1)
