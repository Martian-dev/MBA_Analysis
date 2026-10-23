"""Replay-safe delivery of server-confirmed orders from SQLite to Kafka."""
import os
from pathlib import Path
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.order_store import OrderStore
from .serialization import JsonSerializer
from .topics import EVENTS_TOPIC


def main():
    default = Path(__file__).resolve().parents[1] / '.runtime/marketplace/orders.jsonl'
    store = OrderStore(os.getenv('ORDER_STORE_PATH', str(default)))
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        value_serializer=JsonSerializer())
    def publish(event):
        producer.send(EVENTS_TOPIC, key=event['user_id'].encode(), value=event).get(timeout=10)
    try:
        while True:
            try:
                store.drain_events(publish)
            except KafkaError:
                pass  # The saved order remains queued until Kafka acknowledges it.
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == '__main__':
    main()
