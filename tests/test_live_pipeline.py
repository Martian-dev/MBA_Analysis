"""Optional real Kafka + Redis acceptance; all topics are uniquely owned."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from redis import Redis

from pipeline.observability import ActivityLog, CouponLedger
from pipeline.serialization import JsonDeserializer


@pytest.mark.skipif(not os.getenv('KAFKA_TEST_BOOTSTRAP'), reason='Set KAFKA_TEST_BOOTSTRAP for real broker acceptance')
def test_poison_record_and_replay_preserve_one_redeemable_coupon():
    broker = os.environ['KAFKA_TEST_BOOTSTRAP']
    redis_url = os.environ.get('REDIS_TEST_URL', 'redis://localhost:6388/15')
    client = Redis.from_url(redis_url, decode_responses=True)
    before = set(client.scan_iter())
    token = uuid.uuid4().hex
    events_topic, coupons_topic = 'audit-events-' + token, 'audit-coupons-' + token
    admin = KafkaAdminClient(bootstrap_servers=broker)
    producer = None; worker = None; reader = None
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1) for name in (events_topic, coupons_topic)])
        # This rule belongs to the test user trace and is removed with the fixture.
        client.hset('mba:rules:v1:audit-A', 'audit-B', json.dumps({'lift': 2.1, 'support': .02, 'confidence': .3}))
        env = {**os.environ, 'REDIS_URL': redis_url, 'EVENTS_TOPIC': events_topic, 'COUPONS_TOPIC': coupons_topic}
        worker = subprocess.Popen([sys.executable, '-m', 'pipeline.consumer', '--bootstrap-servers', broker,
            '--group-id', 'audit-' + token, '--max-events', '7'], cwd=Path(__file__).parents[1],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        producer = KafkaProducer(bootstrap_servers=broker)
        base = dict(user_id=token, session_id='s', product_id='audit-A', event_time='2026-10-22T12:00:00Z')
        trace = [
            {**base, 'event_id': 'add', 'event_type': 'cart_item_added', 'quantity': 2},
            {**base, 'event_id': 'remove', 'event_type': 'cart_item_removed'},
            {**base, 'event_id': 'start', 'event_type': 'product_view_started', 'product_id': 'audit-B'},
            {**base, 'event_id': 'end', 'event_type': 'product_view_ended', 'product_id': 'audit-B',
             'event_time': '2026-10-22T12:01:00Z', 'dwell_seconds': 60},
        ]
        for event in trace + [trace[-1]]:
            producer.send(events_topic, key=token.encode(), value=json.dumps(event).encode()).get(timeout=10)
        producer.send(events_topic, key=token.encode(), value=b'not-json').get(timeout=10)
        producer.send(events_topic, key=token.encode(), value=None).get(timeout=10)
        stdout, stderr = worker.communicate(timeout=45)
        assert worker.returncode == 0, stderr
        available = CouponLedger(client).available(user_id=token, session_id='s')
        assert len(available) == 1
        assert any(event['kind'] == 'event_rejected' for event in ActivityLog(client).snapshot()['events'])
        reader = KafkaConsumer(coupons_topic, bootstrap_servers=broker, auto_offset_reset='earliest',
            consumer_timeout_ms=2000, value_deserializer=JsonDeserializer())
        decisions = [message.value for message in reader]
        assert len(decisions) == 1
        assert decisions[0]['coupon_id'] == available[0]['coupon_id']
    finally:
        if worker and worker.poll() is None:
            worker.terminate(); worker.wait(timeout=10)
        if producer:
            producer.close()
        if reader:
            reader.close()
        admin.delete_topics([events_topic, coupons_topic]); admin.close()
        created = set(client.scan_iter()) - before
        if created:
            client.delete(*created)
