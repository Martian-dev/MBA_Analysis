"""Owned browser session -> HTTP -> Kafka -> durable coupon -> checkout."""
import csv
import http.cookiejar
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit
import uuid

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from redis import Redis

from pipeline.rules import load_rules


@pytest.mark.skipif(not os.getenv('KAFKA_TEST_BOOTSTRAP'), reason='Set KAFKA_TEST_BOOTSTRAP for HTTP acceptance')
@pytest.mark.parametrize('trigger', ['product_view_ended', 'basket_opened', 'recommendation_clicked'])
def test_owned_shopper_receives_and_redeems_live_coupon(tmp_path, trigger):
    broker = os.environ['KAFKA_TEST_BOOTSTRAP']; token = uuid.uuid4().hex
    parts = urlsplit(os.getenv('REDIS_TEST_URL', 'redis://localhost:6388/15'))
    redis_url = urlunsplit(parts._replace(path='/13'))
    client = Redis.from_url(redis_url, decode_responses=True)
    before = set(client.scan_iter())
    events, coupons = 'audit-http-events-' + token, 'audit-http-coupons-' + token
    admin = KafkaAdminClient(bootstrap_servers=broker)
    server = None; worker = None; miner = None
    project = Path(__file__).parents[1]
    browser = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    with socket.socket() as port_probe:
        port_probe.bind(('127.0.0.1', 0))
        port = port_probe.getsockname()[1]
    origin = f'http://127.0.0.1:{port}'
    def request(path, payload=None, csrf=None):
        data = None if payload is None else json.dumps(payload).encode()
        headers = {'Content-Type': 'application/json'}
        if csrf: headers['X-CSRF-Token'] = csrf
        with browser.open(urllib.request.Request(origin + path, data=data, headers=headers), timeout=10) as response:
            return response.status, response.read().decode()
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1) for name in (events, coupons)])
        load_rules(project / 'app/rules.csv', redis_url)
        env = {**os.environ, 'KAFKA_BOOTSTRAP_SERVERS': broker, 'REDIS_URL': redis_url,
            'ADAPTIVE_REDIS_URL': redis_url, 'EVENTS_TOPIC': events, 'COUPONS_TOPIC': coupons,
            'DWELL_THRESHOLD_SECONDS': '.1', 'RULES_PATH': str(project / 'app/rules.csv'),
            'ORDER_STORE_PATH': str(tmp_path / 'orders.sqlite3')}
        server = subprocess.Popen([sys.executable, '-m', 'flask', '--app', 'app.main', 'run', '--host', '127.0.0.1', '--port', str(port)],
            cwd=project, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        for _ in range(100):
            try:
                _, html = request('/')
                break
            except (urllib.error.URLError, ConnectionError):
                if server.poll() is not None: pytest.fail(server.stderr.read().decode())
                time.sleep(.05)
        else: pytest.fail('HTTP server did not become ready')
        user = re.search('data-shopper-id="([^"]+)"', html).group(1)
        csrf = re.search('data-csrf-token="([^"]+)"', html).group(1)
        with (project / 'app/rules.csv').open() as handle:
            qualifying = [row for row in csv.DictReader(handle) if float(row['support']) >= .012 and float(row['confidence']) >= .1 and float(row['lift']) >= 1.5]
        antecedent = qualifying[0]['antecedents']
        rule = min((row for row in qualifying if row['antecedents'] == antecedent),
            key=lambda row: (-float(row['confidence']) * math.log1p(float(row['lift'])), row['consequents']))
        event_count = '2' if trigger == 'recommendation_clicked' else '3'
        worker = subprocess.Popen([sys.executable, '-m', 'pipeline.consumer', '--group-id', 'audit-http-' + token, '--max-events', event_count],
            cwd=project, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        base = dict(user_id=user, session_id='s', event_time='2026-10-22T12:00:00Z')
        assert request('/api/events', {**base, 'event_id': uuid.uuid4().hex, 'event_type': 'cart_item_added',
            'product_id': rule['antecedents']}, csrf)[0] == 202
        if trigger in {'product_view_ended', 'basket_opened'}:
            assert request('/api/events', {**base, 'event_id': uuid.uuid4().hex, 'event_type': 'product_view_started',
                'product_id': rule['consequents']}, csrf)[0] == 202
        if trigger == 'product_view_ended':
            time.sleep(.15)
        action = dict(event_id=uuid.uuid4().hex, event_type=trigger)
        if trigger != 'basket_opened':
            action['product_id'] = rule['consequents']
        if trigger == 'product_view_ended':
            action['dwell_seconds'] = 1
        assert request('/api/events', {**base, **action}, csrf)[0] == 202
        stdout, stderr = worker.communicate(timeout=30)
        assert worker.returncode == 0, stderr.decode()
        assert json.loads(stdout.decode().splitlines()[-1])['reason']['trigger'] == trigger
        status, body = request('/api/coupons/available?user_id=' + user + '&session_id=s')
        available = json.loads(body)['coupons']; assert len(available) == 1
        order = dict(order_id='BS-' + token, user_id=user, session_id='s', name='Audit Shopper',
            email='audit@example.com', address='Demo Street', city='Demo City', postal_code='000000',
            cart=[{'product_id': int(rule['consequent_product_id']), 'quantity': 1}], coupons=available)
        status, body = request('/api/checkout', order, csrf)
        confirmed = json.loads(body)['order']
        assert status == 201 and confirmed['savings_inr'] > 0
        assert request('/api/checkout', order, csrf)[0] == 200
        assert json.loads(request('/api/coupons/available?user_id=' + user + '&session_id=s')[1])['coupons'] == []
        # No browser completion event: the committed order itself owns delivery.
        from app.order_store import OrderStore
        from kafka import KafkaProducer
        from pipeline.live_rules import live_snapshots
        producer = KafkaProducer(bootstrap_servers=broker)
        def publish_completion(event):
            producer.send(events, key=user.encode(), value=json.dumps(event).encode()).get(timeout=10)
        try:
            assert OrderStore(tmp_path / 'orders.sqlite3').drain_events(publish_completion) == 1
        finally:
            producer.close()
        miner = subprocess.Popen([sys.executable, '-m', 'pipeline.live_rule_worker', '--group-id',
            'audit-order-miner-' + token, '--max-events', str(int(event_count) + 1)],
            cwd=project, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        _, mining_errors = miner.communicate(timeout=30)
        assert miner.returncode == 0, mining_errors.decode()
        cohort = next(item for item in live_snapshots(client) if item['source'] == 'live_storefront')
        assert cohort['independent_shoppers'] == 1
        assert cohort['rules'] == [], 'A single confirmed order must not fabricate a rule'
    finally:
        for process in (server, worker, miner):
            if process and process.poll() is None:
                process.terminate(); process.wait(timeout=10)
        admin.delete_topics([events, coupons]); admin.close()
        created = set(client.scan_iter()) - before
        if created: client.delete(*created)
