import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pytest
from redis import Redis
from redis.exceptions import RedisError

from pipeline.coupon_engine import CouponEngine, UserState
from pipeline.live_rules import MiningPolicy, LiveRuleRepository, mine_rules, serving_live_rules
from pipeline.live_simulator import SimulationCatalog, synthetic_purchase
from pipeline.rules import RedisRuleLookup


def transactions():
    # Each chronological half: 25 AB, 5 A, 5 B, 65 C. The denominator includes
    # singleton/background baskets and the two halves have disjoint shoppers.
    return [{'order_id': f'o-{i}', 'user_id': f'u-{i}', 'timestamp': 1000 + i,
        'products': ['A', 'B'] if i % 100 < 25 else ['A'] if i % 100 < 30
        else ['B'] if i % 100 < 35 else ['C']} for i in range(200)]


def test_metrics_are_computed_from_all_completed_baskets_and_later_holdout():
    result = mine_rules(transactions(), 'storefront', MiningPolicy())
    assert result['independent_shoppers'] == 200
    assert result['discovery_transactions'] == result['validation_transactions'] == 100
    assert len(result['rules']) == 2
    rule = next(row for row in result['rules'] if row['antecedents'] == 'A')
    assert rule['support'] == .25
    assert rule['confidence'] == pytest.approx(25 / 30)
    assert rule['lift'] == pytest.approx(.25 / (.3 * .3))
    assert rule['validation']['pair_count'] == 25
    assert rule['adjusted_p_value'] < .05
    assert rule['source'] == 'live_storefront'
    assert result['version'] == mine_rules(transactions(), 'storefront', MiningPolicy())['version']


def test_discovery_only_pattern_does_not_pass_later_validation():
    rows = transactions()
    for row in rows[100:]:
        row['products'] = ['A'] if row['timestamp'] % 2 else ['B']
    assert mine_rules(rows, 'storefront', MiningPolicy())['rules'] == []


@pytest.mark.parametrize('case', ['one_shopper', 'same_order', 'too_few', 'ubiquitous_pair'])
def test_sparse_repeated_or_non_associated_evidence_cannot_publish(case):
    rows = transactions()
    if case == 'one_shopper':
        rows = [{**row, 'user_id': 'one-user'} for row in rows]
    elif case == 'same_order':
        rows = [{**row, 'order_id': 'one-order'} for row in rows]
    elif case == 'too_few':
        rows = rows[:199]
    else:
        rows = [{**row, 'products': ['A', 'B']} for row in rows]
    assert mine_rules(rows, 'storefront', MiningPolicy())['rules'] == []


def test_duplicate_line_items_do_not_increase_pair_counts():
    rows = [{**row, 'products': row['products'] * 3} for row in transactions()]
    result = mine_rules(rows, 'storefront', MiningPolicy())
    assert result['rules'][0]['validation']['pair_count'] == 25


def test_synthetic_completed_orders_are_labeled_and_do_not_copy_rule_metrics():
    catalog = SimulationCatalog.from_csv('app/rules.csv')
    orders = [synthetic_purchase(catalog, i, random.Random(i), 'test') for i in range(300)]
    assert all(order['traffic_source'] == 'synthetic' and order['event_type'] == 'checkout_completed' for order in orders)
    assert len({order['user_id'] for order in orders}) == 300
    assert all('lift' not in order and 'support' not in order for order in orders)
    assert all(len(order['product_ids']) == len(order['catalog_product_ids']) for order in orders)


@pytest.fixture
def client():
    client = Redis.from_url(os.getenv('REDIS_TEST_URL', 'redis://localhost:6388/15'), decode_responses=True)
    try:
        client.ping()
    except RedisError:
        pytest.skip('Set REDIS_TEST_URL to an isolated Redis for live-miner acceptance')
    before = set(client.scan_iter())
    yield client
    created = set(client.scan_iter()) - before
    if created:
        client.delete(*created)


def completed(row, source='storefront'):
    return {'event_id': uuid.uuid4().hex, 'event_type': 'checkout_completed',
        'event_time': datetime.fromtimestamp(row['timestamp'], timezone.utc).isoformat(),
        'order_id': row['order_id'], 'user_id': row['user_id'], 'session_id': 's',
        'product_ids': row['products'], 'traffic_source': source}


def test_real_redis_replay_restart_source_isolation_publication_and_coupon_lookup(client, monkeypatch):
    monkeypatch.setenv('LIVE_RULE_MINING_ENABLED', '1')
    monkeypatch.setenv('LIVE_RULES_ALLOW_SYNTHETIC', '0')
    now = [1201]
    repository = LiveRuleRepository(client, {'A': 1, 'B': 2, 'C': 3}, clock=lambda: now[0])
    rows = transactions()
    for row in rows:
        action = completed(row)
        assert repository.ingest(action) == 'accepted'
        assert repository.ingest({**action, 'event_id': uuid.uuid4().hex}) == 'duplicate_order'
    restarted = LiveRuleRepository(client, repository.products, clock=lambda: now[0])
    snapshots = restarted.refresh()
    real = next(snapshot for snapshot in snapshots if snapshot['source'] == 'live_storefront')
    synthetic = next(snapshot for snapshot in snapshots if snapshot['source'] == 'live_synthetic')
    assert len(real['rules']) == 2 and synthetic['rules'] == []
    assert real['independent_shoppers'] == 200
    rule = RedisRuleLookup(client).lookup('A', 'B')
    assert rule['source'] == 'live_storefront'
    decision, reason = CouponEngine(RedisRuleLookup(client)).evaluate({
        'event_id': uuid.uuid4().hex, 'event_type': 'recommendation_clicked',
        'event_time': '2026-10-23T12:00:00Z', 'user_id': 'new-shopper', 'session_id': 's', 'product_id': 'B',
    }, UserState(cart={'A': 1}))
    assert reason == 'coupon_issued' and decision['rule_source'] == 'live_storefront'
    assert decision['rule_version'] == real['version']
    now[0] += 86401
    restarted.refresh()
    assert serving_live_rules(client) == []


def test_sources_do_not_combine_to_meet_sample_gate_and_synthetic_serving_is_explicit(client, monkeypatch):
    repository = LiveRuleRepository(client, {'A': 1, 'B': 2, 'C': 3}, clock=lambda: 1201)
    rows = transactions()
    for row in rows[:100]:
        repository.ingest(completed(row))
    for row in rows[100:]:
        repository.ingest(completed(row, 'synthetic'))
    assert all(snapshot['rules'] == [] for snapshot in repository.refresh())
    for row in rows[:100]:
        repository.ingest(completed(row, 'synthetic'))
    repository.refresh()
    monkeypatch.setenv('LIVE_RULES_ALLOW_SYNTHETIC', '0')
    assert serving_live_rules(client) == []
    monkeypatch.setenv('LIVE_RULES_ALLOW_SYNTHETIC', '1')
    assert len(serving_live_rules(client)) == 2


def test_views_unknown_products_unattributed_orders_and_old_transactions_do_not_enter_training(client):
    repository = LiveRuleRepository(client, {'A': 1, 'B': 2, 'C': 3}, clock=lambda: 100000)
    action = completed(transactions()[0])
    assert repository.ingest({**action, 'event_type': 'product_view_started', 'product_id': 'A'}) == 'ignored'
    assert repository.ingest({**action, 'product_ids': ['unknown']}) == 'invalid_basket'
    assert repository.ingest({**action, 'traffic_source': 'untrusted'}) == 'unattributed_transaction'
    assert repository.ingest({**action, 'event_time': '1970-01-01T00:00:00Z'}) == 'outside_window'


@pytest.mark.skipif(not os.getenv('KAFKA_TEST_BOOTSTRAP'), reason='Set KAFKA_TEST_BOOTSTRAP for real mining acceptance')
def test_kafka_completed_orders_produce_new_visible_and_servable_rules(client, monkeypatch):
    import subprocess
    import sys
    from pathlib import Path
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from app import main

    broker = os.environ['KAFKA_TEST_BOOTSTRAP']
    token = uuid.uuid4().hex
    topic = 'audit-mining-' + token
    admin = KafkaAdminClient(bootstrap_servers=broker)
    producer = worker = None
    names = {'A': 'English Muffins', 'B': 'Organic Raspberries', 'C': 'Banana'}
    assert main.rules_df[(main.rules_df.antecedents == names['A']) & (main.rules_df.consequents == names['B'])].empty
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        producer = KafkaProducer(bootstrap_servers=broker)
        now = time.time()
        actions = []
        for index, row in enumerate(transactions()):
            row = {**row, 'order_id': token + row['order_id'], 'user_id': token + row['user_id'],
                   'timestamp': now - 201 + index, 'products': [names[item] for item in row['products']]}
            actions.append(completed(row, 'synthetic'))
        for action in actions + [actions[0]]:
            producer.send(topic, key=token.encode(), value=json.dumps(action).encode()).get(timeout=10)
        producer.send(topic, key=token.encode(), value=b'poison').get(timeout=10)
        environment = {**os.environ, 'EVENTS_TOPIC': topic, 'KAFKA_BOOTSTRAP_SERVERS': broker,
            'REDIS_URL': os.environ['REDIS_TEST_URL']}
        worker = subprocess.Popen([sys.executable, '-m', 'pipeline.live_rule_worker',
            '--group-id', 'audit-miner-' + token, '--max-events', '202'],
            cwd=Path(__file__).parents[1], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = worker.communicate(timeout=45)
        assert worker.returncode == 0, stderr.decode()
        monkeypatch.setenv('LIVE_RULE_MINING_ENABLED', '1')
        monkeypatch.setenv('LIVE_RULES_ALLOW_SYNTHETIC', '1')
        monkeypatch.setattr(main, 'rule_snapshot_client', lambda: client)
        with main.app.test_client() as browser:
            data = browser.get('/api/rules?source=live_synthetic').get_json()
            assert data['live_rule_mining_enabled'] is True
            assert data['total'] == 2
            rule = next(rule for rule in data['rules'] if rule['antecedent']['product_id'] == 116)
            assert rule['consequent']['product_id'] == 27966
            assert rule['validation']['pair_count'] == 25
            assert rule['active_for_serving'] is True
            assert rule['version'].startswith('live-')
            assert 'user_id' not in json.dumps(data)
            detail = browser.get('/api/products/116').get_json()
            assert any(item['product_id'] == 27966 and item['rule_source'] == 'live_synthetic'
                       for item in detail['related_products'])
        assert RedisRuleLookup(client).lookup(names['A'], names['B'])['source'] == 'live_synthetic'
        assert '"rule_count": 2' in stdout.decode()
    finally:
        if worker and worker.poll() is None:
            worker.terminate(); worker.wait(timeout=10)
        if producer:
            producer.close()
        admin.delete_topics([topic]); admin.close()
