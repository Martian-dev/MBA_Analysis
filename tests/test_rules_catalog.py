import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app import main
from pipeline.adaptive_redis import RedisFeatureStore, create_redis_adaptive_service
from tests.test_shared_adaptive import MemoryRedis


@pytest.fixture
def client():
    main.app.config['TESTING'] = True
    with main.app.test_client() as client:
        yield client


def test_rules_page_links_and_default_catalog_only_show_eligible_directed_pairs(client):
    assert client.get('/rules').status_code == 200
    assert b'href="/rules"' in client.get('/').data
    data = client.get('/api/rules?per_page=50').get_json()
    assert data['total'] == data['eligible_total'] > 0
    assert data['historical_total'] == len(main.rules_df)
    assert data['live_rule_mining_enabled'] is False
    assert data['snapshot_version'] == main.rules_snapshot_version
    for rule in data['rules']:
        assert rule['coupon_eligible'] is True
        assert rule['failed_thresholds'] == []
        for metric in ('lift', 'support', 'confidence'):
            assert rule[metric] >= data['policy']['min_' + metric]
        assert client.get('/product/' + str(rule['antecedent']['product_id'])).status_code == 200
        assert client.get('/product/' + str(rule['consequent']['product_id'])).status_code == 200


def test_all_rules_explain_the_actual_yellow_onion_rejection(client):
    data = client.get('/api/rules?eligible=false&q=Yellow%20Onions&per_page=50').get_json()
    rule = next(row for row in data['rules'] if row['antecedent']['product_name'] == 'Large Lemon'
                and row['consequent']['product_name'] == 'Yellow Onions')
    assert rule['coupon_eligible'] is False
    assert rule['failed_thresholds'] == ['support']
    assert rule['lift'] > 4


def test_filters_sort_and_pagination_are_consistent(client):
    first = client.get('/api/rules?eligible=false&sort=confidence&per_page=3').get_json()
    second = client.get('/api/rules?eligible=false&sort=confidence&per_page=3&page=2').get_json()
    assert first['rules'][0]['confidence'] >= first['rules'][-1]['confidence'] >= second['rules'][0]['confidence']
    assert first['pagination']['page'] == 1 and second['pagination']['page'] == 2
    assert first['rules'] != second['rules']
    empty = client.get('/api/rules?q=there-is-no-such-product').get_json()
    assert empty['rules'] == [] and empty['total'] == 0
    assert client.get('/api/rules?page=bad').status_code == 400
    assert client.get('/api/rules?eligible=maybe').status_code == 400
    assert client.get('/api/rules?sort=unsafe').status_code == 400


def test_listing_uses_the_same_configured_thresholds_as_the_consumer(client, monkeypatch):
    monkeypatch.setenv('MIN_SUPPORT', '.02')
    data = client.get('/api/rules').get_json()
    assert data['policy']['min_support'] == .02
    assert all(rule['support'] >= .02 for rule in data['rules'])


def test_live_inspection_is_aggregate_read_only_and_distinguishes_legacy_names(client, monkeypatch):
    redis = MemoryRedis()
    service = create_redis_adaptive_service(redis)
    store = service.features
    product_id = '24852'
    redis.set(store._value_key(store._positive, product_id), 2.5)
    redis.set(store._value_key(store._impressions, product_id), 4)
    redis.set(store._value_key(store._positive, 'Banana'), 10)
    redis.set(f'{service.namespace}:bandit:updates', 7)
    monkeypatch.setattr(main, 'adaptive_service', service)
    before = dict(redis.values)
    data = client.get('/api/rules/live').get_json()
    assert data['status'] == 'live'
    assert data['live_rule_mining_enabled'] is False
    assert data['feedback_updates'] == 7
    assert data['products'] == [{'product_id': 24852, 'product_name': 'Banana', 'impressions': 4,
                                'positive_weight': 2.5, 'legacy_positive_weight': 10}]
    assert redis.values == before
    redis.set(store._value_key(store._positive, product_id), 3.5)
    assert client.get('/api/rules/live').get_json()['products'][0]['positive_weight'] == 3.5
    redis.set('secret:user:identity', 'private')
    assert 'private' not in client.get('/api/rules/live').get_data(as_text=True)


def test_live_unavailable_does_not_hide_historical_rules(client, monkeypatch):
    from pipeline.adaptive import create_adaptive_service
    monkeypatch.setattr(main, 'adaptive_service', create_adaptive_service())
    assert client.get('/api/rules/live').get_json()['status'] == 'not_shared'
    redis = MemoryRedis()
    service = create_redis_adaptive_service(redis)
    monkeypatch.setattr(main, 'adaptive_service', service)
    def unavailable(*args):
        raise RedisConnectionError('unavailable')
    monkeypatch.setattr(redis, 'mget', unavailable)
    assert client.get('/api/rules/live').status_code == 503
    assert client.get('/api/rules').status_code == 200
