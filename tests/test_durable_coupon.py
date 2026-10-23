import json
import os
import subprocess
import time
import uuid

import pytest
import redis

from pipeline.consumer import decode_event
from pipeline.durable_coupon import DurableCouponProcessor
from pipeline.observability import CouponLedger


@pytest.fixture
def client():
    client = redis.Redis.from_url(os.getenv('REDIS_TEST_URL', 'redis://localhost:6388/15'), decode_responses=True)
    try:
        client.ping()
    except redis.RedisError:
        pytest.skip('Run an isolated Redis and set REDIS_TEST_URL for durability tests')
    before = set(client.scan_iter())
    yield client
    created = set(client.scan_iter()) - before
    if created:
        client.delete(*created)


class Rules:
    def lookup(self, a, b):
        return {'support': .02, 'confidence': .3, 'lift': 2.1}

    def consequents(self, antecedent):
        return {'B': self.lookup(antecedent, 'B')} if antecedent == 'A' else {}


def events():
    user = str(uuid.uuid4())
    base = dict(event_time='2026-10-22T12:00:00Z', user_id=user, session_id='s', product_id='A')
    return [
        {**base, 'event_id': user+'-cart', 'event_type': 'cart_item_added', 'quantity': 2},
        {**base, 'event_id': user+'-remove', 'event_type': 'cart_item_removed'},
        {**base, 'event_id': user+'-start', 'event_type': 'product_view_started', 'product_id': 'B'},
        {**base, 'event_id': user+'-end', 'event_type': 'product_view_ended', 'product_id': 'B',
         'event_time': '2026-10-22T12:01:00Z', 'dwell_seconds': 60},
    ]


def test_cart_survives_restart_and_replayed_quantity_events_are_idempotent(client):
    trace = events()
    for event in trace[:-1]:
        DurableCouponProcessor(client, Rules()).process(event)
        DurableCouponProcessor(client, Rules()).process(event)
    decision, reason = DurableCouponProcessor(client, Rules()).process(trace[-1])
    assert reason == 'coupon_issued'
    available = CouponLedger(client).available(user_id=trace[0]['user_id'], session_id='s')
    assert len(available) == 1
    assert available[0]['coupon_id'] == decision['coupon_id']


def test_crash_after_commit_before_delivery_recovers_one_logical_coupon(client):
    trace = events()
    first = DurableCouponProcessor(client, Rules())
    for event in trace:
        decision, _ = first.process(event)
    sent = []
    def crash_after_send(coupon):
        sent.append(coupon['coupon_id'])
        raise SystemExit('process killed before outbox acknowledgement')
    with pytest.raises(SystemExit):
        first.drain(crash_after_send)
    restarted = DurableCouponProcessor(client, Rules())
    assert restarted.process(trace[-1])[0] == decision
    restarted.drain(lambda coupon: sent.append(coupon['coupon_id']))
    assert sent == [decision['coupon_id'], decision['coupon_id']]
    assert restarted.drain(lambda coupon: pytest.fail('outbox already acknowledged')) == 0
    assert len(CouponLedger(client).available(user_id=trace[0]['user_id'], session_id='s')) == 1


@pytest.mark.parametrize('raw', [b'no json', b'[]', b'{"event_type": []}', b'\xff', None])
def test_poison_records_are_decode_errors(raw):
    with pytest.raises(ValueError):
        decode_event(raw)


def test_oversized_numeric_dwell_is_a_poison_record_not_a_worker_crash():
    payload = events()[-1]
    payload['dwell_seconds'] = 10**1000
    with pytest.raises(ValueError):
        decode_event(json.dumps(payload).encode())


def test_zero_reward_settlement_survives_service_restart(client):
    from pipeline.adaptive_redis import create_redis_adaptive_service
    namespace = 'audit-outcomes:' + uuid.uuid4().hex
    now = [1000]
    first = create_redis_adaptive_service(client, namespace=namespace, clock=lambda: now[0], feedback_window_seconds=10)
    decision = first.rank_candidates(user_id='u', session_id='s', snapshot_version='v',
        candidates=[{'product_id': 'A', 'prior_score': .9}])
    first.observe_event(dict(event_id='expose', event_type='recommendation_impression', product_id='A',
        user_id='u', session_id='s', event_time='2026-10-22T12:00:00Z', impression_id=decision['impression_id']))
    now[0] += 11
    restarted = create_redis_adaptive_service(client, namespace=namespace, clock=lambda: now[0])
    assert restarted.settle_expired() == 1
    assert restarted.settle_expired() == 0


def test_cart_snapshot_rehydrates_a_new_session_without_duplicate_adds(client):
    trace = events()
    snapshot = {key: trace[0][key] for key in ('user_id', 'session_id', 'event_time')}
    snapshot.update(event_id=uuid.uuid4().hex, event_type='cart_synchronized', cart=[{'product_id': 'A', 'quantity': 2}])
    DurableCouponProcessor(client, Rules()).process(snapshot)
    DurableCouponProcessor(client, Rules()).process(trace[1])
    DurableCouponProcessor(client, Rules()).process(trace[2])
    assert DurableCouponProcessor(client, Rules()).process(trace[3])[1] == 'coupon_issued'


@pytest.mark.parametrize('trigger', ['basket_opened', 'recommendation_clicked'])
def test_new_triggers_commit_one_recoverable_coupon_and_deduplicate_across_paths(client, trigger):
    trace = events()
    DurableCouponProcessor(client, Rules()).process(trace[0])
    DurableCouponProcessor(client, Rules()).process(trace[2])
    action = {key: trace[0][key] for key in ('user_id', 'session_id', 'event_time')}
    action.update(event_id=uuid.uuid4().hex, event_type=trigger)
    if trigger == 'recommendation_clicked':
        action['product_id'] = 'B'
    first, reason = DurableCouponProcessor(client, Rules()).process(action)
    assert reason == 'coupon_issued'
    assert first['reason']['trigger'] == trigger
    assert DurableCouponProcessor(client, Rules()).process(action)[0] == first
    clicked = {**action, 'event_id': uuid.uuid4().hex, 'event_type': 'recommendation_clicked', 'product_id': 'B'}
    assert DurableCouponProcessor(client, Rules()).process(clicked)[1] == 'coupon_already_issued'
    for view in trace[2:]:
        result = DurableCouponProcessor(client, Rules()).process(view)
    assert result[1] == 'coupon_already_issued'
    assert len(CouponLedger(client).available(user_id=action['user_id'], session_id='s')) == 1
    delivered = []
    DurableCouponProcessor(client, Rules()).drain(lambda coupon: delivered.append(coupon['coupon_id']))
    assert delivered.count(first['coupon_id']) == 1


def test_basket_holdout_without_a_product_id_is_logged_without_issuing(client):
    trace = events()
    processor = DurableCouponProcessor(client, Rules(), holdout_fraction=1)
    processor.process(trace[0])
    processor.process(trace[2])
    action = {key: trace[0][key] for key in ('user_id', 'session_id', 'event_time')}
    action.update(event_id=uuid.uuid4().hex, event_type='basket_opened')
    assert processor.process(action) == (None, 'experiment_control')
    assert CouponLedger(client).available(user_id=action['user_id'], session_id='s') == []


@pytest.mark.skipif(not os.getenv('REDIS_TEST_CONTAINER'), reason='Set by scripts/verify_audit.sh for AOF restart acceptance')
def test_cart_and_view_survive_a_real_redis_container_restart(client):
    container = os.environ['REDIS_TEST_CONTAINER']
    assert container.startswith('mba-verification-redis-'), 'Only restart a verification-owned Redis container'
    trace = events()
    for event in trace[:-1]:
        DurableCouponProcessor(client, Rules()).process(event)
    subprocess.run(['docker', 'restart', container], check=True, capture_output=True, timeout=20)
    for _ in range(100):
        try:
            client.ping(); break
        except redis.RedisError:
            time.sleep(.05)
    assert DurableCouponProcessor(client, Rules()).process(trace[-1])[1] == 'coupon_issued'
