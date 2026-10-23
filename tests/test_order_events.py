import json

import pytest

from app import main
from app.order_store import OrderStore
from pipeline.live_rules import LiveRuleRepository


@pytest.fixture
def confirmed(tmp_path, monkeypatch):
    path = tmp_path / 'orders.sqlite3'
    monkeypatch.setitem(main.app.config, 'ORDER_STORE_PATH', str(path))
    monkeypatch.setitem(main.app.config, 'TESTING', True)
    class Ledger:
        def redeem(self, *args, **kwargs):
            return []
    monkeypatch.setattr(main, 'coupon_ledger', lambda: Ledger())
    with main.app.test_client() as browser:
        with browser.session_transaction() as session:
            session['user_id'] = 'order-shopper'; session['csrf_token'] = 'csrf'
        payload = {'order_id': 'BS-durable-order-123', 'user_id': 'order-shopper', 'session_id': 's',
            'name': 'Shopper', 'email': 'test@example.com', 'address': 'Demo', 'city': 'Demo',
            'postal_code': '000000', 'cart': [{'product_id': 24852, 'quantity': 2}], 'coupons': []}
        response = browser.post('/api/checkout', json=payload, headers={'X-CSRF-Token': 'csrf'})
        assert response.status_code == 201
        yield browser, OrderStore(path), response.get_json()['order'], payload


def test_confirmed_order_durably_enqueues_completion_without_browser_event(confirmed):
    browser, store, order, payload = confirmed
    sent = []
    assert store.drain_events(sent.append) == 1
    assert sent[0]['event_time'] == order['created_at']
    assert sent[0]['traffic_source'] == 'storefront'
    assert sent[0]['product_ids'] == ['Banana']
    assert sent[0]['catalog_product_ids'] == [24852]
    assert browser.post('/api/checkout', json=payload, headers={'X-CSRF-Token': 'csrf'}).status_code == 200
    assert store.drain_events(sent.append) == 0


def test_crash_after_send_retries_the_same_order_event_id(confirmed):
    _, store, _, _ = confirmed
    sent = []
    def crash(event):
        sent.append(event)
        raise SystemExit('delivery acknowledged after process crash')
    with pytest.raises(SystemExit):
        store.drain_events(crash)
    restarted = OrderStore(store.path)
    assert restarted.drain_events(sent.append) == 1
    assert len(sent) == 2 and sent[0] == sent[1]
    assert restarted.drain_events(sent.append) == 0


def test_late_browser_completion_cannot_redate_an_old_purchase_or_spoof_source(confirmed, monkeypatch):
    browser, store, order, _ = confirmed
    order['created_at'] = '2020-01-01T00:00:00Z'
    with store.transaction() as connection:
        connection.execute('UPDATE orders SET payload=? WHERE order_id=?', (json.dumps(order), order['order_id']))
    sent = []
    class Future:
        def get(self, **kwargs):
            return None
    class Producer:
        def send(self, topic, **kwargs):
            sent.append(kwargs['value']); return Future()
    monkeypatch.setattr(main, 'event_producer', lambda: Producer())
    monkeypatch.setattr(main, '_event_retry_after', 0)
    response = browser.post('/api/events', json={
        'event_id': 'forged', 'event_type': 'checkout_completed', 'event_time': '2026-10-23T12:00:00Z',
        'user_id': order['user_id'], 'session_id': 's', 'order_id': order['order_id'],
        'product_ids': ['forged'], 'traffic_source': 'synthetic'}, headers={'X-CSRF-Token': 'csrf'})
    assert response.status_code == 202
    assert sent[0]['event_time'] == order['created_at']
    assert sent[0]['traffic_source'] == 'storefront'
    assert sent[0]['product_ids'] == ['Banana']
    assert LiveRuleRepository(None, {'Banana': 24852}).ingest(sent[0]) == 'outside_window'
