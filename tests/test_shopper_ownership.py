from app import main


def identity(client):
    client.get('/')
    with client.session_transaction() as session:
        return session['user_id'], session['csrf_token']


def test_another_browser_cannot_read_coupons_or_events():
    with main.app.test_client() as owner, main.app.test_client() as other:
        user, _ = identity(owner)
        identity(other)
        assert other.get(f'/api/coupons/available?user_id={user}&session_id=s').status_code == 403
        assert other.get(f'/api/activity?user_id={user}&session_id=s').status_code == 403
        assert other.get(f'/api/activity/stream?user_id={user}&session_id=s').status_code == 403


def test_unscoped_coupon_lookup_is_limited_to_signed_shopper(monkeypatch):
    seen = []
    class Ledger:
        def available(self, **scope):
            seen.append(scope); return []
    monkeypatch.setattr(main, 'coupon_ledger', lambda: Ledger())
    with main.app.test_client() as client:
        user, _ = identity(client)
        assert client.get('/api/coupons/available').status_code == 200
        assert seen == [{'user_id': user, 'session_id': None}]


def test_writes_require_both_owned_identity_and_csrf():
    with main.app.test_client() as client:
        user, token = identity(client)
        event = dict(event_id='e', event_type='cart_item_added', event_time='2026-10-22T12:00:00Z',
                     user_id=user, session_id='s', product_id='Banana')
        assert client.post('/api/events', json=event).status_code == 403
        assert client.post('/api/events', json={**event, 'csrf_token': 'é'}).status_code == 403
        assert client.post('/api/events', json={**event, 'csrf_token': '\ud800'}).status_code == 403
        assert client.post('/api/events', json={**event, 'user_id': 'victim'},
                           headers={'X-CSRF-Token': token}).status_code == 403


def test_operations_view_requires_separate_operator_access():
    with main.app.test_client() as client:
        identity(client)
        assert client.get('/viewer').status_code == 403
        assert client.get('/viewer?token=wrong').status_code == 403
        assert client.get('/viewer?token=%C3%A9').status_code == 403
