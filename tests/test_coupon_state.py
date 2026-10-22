from pipeline.coupon_engine import CouponEngine, UserState


class Rules:
    def lookup(self, a, b):
        return {'support': .02, 'confidence': .3, 'lift': 2.1}


def event(kind, product='A', **extra):
    return dict(event_id=kind, event_type=kind, product_id=product, user_id='u', session_id='s',
                event_time='2026-10-22T12:00:00Z', **extra)


def finish(engine, state):
    engine.evaluate(event('product_view_started', 'B'), state)
    ended = event('product_view_ended', 'B', dwell_seconds=60)
    ended['event_time'] = '2026-10-22T12:01:00Z'
    return engine.evaluate(ended, state)


def test_removing_one_of_two_units_preserves_supporting_cart_evidence():
    engine = CouponEngine(Rules()); state = UserState()
    engine.evaluate(event('cart_item_added', quantity=2), state)
    engine.evaluate(event('cart_item_removed'), state)
    decision, reason = finish(engine, state)
    assert reason == 'coupon_issued'
    assert decision['supporting_cart_item'] == 'A'


def test_dwell_requires_a_matching_start():
    engine = CouponEngine(Rules()); state = UserState(cart={'A': 1})
    assert engine.evaluate(event('product_view_ended', 'B', dwell_seconds=60), state)[1] == 'missing_view_start'


def test_forged_dwell_cannot_exceed_elapsed_view_time():
    engine = CouponEngine(Rules()); state = UserState(cart={'A': 1})
    engine.evaluate(event('product_view_started', 'B'), state)
    assert engine.evaluate(event('product_view_ended', 'B', dwell_seconds=60), state)[1] == 'below_dwell_threshold'
