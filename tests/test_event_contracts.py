import pytest

from pipeline.contracts import validate_event
from pipeline.coupon_engine import CouponEngine


class NoRules:
    def lookup(self, antecedent, consequent):
        return None


def base_event(event_type, **extra):
    return {
        "event_id": f"event-{event_type}",
        "event_type": event_type,
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        **extra,
    }


@pytest.mark.parametrize(
    ("event_type", "extra"),
    [
        ("search_performed", {"query": "avocado"}),
        ("basket_opened", {}),
        ("recommendation_impression", {"product_id": "p1", "impression_id": "imp-1"}),
        ("recommendation_clicked", {"product_id": "p1", "impression_id": "imp-1"}),
        ("coupon_applied", {"product_id": "p1", "coupon_id": "coupon-1"}),
        ("checkout_started", {"order_id": "order-1", "product_ids": ["p1"]}),
        ("checkout_completed", {"order_id": "order-1", "product_ids": ["p1", "p2"]}),
    ],
)
def test_marketplace_events_have_conditional_contracts(event_type, extra):
    payload = base_event(event_type, **extra)
    assert validate_event(payload) is payload


def test_search_requires_a_query_instead_of_a_product():
    with pytest.raises(ValueError, match="query"):
        validate_event(base_event("search_performed"))


def test_recommendation_requires_a_product():
    with pytest.raises(ValueError, match="product_id"):
        validate_event(base_event("recommendation_clicked", impression_id="imp-1"))


def test_checkout_requires_order_and_accepts_no_product():
    payload = base_event("checkout_completed", order_id="order-1")
    assert validate_event(payload) is payload


def test_coupon_engine_ignores_supported_non_policy_events():
    decision, reason = CouponEngine(NoRules()).evaluate(
        base_event("checkout_completed", order_id="order-1", product_ids=["p1"])
    )
    assert decision is None
    assert reason == "ignored"
