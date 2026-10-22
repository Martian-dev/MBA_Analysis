from pipeline.coupon_engine import CouponEngine
from pipeline.live_simulator import SimulationCatalog, plan_session
import random


class FakeRules:
    def __init__(self, rules):
        self.rules = rules

    def lookup(self, antecedent, consequent):
        return self.rules.get((antecedent, consequent))


def event(event_type, product, **extra):
    return {
        "event_id": f"event-{event_type}-{product}",
        "event_type": event_type,
        "event_time": "2026-09-20T00:00:00+00:00",
        "user_id": "u1",
        "session_id": "s1",
        "product_id": product,
        **extra,
    }


def test_hesitation_on_associated_product_issues_coupon():
    engine = CouponEngine(
        FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}})
    )
    assert engine.process(event("cart_item_added", "avocado")) is None
    assert engine.process(event("product_view_started", "tortillas")) is None
    decision = engine.process(event("product_view_ended", "tortillas", dwell_seconds=46))
    assert decision["event_type"] == "coupon_issued"
    assert decision["discount_percent"] == 5
    assert decision["supporting_cart_item"] == "avocado"


def test_short_dwell_does_not_issue_coupon():
    engine = CouponEngine(FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}}))
    engine.process(event("cart_item_added", "avocado"))
    assert engine.process(event("product_view_ended", "tortillas", dwell_seconds=44)) is None


def test_product_already_in_cart_does_not_issue_coupon():
    engine = CouponEngine(FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}}))
    engine.process(event("cart_item_added", "avocado"))
    engine.process(event("cart_item_added", "tortillas"))
    assert engine.process(event("product_view_ended", "tortillas", dwell_seconds=60)) is None


def test_low_lift_does_not_issue_coupon():
    engine = CouponEngine(FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 1.49}}))
    engine.process(event("cart_item_added", "avocado"))
    assert engine.process(event("product_view_ended", "tortillas", dwell_seconds=60)) is None


def test_dwell_alone_is_not_enough_without_rule_confidence():
    engine = CouponEngine(
        FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.09, "lift": 2.1}})
    )
    engine.process(event("cart_item_added", "avocado"))
    decision, reason = engine.evaluate(event("product_view_ended", "tortillas", dwell_seconds=60))
    assert decision is None
    assert reason == "below_confidence_threshold"


def test_dwell_alone_is_not_enough_without_rule_support():
    engine = CouponEngine(
        FakeRules({("avocado", "tortillas"): {"support": 0.011, "confidence": 0.3, "lift": 2.1}})
    )
    engine.process(event("cart_item_added", "avocado"))
    decision, reason = engine.evaluate(event("product_view_ended", "tortillas", dwell_seconds=60))
    assert decision is None
    assert reason == "below_support_threshold"


def test_coupon_is_idempotent_for_user_and_product():
    engine = CouponEngine(FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}}))
    engine.process(event("cart_item_added", "avocado"))
    first = engine.process(event("product_view_ended", "tortillas", dwell_seconds=60))
    second = engine.process({**event("product_view_ended", "tortillas", dwell_seconds=60), "event_id": "second"})
    assert first is not None
    assert second is None


def test_coupon_guardrail_resets_for_a_new_session():
    engine = CouponEngine(
        FakeRules({("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}})
    )
    engine.process(event("cart_item_added", "avocado"))
    first = engine.process(event("product_view_ended", "tortillas", dwell_seconds=60))
    engine.process({**event("cart_item_added", "avocado"), "session_id": "s2"})
    second = engine.process({
        **event("product_view_ended", "tortillas", dwell_seconds=60),
        "event_id": "second-session-event",
        "session_id": "s2",
    })
    assert first is not None
    assert second is not None
    assert second["session_id"] == "s2"


def test_coupon_guardrail_is_shared_across_engine_restarts_for_one_session():
    class SharedClaims:
        def __init__(self):
            self.claimed = set()

        def claim(self, coupon_claim):
            if coupon_claim in self.claimed:
                return False
            self.claimed.add(coupon_claim)
            return True

        def release(self, coupon_claim):
            self.claimed.discard(coupon_claim)

    rules = FakeRules({
        ("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}
    })
    claims = SharedClaims()
    first_engine = CouponEngine(rules, claims=claims)
    second_engine = CouponEngine(rules, claims=claims)
    first_engine.process(event("cart_item_added", "avocado"))
    second_engine.process(event("cart_item_added", "avocado"))

    assert first_engine.process(
        event("product_view_ended", "tortillas", dwell_seconds=60)
    ) is not None
    duplicate, reason = second_engine.evaluate({
        **event("product_view_ended", "tortillas", dwell_seconds=60),
        "event_id": "after-restart",
    })
    assert duplicate is None
    assert reason == "coupon_already_issued"


def test_failed_coupon_delivery_can_release_its_claim_for_retry():
    engine = CouponEngine(FakeRules({
        ("avocado", "tortillas"): {"support": 0.02, "confidence": 0.3, "lift": 2.1}
    }))
    engine.process(event("cart_item_added", "avocado"))
    view_event = event("product_view_ended", "tortillas", dwell_seconds=60)
    first = engine.process(view_event)
    engine.release_claim(first)

    retry = engine.process(view_event)

    assert retry is not None
    assert retry["coupon_id"] == first["coupon_id"]


def test_evaluate_exposes_suppression_reason():
    engine = CouponEngine(FakeRules({}))
    decision, reason = engine.evaluate(event("product_view_ended", "tortillas", dwell_seconds=20))
    assert decision is None
    assert reason == "below_dwell_threshold"


def test_simulator_randomizes_real_catalog_products_and_keeps_branches_meaningful():
    catalog = SimulationCatalog.from_csv("app/rules.csv")
    plans = [plan_session(catalog, index, random.Random(index)) for index in range(80)]

    assert len({plan["viewed_product"] for plan in plans}) > 1
    assert all(set(plan["cart"]).issubset(set(catalog.products)) for plan in plans)
    assert all(plan["viewed_product"] in catalog.products for plan in plans)

    eligible = [plan for plan in plans if plan["scenario"] == "eligible"]
    no_match = [plan for plan in plans if plan["scenario"] == "no_match"]
    assert eligible and no_match
    assert all(
        plan["viewed_product"] in catalog.qualifying_consequents.get(plan["cart"][0], set())
        for plan in eligible
    )
    assert all(
        plan["viewed_product"] not in catalog.qualifying_consequents.get(plan["cart"][0], set())
        for plan in no_match
    )
