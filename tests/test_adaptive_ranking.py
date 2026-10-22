import pytest

from pipeline.adaptive import (
    AdaptiveRankingService,
    CandidateSnapshot,
    CouponGuardrails,
    InMemoryLinUCBState,
    RankingRequest,
)
from pipeline.adaptive_replay import run_replay


def candidate(product_id, prior_score, **extra):
    return {
        "product_id": product_id,
        "prior_score": prior_score,
        "snapshot_version": "bootstrap-2026-10-21",
        **extra,
    }


def event(event_id, event_type, product_id=None, **extra):
    payload = {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        **extra,
    }
    if product_id is not None:
        payload["product_id"] = product_id
    return payload


def test_rewarded_feedback_changes_a_later_ranking():
    service = AdaptiveRankingService(alpha=0.05, historical_weight=0.25)
    snapshot = CandidateSnapshot.from_records(
        "bootstrap-2026-10-21",
        [candidate("bananas", 0.60), candidate("berries", 0.55)],
    )
    request = RankingRequest("shopper-1", "session-1", limit=2)

    first = service.rank(request, snapshot)
    assert [item.product_id for item in first.items] == ["bananas", "berries"]
    assert first.policy_mode == "historical_cold_start"

    feedback = service.consume_feedback(
        event(
            "click-1",
            "recommendation_clicked",
            "berries",
            impression_id=first.impression_id,
        )
    )
    assert feedback.status == "learned"
    assert feedback.updated_products == ("berries",)

    second = service.rank(request, snapshot)
    assert [item.product_id for item in second.items] == ["berries", "bananas"]
    assert second.policy_mode == "adaptive_linucb"
    assert second.items[0].explanation["live_features"]["session_product_interest"] > 0


def test_feedback_without_a_recorded_impression_does_not_train():
    service = AdaptiveRankingService()
    result = service.consume_feedback(
        event("click-1", "recommendation_clicked", "berries", impression_id="missing")
    )
    assert result.status == "ignored_no_impression"
    assert result.updated_products == ()


def test_impression_is_recorded_before_feedback_can_update_policy():
    observed = []

    class RecordingStore:
        def record(self, impression):
            observed.append(impression.impression_id)

        def get(self, impression_id):
            return None

        def latest_for(self, user_id, session_id, product_id):
            return None

    service = AdaptiveRankingService(impressions=RecordingStore())
    snapshot = CandidateSnapshot.from_records("v1", [candidate("p1", 0.5, snapshot_version="v1")])
    decision = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)
    assert observed == [decision.impression_id]


def test_historical_order_is_used_when_adaptive_state_is_unavailable():
    class UnavailableState:
        def score(self, product_id, features, alpha):
            raise RuntimeError("state backend offline")

        def update(self, product_id, features, reward):
            raise RuntimeError("state backend offline")

        def update_many(self, updates):
            raise RuntimeError("state backend offline")

    service = AdaptiveRankingService(bandit_state=UnavailableState())
    snapshot = CandidateSnapshot.from_records(
        "v1",
        [
            candidate("p-low", 0.2, snapshot_version="v1"),
            candidate("p-high", 0.9, snapshot_version="v1"),
        ],
    )
    decision = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)
    assert decision.policy_mode == "historical_fallback"
    assert [item.product_id for item in decision.items] == ["p-high", "p-low"]


def test_historical_order_is_used_when_impression_store_is_unavailable():
    class UnavailableImpressions:
        def record(self, impression):
            raise ConnectionError("impression backend offline")

        def get(self, impression_id):
            raise ConnectionError("impression backend offline")

        def latest_for(self, user_id, session_id, product_id):
            raise ConnectionError("impression backend offline")

    service = AdaptiveRankingService(impressions=UnavailableImpressions())
    snapshot = CandidateSnapshot.from_records(
        "v1",
        [
            candidate("p-low", 0.2, snapshot_version="v1"),
            candidate("p-high", 0.9, snapshot_version="v1"),
        ],
    )

    decision = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)

    assert decision.policy_mode == "historical_fallback"
    assert decision.impression_id.startswith("historical-fallback-")
    assert [item.product_id for item in decision.items] == ["p-high", "p-low"]
    feedback = service.consume_feedback(
        event(
            "click-fallback",
            "recommendation_clicked",
            "p-high",
            impression_id=decision.impression_id,
        )
    )
    assert feedback.status == "state_unavailable"


def test_coupon_guardrails_filter_unsafe_candidates_before_learning():
    guardrails = CouponGuardrails(min_lift=1.5, min_confidence=0.1, min_support=0.01)
    service = AdaptiveRankingService(guardrails=guardrails)
    snapshot = CandidateSnapshot.from_records(
        "v1",
        [
            candidate(
                "safe",
                0.2,
                snapshot_version="v1",
                offer_type="coupon",
                supporting_product_id="cart-item",
                lift=2.0,
                confidence=0.3,
                support=0.02,
            ),
            candidate(
                "unsafe",
                1.0,
                snapshot_version="v1",
                offer_type="coupon",
                supporting_product_id="cart-item",
                lift=1.1,
                confidence=0.8,
                support=0.3,
            ),
        ],
    )
    decision = service.rank(
        RankingRequest("shopper-1", "session-1", cart_product_ids=("cart-item",)), snapshot
    )
    assert [item.product_id for item in decision.items] == ["safe"]
    assert decision.suppressions == {"unsafe": "below_minimum_lift"}


def test_duplicate_feedback_event_is_idempotent():
    service = AdaptiveRankingService(alpha=0.0)
    snapshot = CandidateSnapshot.from_records("v1", [candidate("p1", 0.5, snapshot_version="v1")])
    first = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)
    feedback = event("click-1", "recommendation_clicked", "p1", impression_id=first.impression_id)
    assert service.consume_feedback(feedback).status == "learned"
    assert service.consume_feedback(feedback).status == "duplicate_event"


def test_state_outage_keeps_feedback_retryable_until_commit_succeeds():
    class FailOnceState(InMemoryLinUCBState):
        def __init__(self):
            super().__init__()
            self.available = False

        def update_many(self, updates):
            if not self.available:
                raise ConnectionError("state backend offline")
            super().update_many(updates)

    state = FailOnceState()
    service = AdaptiveRankingService(bandit_state=state)
    snapshot = CandidateSnapshot.from_records(
        "v1", [candidate("p1", 0.5, snapshot_version="v1")]
    )
    decision = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)
    feedback = event(
        "retryable-click", "recommendation_clicked", "p1",
        impression_id=decision.impression_id,
    )

    assert service.consume_feedback(feedback).status == "state_unavailable"
    state.available = True
    assert service.consume_feedback(feedback).status == "learned"
    assert service.consume_feedback(feedback).status == "duplicate_event"


def test_dictionary_adapter_exposes_versions_needed_by_marketplace_feedback():
    service = AdaptiveRankingService()
    response = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=[candidate("p1", 0.5, snapshot_version="v3")],
        snapshot_version="v3",
    )
    assert response["snapshot_version"] == "v3"
    assert response["policy_version"] == "linucb-v1"
    assert response["impression_id"].startswith("adaptive-impression-")
    assert response["items"][0]["product_id"] == "p1"


def test_deterministic_replay_fixture_proves_ranking_change():
    result = run_replay()
    assert result["first_ranking"] == ["bananas", "berries"]
    assert result["second_ranking"] == ["berries", "bananas"]
    assert result["ranking_changed"] is True


def test_cart_and_checkout_feedback_are_attributed_to_the_latest_impression():
    service = AdaptiveRankingService()
    snapshot = CandidateSnapshot.from_records("v1", [candidate("p1", 0.5, snapshot_version="v1")])
    service.rank(RankingRequest("shopper-1", "session-1"), snapshot)

    cart_result = service.consume_feedback(event("cart-1", "cart_item_added", "p1", quantity=1))
    checkout_result = service.consume_feedback(
        event("checkout-1", "checkout_completed", order_id="order-1", product_ids=["p1"])
    )

    assert cart_result.status == "learned"
    assert cart_result.reward == 1.0
    assert checkout_result.status == "learned"
    assert checkout_result.reward == 1.2


def test_coupon_is_frequency_capped_by_guardrail_not_bandit_score():
    service = AdaptiveRankingService()
    snapshot = CandidateSnapshot.from_records(
        "v1",
        [
            candidate(
                "offer",
                0.5,
                snapshot_version="v1",
                offer_type="coupon",
                supporting_product_id="cart-item",
                lift=2.0,
                confidence=0.3,
                support=0.02,
            )
        ],
    )
    request = RankingRequest("shopper-1", "session-1", cart_product_ids=("cart-item",))
    assert [item.product_id for item in service.rank(request, snapshot).items] == ["offer"]
    second = service.rank(request, snapshot)
    assert second.items == ()
    assert second.suppressions == {"offer": "coupon_already_issued"}

    next_session = service.rank(
        RankingRequest("shopper-1", "session-2", cart_product_ids=("cart-item",)),
        snapshot,
    )
    assert [item.product_id for item in next_session.items] == ["offer"]


def test_live_cart_state_is_applied_to_guardrails_even_without_request_cart():
    service = AdaptiveRankingService()
    service.observe_event(event("cart-1", "cart_item_added", "cart-item", quantity=1))
    snapshot = CandidateSnapshot.from_records(
        "v1",
        [
            candidate("cart-item", 1.0, snapshot_version="v1"),
            candidate(
                "offer",
                0.5,
                snapshot_version="v1",
                offer_type="coupon",
                supporting_product_id="cart-item",
                lift=2.0,
                confidence=0.3,
                support=0.02,
            ),
        ],
    )

    decision = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)

    assert [item.product_id for item in decision.items] == ["offer"]
    assert decision.suppressions == {"cart-item": "product_already_in_cart"}


def test_candidate_snapshot_rejects_mixed_versions_and_non_finite_priors():
    with pytest.raises(ValueError, match="expected 'v1'"):
        CandidateSnapshot.from_records(
            "v1", [candidate("p1", 0.5, snapshot_version="another-version")]
        )
    with pytest.raises(ValueError, match="finite"):
        CandidateSnapshot.from_records(
            "v1", [candidate("p1", float("nan"), snapshot_version="v1")]
        )


def test_client_impressions_drive_the_live_product_response_feature():
    service = AdaptiveRankingService()
    snapshot = CandidateSnapshot.from_records(
        "v1", [candidate("p1", 0.5, snapshot_version="v1")]
    )
    first = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)
    for index in range(2):
        service.observe_event(
            event(
                f"impression-{index}",
                "recommendation_impression",
                "p1",
                impression_id=first.impression_id,
            )
        )
    service.observe_event(
        event(
            "click-1",
            "recommendation_clicked",
            "p1",
            impression_id=first.impression_id,
        )
    )

    second = service.rank(RankingRequest("shopper-1", "session-1"), snapshot)

    assert second.items[0].explanation["live_features"]["product_response_rate"] == 0.25
