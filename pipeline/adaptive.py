"""Explainable live ranking built on versioned historical candidates.

The historical snapshot supplies a cold-start prior.  Live client events update
bounded feature counters and a deterministic disjoint LinUCB policy.  Coupon
eligibility remains a separate, non-learning policy boundary.
"""

from __future__ import annotations

import math
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Sequence

from .contracts import validate_event
from .coupon_engine import CouponClaim


FEATURE_NAMES = (
    "bias",
    "historical_prior",
    "session_product_interest",
    "shopper_product_interest",
    "product_response_rate",
    "cart_pair_affinity",
)
POLICY_VERSION = "linucb-v1"


@dataclass(frozen=True)
class HistoricalCandidate:
    product_id: str
    snapshot_version: str
    prior_score: float
    offer_type: str = "recommendation"
    supporting_product_id: str | None = None
    lift: float = 0.0
    confidence: float = 0.0
    support: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateSnapshot:
    version: str
    candidates: tuple[HistoricalCandidate, ...]

    @classmethod
    def from_records(
        cls, version: str, records: Iterable[Mapping[str, Any]]
    ) -> "CandidateSnapshot":
        if not isinstance(version, str) or not version.strip():
            raise ValueError("candidate snapshot version must be non-empty")
        candidates: list[HistoricalCandidate] = []
        seen: set[str] = set()
        known_fields = {
            "product_id",
            "snapshot_version",
            "prior_score",
            "offer_type",
            "supporting_product_id",
            "lift",
            "confidence",
            "support",
        }
        for record in records:
            product_id = str(record.get("product_id", "")).strip()
            record_version = str(record.get("snapshot_version", version)).strip()
            prior_score = float(record.get("prior_score", 0.0))
            offer_type = str(record.get("offer_type", "recommendation"))
            if not product_id:
                raise ValueError("candidate product_id must be non-empty")
            if record_version != version:
                raise ValueError(
                    f"candidate {product_id} has version {record_version!r}; expected {version!r}"
                )
            if product_id in seen:
                raise ValueError(f"duplicate candidate product_id: {product_id}")
            if not math.isfinite(prior_score):
                raise ValueError(f"candidate {product_id} prior_score must be finite")
            if offer_type not in {"recommendation", "coupon"}:
                raise ValueError(f"candidate {product_id} has unsupported offer_type")
            seen.add(product_id)
            candidates.append(
                HistoricalCandidate(
                    product_id=product_id,
                    snapshot_version=record_version,
                    prior_score=prior_score,
                    offer_type=offer_type,
                    supporting_product_id=record.get("supporting_product_id"),
                    lift=float(record.get("lift", 0.0)),
                    confidence=float(record.get("confidence", 0.0)),
                    support=float(record.get("support", 0.0)),
                    metadata={
                        key: value for key, value in record.items() if key not in known_fields
                    },
                )
            )
        return cls(version=version, candidates=tuple(candidates))


@dataclass(frozen=True)
class RankingRequest:
    user_id: str
    session_id: str
    cart_product_ids: tuple[str, ...] = ()
    limit: int = 5

    def __post_init__(self) -> None:
        if not self.user_id.strip() or not self.session_id.strip():
            raise ValueError("ranking requires non-empty user_id and session_id")
        if self.limit < 1:
            raise ValueError("ranking limit must be positive")


@dataclass(frozen=True)
class RankedCandidate:
    product_id: str
    score: float
    offer_type: str
    explanation: Mapping[str, Any]


@dataclass(frozen=True)
class RankingDecision:
    impression_id: str
    snapshot_version: str
    user_id: str
    session_id: str
    policy_mode: str
    items: tuple[RankedCandidate, ...]
    suppressions: Mapping[str, str]
    policy_version: str = POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "impression_id": self.impression_id,
            "policy_version": self.policy_version,
            "snapshot_version": self.snapshot_version,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "policy_mode": self.policy_mode,
            "items": [
                {
                    "product_id": item.product_id,
                    "score": item.score,
                    "offer_type": item.offer_type,
                    "explanation": dict(item.explanation),
                }
                for item in self.items
            ],
            "suppressions": dict(self.suppressions),
        }


@dataclass(frozen=True)
class FeedbackResult:
    status: str
    updated_products: tuple[str, ...] = ()
    reward: float | None = None


@dataclass(frozen=True)
class _DisplayedCandidate:
    product_id: str
    features: tuple[float, ...]
    offer_type: str


@dataclass(frozen=True)
class _RecordedImpression:
    impression_id: str
    user_id: str
    session_id: str
    candidates: tuple[_DisplayedCandidate, ...]


class ImpressionStore(Protocol):
    def record(self, impression: _RecordedImpression) -> None: ...

    def get(self, impression_id: str) -> _RecordedImpression | None: ...

    def latest_for(
        self, user_id: str, session_id: str, product_id: str
    ) -> _RecordedImpression | None: ...


class InMemoryImpressionStore:
    def __init__(self, max_impressions: int = 10_000) -> None:
        self.max_impressions = max_impressions
        self._items: OrderedDict[str, _RecordedImpression] = OrderedDict()
        self._latest: dict[tuple[str, str, str], str] = {}

    def record(self, impression: _RecordedImpression) -> None:
        self._items[impression.impression_id] = impression
        for candidate in impression.candidates:
            self._latest[(impression.user_id, impression.session_id, candidate.product_id)] = (
                impression.impression_id
            )
        while len(self._items) > self.max_impressions:
            expired_id, expired = self._items.popitem(last=False)
            for candidate in expired.candidates:
                key = (expired.user_id, expired.session_id, candidate.product_id)
                if self._latest.get(key) == expired_id:
                    self._latest.pop(key, None)

    def get(self, impression_id: str) -> _RecordedImpression | None:
        return self._items.get(impression_id)

    def latest_for(
        self, user_id: str, session_id: str, product_id: str
    ) -> _RecordedImpression | None:
        impression_id = self._latest.get((user_id, session_id, product_id))
        return self.get(impression_id) if impression_id else None


class InMemoryOutcomes:
    """One matured outcome per actually displayed recommendation."""
    def __init__(self):
        self.pending = {}
        self.statuses = OrderedDict()

    def expose(self, impression_id, product_id, deadline):
        key = (impression_id, product_id)
        if key not in self.statuses:
            self.pending.setdefault(key, deadline)

    def respond(self, impression_id, product_id):
        self.statuses[(impression_id, product_id)] = 'responded'

    def status(self, impression_id, product_id):
        return self.statuses.get((impression_id, product_id))

    def due(self, now):
        return [key for key, deadline in self.pending.items() if deadline <= now]

    def close(self, impression_id, product_id):
        key = (impression_id, product_id)
        self.pending.pop(key, None)
        self.statuses[key] = 'closed'
        while len(self.statuses) > 50_000:
            self.statuses.popitem(last=False)


class BanditState(Protocol):
    def score(
        self, product_id: str, features: Sequence[float], alpha: float
    ) -> tuple[float, float, tuple[float, ...]]: ...

    def update(self, product_id: str, features: Sequence[float], reward: float) -> None: ...

    def update_many(
        self, updates: Sequence[tuple[str, Sequence[float], float]]
    ) -> None: ...


@dataclass
class _ActionState:
    inverse_covariance: list[list[float]]
    reward_vector: list[float]


class InMemoryLinUCBState:
    """Disjoint LinUCB state using Sherman-Morrison inverse updates."""

    def __init__(
        self, dimensions: int = len(FEATURE_NAMES), max_actions: int = 50_000
    ) -> None:
        self.dimensions = dimensions
        self.max_actions = max_actions
        self._actions: OrderedDict[str, _ActionState] = OrderedDict()
        self._update_count = 0

    def has_observations(self) -> bool:
        return self._update_count > 0

    def _state(self, product_id: str) -> _ActionState:
        if product_id not in self._actions:
            identity = [
                [1.0 if row == column else 0.0 for column in range(self.dimensions)]
                for row in range(self.dimensions)
            ]
            self._actions[product_id] = _ActionState(identity, [0.0] * self.dimensions)
        self._actions.move_to_end(product_id)
        while len(self._actions) > self.max_actions:
            self._actions.popitem(last=False)
        return self._actions[product_id]

    def score(
        self, product_id: str, features: Sequence[float], alpha: float
    ) -> tuple[float, float, tuple[float, ...]]:
        state = self._state(product_id)
        projected = _matrix_vector(state.inverse_covariance, features)
        theta = _matrix_vector(state.inverse_covariance, state.reward_vector)
        contributions = tuple(weight * value for weight, value in zip(theta, features))
        estimate = sum(contributions)
        exploration = alpha * math.sqrt(max(0.0, _dot(features, projected)))
        return estimate, exploration, contributions

    def update(self, product_id: str, features: Sequence[float], reward: float) -> None:
        self.update_many(((product_id, features, reward),))

    def update_many(
        self, updates: Sequence[tuple[str, Sequence[float], float]]
    ) -> None:
        """Apply one feedback event's action updates as a single adapter call."""
        for product_id, features, reward in updates:
            self._update_one(product_id, features, reward)

    def _update_one(self, product_id: str, features: Sequence[float], reward: float) -> None:
        state = self._state(product_id)
        projected = _matrix_vector(state.inverse_covariance, features)
        denominator = 1.0 + _dot(features, projected)
        for row in range(self.dimensions):
            for column in range(self.dimensions):
                state.inverse_covariance[row][column] -= (
                    projected[row] * projected[column] / denominator
                )
            state.reward_vector[row] += reward * features[row]
        self._update_count += 1


class LiveFeatureStore:
    """Bounded live signals at session, shopper, product, and pair scopes."""

    def __init__(self, max_feature_keys: int = 50_000, max_sessions: int = 10_000) -> None:
        self.max_feature_keys = max_feature_keys
        self.max_sessions = max_sessions
        self._session_product: defaultdict[tuple[str, str, str], float] = defaultdict(float)
        self._shopper_product: defaultdict[tuple[str, str], float] = defaultdict(float)
        self._product_positive: defaultdict[str, float] = defaultdict(float)
        self._product_impressions: defaultdict[str, int] = defaultdict(int)
        self._pair_affinity: defaultdict[tuple[str, str], float] = defaultdict(float)
        self._carts: defaultdict[tuple[str, str], dict[str, int]] = defaultdict(dict)

    def features(
        self, request: RankingRequest, candidate: HistoricalCandidate
    ) -> tuple[float, ...]:
        product_id = candidate.product_id
        session_key = (request.user_id, request.session_id, product_id)
        shopper_key = (request.user_id, product_id)
        cart = set(request.cart_product_ids) | set(self._carts.get(
            (request.user_id, request.session_id), set()
        ))
        pair_affinity = max(
            (
                self._pair_affinity.get(_pair_key(item, product_id), 0.0)
                for item in cart
                if item != product_id
            ),
            default=0.0,
        )
        impressions = self._product_impressions.get(product_id, 0)
        response_rate = self._product_positive.get(product_id, 0.0) / max(
            1.0, float(impressions)
        )
        return (
            1.0,
            _bounded(candidate.prior_score),
            _bounded(self._session_product.get(session_key, 0.0) / 4.0, lower=-1.0),
            _bounded(self._shopper_product.get(shopper_key, 0.0) / 8.0, lower=-1.0),
            _bounded(response_rate, lower=-1.0),
            _bounded(pair_affinity / 4.0, lower=-1.0),
        )

    def cart_product_ids(self, user_id: str, session_id: str) -> frozenset[str]:
        return frozenset(self._carts.get((user_id, session_id), set()))

    def observe(self, event: Mapping[str, Any]) -> None:
        event_type = str(event["event_type"])
        user_id = str(event["user_id"])
        session_id = str(event["session_id"])
        product_id = event.get("product_id")
        if event_type == "recommendation_impression" and product_id:
            self._increment(self._product_impressions, str(product_id), 1)
        elif event_type == "product_view_ended" and product_id:
            self._adjust(user_id, session_id, str(product_id), 0.25)
        elif event_type == "recommendation_clicked" and product_id:
            self._adjust(user_id, session_id, str(product_id), 1.0)
            self._increment(self._product_positive, str(product_id), 0.5)
        elif event_type == "cart_item_added" and product_id:
            product_id = str(product_id)
            cart = self._carts[(user_id, session_id)]
            self._trim(self._carts, self.max_sessions)
            for cart_item in cart:
                self._increment(self._pair_affinity, _pair_key(cart_item, product_id), 1.0)
            cart[product_id] = event.get('cart_quantity', min(99, cart.get(product_id, 0) + event.get('quantity', 1)))
            self._adjust(user_id, session_id, product_id, 2.0)
            self._increment(self._product_positive, product_id, 1.0)
        elif event_type == "cart_item_removed" and product_id:
            product_id = str(product_id)
            cart = self._carts.get((user_id, session_id))
            if cart is not None:
                remaining = event.get('cart_quantity', max(0, cart.get(product_id, 0) - event.get('quantity', 1)))
                if remaining:
                    cart[product_id] = remaining
                else:
                    cart.pop(product_id, None)
            self._adjust(user_id, session_id, product_id, -1.5)
        elif event_type == 'cart_synchronized':
            self._carts[(user_id, session_id)] = {item['product_id']: item['quantity'] for item in event['cart']}
            self._trim(self._carts, self.max_sessions)
        elif event_type == "coupon_applied" and product_id:
            self._adjust(user_id, session_id, str(product_id), 1.5)
            self._increment(self._product_positive, str(product_id), 0.75)
        elif event_type == "checkout_completed":
            self._carts.pop((user_id, session_id), None)
            product_ids = [str(item) for item in event.get("product_ids", [])]
            for item in product_ids:
                self._adjust(user_id, session_id, item, 2.0)
                self._increment(self._product_positive, item, 1.0)
            for index, left in enumerate(product_ids):
                for right in product_ids[index + 1 :]:
                    self._increment(self._pair_affinity, _pair_key(left, right), 1.0)

    def _adjust(self, user_id: str, session_id: str, product_id: str, amount: float) -> None:
        self._increment(self._session_product, (user_id, session_id, product_id), amount)
        self._increment(self._shopper_product, (user_id, product_id), amount)

    def _increment(self, mapping: dict[Any, Any], key: Any, amount: float) -> None:
        mapping[key] = mapping.get(key, 0) + amount
        self._trim(mapping, self.max_feature_keys)

    @staticmethod
    def _trim(mapping: dict[Any, Any], maximum: int) -> None:
        while len(mapping) > maximum:
            mapping.pop(next(iter(mapping)))


class CouponGuardrails:
    """Deterministic coupon constraints kept outside the bandit policy."""

    def __init__(
        self,
        min_lift: float = 1.5,
        min_confidence: float = 0.10,
        min_support: float = 0.012,
        max_issued: int = 50_000,
    ) -> None:
        self.min_lift = min_lift
        self.min_confidence = min_confidence
        self.min_support = min_support
        self.max_issued = max_issued
        self._issued: OrderedDict[CouponClaim, None] = OrderedDict()

    def suppression_reason(
        self, request: RankingRequest, candidate: HistoricalCandidate
    ) -> str | None:
        if candidate.product_id in request.cart_product_ids:
            return "product_already_in_cart"
        if candidate.offer_type != "coupon":
            return None
        if (
            not candidate.supporting_product_id
            or candidate.supporting_product_id not in request.cart_product_ids
        ):
            return "supporting_product_not_in_cart"
        if candidate.lift < self.min_lift:
            return "below_minimum_lift"
        if candidate.confidence < self.min_confidence:
            return "below_minimum_confidence"
        if candidate.support < self.min_support:
            return "below_minimum_support"
        claim = CouponClaim(request.user_id, request.session_id, candidate.product_id)
        if claim in self._issued:
            return "coupon_already_issued"
        return None

    def record_issued(
        self, request: RankingRequest, candidate: HistoricalCandidate
    ) -> None:
        claim = CouponClaim(request.user_id, request.session_id, candidate.product_id)
        self._issued[claim] = None
        self._issued.move_to_end(claim)
        while len(self._issued) > self.max_issued:
            self._issued.popitem(last=False)


_EVENT_REWARDS = {
    "recommendation_clicked": 0.4,
    "cart_item_added": 1.0,
    "cart_item_removed": -1.0,
    "coupon_applied": 0.8,
    "checkout_completed": 1.2,
}


class AdaptiveRankingService:
    """Public adaptive-decision seam used by HTTP handlers and replay jobs.

    Event IDs are deduplicated across the most recent 50,000 successfully
    applied events. Durable deployments should provide the same bounded claim
    semantics in their shared state adapter.
    """

    def __init__(
        self,
        *,
        alpha: float = 0.15,
        historical_weight: float = 0.25,
        features: LiveFeatureStore | None = None,
        bandit_state: BanditState | None = None,
        impressions: ImpressionStore | None = None,
        guardrails: CouponGuardrails | None = None,
        outcomes=None,
        feedback_window_seconds: float = 300,
        clock=time.time,
    ) -> None:
        self.alpha = alpha
        self.historical_weight = historical_weight
        self.features = features or LiveFeatureStore()
        self.bandit_state = bandit_state or InMemoryLinUCBState()
        self.impressions = impressions or InMemoryImpressionStore()
        self.guardrails = guardrails or CouponGuardrails()
        self.outcomes = outcomes if outcomes is not None else InMemoryOutcomes()
        self.feedback_window_seconds = feedback_window_seconds
        self.clock = clock
        self._decision_sequence = 0
        self._processed_events: OrderedDict[str, None] = OrderedDict()

    def rank(self, request: RankingRequest, snapshot: CandidateSnapshot) -> RankingDecision:
        self.settle_expired()
        live_cart = self.features.cart_product_ids(request.user_id, request.session_id)
        effective_cart = tuple(dict.fromkeys((*request.cart_product_ids, *sorted(live_cart))))
        effective_request = RankingRequest(
            user_id=request.user_id,
            session_id=request.session_id,
            cart_product_ids=effective_cart,
            limit=request.limit,
        )
        safe: list[HistoricalCandidate] = []
        suppressions: dict[str, str] = {}
        vectors: dict[str, tuple[float, ...]] = {}
        for candidate in snapshot.candidates:
            reason = self.guardrails.suppression_reason(effective_request, candidate)
            if reason:
                suppressions[candidate.product_id] = reason
                continue
            safe.append(candidate)
            vectors[candidate.product_id] = self.features.features(effective_request, candidate)

        observations = getattr(self.bandit_state, "has_observations", None)
        if callable(observations) and not observations():
            mode = "historical_cold_start"
            scored = _historical_scores(safe, vectors)
        else:
            mode = "adaptive_linucb"
            scored = []
            try:
                for candidate in safe:
                    vector = vectors[candidate.product_id]
                    estimate, exploration, contributions = self.bandit_state.score(
                        candidate.product_id, vector, self.alpha
                    )
                    historical = self.historical_weight * candidate.prior_score
                    score = historical + estimate + exploration
                    scored.append(
                        (
                            candidate,
                            score,
                            {
                                "historical_prior": historical,
                                "learned_estimate": estimate,
                                "exploration_bonus": exploration,
                                "feature_contributions": dict(
                                    zip(FEATURE_NAMES, contributions)
                                ),
                                "live_features": dict(
                                    zip(FEATURE_NAMES[2:], vector[2:])
                                ),
                            },
                        )
                    )
            except (RuntimeError, ConnectionError, TimeoutError):
                mode = "historical_fallback"
                scored = _historical_scores(safe, vectors)

        scored.sort(key=lambda item: (-item[1], item[0].product_id))
        selected = scored[: request.limit]
        self._decision_sequence += 1
        impression_id = f"adaptive-impression-{self._decision_sequence:08d}"
        recorded = _RecordedImpression(
            impression_id=impression_id,
            user_id=request.user_id,
            session_id=request.session_id,
            candidates=tuple(
                _DisplayedCandidate(
                    candidate.product_id,
                    vectors[candidate.product_id],
                    candidate.offer_type,
                )
                for candidate, _, _ in selected
            ),
        )
        # This write deliberately precedes returning the decision. Feedback is
        # never eligible to train against an unrecorded impression.
        try:
            self.impressions.record(recorded)
        except (RuntimeError, ConnectionError, TimeoutError):
            mode = "historical_fallback"
            impression_id = f"historical-fallback-{self._decision_sequence:08d}"
            historical = _historical_scores(safe, vectors)
            historical.sort(key=lambda item: (-item[1], item[0].product_id))
            selected = historical[: request.limit]
        items = tuple(
            RankedCandidate(
                product_id=candidate.product_id,
                score=round(score, 8),
                offer_type=candidate.offer_type,
                explanation=explanation,
            )
            for candidate, score, explanation in selected
        )
        self._record_issued_coupons(request, (candidate for candidate, _, _ in selected))
        return RankingDecision(
            impression_id=impression_id,
            snapshot_version=snapshot.version,
            user_id=request.user_id,
            session_id=request.session_id,
            policy_mode=mode,
            items=items,
            suppressions=suppressions,
        )

    def _record_issued_coupons(
        self,
        request: RankingRequest,
        candidates: Iterable[HistoricalCandidate],
    ) -> None:
        for candidate in candidates:
            if candidate.offer_type == "coupon":
                self.guardrails.record_issued(request, candidate)

    def rank_candidates(
        self,
        *,
        user_id: str,
        session_id: str,
        candidates: Iterable[Mapping[str, Any]],
        snapshot_version: str,
        cart_product_ids: Iterable[str] = (),
        limit: int = 5,
    ) -> dict[str, Any]:
        """App-friendly dictionary adapter with no Spark/Kafka dependency."""

        snapshot = CandidateSnapshot.from_records(snapshot_version, candidates)
        request = RankingRequest(
            user_id=user_id,
            session_id=session_id,
            cart_product_ids=tuple(cart_product_ids),
            limit=limit,
        )
        return self.rank(request, snapshot).to_dict()

    def consume_feedback(self, event: dict[str, Any]) -> FeedbackResult:
        event = validate_event(event)
        event_id = event["event_id"]
        if event_id in self._processed_events:
            return FeedbackResult("duplicate_event")
        event_type = event["event_type"]
        reward = _EVENT_REWARDS.get(event_type)
        if reward is None:
            if event_type == 'recommendation_impression':
                impression = self._find_impression(event, event['product_id'])
                if impression and any(item.product_id == event['product_id'] for item in impression.candidates):
                    self.outcomes.expose(impression.impression_id, event['product_id'],
                                         self.clock() + self.feedback_window_seconds)
            self.features.observe(event)
            self._mark_processed(event_id)
            return FeedbackResult("observed_no_reward")

        product_ids = list(event.get("product_ids", []))
        if event.get("product_id"):
            product_ids.append(event["product_id"])
        updates: list[tuple[str, Sequence[float], float]] = []
        responses = []
        for product_id in dict.fromkeys(product_ids):
            try:
                impression = self._find_impression(event, product_id)
            except (RuntimeError, ConnectionError, TimeoutError):
                return FeedbackResult("state_unavailable")
            if impression is None:
                continue
            if self.outcomes.status(impression.impression_id, product_id) == 'closed':
                continue
            displayed = next(
                (
                    candidate
                    for candidate in impression.candidates
                    if candidate.product_id == product_id
                ),
                None,
            )
            if displayed is None:
                continue
            updates.append((product_id, displayed.features, reward))
            responses.append((impression.impression_id, product_id))
        if not updates:
            self.features.observe(event)
            self._mark_processed(event_id)
            return FeedbackResult("ignored_no_impression")
        try:
            self.bandit_state.update_many(updates)
        except (RuntimeError, ConnectionError, TimeoutError):
            return FeedbackResult("state_unavailable")
        for impression_id, product_id in responses:
            self.outcomes.respond(impression_id, product_id)
        self.features.observe(event)
        self._mark_processed(event_id)
        return FeedbackResult("learned", tuple(item[0] for item in updates), reward)

    def settle_expired(self):
        """Mature exposed recommendations without treating missing data as a click."""
        settled = 0
        for impression_id, product_id in self.outcomes.due(self.clock()):
            impression = self.impressions.get(impression_id)
            if impression and self.outcomes.status(impression_id, product_id) != 'responded':
                candidate = next((item for item in impression.candidates if item.product_id == product_id), None)
                if candidate:
                    self.bandit_state.update_many(((product_id, candidate.features, 0.0),))
                    settled += 1
            self.outcomes.close(impression_id, product_id)
        return settled

    def _mark_processed(self, event_id: str) -> None:
        self._processed_events[event_id] = None
        if len(self._processed_events) > 50_000:
            self._processed_events.popitem(last=False)

    def observe_event(self, event: dict[str, Any]) -> FeedbackResult:
        """Alias used by stream processors at the live feature boundary."""

        return self.consume_feedback(event)

    def _find_impression(
        self, event: Mapping[str, Any], product_id: str
    ) -> _RecordedImpression | None:
        explicit_id = event.get("impression_id")
        if explicit_id:
            impression = self.impressions.get(str(explicit_id))
        else:
            impression = self.impressions.latest_for(
                str(event["user_id"]), str(event["session_id"]), product_id
            )
        if impression is None:
            return None
        if impression.user_id != event["user_id"] or impression.session_id != event["session_id"]:
            return None
        return impression


def create_adaptive_service(**options: Any) -> AdaptiveRankingService:
    """Create the in-process service used by Flask and deterministic demos."""

    return AdaptiveRankingService(**options)


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _historical_scores(
    candidates: Iterable[HistoricalCandidate],
    vectors: Mapping[str, tuple[float, ...]],
) -> list[tuple[HistoricalCandidate, float, Mapping[str, Any]]]:
    return [
        (
            candidate,
            candidate.prior_score,
            {
                "historical_prior": candidate.prior_score,
                "learned_estimate": 0.0,
                "exploration_bonus": 0.0,
                "feature_contributions": {},
                "live_features": dict(
                    zip(FEATURE_NAMES[2:], vectors[candidate.product_id][2:])
                ),
            },
        )
        for candidate in candidates
    ]


def _matrix_vector(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> list[float]:
    return [_dot(row, vector) for row in matrix]


def _bounded(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return min(upper, max(lower, float(value)))


def _pair_key(left: str, right: str) -> tuple[str, str]:
    return tuple(sorted((left, right)))  # type: ignore[return-value]
