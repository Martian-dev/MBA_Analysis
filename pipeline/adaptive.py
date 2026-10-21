"""Explainable live ranking built on versioned historical candidates.

The historical snapshot supplies a cold-start prior.  Live client events update
bounded feature counters and a deterministic disjoint LinUCB policy.  Coupon
eligibility remains a separate, non-learning policy boundary.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Sequence

from .contracts import validate_event


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
    def __init__(self) -> None:
        self._items: dict[str, _RecordedImpression] = {}
        self._latest: dict[tuple[str, str, str], str] = {}

    def record(self, impression: _RecordedImpression) -> None:
        self._items[impression.impression_id] = impression
        for candidate in impression.candidates:
            self._latest[(impression.user_id, impression.session_id, candidate.product_id)] = (
                impression.impression_id
            )

    def get(self, impression_id: str) -> _RecordedImpression | None:
        return self._items.get(impression_id)

    def latest_for(
        self, user_id: str, session_id: str, product_id: str
    ) -> _RecordedImpression | None:
        impression_id = self._latest.get((user_id, session_id, product_id))
        return self.get(impression_id) if impression_id else None


class BanditState(Protocol):
    def score(
        self, product_id: str, features: Sequence[float], alpha: float
    ) -> tuple[float, float, tuple[float, ...]]: ...

    def update(self, product_id: str, features: Sequence[float], reward: float) -> None: ...


@dataclass
class _ActionState:
    inverse_covariance: list[list[float]]
    reward_vector: list[float]


class InMemoryLinUCBState:
    """Disjoint LinUCB state using Sherman-Morrison inverse updates."""

    def __init__(self, dimensions: int = len(FEATURE_NAMES)) -> None:
        self.dimensions = dimensions
        self._actions: dict[str, _ActionState] = {}
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

    def __init__(self) -> None:
        self._session_product: defaultdict[tuple[str, str, str], float] = defaultdict(float)
        self._shopper_product: defaultdict[tuple[str, str], float] = defaultdict(float)
        self._product_positive: defaultdict[str, float] = defaultdict(float)
        self._product_impressions: defaultdict[str, int] = defaultdict(int)
        self._pair_affinity: defaultdict[tuple[str, str], float] = defaultdict(float)
        self._carts: defaultdict[tuple[str, str], set[str]] = defaultdict(set)

    def features(
        self, request: RankingRequest, candidate: HistoricalCandidate
    ) -> tuple[float, ...]:
        product_id = candidate.product_id
        session_key = (request.user_id, request.session_id, product_id)
        shopper_key = (request.user_id, product_id)
        cart = set(request.cart_product_ids) | self._carts[(request.user_id, request.session_id)]
        pair_affinity = max(
            (
                self._pair_affinity[_pair_key(item, product_id)]
                for item in cart
                if item != product_id
            ),
            default=0.0,
        )
        impressions = self._product_impressions[product_id]
        response_rate = self._product_positive[product_id] / max(1.0, float(impressions))
        return (
            1.0,
            _bounded(candidate.prior_score),
            _bounded(self._session_product[session_key] / 4.0, lower=-1.0),
            _bounded(self._shopper_product[shopper_key] / 8.0, lower=-1.0),
            _bounded(response_rate, lower=-1.0),
            _bounded(pair_affinity / 4.0, lower=-1.0),
        )

    def cart_product_ids(self, user_id: str, session_id: str) -> frozenset[str]:
        return frozenset(self._carts[(user_id, session_id)])

    def observe(self, event: Mapping[str, Any]) -> None:
        event_type = str(event["event_type"])
        user_id = str(event["user_id"])
        session_id = str(event["session_id"])
        product_id = event.get("product_id")
        if event_type == "recommendation_impression" and product_id:
            self._product_impressions[str(product_id)] += 1
        elif event_type == "product_view_ended" and product_id:
            self._adjust(user_id, session_id, str(product_id), 0.25)
        elif event_type == "recommendation_clicked" and product_id:
            self._adjust(user_id, session_id, str(product_id), 1.0)
            self._product_positive[str(product_id)] += 0.5
        elif event_type == "cart_item_added" and product_id:
            product_id = str(product_id)
            cart = self._carts[(user_id, session_id)]
            for cart_item in cart:
                self._pair_affinity[_pair_key(cart_item, product_id)] += 1.0
            cart.add(product_id)
            self._adjust(user_id, session_id, product_id, 2.0)
            self._product_positive[product_id] += 1.0
        elif event_type == "cart_item_removed" and product_id:
            product_id = str(product_id)
            self._carts[(user_id, session_id)].discard(product_id)
            self._adjust(user_id, session_id, product_id, -1.5)
        elif event_type == "coupon_applied" and product_id:
            self._adjust(user_id, session_id, str(product_id), 1.5)
            self._product_positive[str(product_id)] += 0.75
        elif event_type == "checkout_completed":
            product_ids = [str(item) for item in event.get("product_ids", [])]
            for item in product_ids:
                self._adjust(user_id, session_id, item, 2.0)
                self._product_positive[item] += 1.0
            for index, left in enumerate(product_ids):
                for right in product_ids[index + 1 :]:
                    self._pair_affinity[_pair_key(left, right)] += 1.0

    def _adjust(self, user_id: str, session_id: str, product_id: str, amount: float) -> None:
        self._session_product[(user_id, session_id, product_id)] += amount
        self._shopper_product[(user_id, product_id)] += amount


class CouponGuardrails:
    """Deterministic coupon constraints kept outside the bandit policy."""

    def __init__(
        self,
        min_lift: float = 1.5,
        min_confidence: float = 0.10,
        min_support: float = 0.012,
    ) -> None:
        self.min_lift = min_lift
        self.min_confidence = min_confidence
        self.min_support = min_support
        self._issued: set[tuple[str, str]] = set()

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
        if (request.user_id, candidate.product_id) in self._issued:
            return "coupon_already_issued"
        return None

    def record_issued(self, user_id: str, product_id: str) -> None:
        self._issued.add((user_id, product_id))


_EVENT_REWARDS = {
    "recommendation_clicked": 0.4,
    "cart_item_added": 1.0,
    "cart_item_removed": -1.0,
    "coupon_applied": 0.8,
    "checkout_completed": 1.2,
}


class AdaptiveRankingService:
    """Public adaptive-decision seam used by HTTP handlers and replay jobs."""

    def __init__(
        self,
        *,
        alpha: float = 0.15,
        historical_weight: float = 0.25,
        features: LiveFeatureStore | None = None,
        bandit_state: BanditState | None = None,
        impressions: ImpressionStore | None = None,
        guardrails: CouponGuardrails | None = None,
    ) -> None:
        self.alpha = alpha
        self.historical_weight = historical_weight
        self.features = features or LiveFeatureStore()
        self.bandit_state = bandit_state or InMemoryLinUCBState()
        self.impressions = impressions or InMemoryImpressionStore()
        self.guardrails = guardrails or CouponGuardrails()
        self._decision_sequence = 0
        self._processed_events: set[str] = set()

    def rank(self, request: RankingRequest, snapshot: CandidateSnapshot) -> RankingDecision:
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
        items = tuple(
            RankedCandidate(
                product_id=candidate.product_id,
                score=round(score, 8),
                offer_type=candidate.offer_type,
                explanation=explanation,
            )
            for candidate, score, explanation in selected
        )
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
        self.impressions.record(recorded)
        for candidate, _, _ in selected:
            if candidate.offer_type == "coupon":
                self.guardrails.record_issued(request.user_id, candidate.product_id)
        return RankingDecision(
            impression_id=impression_id,
            snapshot_version=snapshot.version,
            user_id=request.user_id,
            session_id=request.session_id,
            policy_mode=mode,
            items=items,
            suppressions=suppressions,
        )

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
        self._processed_events.add(event_id)
        self.features.observe(event)
        event_type = event["event_type"]
        reward = _EVENT_REWARDS.get(event_type)
        if reward is None:
            return FeedbackResult("observed_no_reward")

        product_ids = list(event.get("product_ids", []))
        if event.get("product_id"):
            product_ids.append(event["product_id"])
        updated: list[str] = []
        for product_id in dict.fromkeys(product_ids):
            impression = self._find_impression(event, product_id)
            if impression is None:
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
            try:
                self.bandit_state.update(product_id, displayed.features, reward)
            except (RuntimeError, ConnectionError, TimeoutError):
                return FeedbackResult("state_unavailable")
            updated.append(product_id)
        if not updated:
            return FeedbackResult("ignored_no_impression")
        return FeedbackResult("learned", tuple(updated), reward)

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
