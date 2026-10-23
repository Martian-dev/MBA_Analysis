"""Shared Redis state adapters for the adaptive ranking service.

The core adaptive module deliberately depends on small protocols.  This module
implements those protocols with Redis so HTTP and Structured Streaming workers
can record an impression, consume feedback, and rank against the same state.
"""

from __future__ import annotations

import hashlib
import json
import math
from contextlib import nullcontext
from typing import Any, Mapping, Sequence

from redis.exceptions import RedisError

from .adaptive import (
    FEATURE_NAMES,
    AdaptiveRankingService,
    CandidateSnapshot,
    FeedbackResult,
    HistoricalCandidate,
    RankingDecision,
    RankingRequest,
    RankedCandidate,
    _DisplayedCandidate,
    _RecordedImpression,
)
from .contracts import validate_event

DEFAULT_ADAPTIVE_NAMESPACE = "mba:adaptive:v1"
DEFAULT_STATE_TTL_SECONDS = 7 * 24 * 60 * 60


def _text(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def _field(*parts: str) -> str:
    return json.dumps(parts, separators=(",", ":"), ensure_ascii=False)


def _digest(*parts: str) -> str:
    payload = "\0".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _bounded(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return min(upper, max(lower, float(value)))


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _matrix_vector(matrix: Sequence[Sequence[float]], vector: Sequence[float]) -> list[float]:
    return [_dot(row, vector) for row in matrix]


class RedisFeatureStore:
    """Live feature counters shared across application and stream workers."""

    def __init__(
        self,
        client: Any,
        namespace: str,
        *,
        writer: Any | None = None,
        session_ttl_seconds: int = 86_400,
        state_ttl_seconds: int = DEFAULT_STATE_TTL_SECONDS,
    ) -> None:
        self.client = client
        self.writer = writer if writer is not None else client
        self.namespace = namespace
        self.session_ttl_seconds = session_ttl_seconds
        self.state_ttl_seconds = state_ttl_seconds
        self._session = f"{namespace}:features:session-product"
        self._shopper = f"{namespace}:features:shopper-product"
        self._positive = f"{namespace}:features:product-positive"
        self._impressions = f"{namespace}:features:product-impressions"
        self._pairs = f"{namespace}:features:product-pairs"

    def _value_key(self, scope: str, field: str) -> str:
        return f"{scope}:{_digest(field)}"

    def _number(self, scope: str, field: str) -> float:
        return float(_text(self.client.get(self._value_key(scope, field))) or 0.0)

    def _increment(self, scope: str, field: str, amount: float) -> None:
        key = self._value_key(scope, field)
        self.writer.incrbyfloat(key, amount)
        self.writer.expire(key, self.state_ttl_seconds)

    def _cart_key(self, user_id: str, session_id: str) -> str:
        return f"{self.namespace}:cart:{_digest(user_id, session_id)}"

    def product_signals(self, product_ids: Sequence[str]) -> list[dict[str, Any]]:
        """Read aggregate ranking inputs, never shopper/session records or scores."""
        keys = [self._value_key(scope, product_id) for product_id in product_ids
                for scope in (self._impressions, self._positive)]
        values = self.client.mget(keys) if keys else []
        return [
            {'product_id': product_id, 'impressions': float(_text(values[index * 2]) or 0),
             'positive_weight': float(_text(values[index * 2 + 1]) or 0)}
            for index, product_id in enumerate(product_ids)
        ]

    def _cart(self, user_id: str, session_id: str) -> set[str]:
        return {_text(value) or "" for value in self.client.smembers(self._cart_key(user_id, session_id))}

    def features(
        self, request: RankingRequest, candidate: HistoricalCandidate
    ) -> tuple[float, ...]:
        product_id = candidate.product_id
        cart = set(request.cart_product_ids) | self._cart(request.user_id, request.session_id)
        pair_affinity = max(
            (
                self._number(self._pairs, _field(*sorted((item, product_id))))
                for item in cart
                if item != product_id
            ),
            default=0.0,
        )
        impressions = self._number(self._impressions, product_id)
        response_rate = self._number(self._positive, product_id) / max(1.0, impressions)
        return (
            1.0,
            _bounded(candidate.prior_score),
            _bounded(
                self._number(
                    self._session,
                    _field(request.user_id, request.session_id, product_id),
                )
                / 4.0,
                lower=-1.0,
            ),
            _bounded(
                self._number(self._shopper, _field(request.user_id, product_id)) / 8.0,
                lower=-1.0,
            ),
            _bounded(response_rate, lower=-1.0),
            _bounded(pair_affinity / 4.0, lower=-1.0),
        )

    def cart_product_ids(self, user_id: str, session_id: str) -> frozenset[str]:
        return frozenset(self._cart(user_id, session_id))

    def observe(self, event: Mapping[str, Any]) -> None:
        event_type = str(event["event_type"])
        user_id = str(event["user_id"])
        session_id = str(event["session_id"])
        product_id = str(event["product_id"]) if event.get("product_id") else None
        if event_type == "recommendation_impression" and product_id:
            self._increment(self._impressions, product_id, 1)
        elif event_type == "product_view_ended" and product_id:
            self._adjust(user_id, session_id, product_id, 0.25)
        elif event_type == "recommendation_clicked" and product_id:
            self._adjust(user_id, session_id, product_id, 1.0)
            self._increment(self._positive, product_id, 0.5)
        elif event_type == "cart_item_added" and product_id:
            cart_key = self._cart_key(user_id, session_id)
            quantity_key = cart_key + ':quantities'
            quantity = event.get('cart_quantity', min(99, int(self.client.hget(quantity_key, product_id) or 0) + event.get('quantity', 1)))
            self.writer.hset(quantity_key, product_id, quantity)
            self.writer.expire(quantity_key, self.session_ttl_seconds)
            for cart_item in self._cart(user_id, session_id):
                pair = _field(*sorted((cart_item, product_id)))
                self._increment(self._pairs, pair, 1.0)
            self.writer.sadd(cart_key, product_id)
            self.writer.expire(cart_key, self.session_ttl_seconds)
            self._adjust(user_id, session_id, product_id, 2.0)
            self._increment(self._positive, product_id, 1.0)
        elif event_type == "cart_item_removed" and product_id:
            cart_key = self._cart_key(user_id, session_id)
            quantity_key = cart_key + ':quantities'
            remaining = event.get('cart_quantity', max(0, int(self.client.hget(quantity_key, product_id) or 0) - event.get('quantity', 1)))
            self.writer.hset(quantity_key, product_id, remaining)
            if not remaining:
                self.writer.srem(cart_key, product_id)
            self._adjust(user_id, session_id, product_id, -1.5)
        elif event_type == 'cart_synchronized':
            cart_key = self._cart_key(user_id, session_id)
            self.writer.delete(cart_key, cart_key + ':quantities')
            for item in event['cart']:
                self.writer.sadd(cart_key, item['product_id'])
                self.writer.hset(cart_key + ':quantities', item['product_id'], item['quantity'])
            self.writer.expire(cart_key, self.session_ttl_seconds)
            self.writer.expire(cart_key + ':quantities', self.session_ttl_seconds)
        elif event_type == "coupon_applied" and product_id:
            self._adjust(user_id, session_id, product_id, 1.5)
            self._increment(self._positive, product_id, 0.75)
        elif event_type == "checkout_completed":
            cart_key = self._cart_key(user_id, session_id)
            self.writer.delete(cart_key, cart_key + ':quantities')
            product_ids = [str(item) for item in event.get("product_ids", [])]
            for item in product_ids:
                self._adjust(user_id, session_id, item, 2.0)
                self._increment(self._positive, item, 1.0)
            for index, left in enumerate(product_ids):
                for right in product_ids[index + 1 :]:
                    self._increment(self._pairs, _field(*sorted((left, right))), 1.0)

    def _adjust(
        self, user_id: str, session_id: str, product_id: str, amount: float
    ) -> None:
        self._increment(
            self._session, _field(user_id, session_id, product_id), amount
        )
        self._increment(self._shopper, _field(user_id, product_id), amount)


class RedisImpressionStore:
    """Recorded decisions and product lookup indexes with bounded retention."""

    def __init__(self, client: Any, namespace: str, *, ttl_seconds: int = 86_400) -> None:
        self.client = client
        self.namespace = namespace
        self.ttl_seconds = ttl_seconds

    def _impression_key(self, impression_id: str) -> str:
        return f"{self.namespace}:impression:{_digest(impression_id)}"

    def _latest_key(self, user_id: str, session_id: str, product_id: str) -> str:
        return f"{self.namespace}:impression-latest:{_digest(user_id, session_id, product_id)}"

    def record(self, impression: _RecordedImpression) -> None:
        value = json.dumps(
            {
                "impression_id": impression.impression_id,
                "user_id": impression.user_id,
                "session_id": impression.session_id,
                "candidates": [
                    {
                        "product_id": candidate.product_id,
                        "features": list(candidate.features),
                        "offer_type": candidate.offer_type,
                    }
                    for candidate in impression.candidates
                ],
            },
            separators=(",", ":"),
        )
        self.client.set(
            self._impression_key(impression.impression_id),
            value,
            ex=self.ttl_seconds,
        )
        for candidate in impression.candidates:
            self.client.set(
                self._latest_key(
                    impression.user_id, impression.session_id, candidate.product_id
                ),
                impression.impression_id,
                ex=self.ttl_seconds,
            )

    def get(self, impression_id: str) -> _RecordedImpression | None:
        raw = _text(self.client.get(self._impression_key(impression_id)))
        if raw is None:
            return None
        payload = json.loads(raw)
        return _RecordedImpression(
            impression_id=payload["impression_id"],
            user_id=payload["user_id"],
            session_id=payload["session_id"],
            candidates=tuple(
                _DisplayedCandidate(
                    product_id=item["product_id"],
                    features=tuple(float(value) for value in item["features"]),
                    offer_type=item["offer_type"],
                )
                for item in payload["candidates"]
            ),
        )

    def latest_for(
        self, user_id: str, session_id: str, product_id: str
    ) -> _RecordedImpression | None:
        impression_id = _text(
            self.client.get(self._latest_key(user_id, session_id, product_id))
        )
        return self.get(impression_id) if impression_id else None


class RedisLinUCBState:
    """Disjoint LinUCB matrices persisted per product in Redis."""

    def __init__(
        self,
        client: Any,
        namespace: str,
        *,
        writer: Any | None = None,
        state_ttl_seconds: int = DEFAULT_STATE_TTL_SECONDS,
    ) -> None:
        self.client = client
        self.writer = writer if writer is not None else client
        self.namespace = namespace
        self.dimensions = len(FEATURE_NAMES)
        self.state_ttl_seconds = state_ttl_seconds
        self._updates_key = f"{namespace}:bandit:updates"
        self._staged_states = {}

    def _state_key(self, product_id: str) -> str:
        return f"{self.namespace}:bandit:{_digest(product_id)}"

    def _new_state(self) -> dict[str, list[Any]]:
        return {
            "inverse_covariance": [
                [1.0 if row == column else 0.0 for column in range(self.dimensions)]
                for row in range(self.dimensions)
            ],
            "reward_vector": [0.0] * self.dimensions,
        }

    def _load(self, product_id: str) -> dict[str, Any]:
        if self.writer is not self.client and product_id in self._staged_states:
            return self._staged_states[product_id]
        raw = _text(self.client.get(self._state_key(product_id)))
        state = json.loads(raw) if raw else self._new_state()
        if self.writer is not self.client:
            self._staged_states[product_id] = state
        return state

    def has_observations(self) -> bool:
        return int(_text(self.client.get(self._updates_key)) or 0) > 0

    def score(
        self, product_id: str, features: Sequence[float], alpha: float
    ) -> tuple[float, float, tuple[float, ...]]:
        state = self._load(product_id)
        projected = _matrix_vector(state["inverse_covariance"], features)
        theta = _matrix_vector(state["inverse_covariance"], state["reward_vector"])
        contributions = tuple(weight * value for weight, value in zip(theta, features))
        estimate = sum(contributions)
        exploration = alpha * math.sqrt(max(0.0, _dot(features, projected)))
        return estimate, exploration, contributions

    def update(self, product_id: str, features: Sequence[float], reward: float) -> None:
        self.update_many(((product_id, features, reward),))

    def update_many(
        self, updates: Sequence[tuple[str, Sequence[float], float]]
    ) -> None:
        """Apply one feedback event under a shared lock before claiming it done."""

        lock = (
            self.client.lock(
                f"{self.namespace}:feedback-commit-lock",
                timeout=30,
                blocking_timeout=5,
            )
            if self.writer is self.client
            else nullcontext()
        )
        with lock:
            for product_id, features, reward in updates:
                self._update_unlocked(product_id, features, reward)
            for _ in updates:
                self.writer.incr(self._updates_key)
            self.writer.expire(self._updates_key, self.state_ttl_seconds)

    def _update_unlocked(
        self, product_id: str, features: Sequence[float], reward: float
    ) -> None:
        state_key = self._state_key(product_id)
        state = self._load(product_id)
        inverse = state["inverse_covariance"]
        projected = _matrix_vector(inverse, features)
        denominator = 1.0 + _dot(features, projected)
        for row in range(self.dimensions):
            for column in range(self.dimensions):
                inverse[row][column] -= (
                    projected[row] * projected[column] / denominator
                )
            state["reward_vector"][row] += reward * features[row]
        self.writer.set(
            state_key,
            json.dumps(state, separators=(",", ":")),
            ex=self.state_ttl_seconds,
        )


class RedisAdaptiveRankingService(AdaptiveRankingService):
    """Adaptive service with shared sequencing and event deduplication."""

    def __init__(
        self,
        client: Any,
        *,
        namespace: str = DEFAULT_ADAPTIVE_NAMESPACE,
        impression_ttl_seconds: int = 86_400,
        dedup_ttl_seconds: int = 86_400,
        state_ttl_seconds: int = DEFAULT_STATE_TTL_SECONDS,
        **options: Any,
    ) -> None:
        self.redis_client = client
        self.namespace = namespace.rstrip(":")
        self.dedup_ttl_seconds = dedup_ttl_seconds
        self.state_ttl_seconds = state_ttl_seconds
        super().__init__(
            features=RedisFeatureStore(
                client, self.namespace, state_ttl_seconds=state_ttl_seconds
            ),
            bandit_state=RedisLinUCBState(
                client, self.namespace, state_ttl_seconds=state_ttl_seconds
            ),
            impressions=RedisImpressionStore(
                client, self.namespace, ttl_seconds=impression_ttl_seconds
            ),
            outcomes=RedisOutcomes(client, self.namespace),
            **options,
        )

    def rank(
        self, request: RankingRequest, snapshot: CandidateSnapshot
    ) -> RankingDecision:
        try:
            sequence = int(self.redis_client.incr(f"{self.namespace}:decision-sequence"))
            self._decision_sequence = sequence - 1
            return super().rank(request, snapshot)
        except RedisError:
            return self._historical_fallback(request, snapshot)

    def _historical_fallback(
        self, request: RankingRequest, snapshot: CandidateSnapshot
    ) -> RankingDecision:
        """Return a safe decision without touching unavailable Redis state."""
        scored: list[tuple[HistoricalCandidate, float]] = []
        suppressions: dict[str, str] = {}
        for candidate in snapshot.candidates:
            reason = self.guardrails.suppression_reason(request, candidate)
            if reason:
                suppressions[candidate.product_id] = reason
            else:
                scored.append((candidate, self.historical_weight * candidate.prior_score))
        scored.sort(key=lambda item: (-item[1], item[0].product_id))
        selected = scored[: request.limit]
        self._record_issued_coupons(request, (candidate for candidate, _ in selected))
        self._decision_sequence += 1
        return RankingDecision(
            impression_id=f"historical-fallback-unavailable-{self._decision_sequence:08d}",
            snapshot_version=snapshot.version,
            user_id=request.user_id,
            session_id=request.session_id,
            policy_mode="historical_fallback",
            items=tuple(
                RankedCandidate(
                    product_id=candidate.product_id,
                    score=round(score, 8),
                    offer_type=candidate.offer_type,
                    explanation={
                        "historical_prior": score,
                        "learned_estimate": 0.0,
                        "exploration_bonus": 0.0,
                        "fallback_reason": "shared_state_unavailable",
                    },
                )
                for candidate, score in selected
            ),
            suppressions=suppressions,
        )

    def consume_feedback(self, event: dict[str, Any]) -> FeedbackResult:
        event = validate_event(event)
        event_id = event["event_id"]
        dedup_key = f"{self.namespace}:event:{_digest(event_id)}"
        try:
            with self.redis_client.lock(
                f"{self.namespace}:feedback-commit-lock",
                timeout=30,
                blocking_timeout=5,
            ):
                if self.redis_client.get(dedup_key) is not None:
                    return FeedbackResult("duplicate_event")

                transaction = self.redis_client.pipeline(transaction=True)
                original_features = self.features
                original_bandit = self.bandit_state
                original_outcomes = self.outcomes
                self.outcomes = RedisOutcomes(self.redis_client, self.namespace, writer=transaction)
                self.features = RedisFeatureStore(
                    self.redis_client,
                    self.namespace,
                    writer=transaction,
                    state_ttl_seconds=self.state_ttl_seconds,
                )
                self.bandit_state = RedisLinUCBState(
                    self.redis_client,
                    self.namespace,
                    writer=transaction,
                    state_ttl_seconds=self.state_ttl_seconds,
                )
                # The distributed claim is authoritative; the base class's local
                # cache must not extend the configured Redis retention window.
                self._processed_events.pop(event_id, None)
                try:
                    result = super().consume_feedback(event)
                finally:
                    self.features = original_features
                    self.bandit_state = original_bandit
                    self.outcomes = original_outcomes
                if result.status == "state_unavailable":
                    self._processed_events.pop(event_id, None)
                    return result
                transaction.set(
                    dedup_key, "applied", ex=self.dedup_ttl_seconds
                )
                transaction.execute()
                return result
        except RedisError:
            self._processed_events.pop(event_id, None)
            return FeedbackResult("state_unavailable")
        except Exception:
            self._processed_events.pop(event_id, None)
            raise

    def settle_expired(self):
        with self.redis_client.lock(f'{self.namespace}:feedback-commit-lock', timeout=30, blocking_timeout=5):
            transaction = self.redis_client.pipeline(transaction=True)
            original_bandit, original_outcomes = self.bandit_state, self.outcomes
            self.bandit_state = RedisLinUCBState(self.redis_client, self.namespace, writer=transaction,
                                                state_ttl_seconds=self.state_ttl_seconds)
            self.outcomes = RedisOutcomes(self.redis_client, self.namespace, writer=transaction)
            try:
                result = super().settle_expired()
                transaction.execute()
                return result
            finally:
                self.bandit_state, self.outcomes = original_bandit, original_outcomes


class RedisOutcomes:
    def __init__(self, client, namespace, writer=None):
        self.client = client
        self.writer = writer if writer is not None else client
        self.namespace = namespace
        self.pending = f'{namespace}:outcomes:pending'

    def _member(self, impression_id, product_id):
        return json.dumps([impression_id, product_id], separators=(',', ':'))

    def _key(self, impression_id, product_id):
        return f'{self.namespace}:outcome:{_digest(impression_id, product_id)}'

    def expose(self, impression_id, product_id, deadline):
        if self.status(impression_id, product_id) is None:
            self.writer.zadd(self.pending, {self._member(impression_id, product_id): deadline}, nx=True)

    def respond(self, impression_id, product_id):
        self.writer.set(self._key(impression_id, product_id), 'responded', ex=86_400)

    def status(self, impression_id, product_id):
        return _text(self.client.get(self._key(impression_id, product_id)))

    def due(self, now):
        return [tuple(json.loads(member)) for member in self.client.zrangebyscore(self.pending, '-inf', now, start=0, num=500)]

    def close(self, impression_id, product_id):
        self.writer.zrem(self.pending, self._member(impression_id, product_id))
        self.writer.set(self._key(impression_id, product_id), 'closed', ex=86_400)


def create_redis_adaptive_service(
    client: Any, **options: Any
) -> RedisAdaptiveRankingService:
    """Create an adaptive service whose mutable state is shared through Redis."""

    return RedisAdaptiveRankingService(client, **options)
