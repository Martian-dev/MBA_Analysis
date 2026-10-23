from __future__ import annotations

import threading
from redis.exceptions import ConnectionError as RedisConnectionError

from pipeline.adaptive_redis import create_redis_adaptive_service
from pipeline.adaptive_streaming import AdaptiveForeachBatch, consume_with


class _Lock:
    def __init__(self, lock: threading.RLock) -> None:
        self.lock = lock

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.lock.release()


class MemoryPipeline:
    def __init__(self, client) -> None:
        self.client = client
        self.operations = []

    def set(self, key, value, *, nx=False, ex=None):
        self.operations.append(("set", key, value, nx, ex))
        return self

    def incr(self, key):
        self.operations.append(("incr", key))
        return self

    def incrbyfloat(self, key, amount):
        self.operations.append(("incrbyfloat", key, amount))
        return self

    def sadd(self, key, *values):
        self.operations.append(("sadd", key, values))
        return self

    def srem(self, key, *values):
        self.operations.append(("srem", key, values))
        return self

    def expire(self, key, seconds):
        self.operations.append(("expire", key, seconds))
        return self

    def zadd(self, key, mapping, nx=False):
        self.operations.append(('zadd', key, mapping, nx))
        return self

    def zrem(self, key, member):
        self.operations.append(('zrem', key, member))
        return self

    def hset(self, key, field, value):
        self.operations.append(('hset', key, field, value))
        return self

    def delete(self, *keys):
        self.operations.append(('delete_many', keys))
        return self

    def execute(self):
        with self.client._mutex:
            if self.client.fail_next_transaction:
                self.client.fail_next_transaction = False
                raise RedisConnectionError("transaction interrupted before commit")
            results = []
            for operation in self.operations:
                if operation[0] == 'delete_many':
                    self.client.delete(*operation[1]); continue
                method = getattr(self.client, operation[0])
                if operation[0] == "set":
                    _, key, value, nx, ex = operation
                    results.append(method(key, value, nx=nx, ex=ex))
                elif operation[0] in {"sadd", "srem"}:
                    results.append(method(operation[1], *operation[2]))
                elif operation[0] == 'zadd':
                    results.append(method(operation[1], operation[2], nx=operation[3]))
                else:
                    results.append(method(*operation[1:]))
            return results


class MemoryRedis:
    """Small shared Redis test double covering the adapter's public commands."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.sorted_sets = {}
        self.expirations: dict[str, int] = {}
        self.fail_next_transaction = False
        self._mutex = threading.RLock()
        self._locks: dict[str, threading.RLock] = {}

    def get(self, key):
        with self._mutex:
            return self.values.get(key)

    def mget(self, keys):
        with self._mutex:
            return [self.values.get(key) for key in keys]

    def set(self, key, value, *, nx=False, ex=None):
        with self._mutex:
            if nx and key in self.values:
                return False
            self.values[key] = str(value)
            if ex is not None:
                self.expirations[key] = int(ex)
            return True

    def delete(self, *keys):
        with self._mutex:
            for key in keys:
                self.values.pop(key, None)
                self.hashes.pop(key, None)
                self.sets.pop(key, None)
                self.expirations.pop(key, None)

    def incr(self, key):
        with self._mutex:
            value = int(self.values.get(key, "0")) + 1
            self.values[key] = str(value)
            return value

    def incrbyfloat(self, key, amount):
        with self._mutex:
            value = float(self.values.get(key, "0")) + float(amount)
            self.values[key] = str(value)
            return value

    def hget(self, key, field):
        with self._mutex:
            return self.hashes.get(key, {}).get(str(field))

    def hset(self, key, field=None, value=None, *, mapping=None):
        with self._mutex:
            target = self.hashes.setdefault(key, {})
            if mapping is not None:
                target.update({str(name): str(item) for name, item in mapping.items()})
            elif field is not None:
                target[str(field)] = str(value)

    def hincrbyfloat(self, key, field, amount):
        with self._mutex:
            target = self.hashes.setdefault(key, {})
            value = float(target.get(str(field), "0")) + float(amount)
            target[str(field)] = str(value)
            return value

    def smembers(self, key):
        with self._mutex:
            return set(self.sets.get(key, set()))

    def zadd(self, key, mapping, nx=False):
        with self._mutex:
            target = self.sorted_sets.setdefault(key, {})
            for member, score in mapping.items():
                if not nx or member not in target:
                    target[member] = score

    def zrangebyscore(self, key, minimum, maximum, start=0, num=500):
        with self._mutex:
            return [member for member, score in sorted(self.sorted_sets.get(key, {}).items(), key=lambda item: item[1])
                    if float(minimum) <= score <= float(maximum)][start:start + num]

    def zrem(self, key, member):
        with self._mutex:
            self.sorted_sets.get(key, {}).pop(member, None)

    def sadd(self, key, *values):
        with self._mutex:
            self.sets.setdefault(key, set()).update(str(value) for value in values)

    def srem(self, key, *values):
        with self._mutex:
            self.sets.setdefault(key, set()).difference_update(str(value) for value in values)

    def expire(self, key, seconds):
        self.expirations[key] = int(seconds)
        return True

    def pipeline(self, transaction=True):
        assert transaction is True
        return MemoryPipeline(self)

    def lock(self, name, **options):
        del options
        with self._mutex:
            lock = self._locks.setdefault(name, threading.RLock())
        return _Lock(lock)


class FakeRow:
    def __init__(self, payload):
        self.payload = payload

    def asDict(self, recursive=True):
        del recursive
        return self.payload


class FakeBatch:
    def __init__(self, rows):
        self.rows = rows

    def toLocalIterator(self):
        return iter(self.rows)


def test_foreach_batch_feedback_changes_ranking_in_another_service_instance():
    redis = MemoryRedis()
    options = {"alpha": 0.05, "historical_weight": 0.25}
    serving = create_redis_adaptive_service(redis, namespace="shared-test", **options)
    streaming = create_redis_adaptive_service(redis, namespace="shared-test", **options)
    later_worker = create_redis_adaptive_service(redis, namespace="shared-test", **options)
    candidates = [
        {"product_id": "101", "prior_score": 0.9},
        {"product_id": "202", "prior_score": 0.8},
    ]

    first = serving.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-run-v1",
        limit=2,
    )
    assert [item["product_id"] for item in first["items"]] == ["101", "202"]

    event = {
        "event_id": "click-202",
        "event_type": "recommendation_clicked",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "product_id": "Display name",
        "catalog_product_id": 202,
        "impression_id": first["impression_id"],
    }
    AdaptiveForeachBatch(consume_with(streaming))(FakeBatch([FakeRow(event)]), 7)

    second = later_worker.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-run-v1",
        limit=2,
    )
    assert second["policy_mode"] == "adaptive_linucb"
    assert [item["product_id"] for item in second["items"]] == ["202", "101"]
    assert second["items"][0]["explanation"]["live_features"]["session_product_interest"] > 0
    assert second["impression_id"] != first["impression_id"]

    duplicate = serving.observe_event({**event, "product_id": "202"})
    assert duplicate.status == "duplicate_event"


def test_unavailable_shared_state_returns_public_historical_fallback():
    class UnavailableRedis:
        def incr(self, key):
            del key
            raise RedisConnectionError("redis offline")

    service = create_redis_adaptive_service(
        UnavailableRedis(), alpha=0.05, historical_weight=0.25
    )

    decision = service.rank_candidates(
        user_id="shopper-offline",
        session_id="session-offline",
        candidates=[
            {"product_id": "low", "prior_score": 0.2},
            {"product_id": "high", "prior_score": 0.9},
        ],
        snapshot_version="spark-v1",
        limit=2,
    )

    assert decision["policy_mode"] == "historical_fallback"
    assert [item["product_id"] for item in decision["items"]] == ["high", "low"]
    assert (
        decision["items"][0]["explanation"]["fallback_reason"]
        == "shared_state_unavailable"
    )


def test_unavailable_shared_state_preserves_coupon_frequency_guardrail():
    class UnavailableRedis:
        def incr(self, key):
            del key
            raise RedisConnectionError("redis offline")

    service = create_redis_adaptive_service(UnavailableRedis())
    candidates = [{
        "product_id": "offer",
        "prior_score": 0.8,
        "offer_type": "coupon",
        "supporting_product_id": "cart-item",
        "lift": 2.0,
        "confidence": 0.3,
        "support": 0.02,
    }]
    arguments = {
        "user_id": "shopper-offline",
        "session_id": "session-offline",
        "candidates": candidates,
        "snapshot_version": "spark-v1",
        "cart_product_ids": ["cart-item"],
    }

    first = service.rank_candidates(**arguments)
    assert [item["product_id"] for item in first["items"]] == ["offer"]
    second = service.rank_candidates(**arguments)

    assert second["items"] == []
    assert second["suppressions"] == {"offer": "coupon_already_issued"}


def test_feedback_transaction_is_retryable_without_partial_updates():
    redis = MemoryRedis()
    service = create_redis_adaptive_service(
        redis, namespace="atomic-test", alpha=0.05, historical_weight=0.25
    )
    candidates = [
        {"product_id": "101", "prior_score": 0.9},
        {"product_id": "202", "prior_score": 0.8},
    ]
    first = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-v1",
        limit=2,
    )
    feedback = {
        "event_id": "atomic-click",
        "event_type": "recommendation_clicked",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "product_id": "202",
        "impression_id": first["impression_id"],
    }

    redis.fail_next_transaction = True
    assert service.observe_event(feedback).status == "state_unavailable"
    after_failure = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-v1",
        limit=2,
    )
    assert after_failure["policy_mode"] == "historical_cold_start"
    failed_features = after_failure["items"][1]["explanation"]["live_features"]
    assert failed_features["session_product_interest"] == 0

    assert service.observe_event(feedback).status == "learned"
    assert service.observe_event(feedback).status == "duplicate_event"
    after_retry = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-v1",
        limit=2,
    )
    retried_features = after_retry["items"][0]["explanation"]["live_features"]
    assert retried_features["session_product_interest"] == 0.25


def test_shared_feature_and_bandit_state_have_explicit_retention():
    redis = MemoryRedis()
    service = create_redis_adaptive_service(
        redis,
        namespace="retention-test",
        state_ttl_seconds=60,
        alpha=0.05,
        historical_weight=0.25,
    )
    candidates = [{"product_id": "202", "prior_score": 0.8}]
    decision = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-v1",
    )
    feedback = {
        "event_id": "retained-click",
        "event_type": "recommendation_clicked",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "product_id": "202",
        "impression_id": decision["impression_id"],
    }

    assert service.observe_event(feedback).status == "learned"
    retained_keys = [
        key
        for key in redis.values
        if ":features:" in key or ":bandit:" in key
    ]
    assert retained_keys
    assert {redis.expirations[key] for key in retained_keys} == {60}
