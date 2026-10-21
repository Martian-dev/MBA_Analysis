from __future__ import annotations

import threading

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


class MemoryRedis:
    """Small shared Redis test double covering the adapter's public commands."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set[str]] = {}
        self._mutex = threading.RLock()
        self._locks: dict[str, threading.RLock] = {}

    def get(self, key):
        with self._mutex:
            return self.values.get(key)

    def set(self, key, value, *, nx=False, ex=None):
        del ex
        with self._mutex:
            if nx and key in self.values:
                return False
            self.values[key] = str(value)
            return True

    def delete(self, *keys):
        with self._mutex:
            for key in keys:
                self.values.pop(key, None)
                self.hashes.pop(key, None)
                self.sets.pop(key, None)

    def incr(self, key):
        with self._mutex:
            value = int(self.values.get(key, "0")) + 1
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

    def sadd(self, key, *values):
        with self._mutex:
            self.sets.setdefault(key, set()).update(str(value) for value in values)

    def srem(self, key, *values):
        with self._mutex:
            self.sets.setdefault(key, set()).difference_update(str(value) for value in values)

    def expire(self, key, seconds):
        del key, seconds
        return True

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
