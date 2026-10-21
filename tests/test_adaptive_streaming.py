import json

import pytest

from pipeline.adaptive import FeedbackResult, create_adaptive_service
from pipeline.adaptive_streaming import AdaptiveForeachBatch, consume_with


class FakeRow:
    def __init__(self, payload):
        self.payload = payload

    def asDict(self, recursive=True):
        return self.payload


class FakeBatch:
    def __init__(self, rows):
        self.rows = rows

    def toLocalIterator(self):
        return iter(self.rows)


def test_foreach_batch_boundary_delivers_validated_events_and_batch_metadata():
    observed = []
    boundary = AdaptiveForeachBatch(lambda payload, batch_id: observed.append((payload, batch_id)))
    payload = {
        "event_id": "search-1",
        "event_type": "search_performed",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "query": "berries",
    }
    batch = FakeBatch([FakeRow({"event_json": json.dumps(payload)})])

    boundary(batch, 7)

    assert observed == [(payload, 7)]


def test_foreach_batch_boundary_accepts_parsed_spark_rows():
    observed = []
    boundary = AdaptiveForeachBatch(lambda payload, batch_id: observed.append((payload, batch_id)))
    payload = {
        "event_id": "checkout-1",
        "event_type": "checkout_completed",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "order_id": "order-1",
        "product_ids": ["p1"],
    }

    boundary(FakeBatch([FakeRow(payload)]), 8)

    assert observed == [(payload, 8)]


def test_foreach_batch_prefers_catalog_ids_and_changes_numeric_ranking():
    service = create_adaptive_service(alpha=0.05, historical_weight=0.25)
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
    assert [item["product_id"] for item in first["items"]] == ["101", "202"]
    payload = {
        "event_id": "click-catalog-202",
        "event_type": "recommendation_clicked",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "product_id": "Display name only",
        "catalog_product_id": 202,
        "impression_id": first["impression_id"],
    }

    AdaptiveForeachBatch(consume_with(service))(FakeBatch([FakeRow(payload)]), 9)
    second = service.rank_candidates(
        user_id="shopper-1",
        session_id="session-1",
        candidates=candidates,
        snapshot_version="spark-v1",
        limit=2,
    )

    assert [item["product_id"] for item in second["items"]] == ["202", "101"]


def test_foreach_batch_does_not_dead_letter_consumer_failures():
    boundary = AdaptiveForeachBatch(lambda payload, batch_id: (_ for _ in ()).throw(ValueError("state")))
    payload = {
        "event_id": "search-consumer-failure",
        "event_type": "search_performed",
        "event_time": "2026-10-21T12:00:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "query": "berries",
    }

    with pytest.raises(ValueError, match="state"):
        boundary(FakeBatch([FakeRow(payload)]), 10)
    assert boundary.rejected_count == 0


def test_foreach_batch_rejects_poison_row_and_continues_valid_events():
    observed = []
    rejected = []
    boundary = AdaptiveForeachBatch(
        lambda payload, batch_id: observed.append((payload, batch_id)),
        lambda values, batch_id, error: rejected.append((values, batch_id, error)),
    )
    valid = {
        "event_id": "search-2",
        "event_type": "search_performed",
        "event_time": "2026-10-21T12:01:00+00:00",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "query": "bananas",
    }
    batch = FakeBatch(
        [
            FakeRow({"event_json": "not-json"}),
            FakeRow({"event_json": json.dumps(valid)}),
        ]
    )

    boundary(batch, 9)

    assert observed == [(valid, 9)]
    assert boundary.rejected_count == 1
    assert rejected[0][0] == {"event_json": "not-json"}
    assert rejected[0][1] == 9


def test_foreach_batch_rejects_invalid_timestamp_from_raw_spark_row():
    rejected = []
    boundary = AdaptiveForeachBatch(
        lambda payload, batch_id: None,
        lambda values, batch_id, error: rejected.append((values, batch_id, error)),
    )
    payload = {
        "event_id": "bad-time",
        "event_type": "search_performed",
        "event_time": "not-a-timestamp",
        "user_id": "shopper-1",
        "session_id": "session-1",
        "query": "berries",
    }

    boundary(
        FakeBatch(
            [FakeRow({"event_json": json.dumps(payload), "event_time_timestamp": None})]
        ),
        11,
    )

    assert rejected[0][0]["event_json"] == json.dumps(payload)
    assert rejected[0][1] == 11
    assert "ISO-8601" in rejected[0][2]


def test_stream_consumer_retries_when_shared_state_is_unavailable():
    class UnavailableService:
        def observe_event(self, payload):
            del payload
            return FeedbackResult("state_unavailable")

    consume = consume_with(UnavailableService())

    with pytest.raises(RuntimeError, match="adaptive state unavailable"):
        consume({"event_id": "retry-me"}, 12)
