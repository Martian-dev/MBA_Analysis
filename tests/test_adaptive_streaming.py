import json

from pipeline.adaptive_streaming import AdaptiveForeachBatch


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
