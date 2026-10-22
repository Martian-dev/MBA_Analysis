import pytest

from app.main import app
from pipeline.contracts import validate_event


def payload(**changes):
    return {
        "event_id": "event-1", "event_type": "product_view_ended",
        "event_time": "2026-10-22T12:00:00Z", "user_id": "u", "session_id": "s",
        "product_id": "p", "dwell_seconds": 60, **changes,
    }


@pytest.mark.parametrize("changes", [
    {"event_type": []}, {"event_type": {}},
    {"dwell_seconds": float("nan")}, {"dwell_seconds": float("inf")},
    {'dwell_seconds': 10**1000},
    {"event_type": "cart_item_added", "quantity": 100},
    {"catalog_product_id": {}}, {"product_ids": [None]},
])
def test_invalid_events_are_rejected_as_validation_errors(changes):
    with pytest.raises(ValueError):
        validate_event(payload(**changes))


@pytest.mark.parametrize("path", ["/api/cart-recommend", "/api/checkout"])
@pytest.mark.parametrize("body", [["bad"], "bad", 12, True])
def test_http_requires_a_json_object(path, body):
    with app.test_client() as client:
        assert client.post(path, json=body).status_code == 400


def test_http_rejects_unhashable_event_type_without_server_error():
    with app.test_client() as client:
        assert client.post("/api/events", json=payload(event_type=[])).status_code == 400
