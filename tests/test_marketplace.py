"""Behavior tests for the public marketplace HTTP seam."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app import main
from pipeline.adaptive import create_adaptive_service


@pytest.fixture()
def client(tmp_path):
    main.app.config.update(TESTING=True, ORDER_STORE_PATH=str(tmp_path / "orders.jsonl"))
    with main.app.test_client() as test_client:
        yield test_client


def test_full_catalog_is_paginated_and_carries_instacart_metadata(client):
    response = client.get("/api/products?per_page=12&page=2")
    assert response.status_code == 200
    payload = response.get_json()

    assert payload["total"] == 49_688
    assert payload["pagination"]["page"] == 2
    assert payload["pagination"]["pages"] == 4_141
    assert len(payload["products"]) == 12
    product = payload["products"][0]
    assert {"product_id", "product_name", "aisle", "department", "price_inr"} <= product.keys()
    assert product["price_is_illustrative"] is True


def test_catalog_search_and_department_and_aisle_filters_compose(client):
    filters = client.get("/api/catalog/filters?department_id=4").get_json()
    produce_aisles = {aisle["aisle_id"] for aisle in filters["aisles"]}
    assert 24 in produce_aisles  # fresh fruits

    response = client.get("/api/products?q=banana&department_id=4&aisle_id=24&per_page=60")
    payload = response.get_json()
    assert payload["total"] > 0
    assert all("banana" in product["product_name"].lower() for product in payload["products"])
    assert all(product["department_id"] == 4 and product["aisle_id"] == 24 for product in payload["products"])


def test_deep_linked_product_page_and_detail_api_include_related_products(client):
    page = client.get("/product/24852")  # Banana
    detail = client.get("/api/products/24852")
    assert page.status_code == 200
    assert b'data-product-id="24852"' in page.data
    assert detail.status_code == 200
    payload = detail.get_json()
    assert payload["product"]["product_name"] == "Banana"
    assert payload["related_products"]
    assert all(item["product_id"] != 24852 for item in payload["related_products"])
    assert client.get("/api/products/999999").status_code == 404


def test_cart_recommendation_accepts_catalog_ids_and_excludes_cart(client):
    response = client.post(
        "/api/cart-recommend",
        json={"cart": [24852], "user_id": "shopper-market", "session_id": "session-market"},
    )
    assert response.status_code == 200
    payload = response.get_json()
    recommendations = payload["recommendations"]
    assert recommendations
    assert all(product["product_id"] != 24852 for product in recommendations)
    assert all(product["supporting_product_id"] == 24852 for product in recommendations)
    assert all(product["recommendation_id"] for product in recommendations)
    assert all(product["impression_id"] == payload["impression_id"] for product in recommendations)
    assert payload["policy_mode"] in {"historical_cold_start", "adaptive_linucb"}
    assert payload["policy_version"] == "linucb-v1"


def test_product_recommendations_adapt_after_http_feedback(client, monkeypatch):
    class Future:
        def get(self, timeout):
            assert timeout == 10

    class Producer:
        def send(self, topic, key, value):
            return Future()

    monkeypatch.setattr(main, "adaptive_service", create_adaptive_service(alpha=0.05, historical_weight=0.25))
    monkeypatch.setattr(main, "event_producer", lambda: Producer())
    identity = "?user_id=adaptive-shopper&session_id=adaptive-session"
    first = client.get(f"/api/products/24852{identity}").get_json()
    first_names = [item["product_name"] for item in first["related_products"]]
    rewarded = first["related_products"][1]
    assert first["recommendation"]["policy_mode"] == "historical_cold_start"

    feedback = client.post("/api/events", json={
        "event_id": "market-feedback-1",
        "event_type": "recommendation_clicked",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "adaptive-shopper",
        "session_id": "adaptive-session",
        "product_id": rewarded["product_name"],
        "impression_id": rewarded["impression_id"],
    })
    assert feedback.status_code == 202
    assert feedback.get_json()["adaptive_feedback_status"] == "learned"

    second = client.get(f"/api/products/24852{identity}").get_json()
    second_names = [item["product_name"] for item in second["related_products"]]
    assert second["recommendation"]["policy_mode"] == "adaptive_linucb"
    assert second_names[0] == rewarded["product_name"]
    assert second_names != first_names

    legacy_route = client.get(f"/api/recommend?product_id=24852&{identity[1:]}").get_json()
    assert legacy_route["recommendation"]["impression_id"]
    assert all(item["impression_id"] for item in legacy_route["recommendations"])


def test_event_api_validates_and_publishes_through_existing_stream_seam(client, monkeypatch):
    sent = []

    class Future:
        def get(self, timeout):
            assert timeout == 10

    class Producer:
        def send(self, topic, key, value):
            sent.append((topic, key, value))
            return Future()

    monkeypatch.setattr(main, "event_producer", lambda: Producer())
    event = {
        "event_id": "market-event-1",
        "event_type": "cart_item_added",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "shopper-test",
        "session_id": "session-test",
        "product_id": "Banana",
        "catalog_product_id": 24852,
        "quantity": 1,
    }
    response = client.post("/api/events", json=event)
    assert response.status_code == 202
    assert sent[0][1] == b"shopper-test"
    assert sent[0][2]["catalog_product_id"] == 24852
    assert client.post("/api/events", json={"event_type": "cart_item_added"}).status_code == 400


def test_checkout_persists_a_demo_order_and_owner_can_look_it_up(client):
    checkout = {
        "order_id": "BS-test-order-001",
        "user_id": "shopper-test",
        "session_id": "session-test",
        "name": "Test Shopper",
        "email": "shopper@example.com",
        "address": "12 Market Road",
        "city": "Bengaluru",
        "postal_code": "560001",
        "cart": [{"product_id": 24852, "quantity": 2}, {"product_id": 1, "quantity": 1}],
        "coupons": [{"coupon_id": "coupon-test", "product_id": 24852, "discount_percent": 5}],
    }
    response = client.post("/api/checkout", json=checkout)
    assert response.status_code == 201
    order = response.get_json()["order"]
    assert order["status"] == "confirmed_demo"
    assert order["savings_inr"] > 0
    assert order["total_inr"] == pytest.approx(order["subtotal_inr"] - order["savings_inr"])
    assert "no payment" in order["price_disclosure"].lower()

    lookup = client.get("/api/orders/BS-test-order-001?user_id=shopper-test")
    assert lookup.status_code == 200
    assert lookup.get_json()["order"]["delivery"]["city"] == "Bengaluru"
    assert client.get("/api/orders/BS-test-order-001?user_id=someone-else").status_code == 404
    assert client.post("/api/checkout", json=checkout).status_code == 409


def test_checkout_rejects_empty_or_malformed_orders(client):
    response = client.post("/api/checkout", json={"order_id": "BS-short"})
    assert response.status_code == 400
    assert "fields" in response.get_json()

    response = client.post("/api/checkout", json={
        "order_id": "BS-empty-order-001", "user_id": "u", "session_id": "s", "name": "A",
        "email": "a@example.com", "address": "Street", "city": "City", "postal_code": "12345", "cart": [],
    })
    assert response.status_code == 400
    assert response.get_json()["error"] == "The basket is empty"
