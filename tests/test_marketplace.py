"""Behavior tests for the public marketplace HTTP seam."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest
from kafka.errors import KafkaError
from redis.exceptions import ConnectionError as RedisConnectionError

from app import main
from pipeline.adaptive import create_adaptive_service
from pipeline.adaptive_redis import create_redis_adaptive_service


@pytest.fixture()
def client(tmp_path):
    main.app.config.update(TESTING=True, ORDER_STORE_PATH=str(tmp_path / "orders.jsonl"))
    with main.app.test_client() as test_client:
        with test_client.session_transaction() as session:
            session['user_id'] = 'shopper-test'
            session['csrf_token'] = 'test-csrf'
        test_client.environ_base['HTTP_X_CSRF_TOKEN'] = 'test-csrf'
        yield test_client


def test_catalog_only_lists_products_covered_by_the_active_rule_snapshot(client):
    response = client.get("/api/products?per_page=60")
    assert response.status_code == 200
    payload = response.get_json()

    expected = len(main.catalog_products)
    assert payload["total"] == expected
    assert payload["pagination"]["page"] == 1
    assert payload["pagination"]["pages"] == (expected + 59) // 60
    assert len(payload["products"]) == min(60, expected)
    assert main.products_by_id[24852]['product_name'] == 'Banana'
    assert "Smart Ones Classic Favorites Mini Rigatoni With Vodka Cream Sauce" not in {
        product["product_name"] for product in payload["products"]
    }
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
    assert client.get("/product/4").status_code == 404
    assert client.get("/api/products/4").status_code == 404
    assert client.get("/api/products/999999").status_code == 404


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is required for the browserless UI regression test")
@pytest.mark.parametrize('script', ['product_page_recommendation.mjs', 'coupon_triggers.mjs', 'rules_page.mjs'])
def test_storefront_recommendations_and_coupon_trigger_interactions(script):
    completed = subprocess.run(
        ["node", f"tests/frontend/{script}"],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_spark_serving_artifact_becomes_versioned_http_prior():
    rules_path = Path(__file__).parent / "fixtures" / "spark-rules-serving.csv"
    code = """
import json
from app.main import app
with app.test_client() as client:
    with client.session_transaction() as session:
        session['user_id'] = 'u'
    response = client.get('/api/recommend?item=European%20Cucumber&user_id=u&session_id=s')
    print(json.dumps(response.get_json()))
"""
    environment = {**os.environ, "RULES_PATH": str(rules_path)}
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    payload = json.loads(completed.stdout.splitlines()[-1])

    assert payload["recommendation"]["snapshot_version"] == "fixture-spark-v1"
    assert payload["recommendations"][0]["product_name"] == "Banana"


def test_spark_rule_snapshot_defines_the_public_marketplace_catalog():
    rules_path = Path(__file__).parent / "fixtures" / "spark-rules-serving.csv"
    code = """
import json
from app.main import app
with app.test_client() as client:
    catalog = client.get('/api/products?per_page=60').get_json()
    excluded_page_status = client.get('/product/4').status_code
    print(json.dumps({'catalog': catalog, 'excluded_page_status': excluded_page_status}))
"""
    environment = {**os.environ, "RULES_PATH": str(rules_path)}
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    payload = json.loads(completed.stdout.splitlines()[-1])

    assert payload["catalog"]["total"] == 2
    assert {product["product_id"] for product in payload["catalog"]["products"]} == {45, 24852}
    assert payload["excluded_page_status"] == 404


def test_cart_recommendation_accepts_catalog_ids_and_excludes_cart(client):
    response = client.post(
        "/api/cart-recommend",
        json={"cart": [24852], "user_id": "shopper-test", "session_id": "session-market"},
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
    with client.session_transaction() as session:
        session['user_id'] = 'adaptive-shopper'
    class Future:
        def get(self, timeout):
            assert timeout == 2

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
        "catalog_product_id": rewarded["product_id"],
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


def test_als_candidate_reaches_the_http_response_with_large_popularity_catalog(client, monkeypatch):
    from pipeline.candidates import HistoricalCandidates
    monkeypatch.setattr(main, 'adaptive_service', create_adaptive_service())
    excluded = {24852} | set(main.rules_df.loc[main.rules_df.antecedent_product_id.eq(24852), 'consequent_product_id'])
    product_id = next(product_id for product_id in main.products_by_id if product_id not in excluded)
    monkeypatch.setattr(main, 'historical_candidates', HistoricalCandidates(
        {str(item['product_id']): 10_000 - index for index, item in enumerate(main.catalog_products)},
        [{'user_id': 10, 'product_id': product_id, 'score': 1, 'rank': 1}]))
    payload = client.get('/api/products/24852?user_id=shopper-test&session_id=s').get_json()
    assert len(payload['related_products']) <= 6
    assert any(item['product_id'] == product_id and item['recommendation_source'] == 'als_cold_start_fallback'
               for item in payload['related_products'])


def test_product_api_uses_historical_order_when_shared_redis_is_offline(client, monkeypatch):
    with client.session_transaction() as session:
        session['user_id'] = 'offline-shopper'
    class UnavailableRedis:
        def incr(self, key):
            del key
            raise RedisConnectionError("redis offline")

    monkeypatch.setattr(
        main, "adaptive_service", create_redis_adaptive_service(UnavailableRedis())
    )

    response = client.get(
        "/api/products/24852?user_id=offline-shopper&session_id=offline-session"
    )

    assert response.status_code == 200
    assert response.get_json()["recommendation"]["policy_mode"] == "historical_fallback"


def test_event_api_validates_and_publishes_through_existing_stream_seam(client, monkeypatch):
    sent = []

    class Future:
        def get(self, timeout):
            assert timeout == 2

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


def test_event_api_fails_fast_during_kafka_outage(client, monkeypatch):
    with client.session_transaction() as session:
        session['user_id'] = 'offline-shopper'
    send_calls = []

    class Producer:
        def send(self, topic, key, value):
            send_calls.append(value["event_id"])
            raise KafkaError("broker unavailable")

    monkeypatch.setattr(main, "event_producer", lambda: Producer())
    monkeypatch.setattr(main.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(main, "_event_retry_after", 0.0)
    event = {
        "event_id": "offline-event-1",
        "event_type": "cart_item_added",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "offline-shopper",
        "session_id": "offline-session",
        "product_id": "Banana",
        "quantity": 1,
    }

    assert client.post("/api/events", json=event).status_code == 503
    event["event_id"] = "offline-event-2"
    assert client.post("/api/events", json=event).status_code == 503
    assert send_calls == ["offline-event-1"]


def test_checkout_persists_a_demo_order_and_owner_can_look_it_up(client, monkeypatch):
    class CouponLedger:
        def redeem(self, coupon_ids, **kwargs):
            assert coupon_ids == ["coupon-test"]
            assert kwargs == {
                "user_id": "shopper-test", "session_id": "session-test",
                "order_id": "BS-test-order-001",
                "allowed_product_ids": {"24852", "47766", "Banana", "Organic Avocado"},
            }
            return [{
                "coupon_id": "coupon-test", "product_id": "Banana",
                "discount_percent": "5",
            }]

    monkeypatch.setattr(main, "coupon_ledger", lambda: CouponLedger())
    checkout = {
        "order_id": "BS-test-order-001",
        "user_id": "shopper-test",
        "session_id": "session-test",
        "name": "Test Shopper",
        "email": "shopper@example.com",
        "address": "12 Market Road",
        "city": "Bengaluru",
        "postal_code": "560001",
        "cart": [{"product_id": 24852, "quantity": 2}, {"product_id": 47766, "quantity": 1}],
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
    retry = client.post('/api/checkout', json=checkout)
    assert retry.status_code == 200
    assert retry.get_json()['order'] == order
    assert client.post('/api/checkout', json={**checkout, 'city': 'Different city'}).status_code == 409


def test_checkout_ignores_unissued_client_discount(client, monkeypatch):
    class NoCouponsLedger:
        def redeem(self, coupon_ids, **kwargs):
            return []

    monkeypatch.setattr(main, "coupon_ledger", lambda: NoCouponsLedger())
    response = client.post("/api/checkout", json={
        "order_id": "BS-unissued-coupon-001",
        "user_id": "shopper-test", "session_id": "session-test", "name": "A",
        "email": "a@example.com", "address": "Street", "city": "City",
        "postal_code": "12345", "cart": [{"product_id": 24852, "quantity": 1}],
        "coupons": [{"coupon_id": "made-up", "product_id": 24852, "discount_percent": 50}],
    })

    assert response.status_code == 201
    assert response.get_json()["order"]["savings_inr"] == 0


def test_recommendation_lookup_only_resolves_rule_covered_products(client):
    exact = client.get("/api/recommend?item=Banana")
    casefolded = client.get("/api/recommend?item=banana")
    outside_snapshot = client.get("/api/recommend?item=Biotin%201000%20mcg")

    assert exact.get_json()["product"]["product_id"] == 24852
    assert casefolded.get_json()["product"]["product_id"] == 24852
    assert outside_snapshot.status_code == 400


def test_closed_basket_drawer_is_inert_modal_markup(client):
    page = client.get("/")

    assert page.status_code == 200
    assert re.search(rb'data-catalog-scope="[0-9a-f]{16}"', page.data)
    assert b'role="dialog"' in page.data
    assert b'aria-modal="true"' in page.data
    assert b'aria-hidden="true" inert' in page.data


def test_available_coupon_api_and_viewer_expose_session_owned_offers(client, monkeypatch):
    class Ledger:
        def available(self, **kwargs):
            assert kwargs == {"user_id": 'shopper-test', "session_id": None}
            return [{
                "coupon_id": "coupon:test",
                "user_id": "shopper-test",
                "session_id": "session-test",
                "product_id": "Banana",
                "discount_percent": "5",
                "issued_at": "2026-10-21T12:00:00+00:00",
            }]

    monkeypatch.setattr(main, "coupon_ledger", lambda: Ledger())
    response = client.get("/api/coupons/available")

    assert response.status_code == 200
    assert response.get_json()["coupons"][0]["session_id"] == "session-test"
    assert response.get_json()["total"] == 1
    assert client.get('/viewer').status_code == 403
    with client.session_transaction() as session:
        session['operator'] = True
    assert b'id="available-coupons"' in client.get("/viewer").data


def test_available_coupon_api_requires_complete_session_identity(client):
    response = client.get("/api/coupons/available?user_id=shopper-test")

    assert response.status_code == 400


def test_available_coupon_api_returns_the_complete_active_ledger(client, monkeypatch):
    class Ledger:
        def available(self, **kwargs):
            return [
                {"coupon_id": f"coupon:{index}", "session_id": f"session:{index}"}
                for index in range(3)
            ]

    monkeypatch.setattr(main, "coupon_ledger", lambda: Ledger())
    payload = client.get("/api/coupons/available").get_json()

    assert payload["total"] == 3
    assert len(payload["coupons"]) == 3


def test_browser_coupon_cache_is_session_scoped():
    source = (Path(__file__).parents[1] / "app/static/js/marketplace.js").read_text()

    assert "sessionStorage.getItem(COUPON_KEY)" in source
    assert "sessionStorage.setItem(COUPON_KEY" in source
    assert "sessionStorage.removeItem(COUPON_KEY)" in source
    assert "localStorage.getItem(COUPON_KEY)" not in source
    assert "localStorage.setItem(COUPON_KEY" not in source


def test_checkout_rejects_empty_or_malformed_orders(client):
    response = client.post("/api/checkout", json={"order_id": "BS-short"})
    assert response.status_code == 400
    assert "fields" in response.get_json()

    response = client.post("/api/checkout", json={
        "order_id": "BS-empty-order-001", "user_id": "shopper-test", "session_id": "s", "name": "A",
        "email": "a@example.com", "address": "Street", "city": "City", "postal_code": "12345", "cart": [],
    })
    assert response.status_code == 400
    assert response.get_json()["error"] == "The basket is empty"
