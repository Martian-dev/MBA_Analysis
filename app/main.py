"""Flask storefront and Signal Room HTTP application."""

from __future__ import annotations

import csv
import json
import os
import re
import secrets
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, Response, abort, jsonify, render_template, request, stream_with_context
from kafka import KafkaProducer
from kafka.errors import KafkaError
from redis import Redis
from redis.exceptions import RedisError

from pipeline.contracts import validate_event
from pipeline.adaptive import create_adaptive_service
from pipeline.observability import ActivityLog
from pipeline.serialization import JsonSerializer
from pipeline.topics import EVENTS_TOPIC

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
RULES_PATH = BASE_DIR / "rules.csv"
MAPPING_PATH = BASE_DIR / "group_mapping.json"
DATA_DIR = Path(os.getenv("INSTACART_DATA_DIR", PROJECT_DIR / "InstacartMBA"))
DEFAULT_ORDER_STORE = PROJECT_DIR / ".runtime" / "marketplace" / "orders.jsonl"
app.config.setdefault("ORDER_STORE_PATH", str(DEFAULT_ORDER_STORE))

rules_df = pd.DataFrame(columns=["antecedents", "consequents", "support", "confidence", "lift"])
item_to_rep_map: dict[str, str] = {}
catalog_products: list[dict[str, Any]] = []
products_by_id: dict[int, dict[str, Any]] = {}
products_by_name: dict[str, dict[str, Any]] = {}
departments: list[dict[str, Any]] = []
aisles: list[dict[str, Any]] = []
_event_producer = None
_event_publish_lock = threading.Lock()
_event_retry_after = 0.0
_order_lock = threading.Lock()
_adaptive_lock = threading.Lock()
adaptive_service = create_adaptive_service()


def _demo_price(product_id: int) -> float:
    """Return a stable illustrative INR price; Instacart has no price field."""
    return round(29 + ((product_id * 73) % 1170), 2)


def _read_lookup(path: Path, id_field: str, name_field: str) -> dict[int, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {int(row[id_field]): row[name_field] for row in csv.DictReader(handle)}


def load_data() -> None:
    """Load the full Instacart catalog and temporary rule artifact once."""
    global rules_df, item_to_rep_map, catalog_products, products_by_id, products_by_name
    global departments, aisles

    try:
        rules_df = pd.read_csv(RULES_PATH)
    except Exception as exc:  # pragma: no cover - startup fallback
        print(f"Could not load association rules: {exc}")
        rules_df = pd.DataFrame(columns=rules_df.columns)

    try:
        item_to_rep_map = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover - startup fallback
        print(f"Could not load representative mapping: {exc}")
        item_to_rep_map = {}

    try:
        department_names = _read_lookup(DATA_DIR / "departments.csv", "department_id", "department")
        aisle_names = _read_lookup(DATA_DIR / "aisles.csv", "aisle_id", "aisle")
        loaded: list[dict[str, Any]] = []
        department_counts: dict[int, int] = {}
        aisle_counts: dict[int, int] = {}
        with (DATA_DIR / "products.csv").open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                product_id = int(row["product_id"])
                department_id = int(row["department_id"])
                aisle_id = int(row["aisle_id"])
                product = {
                    "product_id": product_id,
                    "product_name": row["product_name"],
                    "department_id": department_id,
                    "department": department_names.get(department_id, "other"),
                    "aisle_id": aisle_id,
                    "aisle": aisle_names.get(aisle_id, "other"),
                    "price_inr": _demo_price(product_id),
                    "price_is_illustrative": True,
                }
                loaded.append(product)
                department_counts[department_id] = department_counts.get(department_id, 0) + 1
                aisle_counts[aisle_id] = aisle_counts.get(aisle_id, 0) + 1

        catalog_products = loaded
        products_by_id = {product["product_id"]: product for product in loaded}
        products_by_name = {product["product_name"].casefold(): product for product in loaded}
        departments = [
            {"department_id": key, "department": value, "product_count": department_counts.get(key, 0)}
            for key, value in sorted(department_names.items(), key=lambda item: item[1])
        ]
        aisles = [
            {"aisle_id": key, "aisle": value, "product_count": aisle_counts.get(key, 0)}
            for key, value in sorted(aisle_names.items(), key=lambda item: item[1])
        ]
        print(f"Loaded {len(catalog_products):,} products and {len(rules_df):,} association rules.")
    except OSError as exc:  # pragma: no cover - startup fallback
        print(f"Could not load Instacart catalog from {DATA_DIR}: {exc}")
        catalog_products = []
        products_by_id = {}
        products_by_name = {}
        departments = []
        aisles = []


load_data()


def activity_log() -> ActivityLog:
    return ActivityLog(Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True))


def event_producer() -> KafkaProducer:
    """Lazily create the browser-to-Kafka producer used by the storefront."""
    global _event_producer
    if _event_producer is None:
        _event_producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=JsonSerializer(),
            bootstrap_timeout_ms=1_000,
            max_block_ms=2_000,
        )
    return _event_producer


def _public_product(product: dict[str, Any]) -> dict[str, Any]:
    return dict(product)


def _product_for(value: Any) -> dict[str, Any] | None:
    if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
        return products_by_id.get(int(value))
    if isinstance(value, str):
        return products_by_name.get(value.casefold())
    return None


def _related_products(product: dict[str, Any], limit: int = 6) -> list[dict[str, Any]]:
    """Resolve rule-backed products first, then fill with products from the aisle."""
    name = product["product_name"]
    representative = item_to_rep_map.get(name, name)
    matches = rules_df[rules_df["antecedents"] == representative].sort_values("lift", ascending=False)
    chosen: list[dict[str, Any]] = []
    seen = {product["product_id"]}
    for _, rule in matches.iterrows():
        candidate = products_by_name.get(str(rule["consequents"]).casefold())
        if candidate and candidate["product_id"] not in seen:
            item = _public_product(candidate)
            item["recommendation_reason"] = f"Often paired with {name}"
            item["recommendation_source"] = "historical_rule"
            item["lift"] = round(float(rule["lift"]), 3)
            chosen.append(item)
            seen.add(candidate["product_id"])
            if len(chosen) >= limit:
                return chosen

    for candidate in catalog_products:
        if candidate["aisle_id"] == product["aisle_id"] and candidate["product_id"] not in seen:
            item = _public_product(candidate)
            item["recommendation_reason"] = f"More from {product['aisle']}"
            item["recommendation_source"] = "aisle_fallback"
            chosen.append(item)
            seen.add(candidate["product_id"])
            if len(chosen) >= limit:
                break
    return chosen


def _adaptive_rank_products(
    products: list[dict[str, Any]],
    *,
    user_id: str | None,
    session_id: str | None,
    cart_products: list[dict[str, Any]] | None = None,
    limit: int = 6,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Rank historical candidates when browser identity is available."""
    if not products or not user_id or not session_id:
        return products[:limit], None
    candidates = [
        {
            "product_id": item["product_name"],
            "prior_score": float(item.get("lift") or (1 / (index + 1))),
            "snapshot_version": "marketplace-rules-v1",
            "catalog_product_id": item["product_id"],
            **(
                {"supporting_product_id": item["supporting_product_name"]}
                if item.get("supporting_product_name")
                else {}
            ),
        }
        for index, item in enumerate(products)
    ]
    with _adaptive_lock:
        decision = adaptive_service.rank_candidates(
            user_id=user_id,
            session_id=session_id,
            candidates=candidates,
            snapshot_version="marketplace-rules-v1",
            cart_product_ids=[product["product_name"] for product in (cart_products or [])],
            limit=limit,
        )
    by_name = {item["product_name"]: item for item in products}
    ranked: list[dict[str, Any]] = []
    for ranked_item in decision["items"]:
        product = by_name[ranked_item["product_id"]]
        product["impression_id"] = decision["impression_id"]
        product["policy_mode"] = decision["policy_mode"]
        product["adaptive_score"] = ranked_item["score"]
        product["adaptive_explanation"] = ranked_item["explanation"]
        ranked.append(product)
    metadata = {
        key: decision[key]
        for key in ("impression_id", "policy_version", "policy_mode", "snapshot_version")
    }
    return ranked, metadata


def _query_cart_products() -> list[dict[str, Any]]:
    values = request.args.getlist("cart_product_id")
    return [product for value in values if (product := _product_for(value))]


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/product/<int:product_id>")
def product_page(product_id: int):
    if product_id not in products_by_id:
        abort(404)
    return render_template("product.html", product_id=product_id)


@app.get("/checkout")
def checkout_page():
    return render_template("checkout.html")


@app.get("/order/<order_id>")
def order_page(order_id: str):
    return render_template("order.html", order_id=order_id)


@app.get("/viewer")
def viewer():
    return render_template("viewer.html")


@app.get("/api/activity")
def activity_snapshot():
    user_id = request.args.get("user_id")
    session_id = request.args.get("session_id")
    if (user_id is None) != (session_id is None):
        return jsonify({"error": "user_id and session_id must be supplied together"}), 400
    try:
        return jsonify(activity_log().snapshot(user_id=user_id, session_id=session_id))
    except RedisError:
        return jsonify({"error": "Redis is unavailable. Start the live stack."}), 503


@app.post("/api/events")
def publish_storefront_event():
    global _event_retry_after
    payload = request.get_json(silent=True)
    try:
        event = validate_event(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    with _event_publish_lock:
        if time.monotonic() < _event_retry_after:
            return jsonify({"error": "Kafka is unavailable. Start the live stack."}), 503
        try:
            event_producer().send(
                EVENTS_TOPIC, key=event["user_id"].encode("utf-8"), value=event
            ).get(timeout=2)
        except KafkaError:
            _event_retry_after = time.monotonic() + 5
            return jsonify({"error": "Kafka is unavailable. Start the live stack."}), 503
        _event_retry_after = 0.0
    with _adaptive_lock:
        feedback = adaptive_service.observe_event(event)
    return jsonify({
        "status": "accepted", "event_id": event["event_id"],
        "adaptive_feedback_status": feedback.status,
    }), 202


@app.get("/api/activity/stream")
def activity_stream():
    cursor = request.headers.get("Last-Event-ID") or request.args.get("since", "0-0")
    if not re.fullmatch(r"\d+-\d+", cursor):
        return jsonify({"error": "Invalid stream cursor"}), 400
    user_id = request.args.get("user_id")
    session_id = request.args.get("session_id")
    if (user_id is None) != (session_id is None):
        return jsonify({"error": "user_id and session_id must be supplied together"}), 400
    try:
        log = activity_log()
        log.client.ping()
    except RedisError:
        return jsonify({"error": "Redis is unavailable. Start the live stack."}), 503

    def generate():
        try:
            for item in log.follow(cursor):
                if user_id is not None and (
                    item["payload"].get("user_id") != user_id
                    or item["payload"].get("session_id") != session_id
                ):
                    continue
                yield f"id: {item['id']}\ndata: {json.dumps(item)}\n\n"
        except RedisError:
            yield 'event: disconnect\ndata: {"error":"Redis connection lost"}\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/catalog/filters")
def catalog_filters():
    department_id = request.args.get("department_id", type=int)
    visible_aisles = aisles
    if department_id:
        aisle_ids = {
            product["aisle_id"] for product in catalog_products if product["department_id"] == department_id
        }
        visible_aisles = [aisle for aisle in aisles if aisle["aisle_id"] in aisle_ids]
    return jsonify({"departments": departments, "aisles": visible_aisles})


@app.get("/api/products")
def get_products():
    page = max(request.args.get("page", 1, type=int), 1)
    per_page = min(max(request.args.get("per_page", 24, type=int), 1), 60)
    query = request.args.get("q", "").strip().casefold()
    department_id = request.args.get("department_id", type=int)
    aisle_id = request.args.get("aisle_id", type=int)
    filtered = catalog_products
    if query:
        filtered = [product for product in filtered if query in product["product_name"].casefold()]
    if department_id:
        filtered = [product for product in filtered if product["department_id"] == department_id]
    if aisle_id:
        filtered = [product for product in filtered if product["aisle_id"] == aisle_id]
    total = len(filtered)
    pages = max((total + per_page - 1) // per_page, 1)
    page = min(page, pages)
    start = (page - 1) * per_page
    return jsonify({
        "status": "success",
        "total": total,
        "products": [_public_product(product) for product in filtered[start : start + per_page]],
        "pagination": {
            "page": page, "per_page": per_page, "pages": pages,
            "has_previous": page > 1, "has_next": page < pages,
        },
        "query": request.args.get("q", "").strip(),
    })


@app.get("/api/products/<int:product_id>")
def get_product(product_id: int):
    product = products_by_id.get(product_id)
    if product is None:
        return jsonify({"error": "Product not found"}), 404
    related, recommendation = _adaptive_rank_products(
        _related_products(product, 12),
        user_id=request.args.get("user_id"),
        session_id=request.args.get("session_id"),
        cart_products=_query_cart_products(),
    )
    return jsonify({
        "product": _public_product(product),
        "related_products": related,
        "recommendation": recommendation,
    })


@app.get("/api/recommend")
def get_recommendations():
    product = _product_for(request.args.get("product_id") or request.args.get("item"))
    if product is None:
        return jsonify({"error": "A valid product_id or item is required"}), 400
    related, recommendation = _adaptive_rank_products(
        _related_products(product, 8),
        user_id=request.args.get("user_id"),
        session_id=request.args.get("session_id"),
        cart_products=_query_cart_products(),
        limit=4,
    )
    return jsonify({
        "product": _public_product(product),
        "recommendations": related,
        "recommendation": recommendation,
    })


@app.post("/api/cart-recommend")
def get_cart_recommendations():
    data = request.get_json(silent=True) or {}
    cart_values = data.get("cart", [])
    if not isinstance(cart_values, list):
        return jsonify({"error": "cart must be a list"}), 400
    cart_products = [product for value in cart_values if (product := _product_for(value))]
    if not cart_products:
        return jsonify({"recommendations": []})
    cart_ids = {product["product_id"] for product in cart_products}
    result: list[dict[str, Any]] = []
    seen = set(cart_ids)
    for supporting in cart_products:
        for recommendation in _related_products(supporting, 6):
            if recommendation["product_id"] in seen:
                continue
            recommendation["supporting_product_id"] = supporting["product_id"]
            recommendation["supporting_product_name"] = supporting["product_name"]
            recommendation["recommendation_id"] = f"cart:{supporting['product_id']}:{recommendation['product_id']}"
            result.append(recommendation)
            seen.add(recommendation["product_id"])
            if len(result) >= 12:
                break
        if len(result) >= 12:
            break

    user_id = str(data.get("user_id") or "marketplace-anonymous")
    session_id = str(data.get("session_id") or "marketplace-session")
    ranked, recommendation = _adaptive_rank_products(
        result,
        user_id=user_id,
        session_id=session_id,
        cart_products=cart_products,
    )
    return jsonify({
        "recommendations": ranked,
        **(recommendation or {}),
    })


def _order_records() -> list[dict[str, Any]]:
    path = Path(app.config["ORDER_STORE_PATH"])
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


@app.post("/api/checkout")
def create_order():
    data = request.get_json(silent=True) or {}
    required = ("order_id", "user_id", "session_id", "name", "email", "address", "city", "postal_code")
    missing = [field for field in required if not isinstance(data.get(field), str) or not data[field].strip()]
    if missing:
        return jsonify({"error": "Complete all required checkout fields", "fields": missing}), 400
    if not re.fullmatch(r"BS-[A-Za-z0-9-]{8,64}", data["order_id"]):
        return jsonify({"error": "Invalid order_id"}), 400
    if "@" not in data["email"]:
        return jsonify({"error": "Enter a valid email address", "fields": ["email"]}), 400
    raw_cart = data.get("cart")
    if not isinstance(raw_cart, list) or not raw_cart:
        return jsonify({"error": "The basket is empty"}), 400
    line_items: list[dict[str, Any]] = []
    subtotal = 0.0
    for item in raw_cart:
        if not isinstance(item, dict):
            return jsonify({"error": "Invalid basket item"}), 400
        product = _product_for(item.get("product_id"))
        quantity = item.get("quantity")
        if product is None or isinstance(quantity, bool) or not isinstance(quantity, int) or not 1 <= quantity <= 99:
            return jsonify({"error": "Invalid basket item"}), 400
        line_total = round(product["price_inr"] * quantity, 2)
        subtotal += line_total
        line_items.append({**_public_product(product), "quantity": quantity, "line_total_inr": line_total})

    coupon_by_product: dict[int, dict[str, Any]] = {}
    for coupon in data.get("coupons", []):
        if not isinstance(coupon, dict):
            continue
        product_id = coupon.get("product_id")
        discount = coupon.get("discount_percent")
        if isinstance(product_id, int) and isinstance(discount, (int, float)) and 0 < discount <= 50:
            coupon_by_product[product_id] = coupon
    savings = 0.0
    applied_coupons: list[dict[str, Any]] = []
    for line in line_items:
        coupon = coupon_by_product.get(line["product_id"])
        if coupon:
            savings += line["line_total_inr"] * float(coupon["discount_percent"]) / 100
            applied_coupons.append({
                "coupon_id": str(coupon.get("coupon_id", "demo-coupon")),
                "product_id": line["product_id"],
                "discount_percent": float(coupon["discount_percent"]),
            })
    order = {
        "order_id": data["order_id"], "status": "confirmed_demo",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "user_id": data["user_id"], "session_id": data["session_id"],
        "delivery": {
            "name": data["name"].strip(), "email": data["email"].strip(),
            "address": data["address"].strip(), "city": data["city"].strip(),
            "postal_code": data["postal_code"].strip(),
        },
        "items": line_items, "applied_coupons": applied_coupons,
        "subtotal_inr": round(subtotal, 2), "savings_inr": round(savings, 2),
        "total_inr": round(max(0, subtotal - savings), 2), "currency": "INR",
        "price_disclosure": "All prices are illustrative; no payment was processed.",
    }
    path = Path(app.config["ORDER_STORE_PATH"])
    path.parent.mkdir(parents=True, exist_ok=True)
    with _order_lock:
        if any(record["order_id"] == order["order_id"] for record in _order_records()):
            return jsonify({"error": "This order has already been submitted"}), 409
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(order, ensure_ascii=False) + "\n")
    return jsonify({"status": "confirmed", "order": order}), 201


@app.get("/api/orders/<order_id>")
def get_order(order_id: str):
    user_id = request.args.get("user_id", "")
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    for order in reversed(_order_records()):
        if order["order_id"] == order_id and secrets.compare_digest(order["user_id"], user_id):
            return jsonify({"order": order})
    return jsonify({"error": "Order not found"}), 404


if __name__ == "__main__":
    app.run(debug=True, port=5000)
