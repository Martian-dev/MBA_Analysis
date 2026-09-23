"""Continuously publish randomized shopper sessions using real rule identifiers.

The feed is synthetic, but its cart and viewed products come from the current
rule artifact instead of a hard-coded demo pair. A seed makes a run replayable;
without one, Python's system randomness produces a different feed each time.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer

from .producer import event
from .serialization import JsonSerializer
from .topics import EVENTS_TOPIC

SCENARIO_WEIGHTS = {
    "eligible": 0.55,
    "short_view": 0.20,
    "already_in_cart": 0.15,
    "no_match": 0.10,
}


@dataclass(frozen=True)
class RulePair:
    antecedent: str
    consequent: str
    support: float


@dataclass
class SimulationCatalog:
    products: list[str]
    pairs: list[RulePair]
    qualifying_consequents: dict[str, set[str]]
    product_weights: list[float]

    @classmethod
    def from_csv(
        cls,
        path: str | os.PathLike[str],
        min_lift: float = 1.5,
        min_confidence: float = 0.10,
        min_support: float = 0.012,
    ) -> "SimulationCatalog":
        strongest: dict[tuple[str, str], dict[str, float]] = {}
        products: set[str] = set()
        with Path(path).open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                antecedent = row["antecedents"].strip()
                consequent = row["consequents"].strip()
                products.update((antecedent, consequent))
                metrics = {
                    "support": float(row["support"]),
                    "confidence": float(row["confidence"]),
                    "lift": float(row["lift"]),
                }
                if (
                    antecedent == consequent
                    or metrics["lift"] < min_lift
                    or metrics["confidence"] < min_confidence
                    or metrics["support"] < min_support
                ):
                    continue
                pair = (antecedent, consequent)
                if pair not in strongest or metrics["lift"] > strongest[pair]["lift"]:
                    strongest[pair] = metrics

        pairs = [RulePair(a, c, m["support"]) for (a, c), m in strongest.items()]
        qualifying: dict[str, set[str]] = {}
        for pair in pairs:
            qualifying.setdefault(pair.antecedent, set()).add(pair.consequent)

        # Rule support is the closest available purchase-frequency signal in
        # the provisional artifact. A small floor keeps every product sampleable.
        weights = {product: 0.001 for product in products}
        for pair in pairs:
            weights[pair.antecedent] += pair.support
            weights[pair.consequent] += pair.support
        ordered_products = sorted(products)
        return cls(
            products=ordered_products,
            pairs=pairs,
            qualifying_consequents=qualifying,
            product_weights=[weights[product] for product in ordered_products],
        )

    def choose_pair(self, rng: random.Random) -> RulePair:
        if not self.pairs:
            raise ValueError("rule artifact contains no qualifying pairs")
        return rng.choice(self.pairs)

    def choose_product(self, rng: random.Random) -> str:
        return rng.choices(self.products, weights=self.product_weights, k=1)[0]

    def choose_non_match(self, cart_product: str, rng: random.Random) -> str:
        blocked = self.qualifying_consequents.get(cart_product, set()) | {cart_product}
        candidates = [product for product in self.products if product not in blocked]
        return rng.choice(candidates or self.products)


def plan_session(catalog: SimulationCatalog, index: int, rng: random.Random) -> dict[str, object]:
    """Create a random session plan without touching Kafka or sleeping."""

    scenario = rng.choices(list(SCENARIO_WEIGHTS), weights=list(SCENARIO_WEIGHTS.values()), k=1)[0]
    pair = catalog.choose_pair(rng)
    cart_product = pair.antecedent
    viewed_product = pair.consequent
    if scenario == "no_match":
        cart_product = catalog.choose_product(rng)
        viewed_product = catalog.choose_non_match(cart_product, rng)

    dwell = {
        "eligible": rng.randint(46, 90),
        "short_view": rng.randint(8, 44),
        "already_in_cart": rng.randint(46, 90),
        "no_match": rng.randint(46, 90),
    }[scenario]

    # The supporting cart product is random through the selected rule pair.
    # Add a decoy sometimes so the stream exercises multi-item carts too.
    cart = [cart_product]
    if rng.random() < 0.35:
        decoy = catalog.choose_product(rng)
        if decoy not in cart and decoy != viewed_product:
            cart.append(decoy)
    if scenario == "already_in_cart" and viewed_product not in cart:
        cart.append(viewed_product)

    return {
        "scenario": scenario,
        "cart": cart,
        "viewed_product": viewed_product,
        "dwell": dwell,
        "user_id": f"shopper-{rng.randint(1, 1000):04}",
        "session_id": f"sim-session-{index:08}-{uuid.uuid4().hex[:8]}",
    }


def publish_session(
    producer: KafkaProducer,
    catalog: SimulationCatalog,
    index: int,
    rng: random.Random,
    pause_seconds: float = 0.8,
) -> str:
    plan = plan_session(catalog, index, rng)
    user_id = str(plan["user_id"])
    session_id = str(plan["session_id"])
    viewed_product = str(plan["viewed_product"])
    dwell = int(plan["dwell"])
    events = [event("cart_item_added", product, user_id, session_id) for product in plan["cart"]]
    started = event("product_view_started", viewed_product, user_id, session_id)
    started["event_time"] = (datetime.now(timezone.utc) - timedelta(seconds=dwell)).isoformat()
    events.append(started)
    for payload in events:
        producer.send(EVENTS_TOPIC, key=user_id.encode(), value=payload).get(timeout=10)

    time.sleep(pause_seconds)
    ended = event("product_view_ended", viewed_product, user_id, session_id, dwell_seconds=dwell)
    producer.send(EVENTS_TOPIC, key=user_id.encode(), value=ended).get(timeout=10)
    print(
        f"{session_id}: {plan['scenario']} | cart={','.join(plan['cart'])} | "
        f"view={viewed_product} | simulated dwell {dwell}s",
        flush=True,
    )
    return str(plan["scenario"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--rules", default=os.getenv("RULES_PATH", "app/rules.csv"))
    parser.add_argument("--min-lift", type=float, default=float(os.getenv("MIN_LIFT", "1.5")))
    parser.add_argument("--min-confidence", type=float, default=float(os.getenv("MIN_CONFIDENCE", "0.10")))
    parser.add_argument("--min-support", type=float, default=float(os.getenv("MIN_SUPPORT", "0.012")))
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between sessions")
    parser.add_argument("--sessions", type=int, default=0, help="0 means run continuously")
    parser.add_argument("--seed", type=int, default=None, help="optional seed for reproducible replay")
    args = parser.parse_args()
    if args.interval < 1:
        parser.error("--interval must be at least 1 second")
    catalog = SimulationCatalog.from_csv(args.rules, args.min_lift, args.min_confidence, args.min_support)
    rng = random.Random(args.seed)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, value_serializer=JsonSerializer())
    try:
        index = 0
        while not args.sessions or index < args.sessions:
            publish_session(producer, catalog, index, rng)
            index += 1
            if not args.sessions or index < args.sessions:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
